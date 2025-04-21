use ic_agent::{Agent};
use ic_agent::export::Principal;
use candid::{Encode, Decode, CandidType, IDLValue};
use serde::{Deserialize, Serialize};
use std::error::Error;
use num_traits::ToPrimitive;
use std::fmt;
use std::collections::HashMap;
use tokio::time::{interval, Duration};
use mongodb::{Client, options::ClientOptions, Collection, bson::{doc, to_bson}};
use futures::stream::StreamExt;
use mongodb::bson::Document;
use config as config_rs;

// 设置批量处理的大小
const BATCH_SIZE: u64 = 1000;

// 定义参数结构体
#[derive(CandidType, Deserialize)]
struct GetTransactionsArg {
    start: candid::Nat,
    length: candid::Nat,
}

// Archives 查询的返回类型
#[derive(CandidType, Deserialize, Debug)]
struct ArchiveInfo {
    block_range_end: candid::Nat,
    canister_id: Principal,
    block_range_start: candid::Nat,
}

#[derive(CandidType, Deserialize, Debug)]
struct ArchivesResult(Vec<ArchiveInfo>);

// 定义返回结构体
#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
struct Account {
    owner: Principal,
    subaccount: Option<Vec<u8>>,
}

impl fmt::Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let owner_str = self.owner.to_text();
        let sub_str = match &self.subaccount {
            Some(sub) => {
                if sub.is_empty() {
                    "".to_string()
                } else {
                    format!("0x{}", hex::encode(sub))
                }
            }
            None => "".to_string(),
        };
        if sub_str.is_empty() {
            write!(f, "{}", owner_str)
        } else {
            write!(f, "{}:{}", owner_str, sub_str)
        }
    }
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
struct Transfer {
    to: Account,
    fee: Option<candid::Nat>,
    from: Account,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    amount: candid::Nat,
    spender: Option<Account>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
struct Mint {
    to: Account,
    amount: candid::Nat,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
struct Approve {
    from: Account,
    spender: Account,
    amount: candid::Nat,
    fee: Option<candid::Nat>,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    expected_allowance: Option<candid::Nat>,
    expires_at: Option<u64>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
struct Burn {
    from: Account,
    amount: candid::Nat,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    spender: Option<Account>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
struct Transaction {
    #[serde(rename = "kind")]
    kind: String,
    #[serde(rename = "timestamp")]
    timestamp: u64,
    #[serde(rename = "transfer")]
    transfer: Option<Transfer>,
    #[serde(rename = "mint")]
    mint: Option<Mint>,
    #[serde(rename = "burn")]
    burn: Option<Burn>,
    #[serde(rename = "approve")]
    approve: Option<Approve>,
    // 索引字段用于唯一标识交易
    #[serde(rename = "index", skip_serializing_if = "Option::is_none")]
    index: Option<u64>,
}

#[derive(CandidType, Deserialize, Debug)]
struct ArchivedTransaction {
    callback: Principal,
    start: candid::Nat,
    length: candid::Nat,
}

#[derive(CandidType, Deserialize, Debug)]
struct GetTransactionsResult {
    first_index: candid::Nat,
    log_length: candid::Nat,
    transactions: Vec<Transaction>,
    archived_transactions: Vec<ArchivedTransaction>,
}

// 归档交易结构体，用于ledger canister接口
#[derive(CandidType, Deserialize, Debug)]
struct LedgerArchivedTransaction {
    #[serde(rename = "callback")]
    callback_canister_id: Principal,
    start: candid::Nat,
    length: candid::Nat,
}

// GetTransactionsResult，用于ledger canister
#[derive(CandidType, Deserialize, Debug)]
struct LedgerGetTransactionsResult {
    first_index: candid::Nat,
    log_length: candid::Nat,
    transactions: Vec<Transaction>,
    archived_transactions: Vec<LedgerArchivedTransaction>,
}

// TransactionRange结构体
#[derive(CandidType, Deserialize, Debug)]
struct SimpleTransactionRange {
    transactions: Vec<Transaction>,
}

// Transaction结构体，适应可能的不同格式
#[derive(CandidType, Deserialize, Debug, Clone)]
struct SimpleTransaction {
    pub kind: String,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer: Option<Transfer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint: Option<Mint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burn: Option<Burn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approve: Option<Approve>,
}

// 交易数组
#[derive(CandidType, Deserialize, Debug)]
struct TransactionList(Vec<Transaction>);

// 查询archives方法获取归档信息
async fn fetch_archives(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<Vec<ArchiveInfo>, Box<dyn Error>> {
    let empty_tuple = ();
    let arg_bytes = Encode!(&empty_tuple)?;
    let response = agent.query(canister_id, "archives")
        .with_arg(arg_bytes)
        .call()
        .await?;
    
    let archives_result: ArchivesResult = Decode!(&response, ArchivesResult)?;
    Ok(archives_result.0)
}

// 从归档canister获取交易
async fn fetch_archive_transactions(
    agent: &Agent,
    archive_canister_id: &Principal,
    start: u64,
    length: u64,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    println!("从归档canister获取交易: start={}, length={}", start, length);
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("编码参数失败: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("参数编码失败: {}", e))));
        }
    };
    
    println!("调用归档canister: {}", archive_canister_id);
    let response = match agent.query(archive_canister_id, "get_transactions")
        .with_arg(arg_bytes)
        .call()
        .await {
        Ok(resp) => resp,
        Err(e) => {
            println!("调用归档canister失败: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("调用归档canister失败: {}", e))));
        }
    };
    
    println!("收到归档canister响应，长度: {} 字节", response.len());
    
    // 尝试多种可能的结构解码方式
    
    // 1. 尝试解码为TransactionList(Vec<Transaction>)
    println!("尝试解码为TransactionList...");
    if let Ok(list) = Decode!(&response, TransactionList) {
        println!("成功解码为TransactionList，交易数量: {}", list.0.len());
        
        // 给交易添加索引信息
        let mut indexed_transactions = Vec::new();
        for (i, mut tx) in list.0.into_iter().enumerate() {
            let index = start + i as u64;
            tx.index = Some(index);
            indexed_transactions.push(tx);
        }
        
        return Ok(indexed_transactions);
    }
    
    // 2. 尝试直接解码为Vec<Transaction>
    println!("尝试解码为Vec<Transaction>...");
    if let Ok(transactions) = Decode!(&response, Vec<Transaction>) {
        println!("成功解码为Vec<Transaction>，交易数量: {}", transactions.len());
        
        // 给交易添加索引信息
        let mut indexed_transactions = Vec::new();
        for (i, mut tx) in transactions.into_iter().enumerate() {
            let index = start + i as u64;
            tx.index = Some(index);
            indexed_transactions.push(tx);
        }
        
        return Ok(indexed_transactions);
    }
    
    // 3. 尝试解码为SimpleTransactionRange
    println!("尝试解码为SimpleTransactionRange...");
    if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
        println!("成功解码为SimpleTransactionRange，交易数量: {}", range.transactions.len());
        
        // 给交易添加索引信息
        let mut indexed_transactions = Vec::new();
        for (i, mut tx) in range.transactions.into_iter().enumerate() {
            let index = start + i as u64;
            tx.index = Some(index);
            indexed_transactions.push(tx);
        }
        
        return Ok(indexed_transactions);
    }
    
    // 4. 尝试解码为包含SimpleTransaction的数组，并转换为Transaction
    println!("尝试解码为Vec<SimpleTransaction>...");
    if let Ok(simple_transactions) = Decode!(&response, Vec<SimpleTransaction>) {
        println!("成功解码为Vec<SimpleTransaction>，交易数量: {}", simple_transactions.len());
        
        // 将SimpleTransaction转换为Transaction
        let mut transactions = Vec::new();
        for (i, simple_tx) in simple_transactions.into_iter().enumerate() {
            let mut tx = Transaction {
                kind: simple_tx.kind,
                timestamp: simple_tx.timestamp,
                transfer: simple_tx.transfer,
                mint: simple_tx.mint,
                burn: simple_tx.burn,
                approve: simple_tx.approve,
                index: Some(start + i as u64),
            };
            transactions.push(tx);
        }
        
        return Ok(transactions);
    }
    
    println!("所有解码方法都失败，返回空交易列表");
    Ok(Vec::new())
}

// 查询归档 canister 的交易
async fn fetch_archived_transaction_latest(
    agent: &Agent,
    archived: &ArchivedTransaction,
) -> Result<Option<Transaction>, Box<dyn Error>> {
    let archived_length: u64 = archived.length.0.to_u64().unwrap_or(0);
    if archived_length == 0 {
        return Ok(None);
    }
    let last_index = archived_length - 1;
    let start_value = archived.start.clone() + candid::Nat::from(last_index);
    let start_index = start_value.0.to_u64().unwrap_or(0);
    let arg = GetTransactionsArg {
        start: start_value,
        length: candid::Nat::from(1u64),
    };
    let arg_bytes = Encode!(&arg)?;
    
    println!("调用归档canister: {}", archived.callback);
    let response = agent
        .query(&archived.callback, "get_transactions")
        .with_arg(arg_bytes)
        .call()
        .await?;
    
    println!("收到归档canister响应，长度: {} 字节", response.len());
    
    // 尝试多种解码方法
    
    // 1. 尝试使用GetTransactionsResult解码
    if let Ok(archived_result) = Decode!(&response, GetTransactionsResult) {
        println!("使用GetTransactionsResult成功解码");
        let mut tx_opt = archived_result.transactions.into_iter().next();
        
        if let Some(ref mut tx) = tx_opt {
            tx.index = Some(start_index);
        }
        
        return Ok(tx_opt);
    }
    
    // 2. 尝试使用SimpleTransactionRange解码
    if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
        println!("使用SimpleTransactionRange成功解码");
        if !range.transactions.is_empty() {
            let mut tx = range.transactions[0].clone();
            tx.index = Some(start_index);
            return Ok(Some(tx));
        }
    }
    
    // 3. 尝试直接解码为Vec<Transaction>
    if let Ok(transactions) = Decode!(&response, Vec<Transaction>) {
        println!("使用Vec<Transaction>成功解码");
        if !transactions.is_empty() {
            let mut tx = transactions[0].clone();
            tx.index = Some(start_index);
            return Ok(Some(tx));
        }
    }
    
    println!("所有解码方法均失败");
    Ok(None)
}

// 获取主 canister 交易
async fn fetch_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    start: u64,
    length: u64,
) -> Result<(Vec<Transaction>, u64, u64), Box<dyn Error>> {
    println!("查询ledger交易: start={}, length={}", start, length);
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    let arg_bytes = Encode!(&arg)?;
    let response = agent.query(canister_id, "get_transactions")
        .with_arg(arg_bytes)
        .call()
        .await?;
    
    println!("收到ledger响应，长度: {} 字节", response.len());
    
    // 尝试使用LedgerGetTransactionsResult解析
    match Decode!(&response, LedgerGetTransactionsResult) {
        Ok(result) => {
            println!("成功解码为LedgerGetTransactionsResult");
            println!("first_index: {}, log_length: {}, 交易数: {}, 归档交易数: {}", 
                result.first_index.0, 
                result.log_length.0,
                result.transactions.len(),
                result.archived_transactions.len());
            
            let first_index = result.first_index.0.to_u64().unwrap_or(0);
            let log_length = result.log_length.0.to_u64().unwrap_or(0);
            
            // 给交易添加索引信息
            let mut transactions = Vec::new();
            for (i, mut tx) in result.transactions.into_iter().enumerate() {
                let index = first_index + i as u64;
                tx.index = Some(index);
                transactions.push(tx);
            }
            
            return Ok((transactions, first_index, log_length));
        },
        Err(e) => {
            println!("解析ledger响应失败: {}", e);
            
            // 尝试使用SimpleTransactionRange解析
            if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
                println!("成功使用SimpleTransactionRange解析，交易数: {}", range.transactions.len());
                
                // 由于不知道first_index和log_length，使用传入的start和保守估计
                let first_index = start;
                let log_length = start + range.transactions.len() as u64;
                
                // 给交易添加索引信息
                let mut transactions = Vec::new();
                for (i, mut tx) in range.transactions.into_iter().enumerate() {
                    let index = first_index + i as u64;
                    tx.index = Some(index);
                    transactions.push(tx);
                }
                
                return Ok((transactions, first_index, log_length));
            }
            
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("无法解析ledger响应: {}", e))));
        }
    }
}

// 检查ledger当前状态
async fn get_ledger_status(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<(u64, u64), Box<dyn Error>> {
    println!("检查ledger状态...");
    
    // 查询第一个交易，主要目的是获取first_index和log_length
    let arg = GetTransactionsArg {
        start: candid::Nat::from(0u64),
        length: candid::Nat::from(1u64),
    };
    let arg_bytes = Encode!(&arg)?;
    let response = agent.query(canister_id, "get_transactions")
        .with_arg(arg_bytes)
        .call()
        .await?;
    
    match Decode!(&response, LedgerGetTransactionsResult) {
        Ok(result) => {
            let first_index = result.first_index.0.to_u64().unwrap_or(0);
            let log_length = result.log_length.0.to_u64().unwrap_or(0);
            
            println!("Ledger状态: first_index={}, log_length={}", first_index, log_length);
            Ok((first_index, log_length))
        },
        Err(e) => {
            println!("解析ledger状态失败: {}", e);
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("无法解析ledger状态: {}", e))))
        }
    }
}

fn group_transactions_by_account(transactions: &[Transaction]) -> HashMap<String, Vec<&Transaction>> {
    let mut map: HashMap<String, Vec<&Transaction>> = HashMap::new();
    for tx in transactions {
        // 收集所有相关账户
        let mut accounts = Vec::new();
        if let Some(ref transfer) = tx.transfer {
            accounts.push(transfer.from.to_string());
            accounts.push(transfer.to.to_string());
            if let Some(ref spender) = transfer.spender {
                accounts.push(spender.to_string());
            }
        }
        if let Some(ref mint) = tx.mint {
            accounts.push(mint.to.to_string());
        }
        if let Some(ref approve) = tx.approve {
            accounts.push(approve.from.to_string());
            accounts.push(approve.spender.to_string());
        }
        if let Some(ref burn) = tx.burn {
            accounts.push(burn.from.to_string());
            if let Some(ref spender) = burn.spender {
                accounts.push(spender.to_string());
            }
        }
        // 去重
        accounts.sort();
        accounts.dedup();
        for acc in accounts {
            map.entry(acc).or_default().push(tx);
        }
    }
    map
}

/// 查询某账户下的所有交易
async fn get_account_transactions(
    accounts_col: &Collection<Document>,
    account: &str,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    if let Some(doc) = accounts_col
        .find_one(doc! { "account": account }, None)
        .await?
    {
        if let Some(transactions_bson) = doc.get_array("transactions").ok() {
            let mut txs = Vec::new();
            for tx_bson in transactions_bson {
                let tx: Transaction = mongodb::bson::from_bson(tx_bson.clone())?;
                txs.push(tx);
            }
            return Ok(txs);
        }
    }
    Ok(Vec::new())
}

/// 保存交易到交易集合
async fn save_transaction(
    tx_col: &Collection<Document>,
    tx: &Transaction,
) -> Result<(), Box<dyn Error>> {
    let index = tx.index.unwrap_or(0);
    let tx_bson = to_bson(tx)?;
    
    // 使用索引作为唯一标识保存交易
    tx_col.update_one(
        doc! { "index": index as i64 },
        doc! { "$set": tx_bson.as_document().unwrap().clone() },
        mongodb::options::UpdateOptions::builder().upsert(true).build()
    ).await?;
    
    Ok(())
}

/// 保存账户交易关系
async fn save_account_transaction(
    accounts_col: &Collection<Document>,
    account: &str,
    tx_index: u64,
) -> Result<(), Box<dyn Error>> {
    // 增量追加交易索引到账户集合
    accounts_col.update_one(
        doc! { "account": account },
        doc! { 
            "$set": { "account": account },
            "$addToSet": { "transaction_indices": tx_index as i64 }
        },
        mongodb::options::UpdateOptions::builder().upsert(true).build()
    ).await?;
    
    Ok(())
}

/// 获取最新的交易索引
async fn get_latest_transaction_index(
    tx_col: &Collection<Document>,
) -> Result<Option<u64>, Box<dyn Error>> {
    let options = mongodb::options::FindOneOptions::builder()
        .sort(doc! { "index": -1 })
        .build();
    
    if let Some(doc) = tx_col.find_one(doc! {}, options).await? {
        if let Some(index) = doc.get_i64("index").ok() {
            return Ok(Some(index as u64));
        }
    }
    
    Ok(None)
}

// 直接使用已知的交易起点和偏移量查询数据
async fn sync_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
) -> Result<(), Box<dyn Error>> {
    // 获取数据库中最新的交易索引
    let latest_index = get_latest_transaction_index(tx_col).await?.unwrap_or(25000);
    
    println!("数据库中最新的交易索引: {}", latest_index);
    println!("从索引 {} 开始同步新交易", latest_index + 1);
    
    // 使用增量同步方式查询新交易
    let mut current_index = latest_index + 1;
    let mut retry_count = 0;
    let max_retries = 3;
    
    // 尝试同步交易，每次获取一批
    while retry_count < max_retries {
        let length = BATCH_SIZE;
        println!("查询交易批次: {}-{}", current_index, current_index + length - 1);
        
        match fetch_ledger_transactions(agent, canister_id, current_index, length).await {
            Ok((transactions, _, _)) => {
                if transactions.is_empty() {
                    println!("没有获取到新交易，可能已到达链上最新状态");
                    break;
                }
                
                println!("获取到 {} 笔交易", transactions.len());
                
                // 保存交易到数据库
                for tx in &transactions {
                    // 保存交易
                    save_transaction(tx_col, tx).await?;
                    
                    // 更新账户-交易关系
                    let index = tx.index.unwrap_or(0);
                    let tx_clone = tx.clone();
                    let tx_array = vec![tx_clone];
                    let account_txs = group_transactions_by_account(&tx_array);
                    for (account, _) in &account_txs {
                        save_account_transaction(accounts_col, account, index).await?;
                    }
                }
                
                // 更新当前索引并重置重试计数
                current_index += transactions.len() as u64;
                retry_count = 0;
            },
            Err(e) => {
                println!("获取交易失败: {}，重试 {}/{}", e, retry_count + 1, max_retries);
                retry_count += 1;
                // 可能是索引超出了范围，尝试增加步长
                if retry_count >= max_retries {
                    println!("达到最大重试次数，可能已到达链上最新状态");
                    break;
                }
            }
        }
    }
    
    println!("交易同步完成，当前索引: {}", current_index - 1);
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Config {
    mongodb_url: String,
    database: String,
    ledger_canister_id: String,
    ic_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 读取配置文件
    let settings = config_rs::Config::builder()
        .add_source(config_rs::File::with_name("config"))
        .build()?;
    let cfg: Config = settings.try_deserialize()?;

    // 初始化 MongoDB
    let mongo_client = Client::with_options(
        ClientOptions::parse(&cfg.mongodb_url).await?
    )?;
    let db = mongo_client.database(&cfg.database);
    let accounts_col: Collection<mongodb::bson::Document> = db.collection("accounts");
    let tx_col: Collection<mongodb::bson::Document> = db.collection("transactions");
    
    // 创建索引以提高查询性能
    tx_col.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "index": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        None
    ).await?;
    
    accounts_col.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "account": 1 })
            .build(),
        None
    ).await?;

    let agent = Agent::builder()
        .with_url(&cfg.ic_url)
        .build()?;

    let canister_id = Principal::from_text(&cfg.ledger_canister_id)?;
    
    println!("获取归档信息...");
    let archives = fetch_archives(&agent, &canister_id).await?;
    
    if archives.is_empty() {
        println!("没有找到归档信息");
        return Ok(());
    }
    
    // 打印归档信息
    for archive in &archives {
        println!("找到归档信息: canister_id={}, 范围: {}-{}", 
            archive.canister_id, 
            archive.block_range_start.0, 
            archive.block_range_end.0
        );
    }
    
    let archive_info = &archives[0]; // 使用第一个归档
    let archive_canister_id = &archive_info.canister_id;
    let block_range_start = archive_info.block_range_start.0.to_u64().unwrap_or(0);
    let block_range_end = archive_info.block_range_end.0.to_u64().unwrap_or(0);
    
    // 先处理archive canister的历史交易
    println!("开始同步历史交易从archive canister: {}", archive_canister_id);
    
    // 先尝试单个交易获取测试Candid解码功能
    println!("测试获取单个交易...");
    let test_transactions = fetch_archive_transactions(
        &agent, 
        archive_canister_id, 
        block_range_start, 
        1
    ).await?;
    
    if test_transactions.is_empty() {
        println!("无法从归档canister获取交易，可能是数据结构不匹配。");
        println!("跳过归档canister，直接查询ledger canister。");
    } else {
        println!("测试成功，开始批量获取归档交易...");
        
        // 分批获取并处理归档交易
        let mut current_start = block_range_start;
        
        while current_start <= block_range_end {
            let current_length = std::cmp::min(BATCH_SIZE, block_range_end - current_start + 1);
            println!("获取归档交易批次: {}-{}", current_start, current_start + current_length - 1);
            
            let transactions = fetch_archive_transactions(
                &agent, 
                archive_canister_id, 
                current_start, 
                current_length
            ).await?;
            
            if transactions.is_empty() {
                println!("批次内无交易，跳到下一批次");
                current_start += current_length;
                continue;
            }
            
            println!("获取到 {} 笔交易，保存到数据库", transactions.len());
            
            // 保存交易到数据库
            for tx in &transactions {
                // 保存交易
                save_transaction(&tx_col, tx).await?;
                
                // 更新账户-交易关系
                let index = tx.index.unwrap_or(0);
                let tx_clone = tx.clone();
                let tx_array = vec![tx_clone];
                let account_txs = group_transactions_by_account(&tx_array);
                for (account, _) in &account_txs {
                    save_account_transaction(&accounts_col, account, index).await?;
                }
            }
            
            current_start += current_length;
        }
        
        println!("历史交易同步完成");
    }
    
    // 同步ledger的交易
    println!("开始同步ledger交易...");
    sync_ledger_transactions(&agent, &canister_id, &tx_col, &accounts_col).await?;
    
    println!("初始同步完成，开始实时监控新交易");
    
    // 定时增量同步
    let mut interval = interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        
        println!("执行定时增量同步...");
        match sync_ledger_transactions(&agent, &canister_id, &tx_col, &accounts_col).await {
            Ok(_) => println!("定时增量同步完成"),
            Err(e) => println!("定时增量同步出错: {}", e)
        }
    }
}
