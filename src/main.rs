use ic_agent::{Agent};
use ic_agent::export::Principal;
use candid::{Encode, Decode, CandidType};
use serde::{Deserialize, Serialize};
use std::error::Error;
use num_traits::ToPrimitive;
use std::fmt;
use std::collections::HashMap;
use tokio::time::{interval, Duration};
use mongodb::{Client, options::ClientOptions, Collection, bson::{doc, to_bson}};
use mongodb::bson::Document;
use config as config_rs;
use std::env;

// 设置批量处理的大小
const BATCH_SIZE: u64 = 1000;
// 设置从归档canister获取交易的批量大小
const ARCHIVE_BATCH_SIZE: u64 = 2000;

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
    
    if length == 0 {
        println!("请求长度为0，返回空交易列表");
        return Ok(Vec::new());
    }
    
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
    
    // 添加重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    let mut last_error = None;
    
    while retry_count < max_retries {
        match agent.query(archive_canister_id, "get_transactions")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
                println!("收到归档canister响应，长度: {} 字节", response.len());
                
                // 尝试多种可能的结构解码方式
                
                // 1. 首先尝试解码为SimpleTransactionRange（调整顺序，优先尝试）
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
                
                // 2. 尝试解码为TransactionList(Vec<Transaction>)
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
                
                // 3. 尝试直接解码为Vec<Transaction>
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
                
                // 4. 尝试解码为包含SimpleTransaction的数组，并转换为Transaction
                println!("尝试解码为Vec<SimpleTransaction>...");
                if let Ok(simple_transactions) = Decode!(&response, Vec<SimpleTransaction>) {
                    println!("成功解码为Vec<SimpleTransaction>，交易数量: {}", simple_transactions.len());
                    
                    // 将SimpleTransaction转换为Transaction
                    let mut transactions = Vec::new();
                    for (i, simple_tx) in simple_transactions.into_iter().enumerate() {
                        let tx = Transaction {
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
                
                // 所有解码方法都失败，但API调用成功了，重试可能没用
                println!("所有解码方法都失败，返回空交易列表");
                return Ok(Vec::new());
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = std::time::Duration::from_secs(2 * retry_count); // 指数退避
                println!("调用归档canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    println!("达到最大重试次数 ({}), 调用归档canister失败", max_retries);
    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
        format!("调用归档canister失败，已重试 {} 次: {}", 
            max_retries, last_error.unwrap()))))
}

// 查询归档 canister 的交易
#[allow(dead_code)]
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
    
    // 验证参数
    if length == 0 {
        println!("请求长度为0，返回空交易列表");
        return Ok((Vec::new(), start, start));
    }
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("编码参数失败: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("ledger参数编码失败: {}", e))));
        }
    };
    
    // 实现重试机制
    let max_retries = 3;
    let mut retry_count = 0;
    let mut last_error = None;
    
    while retry_count < max_retries {
        match agent.query(canister_id, "get_transactions")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
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
                        println!("解析ledger响应失败，尝试备用解码方法: {}", e);
                        
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
                        
                        // 如果两种解码方法都失败，但API调用成功，返回空结果
                        println!("所有解码方法都失败，返回空交易列表");
                        return Ok((Vec::new(), start, start));
                    }
                }
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = std::time::Duration::from_secs(2 * retry_count); // 指数退避
                println!("调用ledger canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    println!("达到最大重试次数 ({}), 调用ledger canister失败", max_retries);
    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
        format!("调用ledger canister失败，已重试 {} 次: {}", 
            max_retries, last_error.unwrap()))))
}

// 检查ledger当前状态
#[allow(dead_code)]
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
#[allow(dead_code)]
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
    
    // 尝试将交易转换为BSON格式
    let tx_bson = match to_bson(tx) {
        Ok(bson) => bson,
        Err(e) => {
            println!("无法将交易转换为BSON: {}，索引: {}", e, index);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("将交易(索引:{})转换为BSON失败: {}", index, e))));
        }
    };
    
    let doc = match tx_bson.as_document() {
        Some(doc) => doc.clone(),
        None => {
            println!("无法将BSON转换为Document，索引: {}", index);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("无法将BSON转换为Document，索引: {}", index))));
        }
    };
    
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        // 使用索引作为唯一标识保存交易
        match tx_col.update_one(
            doc! { "index": index as i64 },
            doc! { "$set": doc.clone() }, // 克隆文档以避免所有权移动问题
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        ).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retry_count += 1;
                let wait_time = std::time::Duration::from_millis(500 * retry_count);
                println!("保存交易(索引:{})失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    index, retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
        format!("保存交易(索引:{})失败，已重试 {} 次", index, max_retries))))
}

/// 保存账户交易关系
async fn save_account_transaction(
    accounts_col: &Collection<Document>,
    account: &str,
    tx_index: u64,
) -> Result<(), Box<dyn Error>> {
    if account.trim().is_empty() {
        println!("账户为空，跳过保存账户-交易关系");
        return Ok(());
    }
    
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        // 增量追加交易索引到账户集合
        match accounts_col.update_one(
            doc! { "account": account },
            doc! { 
                "$set": { "account": account },
                "$addToSet": { "transaction_indices": tx_index as i64 }
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        ).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retry_count += 1;
                let wait_time = std::time::Duration::from_millis(500 * retry_count);
                println!("保存账户-交易关系失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
        format!("保存账户-交易关系失败，已重试 {} 次", max_retries))))
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

/// 获取区块链上的第一个交易索引
async fn get_first_transaction_index(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<u64, Box<dyn Error>> {
    println!("尝试获取区块链上的第一个交易索引...");
    
    // 查询第一个交易，主要目的是获取first_index
    let arg = GetTransactionsArg {
        start: candid::Nat::from(0u64),
        length: candid::Nat::from(1u64),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("编码参数失败: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("参数编码失败: {}", e))));
        }
    };
    
    // 实现重试机制
    let max_retries = 3;
    let mut retry_count = 0;
    let mut last_error: Option<String> = None;
    
    while retry_count < max_retries {
        match agent.query(canister_id, "get_transactions")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
                // 尝试解码响应
                match Decode!(&response, LedgerGetTransactionsResult) {
                    Ok(result) => {
                        let first_index = result.first_index.0.to_u64().unwrap_or(0);
                        println!("成功获取区块链初始索引: {}", first_index);
                        return Ok(first_index);
                    },
                    Err(e) => {
                        println!("解析ledger响应失败: {}", e);
                        
                        // 尝试使用SimpleTransactionRange解析
                        if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
                            println!("使用SimpleTransactionRange成功解析");
                            // 由于SimpleTransactionRange没有first_index信息，假设为0
                            return Ok(0);
                        }
                        
                        // 如果所有解码方法都失败，可能需要重试
                        retry_count += 1;
                        last_error = Some(format!("无法解析响应: {}", e));
                    }
                }
            },
            Err(e) => {
                retry_count += 1;
                let error_msg = format!("调用canister失败: {}", e);
                last_error = Some(error_msg.clone());
                let wait_time = std::time::Duration::from_secs(2 * retry_count); // 指数退避
                println!("调用canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, error_msg, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
        last_error.unwrap_or_else(|| "尝试获取区块链初始索引失败，达到最大重试次数".to_string()))))
}

// 直接使用已知的交易起点和偏移量查询数据
async fn sync_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
) -> Result<(), Box<dyn Error>> {
    // 获取数据库里面最新的交易索引
    let latest_index = match get_latest_transaction_index(tx_col).await {
        Ok(Some(index)) => {
            println!("数据库中最新的交易索引: {}", index);
            println!("从索引 {} 开始同步新交易", index + 1);
            index
        },
        Ok(None) | Err(_) => {
            println!("数据库中没有找到交易索引，将从区块链上的第一笔交易开始同步");
            
            // 先尝试获取ledger的状态，得到first_index
            println!("获取区块链初始索引...");
            match get_first_transaction_index(agent, canister_id).await {
                Ok(first_index) => {
                    println!("从区块链获取的初始索引为: {}", first_index);
                    // 返回比first_index小1的值，这样current_index会从first_index开始
                    first_index.saturating_sub(1)
                },
                Err(e) => {
                    println!("获取区块链初始索引失败: {}，尝试直接查询交易", e);
                    // 如果获取失败，尝试从0开始查询
                    0
                }
            }
        }
    };
    
    // 使用增量同步方式查询新交易
    let mut current_index = latest_index + 1;
    let mut retry_count = 0;
    let max_retries = 3;
    let mut consecutive_empty = 0;
    let max_consecutive_empty = 2; // 连续空结果次数阈值
    
    // 尝试同步交易，每次获取一批
    while retry_count < max_retries && consecutive_empty < max_consecutive_empty {
        let length = BATCH_SIZE;
        println!("查询交易批次: {}-{}", current_index, current_index + length - 1);
        
        match fetch_ledger_transactions(agent, canister_id, current_index, length).await {
            Ok((transactions, first_index, log_length)) => {
                // 如果first_index大于current_index，说明有交易被跳过，应该从first_index开始查询
                if first_index > current_index {
                    println!("检测到first_index ({}) 大于 current_index ({}), 调整查询索引", 
                        first_index, current_index);
                    current_index = first_index;
                    continue;
                }
                
                // 如果是第一次查询且初始索引为0，但first_index不是0，则使用first_index
                if current_index == 1 && first_index > 0 {
                    println!("首次查询，调整初始索引为区块链上的first_index: {}", first_index);
                    current_index = first_index;
                    continue;
                }
                
                if transactions.is_empty() {
                    consecutive_empty += 1;
                    println!("没有获取到新交易 ({}/{}), 可能已到达链上最新状态或索引有误", 
                        consecutive_empty, max_consecutive_empty);
                    
                    // 尝试跳到下一个可能的索引位置
                    if log_length > current_index {
                        println!("日志长度 ({}) 大于当前索引 ({}), 尝试从新位置查询", log_length, current_index);
                        current_index = log_length;
                        consecutive_empty = 0; // 重置连续空计数
                    } else {
                        // 如果没有明确的新位置，小幅度向前尝试
                        current_index += BATCH_SIZE / 10; 
                        println!("尝试从新位置 {} 查询", current_index);
                    }
                    
                    // 短暂等待避免过快查询
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue; // 继续下一个循环迭代
                }
                
                // 获取到新交易，重置计数
                consecutive_empty = 0;
                println!("获取到 {} 笔交易", transactions.len());
                
                // 保存交易到数据库
                let mut success_count = 0;
                let mut error_count = 0;
                for tx in &transactions {
                    // 保存交易
                    match save_transaction(tx_col, tx).await {
                        Ok(_) => {
                            success_count += 1;
                            // 更新账户-交易关系
                            let index = tx.index.unwrap_or(0);
                            let tx_clone = tx.clone();
                            let tx_array = vec![tx_clone];
                            let account_txs = group_transactions_by_account(&tx_array);
                            
                            for (account, _) in &account_txs {
                                if let Err(e) = save_account_transaction(accounts_col, account, index).await {
                                    println!("保存账户-交易关系失败 (账户: {}, 交易索引: {}): {}", account, index, e);
                                    error_count += 1;
                                }
                            }
                        },
                        Err(e) => {
                            println!("保存交易失败 (索引: {}): {}", tx.index.unwrap_or(0), e);
                            error_count += 1;
                        }
                    }
                }
                
                println!("成功保存 {} 笔交易，失败 {} 笔", success_count, error_count);
                
                // 更新当前索引并重置重试计数
                current_index += transactions.len() as u64;
                retry_count = 0;
                
                // 当前批次处理完成后，短暂休息以减轻系统负担
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
            Err(e) => {
                println!("获取交易失败: {}，重试 {}/{}", e, retry_count + 1, max_retries);
                retry_count += 1;
                
                // 错误恢复策略
                if retry_count >= max_retries {
                    println!("达到最大重试次数，尝试跳过当前批次...");
                    current_index += BATCH_SIZE / 2; // 跳过部分索引，尝试继续
                    retry_count = 0;
                    consecutive_empty = 0;
                } else {
                    // 指数退避
                    let wait_time = Duration::from_secs(2u64.pow(retry_count as u32));
                    println!("等待 {:?} 后重试", wait_time);
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }
    
    if consecutive_empty >= max_consecutive_empty {
        println!("连续 {} 次获取空结果，认为已达到链上最新状态", consecutive_empty);
    }
    
    println!("交易同步完成，当前索引: {}", current_index - 1);
    Ok(())
}

/// 重置数据库并完全重新同步所有交易
async fn reset_and_sync_all_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
) -> Result<(), Box<dyn Error>> {
    println!("开始重置数据库并重新同步所有交易数据...");
    
    // 清空交易集合
    println!("清空交易集合...");
    match tx_col.delete_many(doc! {}, None).await {
        Ok(result) => println!("已清除 {} 条交易记录", result.deleted_count),
        Err(e) => {
            println!("清除交易集合失败: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("清除交易集合失败: {}", e))));
        }
    }
    
    // 清空账户-交易关系集合
    println!("清空账户-交易关系集合...");
    match accounts_col.delete_many(doc! {}, None).await {
        Ok(result) => println!("已清除 {} 条账户-交易关系记录", result.deleted_count),
        Err(e) => {
            println!("清除账户-交易关系集合失败: {}", e);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, 
                format!("清除账户-交易关系集合失败: {}", e))));
        }
    }
    
    // 重新创建索引
    println!("重新创建索引...");
    match tx_col.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "index": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        None
    ).await {
        Ok(_) => println!("交易索引创建成功"),
        Err(e) => {
            println!("交易索引创建失败: {}", e);
            // 继续执行，不返回错误
        }
    }
    
    match accounts_col.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "account": 1 })
            .build(),
        None
    ).await {
        Ok(_) => println!("账户索引创建成功"),
        Err(e) => {
            println!("账户索引创建失败: {}", e);
            // 继续执行，不返回错误
        }
    }
    
    // 获取归档信息
    println!("获取归档信息...");
    let archives = match fetch_archives(&agent, &canister_id).await {
        Ok(a) => a,
        Err(e) => {
            println!("获取归档信息失败: {}，尝试直接从Ledger获取交易", e);
            Vec::new()
        }
    };
    
    // 处理归档canister
    if !archives.is_empty() {
        println!("找到 {} 个归档canister", archives.len());
        
        for (idx, archive_info) in archives.iter().enumerate() {
            println!("\n处理归档 {}/{}: canister_id={}", idx + 1, archives.len(), archive_info.canister_id);
            let archive_canister_id = &archive_info.canister_id;
            let block_range_start = archive_info.block_range_start.0.to_u64().unwrap_or(0);
            let block_range_end = archive_info.block_range_end.0.to_u64().unwrap_or(0);
            
            println!("归档范围: {}-{}", block_range_start, block_range_end);
            
            // 先测试单个交易的解码
            match fetch_archive_transactions(
                &agent,
                archive_canister_id,
                block_range_start,
                1
            ).await {
                Ok(test_transactions) => {
                    if test_transactions.is_empty() {
                        println!("无法从归档canister获取交易，跳过此归档");
                        continue;
                    }
                    
                    println!("测试获取交易成功，开始批量同步...");
                    println!("使用批量大小: {} 笔交易/批次", ARCHIVE_BATCH_SIZE);
                    
                    // 分批处理归档交易
                    let mut current_start = block_range_start;
                    let mut error_count = 0;
                    let max_consecutive_errors = 3;
                    
                    while current_start <= block_range_end && error_count < max_consecutive_errors {
                        let current_length = std::cmp::min(ARCHIVE_BATCH_SIZE, 
                                           block_range_end.saturating_sub(current_start) + 1);
                                           
                        if current_length == 0 {
                            println!("计算出的批次长度为0，停止处理此归档");
                            break;
                        }
                        
                        println!("获取归档交易批次: {}-{}", current_start, 
                                current_start + current_length - 1);
                        
                        match fetch_archive_transactions(
                            &agent,
                            archive_canister_id,
                            current_start,
                            current_length
                        ).await {
                            Ok(transactions) => {
                                let num_fetched = transactions.len();
                                error_count = 0; // 重置错误计数
                                
                                if num_fetched == 0 {
                                    println!("批次内无交易，跳到下一批次");
                                    current_start += current_length;
                                    if current_start > block_range_end {
                                        println!("已达到归档范围末尾");
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                    continue;
                                }
                                
                                println!("获取到 {} 笔交易，保存到数据库", num_fetched);
                                
                                // 保存交易到数据库
                                let mut success_count = 0;
                                let mut error_count = 0;
                                
                                for tx in &transactions {
                                    match save_transaction(tx_col, tx).await {
                                        Ok(_) => {
                                            success_count += 1;
                                            let index = tx.index.unwrap_or(0);
                                            let tx_clone = tx.clone();
                                            let tx_array = vec![tx_clone];
                                            let account_txs = group_transactions_by_account(&tx_array);
                                            
                                            for (account, _) in &account_txs {
                                                if let Err(e) = save_account_transaction(
                                                    accounts_col, account, index).await {
                                                    println!("保存账户-交易关系失败: {}", e);
                                                    error_count += 1;
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            println!("保存交易失败: {}", e);
                                            error_count += 1;
                                        }
                                    }
                                }
                                
                                println!("保存结果: 成功={}, 失败={}", success_count, error_count);
                                
                                // 推进索引
                                current_start += num_fetched as u64;
                                
                                // 检查是否已到达归档末尾
                                if (num_fetched as u64) < current_length {
                                    println!("获取的交易数量少于请求数量，可能已达归档末尾");
                                    break;
                                }
                            },
                            Err(e) => {
                                error_count += 1;
                                println!("获取归档交易失败 ({}/{}): {}", 
                                    error_count, max_consecutive_errors, e);
                                
                                if error_count >= max_consecutive_errors {
                                    println!("连续错误次数达到上限，中止此归档处理");
                                    break;
                                }
                                
                                println!("短暂休眠后尝试跳过此批次...");
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                current_start += current_length;
                            }
                        }
                        
                        // 在批次之间添加短暂延迟
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    
                    println!("归档 {} 处理完成", archive_canister_id);
                },
                Err(e) => {
                    println!("测试获取归档交易失败: {}, 跳过此归档", e);
                }
            }
        }
        
        println!("\n所有归档处理完毕");
    } else {
        println!("没有找到归档信息，跳过归档处理");
    }
    
    // 同步ledger的交易
    println!("\n开始同步ledger交易...");
    
    // 强制从区块链第一个交易开始同步
    let mut current_index = 0;
    
    // 尝试获取区块链初始索引
    match get_first_transaction_index(agent, canister_id).await {
        Ok(first_index) => {
            println!("获取到区块链初始索引: {}", first_index);
            // 设置起始索引为区块链的第一个交易索引
            current_index = first_index;
        },
        Err(e) => {
            println!("获取区块链初始索引失败: {}，尝试从0开始", e);
        }
    }
    
    let mut retry_count = 0;
    let max_retries = 3;
    let mut consecutive_empty = 0;
    let max_consecutive_empty = 2;
    
    // 开始同步交易
    while retry_count < max_retries && consecutive_empty < max_consecutive_empty {
        let length = BATCH_SIZE;
        println!("查询交易批次: {}-{}", current_index, current_index + length - 1);
        
        match fetch_ledger_transactions(agent, canister_id, current_index, length).await {
            Ok((transactions, first_index, log_length)) => {
                // 如果first_index大于current_index，调整索引
                if first_index > current_index {
                    println!("调整查询索引: {} -> {}", current_index, first_index);
                    current_index = first_index;
                    continue;
                }
                
                if transactions.is_empty() {
                    consecutive_empty += 1;
                    println!("没有获取到交易 ({}/{})", consecutive_empty, max_consecutive_empty);
                    
                    if log_length > current_index {
                        println!("日志长度 {} 大于当前索引 {}, 调整位置", log_length, current_index);
                        current_index = log_length;
                        consecutive_empty = 0;
                    } else {
                        current_index += BATCH_SIZE / 10;
                        println!("尝试从新位置 {} 查询", current_index);
                    }
                    
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                
                // 获取到交易，重置计数
                consecutive_empty = 0;
                println!("获取到 {} 笔交易", transactions.len());
                
                // 保存交易
                let mut success_count = 0;
                let mut error_count = 0;
                
                for tx in &transactions {
                    match save_transaction(tx_col, tx).await {
                        Ok(_) => {
                            success_count += 1;
                            let index = tx.index.unwrap_or(0);
                            let tx_clone = tx.clone();
                            let tx_array = vec![tx_clone];
                            let account_txs = group_transactions_by_account(&tx_array);
                            
                            for (account, _) in &account_txs {
                                if let Err(e) = save_account_transaction(accounts_col, account, index).await {
                                    println!("保存账户-交易关系失败: {}", e);
                                    error_count += 1;
                                }
                            }
                        },
                        Err(e) => {
                            println!("保存交易失败: {}", e);
                            error_count += 1;
                        }
                    }
                }
                
                println!("成功保存 {} 笔交易，失败 {} 笔", success_count, error_count);
                
                // 更新索引并重置重试计数
                current_index += transactions.len() as u64;
                retry_count = 0;
                
                // 短暂休息
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
            Err(e) => {
                println!("获取交易失败: {}，重试 {}/{}", e, retry_count + 1, max_retries);
                retry_count += 1;
                
                if retry_count >= max_retries {
                    println!("达到最大重试次数，尝试跳过当前批次");
                    current_index += BATCH_SIZE / 2;
                    retry_count = 0;
                } else {
                    let wait_time = Duration::from_secs(2u64.pow(retry_count as u32));
                    println!("等待 {:?} 后重试", wait_time);
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }
    
    println!("数据库重置和交易同步完成！");
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
    // 设置全局错误捕获
    let result = run_application().await;
    
    // 处理顶层错误
    if let Err(e) = &result {
        eprintln!("程序执行过程中发生错误: {}", e);
        // 可以在这里添加额外的错误处理逻辑，如发送警报通知等
    }
    
    result
}

// 将主要应用逻辑移到独立函数，便于错误处理
async fn run_application() -> Result<(), Box<dyn Error>> {
    println!("启动索引服务...");
    
    // 获取命令行参数
    let args: Vec<String> = env::args().collect();
    let reset_mode = args.len() > 1 && args[1] == "--reset";
    
    if reset_mode {
        println!("检测到 --reset 参数，将重置数据库并重新同步所有交易");
    }
    
    // 读取配置文件
    let settings = match config_rs::Config::builder()
        .add_source(config_rs::File::with_name("config"))
        .build() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("无法读取配置文件: {}", e);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("配置文件错误: {}", e)
            )));
        }
    };
    
    let cfg: Config = match settings.try_deserialize() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("无法解析配置: {}", e);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("配置解析错误: {}", e)
            )));
        }
    };

    println!("配置加载完成: MongoDB={}, 数据库={}, Ledger Canister={}",
        cfg.mongodb_url, cfg.database, cfg.ledger_canister_id);

    // 初始化 MongoDB
    let mongo_client = match Client::with_options(
        match ClientOptions::parse(&cfg.mongodb_url).await {
            Ok(options) => options,
            Err(e) => {
                eprintln!("MongoDB连接字符串解析失败: {}", e);
                return Err(Box::new(e));
            }
        }
    ) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("无法连接到MongoDB: {}", e);
            return Err(Box::new(e));
        }
    };
    
    println!("已连接到MongoDB");
    
    let db = mongo_client.database(&cfg.database);
    let accounts_col: Collection<mongodb::bson::Document> = db.collection("accounts");
    let tx_col: Collection<mongodb::bson::Document> = db.collection("transactions");
    
    // 初始化IC Agent
    let agent = match Agent::builder()
        .with_url(&cfg.ic_url)
        .build() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("无法初始化IC Agent: {}", e);
            return Err(Box::new(e));
        }
    };
    
    println!("IC Agent初始化完成，连接到: {}", cfg.ic_url);

    let canister_id = match Principal::from_text(&cfg.ledger_canister_id) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("无效的Canister ID格式: {}", e);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("无效的Canister ID: {}", e)
            )));
        }
    };

    // 如果是重置模式，执行完整的数据库重置和重新同步
    if reset_mode {
        println!("开始执行数据库重置和重新同步操作...");
        if let Err(e) = reset_and_sync_all_transactions(&agent, &canister_id, &tx_col, &accounts_col).await {
            eprintln!("数据库重置和重新同步失败: {}", e);
            return Err(e);
        }
        println!("数据库重置和重新同步成功完成！");
        return Ok(());
    }
    
    // 正常模式：继续执行增量同步
    // 创建索引以提高查询性能
    println!("创建或确认数据库索引...");
    match tx_col.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "index": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        None
    ).await {
        Ok(_) => println!("交易索引创建成功"),
        Err(e) => eprintln!("交易索引创建失败: {}", e)
    }
    
    match accounts_col.create_index(
        mongodb::IndexModel::builder()
            .keys(doc! { "account": 1 })
            .build(),
        None
    ).await {
        Ok(_) => println!("账户索引创建成功"),
        Err(e) => eprintln!("账户索引创建失败: {}", e)
    }
    
    println!("获取归档信息...");
    let archives = match fetch_archives(&agent, &canister_id).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("获取归档信息失败: {}", e);
            eprintln!("将继续并尝试直接从Ledger获取交易...");
            Vec::new()
        }
    };
    
    if archives.is_empty() {
        println!("没有找到归档信息");
        println!("交易都存在 ledger canister 里。跳过归档canister，直接查询ledger canister。");
    } else {
        // 打印归档信息
        println!("打印归档信息:");
        for archive in &archives {
            println!("找到归档信息: canister_id={}, 范围: {}-{}",
                archive.canister_id, 
                archive.block_range_start.0, 
                archive.block_range_end.0
            );
        }

        // 处理第一个找到的归档
        let archive_info = &archives[0]; // 使用第一个归档
        let archive_canister_id = &archive_info.canister_id;
        let block_range_start = archive_info.block_range_start.0.to_u64().unwrap_or(0);
        let block_range_end = archive_info.block_range_end.0.to_u64().unwrap_or(0);
        
        // 先处理archive canister的历史交易
        println!("开始同步历史交易从archive canister: {}", archive_canister_id);
        
        // 先尝试单个交易获取测试Candid解码功能
        println!("测试获取单个交易...");
        match fetch_archive_transactions(
            &agent,
            archive_canister_id,
            block_range_start,
            1
        ).await {
            Ok(test_transactions) => {
                if test_transactions.is_empty() {
                    println!("无法从归档canister获取交易（测试单条），可能是数据结构不匹配或归档为空。");
                    println!("将跳过此归档canister处理。");
                    // 跳过归档处理，后续将直接处理 ledger
                } else {
                    println!("测试成功，开始批量获取归档交易...");
                    println!("使用批量大小: {} 笔交易/批次", ARCHIVE_BATCH_SIZE);
                    
                    // 分批获取并处理归档交易
                    let mut current_start = block_range_start;
                    let mut error_count = 0;
                    let max_consecutive_errors = 3;
                    
                    while current_start <= block_range_end && error_count < max_consecutive_errors {
                        let current_length = std::cmp::min(ARCHIVE_BATCH_SIZE, block_range_end.saturating_sub(current_start) + 1);
                        // 检查 current_length 是否为0，避免无效查询
                        if current_length == 0 {
                            println!("计算出的批次长度为0，停止此归档处理。");
                            break;
                        }
                        
                        println!("获取归档交易批次: {}-{}", current_start, current_start + current_length - 1);
                        
                        match fetch_archive_transactions(
                            &agent,
                            archive_canister_id,
                            current_start,
                            current_length
                        ).await {
                            Ok(transactions) => {
                                let num_fetched = transactions.len();
                                error_count = 0; // 重置错误计数
                                
                                if num_fetched == 0 {
                                     // 如果获取到0条交易，但未达到范围末尾，可能意味着此范围确实没有交易，或者有暂时性问题
                                     println!("批次 {}-{} 内无交易。", current_start, current_start + current_length - 1);
                                     // 推进 current_start 以继续下一个批次
                                     current_start += current_length;
                                     // 如果已经查询到了 block_range_end，则结束循环
                                     if current_start > block_range_end {
                                          println!("已达到归档范围末尾。");
                                          break;
                                     }
                                     // 添加短暂延迟避免空轮询过快
                                     tokio::time::sleep(Duration::from_millis(500)).await;
                                     continue; // 继续下一个循环迭代
                                }
                                
                                println!("获取到 {} 笔交易，保存到数据库", num_fetched);
                                
                                // 跟踪保存成功和失败的数量
                                let mut success_count = 0;
                                let mut save_errors = 0;
                                
                                // 保存交易到数据库
                                for tx in &transactions {
                                    match save_transaction(&tx_col, tx).await {
                                        Ok(_) => {
                                            success_count += 1;
                                            
                                            let index = tx.index.unwrap_or(0);
                                            let tx_clone = tx.clone();
                                            let tx_array = vec![tx_clone];
                                            let account_txs = group_transactions_by_account(&tx_array);
                                            
                                            for (account, _) in &account_txs {
                                                if let Err(e) = save_account_transaction(&accounts_col, account, index).await {
                                                    println!("保存账户-交易关系失败: {}", e);
                                                    save_errors += 1;
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            println!("保存交易失败: {}", e);
                                            save_errors += 1;
                                        }
                                    }
                                }
                                
                                println!("保存结果: 成功={}, 失败={}", success_count, save_errors);
                                
                                // 推进索引，确保即使获取数量少于请求数量也能正确前进
                                current_start += num_fetched as u64;
                                
                                // 如果获取到的交易数量小于请求的数量，可能意味着到达了归档的末尾
                                if (num_fetched as u64) < current_length {
                                    println!("获取到的交易数量 ({}) 少于请求数量 ({})，可能已达归档末尾。", num_fetched, current_length);
                                    break; // 结束此归档的处理
                                }
                            },
                            Err(e) => {
                                error_count += 1;
                                println!("获取归档交易批次 {}-{} 失败 ({}/{}): {}", 
                                    current_start, current_start + current_length - 1, 
                                    error_count, max_consecutive_errors, e);
                                
                                if error_count >= max_consecutive_errors {
                                    println!("连续错误次数达到上限，中止此归档处理");
                                    break;
                                }
                                
                                // 实现简单的重试或跳过逻辑
                                println!("短暂休眠后将尝试跳过此批次...");
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                current_start += current_length; // 谨慎地跳过失败的批次
                            }
                        }
                         // 在每个批次请求之间添加小的延迟，避免过于频繁地请求
                         tokio::time::sleep(Duration::from_millis(200)).await;
                    } // end while loop
                    
                    println!("归档 canister {} 的历史交易同步完成。", archive_canister_id);
                } // end else (test transaction successful)
            }, // end Ok case for test transaction fetch
            Err(e) => {
                 println!("测试获取单个归档交易失败: {}", e);
                 println!("将跳过此归档canister处理。");
                 // 不处理此归档，后续将直接处理 ledger
            }
        } // end match for test transaction fetch
        
        // 如果需要处理所有 archives 而不是仅第一个，需要将上述逻辑放入循环中
        println!("所有找到的归档处理完毕 (或跳过)。");
    } // end else block (archives not empty)
    
    // 不论是否有归档，都执行 ledger 同步
    println!("开始同步ledger交易...");
    if let Err(e) = sync_ledger_transactions(&agent, &canister_id, &tx_col, &accounts_col).await {
        eprintln!("同步ledger交易时发生错误: {}", e);
        // 不返回错误，继续执行定时同步逻辑
    } else {
        println!("初始同步完成");
    }
    
    println!("开始实时监控新交易");
    
    // 定时增量同步
    let mut interval = interval(Duration::from_secs(5));
    let mut consecutive_errors = 0;
    let max_consecutive_errors = 5;
    
    loop {
        interval.tick().await;
        
        println!("\n执行定时增量同步...");
        match sync_ledger_transactions(&agent, &canister_id, &tx_col, &accounts_col).await {
            Ok(_) => {
                println!("定时增量同步完成");
                consecutive_errors = 0; // 重置错误计数
            },
            Err(e) => {
                consecutive_errors += 1;
                eprintln!("定时增量同步出错 ({}/{}): {}", consecutive_errors, max_consecutive_errors, e);
                
                if consecutive_errors >= max_consecutive_errors {
                    eprintln!("连续错误次数达到上限 ({}), 等待更长时间后继续...", max_consecutive_errors);
                    // 发生多次连续错误时，等待更长时间再重试
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    consecutive_errors = 0; // 重置计数
                }
            }
        }
    }
}
