use std::error::Error;
use mongodb::Collection;
use mongodb::bson::{doc, Document};
use mongodb::options::FindOptions;
use log::debug;
use crate::models::Transaction;
use crate::db::balances::normalize_account_id;
use futures::stream::TryStreamExt;
use mongodb::options::FindOneOptions;
use crate::db::supply;

/// API模块，提供所有对外查询功能
/// 包括地址、交易和余额的相关查询

/// 查询账户余额
pub async fn get_account_balance(
    balances_col: &Collection<Document>,
    account: &str,
) -> Result<String, Box<dyn Error>> {
    let normalized_account = normalize_account_id(account);
    debug!("查询账户 {} 余额", normalized_account);
    
    if let Some(doc) = balances_col
        .find_one(doc! { "account": &normalized_account }, None)
        .await?
    {
        if let Ok(balance) = doc.get_str("balance") {
            return Ok(balance.to_string());
        }
    }
    
    Ok("0".to_string()) // 默认返回0余额
}

/// 查询账户的交易历史
pub async fn get_account_transactions(
    accounts_col: &Collection<Document>,
    tx_col: &Collection<Document>,
    account: &str,
    limit: Option<i64>,
    skip: Option<i64>,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    let normalized_account = normalize_account_id(account);
    debug!("查询账户 {} 的交易历史", normalized_account);
    
    // 从账户集合获取交易索引列表
    let account_doc = match accounts_col
        .find_one(doc! { "account": &normalized_account }, None)
        .await?
    {
        Some(doc) => doc,
        None => return Ok(Vec::new()), // 账户不存在，返回空列表
    };
    
    let indices = match account_doc.get_array("transaction_indices") {
        Ok(indices) => indices.clone(),
        Err(_) => return Ok(Vec::new()), // 没有交易记录
    };
    
    if indices.is_empty() {
        return Ok(Vec::new());
    }
    
    // 将BSON数组转换为i64数组
    let tx_indices: Vec<i64> = indices.iter()
        .filter_map(|idx| idx.as_i64())
        .collect();
    
    // 设置分页参数
    let limit_val = limit.unwrap_or(50);
    let skip_val = skip.unwrap_or(0);
    
    // 获取交易记录
    let options = FindOptions::builder()
        .sort(doc! { "index": -1 })
        .limit(limit_val)
        .skip(Some(skip_val as u64))
        .build();
    
    let transactions_cursor = tx_col
        .find(doc! { "index": { "$in": &tx_indices } }, options)
        .await?;
    
    // 收集符合条件的交易
    let doc_transactions: Vec<Document> = transactions_cursor.try_collect().await?;
    
    // 将Document转换为Transaction
    let mut transactions: Vec<Transaction> = Vec::with_capacity(doc_transactions.len());
    for doc in doc_transactions {
        let transaction: Transaction = mongodb::bson::from_document(doc)?;
        transactions.push(transaction);
    }
    
    Ok(transactions)
}

/// 查询特定交易详情
pub async fn get_transaction_by_index(
    tx_col: &Collection<Document>,
    index: u64,
) -> Result<Option<Transaction>, Box<dyn Error>> {
    debug!("查询交易索引 {} 的详情", index);
    
    let tx_doc = tx_col.find_one(doc! { "index": index as i64 }, None).await?;
    
    match tx_doc {
        Some(doc) => {
            let transaction: Transaction = mongodb::bson::from_document(doc)?;
            Ok(Some(transaction))
        },
        None => Ok(None),
    }
}

/// 获取最新的交易
pub async fn get_latest_transactions(
    tx_col: &Collection<Document>,
    limit: Option<i64>,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    let limit_val = limit.unwrap_or(20); // 默认获取20条
    debug!("获取最新的 {} 条交易", limit_val);
    
    let options = FindOptions::builder()
        .sort(doc! { "index": -1 })
        .limit(limit_val)
        .build();
    
    let transactions_cursor = tx_col
        .find(doc! {}, options)
        .await?;
    
    // 收集符合条件的交易
    let doc_transactions: Vec<Document> = transactions_cursor.try_collect().await?;
    
    // 将Document转换为Transaction
    let mut transactions: Vec<Transaction> = Vec::with_capacity(doc_transactions.len());
    for doc in doc_transactions {
        let transaction: Transaction = mongodb::bson::from_document(doc)?;
        transactions.push(transaction);
    }
    
    Ok(transactions)
}

/// 获取最新的交易索引
#[allow(dead_code)]
pub async fn get_latest_transaction_index(
    tx_col: &Collection<Document>,
) -> Result<Option<u64>, Box<dyn Error>> {
    debug!("获取最新交易索引");
    
    let find_opts = FindOneOptions::builder()
        .sort(doc! { "index": -1 })
        .build();
    
    if let Some(doc) = tx_col.find_one(doc! {}, find_opts).await? {
        if let Some(index) = doc.get_i64("index").ok() {
            return Ok(Some(index as u64));
        }
    }
    
    Ok(None)
}

/// 搜索交易（多条件查询）
pub async fn search_transactions(
    tx_col: &Collection<Document>,
    query: Document,
    limit: Option<i64>,
    skip: Option<i64>,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    let limit_val = limit.unwrap_or(50);
    let skip_val = skip.unwrap_or(0);
    debug!("搜索交易，条件：{:?}, 限制：{}, 跳过：{}", query, limit_val, skip_val);
    
    let options = FindOptions::builder()
        .sort(doc! { "index": -1 })
        .limit(limit_val)
        .skip(Some(skip_val as u64))
        .build();
    
    let transactions_cursor = tx_col
        .find(query, options)
        .await?;
    
    // 收集符合条件的交易
    let doc_transactions: Vec<Document> = transactions_cursor.try_collect().await?;
    
    // 将Document转换为Transaction
    let mut transactions: Vec<Transaction> = Vec::with_capacity(doc_transactions.len());
    for doc in doc_transactions {
        let transaction: Transaction = mongodb::bson::from_document(doc)?;
        transactions.push(transaction);
    }
    
    Ok(transactions)
}

/// 获取所有账户
pub async fn get_all_accounts(
    accounts_col: &Collection<Document>,
    limit: Option<i64>,
    skip: Option<i64>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let limit_val = limit.unwrap_or(100);
    let skip_val = skip.unwrap_or(0);
    debug!("获取所有账户，限制：{}, 跳过：{}", limit_val, skip_val);
    
    let options = FindOptions::builder()
        .sort(doc! { "account": 1 })
        .limit(limit_val)
        .skip(Some(skip_val as u64))
        .projection(doc! { "account": 1, "_id": 0 })
        .build();
    
    let accounts_cursor = accounts_col
        .find(doc! {}, options)
        .await?;
    
    // 收集账户列表
    let accounts: Vec<Document> = accounts_cursor.try_collect().await?;
    
    // 提取账户名
    let account_names = accounts.iter()
        .filter_map(|doc| doc.get_str("account").ok())
        .map(|s| s.to_string())
        .collect();
    
    Ok(account_names)
}

/// 获取代币总供应量（通过所有账户余额计算）
pub async fn get_total_supply(
    supply_col: &Collection<Document>,
) -> Result<String, Box<dyn Error>> {
    debug!("获取代币总供应量");
    if let Some(value) = supply::get_stored_total_supply(supply_col).await? {
        return Ok(value);
    }
    Ok("0".to_string())
}

/// 统计交易总数
pub async fn get_transaction_count(
    tx_col: &Collection<Document>,
) -> Result<u64, Box<dyn Error>> {
    debug!("统计交易总数");
    
    let count = tx_col.count_documents(doc! {}, None).await?;
    
    Ok(count)
}

/// 统计账户总数
pub async fn get_account_count(
    accounts_col: &Collection<Document>,
) -> Result<u64, Box<dyn Error>> {
    debug!("统计账户总数");
    
    let count = accounts_col.count_documents(doc! {}, None).await?;
    
    Ok(count)
}

/// 获取最近交易中的唯一账户（活跃账户）
pub async fn get_active_accounts(
    tx_col: &Collection<Document>,
    limit: Option<i64>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let limit_val = limit.unwrap_or(1000); // 默认获取最近1000条交易
    debug!("获取活跃账户（最近 {} 条交易）", limit_val);
    
    // 获取最近的交易
    let options = FindOptions::builder()
        .sort(doc! { "index": -1 })
        .limit(limit_val)
        .build();
    
    let transactions_cursor = tx_col
        .find(doc! {}, options)
        .await?;
    
    // 收集交易
    let transactions: Vec<Document> = transactions_cursor.try_collect().await?;
    
    // 提取唯一账户
    let mut accounts = std::collections::HashSet::new();
    
    for tx_doc in transactions {
        // 提取转账交易中的账户
        if let Ok(transfer_doc) = tx_doc.get_document("transfer") {
            if let Ok(from_doc) = transfer_doc.get_document("from") {
                let from_account = from_doc.to_string();
                accounts.insert(from_account);
            }
            if let Ok(to_doc) = transfer_doc.get_document("to") {
                let to_account = to_doc.to_string();
                accounts.insert(to_account);
            }
        }
        
        // 提取铸币交易中的账户
        if let Ok(mint_doc) = tx_doc.get_document("mint") {
            if let Ok(to_doc) = mint_doc.get_document("to") {
                let to_account = to_doc.to_string();
                accounts.insert(to_account);
            }
        }
        
        // 提取销毁交易中的账户
        if let Ok(burn_doc) = tx_doc.get_document("burn") {
            if let Ok(from_doc) = burn_doc.get_document("from") {
                let from_account = from_doc.to_string();
                accounts.insert(from_account);
            }
        }
    }
    
    // 转换为Vector
    let active_accounts: Vec<String> = accounts.into_iter().collect();
    
    Ok(active_accounts)
} 