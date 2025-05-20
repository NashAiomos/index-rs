/**
 * 文件描述: 账户管理模块，负责账户与交易关系的管理
 * 功能概述:
 * - 管理账户与交易索引的关联
 * - 提供账户数据清理功能
 * - 支持查询账户关联的交易
 * - 实现重试机制确保数据一致性
 * 
 * 主要组件:
 * - save_account_transaction函数: 保存账户与交易索引的关系
 * - clear_accounts函数: 清空账户集合
 * - get_account_transactions函数: 查询某账户下的所有交易
 */

use std::error::Error;
use mongodb::{Collection, bson::{doc, Document}};
use tokio::time::Duration;
use log::{info, error, warn, debug};
use crate::models::Transaction;
use crate::utils::create_error;

/// 保存账户与交易索引的关系
pub async fn save_account_transaction(
    accounts_col: &Collection<Document>,
    account: &str,
    tx_index: u64,
) -> Result<(), Box<dyn Error>> {
    if account.trim().is_empty() {
        debug!("账户为空，跳过保存账户-交易关系");
        return Ok(());
    }
    
    // 使用重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        // 向账户文档中添加交易索引
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
                let wait_time = Duration::from_millis(500 * retry_count);
                warn!("保存账户-交易关系失败 (账户: {}, 索引: {}) (尝试 {}/{}): {}，等待 {:?} 后重试",
                    account, tx_index, retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&format!("保存账户-交易关系失败 (账户: {}, 索引: {}), 已重试 {} 次", 
        account, tx_index, max_retries)))
}

/// 清空账户集合
pub async fn clear_accounts(accounts_col: &Collection<Document>) -> Result<u64, Box<dyn Error>> {
    match accounts_col.delete_many(doc! {}, None).await {
        Ok(result) => {
            info!("已清除 {} 条账户记录", result.deleted_count);
            Ok(result.deleted_count)
        },
        Err(e) => {
            error!("清除账户集合失败: {}", e);
            Err(create_error(&format!("清除账户集合失败: {}", e)))
        }
    }
}

/// 查询某账户下的所有交易
#[allow(dead_code)]
pub async fn get_account_transactions(
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
