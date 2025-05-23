/**
 * 文件描述: 交易数据管理模块，负责区块链交易的存储和查询
 * 功能概述:
 * - 存储交易数据到数据库
 * - 查询最新交易索引
 * - 清空交易集合数据
 * 
 * 主要组件:
 * - save_transaction函数: 将交易保存到数据库，支持重试机制
 * - get_latest_transaction_index函数: 查询数据库中最新的交易索引
 * - clear_transactions函数: 清空交易集合中的所有记录
 */

use std::error::Error;
use mongodb::{Collection, bson::{doc, to_bson}};
use mongodb::bson::Document;
use log::{info, error, warn};
use tokio::time::Duration;
use crate::models::Transaction;
use crate::utils::create_error;
use mongodb::options::FindOptions;
use std::convert::TryFrom;

/// 保存交易到交易集合
pub async fn save_transaction(
    tx_col: &Collection<Document>,
    tx: &Transaction,
) -> Result<(), Box<dyn Error>> {
    let index = tx.index.unwrap_or(0);
    
    // 尝试将交易转换为BSON格式
    let tx_bson = match to_bson(tx) {
        Ok(bson) => bson,
        Err(e) => {
            error!("无法将交易转换为BSON: {}，索引: {}", e, index);
            return Err(create_error(&format!("将交易(索引:{})转换为BSON失败: {}", index, e)));
        }
    };
    
    let doc = match tx_bson.as_document() {
        Some(doc) => doc.clone(),
        None => {
            error!("无法将BSON转换为Document，索引: {}", index);
            return Err(create_error(&format!("无法将BSON转换为Document，索引: {}", index)));
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
                let wait_time = Duration::from_millis(500 * retry_count);
                warn!("保存交易(索引:{})失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    index, retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&format!("保存交易(索引:{})失败，已重试 {} 次", index, max_retries)))
}

/// 获取最新的交易索引
pub async fn get_latest_transaction_index(
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

/// 清空交易集合
pub async fn clear_transactions(tx_col: &Collection<Document>) -> Result<u64, Box<dyn Error>> {
    match tx_col.delete_many(doc! {}, None).await {
        Ok(result) => {
            info!("已清除 {} 条交易记录", result.deleted_count);
            Ok(result.deleted_count)
        },
        Err(e) => {
            error!("清除交易集合失败: {}", e);
            Err(create_error(&format!("清除交易集合失败: {}", e)))
        }
    }
}

/// 根据索引范围获取交易
pub async fn get_transactions_by_index_range(
    tx_col: &Collection<Document>,
    start_index: u64,
    end_index: u64,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    if start_index > end_index {
        return Ok(Vec::new());
    }

    let filter = doc! {
        "index": {
            "$gte": start_index as i64,
            "$lte": end_index as i64
        }
    };
    let options = FindOptions::builder()
        .sort(doc! { "index": 1 })
        .build();

    let mut cursor = tx_col.find(filter, options).await?;
    let mut result = Vec::new();

    while cursor.advance().await? {
        let raw_doc = cursor.current();
        let doc = Document::try_from(raw_doc.to_owned())?;
        match mongodb::bson::from_document::<Transaction>(doc) {
            Ok(tx) => result.push(tx),
            Err(e) => {
                warn!("反序列化交易失败: {}", e);
                continue;
            }
        }
    }

    Ok(result)
}
