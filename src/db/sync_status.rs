/**
 * 文件描述: 同步状态管理模块，负责跟踪和管理区块链数据同步进度
 * 功能概述:
 * - 管理同步模式(全量/增量)
 * - 保存最新同步索引
 * - 提供同步状态的查询和更新操作
 * 
 * 主要组件:
 * - SyncStatus结构体: 定义同步状态数据结构
 * - get_sync_status函数: 获取指定代币的最新同步状态
 * - update_sync_status函数: 更新同步状态，支持重试机制
 * - set_incremental_mode函数: 设置为增量同步模式
 * - set_full_sync_mode函数: 设置为全量同步模式
 * - clear_token_sync_status函数: 清除指定代币的同步状态
 * - clear_sync_status函数: 清除所有代币的同步状态
 */

use std::error::Error;
use mongodb::{Collection};
use mongodb::bson::{Document, doc};
use tokio::time::Duration;
use chrono::Utc;
use log::{info, error, warn};
use crate::utils::create_error;

/// 同步状态记录结构
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SyncStatus {
    pub token: String, // 代币标识符
    pub last_synced_index: u64,
    pub last_synced_timestamp: u64,
    pub last_balance_calculated_index: u64,
    pub updated_at: i64,
    pub sync_mode: String, // "full" 或 "incremental"
}

/// 获取指定代币的最新同步状态
pub async fn get_sync_status(
    sync_status_col: &Collection<Document>,
    token_symbol: &str
) -> Result<Option<SyncStatus>, Box<dyn Error>> {
    if let Some(doc) = sync_status_col
        .find_one(doc! { "status_type": "sync_state", "token": token_symbol }, None)
        .await?
    {
        let last_synced_index = doc.get_i64("last_synced_index")
            .unwrap_or(0) as u64;
        let last_synced_timestamp = doc.get_i64("last_synced_timestamp")
            .unwrap_or(0) as u64;
        let updated_at = doc.get_i64("updated_at").unwrap_or(0);
        let last_balance_calculated_index = doc.get_i64("last_balance_calculated_index")
            .unwrap_or(0) as u64;
        let sync_mode = doc.get_str("sync_mode")
            .unwrap_or("incremental")
            .to_string();
        
        return Ok(Some(SyncStatus {
            token: token_symbol.to_string(),
            last_synced_index,
            last_synced_timestamp,
            last_balance_calculated_index,
            updated_at,
            sync_mode,
        }));
    }
    
    Ok(None)
}

/// 更新同步状态
pub async fn update_sync_status(
    sync_status_col: &Collection<Document>,
    token_symbol: &str,
    last_synced_index: u64,
    last_synced_timestamp: u64,
    sync_mode: &str
) -> Result<(), Box<dyn Error>> {
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    let now = Utc::now().timestamp();
    let sync_state = doc! {
        "token": token_symbol,
        "last_synced_index": last_synced_index as i64,
        "last_synced_timestamp": last_synced_timestamp as i64,
        "updated_at": now,
        "sync_mode": sync_mode,
    };
    
    while retry_count < max_retries {
        // 更新同步状态
        match sync_status_col.update_one(
            doc! { "status_type": "sync_state", "token": token_symbol },
            doc! {
                "$set": sync_state.clone()
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        ).await {
            Ok(_) => {
                info!("{}: 同步状态已更新: 索引 {}, 时间戳 {}, 模式 {}", 
                         token_symbol, last_synced_index, last_synced_timestamp, sync_mode);
                return Ok(());
            },
            Err(e) => {
                retry_count += 1;
                let wait_time = Duration::from_millis(500 * retry_count);
                warn!("{}: 更新同步状态失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    token_symbol, retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&format!("{}: 更新同步状态失败，已重试 {} 次", token_symbol, max_retries)))
}

/// 设置同步模式为增量
pub async fn set_incremental_mode(
    sync_status_col: &Collection<Document>,
    token_symbol: &str,
    last_synced_index: u64,
    last_synced_timestamp: u64
) -> Result<(), Box<dyn Error>> {
    info!("{}: 设置为增量同步模式，最新索引: {}", token_symbol, last_synced_index);
    update_sync_status(sync_status_col, token_symbol, last_synced_index, last_synced_timestamp, "incremental").await
}

/// 设置同步模式为全量
pub async fn set_full_sync_mode(
    sync_status_col: &Collection<Document>,
    token_symbol: &str
) -> Result<(), Box<dyn Error>> {
    info!("{}: 设置为全量同步模式", token_symbol);
    update_sync_status(sync_status_col, token_symbol, 0, 0, "full").await
}

/// 清除指定代币的同步状态
#[allow(dead_code)]
pub async fn clear_token_sync_status(
    sync_status_col: &Collection<Document>,
    token_symbol: &str
) -> Result<(), Box<dyn Error>> {
    match sync_status_col.delete_many(doc! { "token": token_symbol }, None).await {
        Ok(result) => {
            info!("{}: 已清除 {} 条同步状态记录", token_symbol, result.deleted_count);
            Ok(())
        },
        Err(e) => {
            error!("{}: 清除同步状态记录失败: {}", token_symbol, e);
            Err(create_error(&format!("{}: 清除同步状态记录失败: {}", token_symbol, e)))
        }
    }
}

/// 清除所有代币的同步状态
pub async fn clear_sync_status(
    sync_status_col: &Collection<Document>
) -> Result<(), Box<dyn Error>> {
    match sync_status_col.delete_many(doc! {}, None).await {
        Ok(result) => {
            info!("已清除所有代币的 {} 条同步状态记录", result.deleted_count);
            Ok(())
        },
        Err(e) => {
            error!("清除所有同步状态记录失败: {}", e);
            Err(create_error(&format!("清除所有同步状态记录失败: {}", e)))
        }
    }
}

/// 更新余额已计算到的最新交易索引
pub async fn update_balance_calculated_index(
    sync_status_col: &Collection<Document>,
    token_symbol: &str,
    last_balance_calculated_index: u64,
) -> Result<(), Box<dyn Error>> {
    let now = Utc::now().timestamp();

    match sync_status_col.update_one(
        doc! { "status_type": "sync_state", "token": token_symbol },
        doc! {
            "$set": {
                "last_balance_calculated_index": last_balance_calculated_index as i64,
                "updated_at": now,
            }
        },
        mongodb::options::UpdateOptions::builder().upsert(true).build()
    ).await {
        Ok(_) => {
            info!("{}: 已更新余额计算进度到索引 {}", token_symbol, last_balance_calculated_index);
            Ok(())
        },
        Err(e) => {
            error!("{}: 更新余额计算进度失败: {}", token_symbol, e);
            Err(create_error(&format!("{}: 更新余额计算进度失败: {}", token_symbol, e)))
        }
    }
}

