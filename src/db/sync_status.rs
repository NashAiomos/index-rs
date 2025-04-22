use std::error::Error;
use mongodb::{Collection};
use mongodb::bson::{Document, doc};
use tokio::time::Duration;
use chrono::Utc;
use crate::utils::create_error;

/// 同步状态记录结构
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub last_synced_index: u64,
    pub last_synced_timestamp: u64,
    pub updated_at: i64,
    pub sync_mode: String, // "full" 或 "incremental"
}

/// 获取最新的同步状态
pub async fn get_sync_status(
    sync_status_col: &Collection<Document>
) -> Result<Option<SyncStatus>, Box<dyn Error>> {
    if let Some(doc) = sync_status_col
        .find_one(doc! { "status_type": "sync_state" }, None)
        .await?
    {
        let last_synced_index = doc.get_i64("last_synced_index")
            .unwrap_or(0) as u64;
        let last_synced_timestamp = doc.get_i64("last_synced_timestamp")
            .unwrap_or(0) as u64;
        let updated_at = doc.get_i64("updated_at").unwrap_or(0);
        let sync_mode = doc.get_str("sync_mode")
            .unwrap_or("incremental")
            .to_string();
        
        return Ok(Some(SyncStatus {
            last_synced_index,
            last_synced_timestamp,
            updated_at,
            sync_mode,
        }));
    }
    
    Ok(None)
}

/// 更新同步状态
pub async fn update_sync_status(
    sync_status_col: &Collection<Document>,
    last_synced_index: u64,
    last_synced_timestamp: u64,
    sync_mode: &str
) -> Result<(), Box<dyn Error>> {
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    let now = Utc::now().timestamp();
    let sync_state = doc! {
        "last_synced_index": last_synced_index as i64,
        "last_synced_timestamp": last_synced_timestamp as i64,
        "updated_at": now,
        "sync_mode": sync_mode,
    };
    
    while retry_count < max_retries {
        // 更新同步状态
        match sync_status_col.update_one(
            doc! { "status_type": "sync_state" },
            doc! {
                "$set": sync_state.clone()
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        ).await {
            Ok(_) => {
                println!("同步状态已更新: 索引 {}, 时间戳 {}, 模式 {}", 
                         last_synced_index, last_synced_timestamp, sync_mode);
                return Ok(());
            },
            Err(e) => {
                retry_count += 1;
                let wait_time = Duration::from_millis(500 * retry_count);
                println!("更新同步状态失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&format!("更新同步状态失败，已重试 {} 次", max_retries)))
}

/// 设置同步模式为增量
pub async fn set_incremental_mode(
    sync_status_col: &Collection<Document>,
    last_synced_index: u64,
    last_synced_timestamp: u64
) -> Result<(), Box<dyn Error>> {
    println!("设置为增量同步模式，最新索引: {}", last_synced_index);
    update_sync_status(sync_status_col, last_synced_index, last_synced_timestamp, "incremental").await
}

/// 设置同步模式为全量
pub async fn set_full_sync_mode(
    sync_status_col: &Collection<Document>
) -> Result<(), Box<dyn Error>> {
    println!("设置为全量同步模式");
    update_sync_status(sync_status_col, 0, 0, "full").await
}

/// 清除同步状态
pub async fn clear_sync_status(
    sync_status_col: &Collection<Document>
) -> Result<(), Box<dyn Error>> {
    match sync_status_col.delete_many(doc! {}, None).await {
        Ok(result) => {
            println!("已清除 {} 条同步状态记录", result.deleted_count);
            Ok(())
        },
        Err(e) => {
            println!("清除同步状态记录失败: {}", e);
            Err(create_error(&format!("清除同步状态记录失败: {}", e)))
        }
    }
} 