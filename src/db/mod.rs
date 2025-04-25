use std::error::Error;
use std::sync::Arc;
use mongodb::{Client, Collection, Database};
use mongodb::bson::Document;
use mongodb::options::{ClientOptions, ResolverConfig};
use log::{info, error};
use tokio::sync::Semaphore;

pub mod transactions;
pub mod accounts;
pub mod balances;
pub mod sync_status;
pub mod supply;

#[derive(Clone)]
/// 数据库连接信息
pub struct DbConnection {
    #[allow(dead_code)]
    pub db: Database,
    pub tx_col: Collection<Document>,
    pub accounts_col: Collection<Document>,
    pub balances_col: Collection<Document>,
    pub total_supply_col: Collection<Document>,
    pub sync_status_col: Collection<Document>,
    pub balance_anomalies_col: Collection<Document>,
    #[allow(dead_code)]
    pub db_semaphore: Arc<Semaphore>,
}

/// 初始化MongoDB连接
pub async fn init_db(mongodb_url: &str, database_name: &str) -> Result<DbConnection, Box<dyn Error>> {
    info!("初始化MongoDB连接: {}", mongodb_url);
    
    let options = ClientOptions::parse_with_resolver_config(mongodb_url, ResolverConfig::cloudflare()).await?;
    
    let mut client_options = options.clone();
    client_options.max_pool_size = Some(20);
    client_options.min_pool_size = Some(5);
    client_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    
    let mongo_client = Client::with_options(client_options)?;
    
    info!("已连接到MongoDB");
    
    let db = mongo_client.database(database_name);
    let accounts_col: Collection<Document> = db.collection("accounts");
    let tx_col: Collection<Document> = db.collection("transactions");
    let balances_col: Collection<Document> = db.collection("balances");
    let total_supply_col: Collection<Document> = db.collection("total_supply");
    let sync_status_col: Collection<Document> = db.collection("sync_status");
    let balance_anomalies_col: Collection<Document> = db.collection("balance_anomalies");
    
    let db_semaphore = Arc::new(Semaphore::new(30));
    
    Ok(DbConnection {
        db,
        tx_col,
        accounts_col,
        balances_col,
        total_supply_col,
        sync_status_col,
        balance_anomalies_col,
        db_semaphore,
    })
}

/// 创建数据库索引
pub async fn create_indexes(conn: &DbConnection) -> Result<(), Box<dyn Error>> {
    info!("创建或确认数据库索引...");
    
    // 交易索引
    match conn.tx_col.create_index(
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "index": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        None
    ).await {
        Ok(_) => info!("交易索引创建成功"),
        Err(e) => error!("交易索引创建失败: {}", e)
    }
    
    // 账户索引
    match conn.accounts_col.create_index(
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "account": 1 })
            .build(),
        None
    ).await {
        Ok(_) => info!("账户索引创建成功"),
        Err(e) => error!("账户索引创建失败: {}", e)
    }
    
    // 余额索引
    match conn.balances_col.create_index(
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "account": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        None
    ).await {
        Ok(_) => info!("余额索引创建成功"),
        Err(e) => error!("余额索引创建失败: {}", e)
    }
    
    // 同步状态索引
    match conn.sync_status_col.create_index(
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "status_type": 1 })
            .options(mongodb::options::IndexOptions::builder().unique(true).build())
            .build(),
        None
    ).await {
        Ok(_) => info!("同步状态索引创建成功"),
        Err(e) => error!("同步状态索引创建失败: {}", e)
    }
    
    Ok(())
}

/// 辅助函数：使用信号量限制并发数，并在释放信号量前执行异步操作
#[allow(dead_code)]
pub async fn with_db_semaphore<F, T>(
    semaphore: Arc<Semaphore>,
    operation_name: &str,
    func: F
) -> Result<T, Box<dyn Error>>
where
    F: FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, Box<dyn Error>>> + Send>>,
{
    // 尝试获取信号量许可
    let permit = match semaphore.acquire().await {
        Ok(permit) => permit,
        Err(e) => {
            error!("无法获取数据库信号量: {}", e);
            return Err(format!("信号量获取失败: {}", e).into());
        }
    };
    
    // 执行操作并释放许可
    let start = std::time::Instant::now();
    let result = func().await;
    let duration = start.elapsed();
    
    // 如果操作时间超过一定阈值，记录警告
    if duration.as_millis() > 500 {
        info!("数据库操作 [{}] 耗时: {:?}", operation_name, duration);
    }
    
    // 释放许可
    drop(permit);
    
    result
}

