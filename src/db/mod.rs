use std::error::Error;
use std::sync::Arc;
use std::collections::HashMap;
use mongodb::{Client, Collection, Database};
use mongodb::bson::Document;
use mongodb::options::{ClientOptions, ResolverConfig};
use log::{info, error};
use tokio::sync::Semaphore;
use crate::models::TokenConfig;

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
    pub collections: HashMap<String, TokenCollections>,
    pub sync_status_col: Collection<Document>,
    #[allow(dead_code)]
    pub db_semaphore: Arc<Semaphore>,
}

impl DbConnection {
    /// 获取指定代币的交易集合
    pub fn get_transactions_collection(&self, token_symbol: &str) -> Collection<Document> {
        if let Some(collections) = self.collections.get(token_symbol) {
            collections.tx_col.clone()
        } else {
            panic!("未找到代币 {} 的集合", token_symbol)
        }
    }
}

#[derive(Clone)]
/// 单个代币的所有集合
pub struct TokenCollections {
    #[allow(dead_code)]
    pub symbol: String,
    pub tx_col: Collection<Document>,
    pub accounts_col: Collection<Document>,
    pub balances_col: Collection<Document>,
    pub total_supply_col: Collection<Document>,
    pub balance_anomalies_col: Collection<Document>,
}

/// 初始化MongoDB连接
pub async fn init_db(mongodb_url: &str, database_name: &str, tokens: &[TokenConfig]) -> Result<DbConnection, Box<dyn Error>> {
    info!("初始化MongoDB连接: {}", mongodb_url);
    
    let options = ClientOptions::parse_with_resolver_config(mongodb_url, ResolverConfig::cloudflare()).await?;
    
    let mut client_options = options.clone();
    client_options.max_pool_size = Some(20);
    client_options.min_pool_size = Some(5);
    client_options.connect_timeout = Some(std::time::Duration::from_secs(10));
    
    let mongo_client = Client::with_options(client_options)?;
    
    info!("已连接到MongoDB");
    
    let db = mongo_client.database(database_name);
    let sync_status_col: Collection<Document> = db.collection("sync_status");
    let db_semaphore = Arc::new(Semaphore::new(30));
    
    // 为每个代币创建集合
    let mut collections = HashMap::new();
    for token in tokens {
        info!("为代币 {} ({}) 创建集合", token.name, token.symbol);
        
        let prefix = token.symbol.to_lowercase();
        let tx_col: Collection<Document> = db.collection(&format!("{}_transactions", prefix));
        let accounts_col: Collection<Document> = db.collection(&format!("{}_accounts", prefix));
        let balances_col: Collection<Document> = db.collection(&format!("{}_balances", prefix));
        let total_supply_col: Collection<Document> = db.collection(&format!("{}_total_supply", prefix));
        let balance_anomalies_col: Collection<Document> = db.collection(&format!("{}_balance_anomalies", prefix));
        
        let token_collections = TokenCollections {
            symbol: token.symbol.clone(),
            tx_col,
            accounts_col,
            balances_col,
            total_supply_col,
            balance_anomalies_col,
        };
        
        collections.insert(token.symbol.clone(), token_collections);
    }
    
    Ok(DbConnection {
        db,
        collections,
        sync_status_col,
        db_semaphore,
    })
}

/// 创建数据库索引
pub async fn create_indexes(conn: &DbConnection) -> Result<(), Box<dyn Error>> {
    info!("创建或确认数据库索引...");
    
    // 为每个代币创建索引
    for (symbol, collections) in &conn.collections {
        info!("为代币 {} 创建索引", symbol);
        
        // 交易索引
        match collections.tx_col.create_index(
            mongodb::IndexModel::builder()
                .keys(mongodb::bson::doc! { "index": 1 })
                .options(mongodb::options::IndexOptions::builder().unique(true).build())
                .build(),
            None
        ).await {
            Ok(_) => info!("{}: 交易索引创建成功", symbol),
            Err(e) => error!("{}: 交易索引创建失败: {}", symbol, e)
        }
        
        // 账户索引
        match collections.accounts_col.create_index(
            mongodb::IndexModel::builder()
                .keys(mongodb::bson::doc! { "account": 1 })
                .build(),
            None
        ).await {
            Ok(_) => info!("{}: 账户索引创建成功", symbol),
            Err(e) => error!("{}: 账户索引创建失败: {}", symbol, e)
        }
        
        // 余额索引
        match collections.balances_col.create_index(
            mongodb::IndexModel::builder()
                .keys(mongodb::bson::doc! { "account": 1 })
                .options(mongodb::options::IndexOptions::builder().unique(true).build())
                .build(),
            None
        ).await {
            Ok(_) => info!("{}: 余额索引创建成功", symbol),
            Err(e) => error!("{}: 余额索引创建失败: {}", symbol, e)
        }
    }
    
    // 同步状态索引
    match conn.sync_status_col.create_index(
        mongodb::IndexModel::builder()
            .keys(mongodb::bson::doc! { "status_type": 1, "token": 1 })
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

