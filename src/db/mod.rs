use std::error::Error;
use mongodb::{Client, options::ClientOptions, Collection, Database};
use mongodb::bson::Document;
use log::{info, error};

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
}

/// 初始化MongoDB连接
pub async fn init_db(mongodb_url: &str, database_name: &str) -> Result<DbConnection, Box<dyn Error>> {
    let client_options = match ClientOptions::parse(mongodb_url).await {
        Ok(options) => options,
        Err(e) => {
            error!("MongoDB连接字符串解析失败: {}", e);
            return Err(Box::new(e));
        }
    };
    
    let mongo_client = match Client::with_options(client_options) {
        Ok(client) => client,
        Err(e) => {
            error!("无法连接到MongoDB: {}", e);
            return Err(Box::new(e));
        }
    };
    
    info!("已连接到MongoDB");
    
    let db = mongo_client.database(database_name);
    let accounts_col: Collection<Document> = db.collection("accounts");
    let tx_col: Collection<Document> = db.collection("transactions");
    let balances_col: Collection<Document> = db.collection("balances");
    let total_supply_col: Collection<Document> = db.collection("total_supply");
    let sync_status_col: Collection<Document> = db.collection("sync_status");
    
    Ok(DbConnection {
        db,
        tx_col,
        accounts_col,
        balances_col,
        total_supply_col,
        sync_status_col,
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

