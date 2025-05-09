use std::error::Error;
use std::sync::Arc;
use warp::{Filter, Rejection, Reply};
use warp::filters::BoxedFilter;
use mongodb::bson::{doc, Document};
use serde::{Serialize, Deserialize};
use log::{info, error};
use crate::db::DbConnection;
use crate::api;

/// API服务器结构体
pub struct ApiServer {
    db_conn: Arc<DbConnection>,
    #[allow(dead_code)]
    token_decimals: u8,
}

/// API查询参数
#[derive(Debug, Deserialize)]
struct QueryParams {
    limit: Option<i64>,
    skip: Option<i64>,
}

/// 通用响应结构
#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    code: u16,
    data: Option<T>,
    error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            code: 200,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            code: 400,
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

impl ApiServer {
    /// 创建新的API服务器实例
    pub fn new(db_conn: DbConnection, token_decimals: u8) -> Self {
        Self {
            db_conn: Arc::new(db_conn),
            token_decimals,
        }
    }

    /// 启动API服务器
    pub async fn start(&self, port: u16) -> Result<(), Box<dyn Error>> {
        info!("启动API服务器，端口: {}", port);

        // 构建API路由
        let api_routes = self.build_routes();

        // 添加CORS支持
        let cors = warp::cors()
            .allow_any_origin()
            .allow_methods(vec!["GET", "POST"])
            .allow_headers(vec!["Content-Type"]);

        // 整合所有路由
        let routes = api_routes
            .with(cors)
            .with(warp::log("api"));

        // 启动服务器
        warp::serve(routes)
            .run(([0, 0, 0, 0], port))
            .await;

        Ok(())
    }

    /// 构建API路由
    fn build_routes(&self) -> BoxedFilter<(impl Reply,)> {
        let db_conn = self.db_conn.clone();

        // 获取账户余额
        let balance = warp::path!("api" / "balance" / String)
            .and(warp::get())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_balance);

        // 获取账户交易历史
        let transactions = warp::path!("api" / "transactions" / String)
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_account_transactions);

        // 获取特定交易详情
        let transaction = warp::path!("api" / "transaction" / u64)
            .and(warp::get())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_transaction);

        // 获取最新交易
        let latest_transactions = warp::path!("api" / "latest_transactions")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_latest_transactions);

        // 获取交易总数
        let tx_count = warp::path!("api" / "tx_count")
            .and(warp::get())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_transaction_count);

        // 获取账户总数
        let account_count = warp::path!("api" / "account_count")
            .and(warp::get())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_account_count);

        // 获取代币总供应量
        let total_supply = warp::path!("api" / "total_supply")
            .and(warp::get())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_total_supply);

        // 获取账户列表
        let accounts = warp::path!("api" / "accounts")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_accounts);

        // 获取活跃账户
        let active_accounts = warp::path!("api" / "active_accounts")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and_then(handle_get_active_accounts);

        // 高级搜索
        let search = warp::path!("api" / "search")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_db(db_conn.clone()))
            .and_then(handle_search_transactions);

        // 合并所有路由
        balance
            .or(transactions)
            .or(transaction)
            .or(latest_transactions)
            .or(tx_count)
            .or(account_count)
            .or(total_supply)
            .or(accounts)
            .or(active_accounts)
            .or(search)
            .boxed()
    }
}

// 辅助函数：将数据库连接注入到处理函数
fn with_db(db_conn: Arc<DbConnection>) -> impl Filter<Extract = (Arc<DbConnection>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || db_conn.clone())
}

// 处理函数：获取账户余额
async fn handle_get_balance(
    account: String,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取账户余额 - account: {}", account);
    
    match api::get_account_balance(&db_conn.balances_col, &account).await {
        Ok(balance) => {
            let response = ApiResponse::success(balance.clone());
            info!("API响应成功: 获取账户余额 - account: {}, balance: {}", account, balance);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<String>::error(&e.to_string());
            error!("API响应错误: 获取账户余额 - account: {}, error: {}", account, e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取账户交易历史
async fn handle_get_account_transactions(
    account: String,
    params: QueryParams,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取账户交易历史 - account: {}, limit: {:?}, skip: {:?}", 
           account, params.limit, params.skip);
    
    match api::get_account_transactions(
        &db_conn.accounts_col,
        &db_conn.tx_col,
        &account,
        params.limit,
        params.skip,
    ).await {
        Ok(transactions) => {
            let response = ApiResponse::success(transactions.clone());
            info!("API响应成功: 获取账户交易历史 - account: {}, transactions_count: {}", 
                  account, transactions.len());
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<Vec<String>>::error(&e.to_string());
            error!("API响应错误: 获取账户交易历史 - account: {}, error: {}", account, e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取特定交易详情
async fn handle_get_transaction(
    index: u64,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取交易详情 - index: {}", index);
    
    match api::get_transaction_by_index(&db_conn.tx_col, index).await {
        Ok(transaction) => {
            let response = ApiResponse::success(transaction.clone());
            info!("API响应成功: 获取交易详情 - index: {}", index);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<Option<String>>::error(&e.to_string());
            error!("API响应错误: 获取交易详情 - index: {}, error: {}", index, e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取最新交易
async fn handle_get_latest_transactions(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取最新交易 - limit: {:?}, skip: {:?}", params.limit, params.skip);
    
    match api::get_latest_transactions(&db_conn.tx_col, params.limit).await {
        Ok(transactions) => {
            let response = ApiResponse::success(transactions.clone());
            info!("API响应成功: 获取最新交易 - 返回交易数: {}", transactions.len());
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<Vec<String>>::error(&e.to_string());
            error!("API响应错误: 获取最新交易 - error: {}", e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取交易总数
async fn handle_get_transaction_count(
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取交易总数");
    
    match api::get_transaction_count(&db_conn.tx_col).await {
        Ok(count) => {
            let response = ApiResponse::success(count);
            info!("API响应成功: 获取交易总数 - count: {}", count);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<u64>::error(&e.to_string());
            error!("API响应错误: 获取交易总数 - error: {}", e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取账户总数
async fn handle_get_account_count(
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取账户总数");
    
    match api::get_account_count(&db_conn.accounts_col).await {
        Ok(count) => {
            let response = ApiResponse::success(count);
            info!("API响应成功: 获取账户总数 - count: {}", count);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<u64>::error(&e.to_string());
            error!("API响应错误: 获取账户总数 - error: {}", e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取代币总供应量
async fn handle_get_total_supply(
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取代币总供应量");
    
    match api::get_total_supply(&db_conn.total_supply_col).await {
        Ok(supply) => {
            let response = ApiResponse::success(supply.clone());
            info!("API响应成功: 获取代币总供应量 - supply: {}", supply);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<String>::error(&e.to_string());
            error!("API响应错误: 获取代币总供应量 - error: {}", e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取账户列表
async fn handle_get_accounts(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取账户列表 - limit: {:?}, skip: {:?}", params.limit, params.skip);
    
    match api::get_all_accounts(&db_conn.accounts_col, params.limit, params.skip).await {
        Ok(accounts) => {
            let response = ApiResponse::success(accounts.clone());
            info!("API响应成功: 获取账户列表 - 返回账户数: {}", accounts.len());
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<Vec<String>>::error(&e.to_string());
            error!("API响应错误: 获取账户列表 - error: {}", e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：获取活跃账户
async fn handle_get_active_accounts(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 获取活跃账户 - limit: {:?}", params.limit);
    
    match api::get_active_accounts(&db_conn.tx_col, params.limit).await {
        Ok(accounts) => {
            let response = ApiResponse::success(accounts.clone());
            info!("API响应成功: 获取活跃账户 - 返回账户数: {}", accounts.len());
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<Vec<String>>::error(&e.to_string());
            error!("API响应错误: 获取活跃账户 - error: {}", e);
            Ok(warp::reply::json(&response))
        }
    }
}

// 处理函数：高级搜索交易
async fn handle_search_transactions(
    query: Document,
    db_conn: Arc<DbConnection>,
) -> Result<impl Reply, Rejection> {
    info!("接收API请求: 高级搜索交易 - 查询条件: {:?}", query);
    
    // 默认限制和偏移量
    let limit = Some(50);
    let skip = Some(0);

    match api::search_transactions(&db_conn.tx_col, query.clone(), limit, skip).await {
        Ok(transactions) => {
            let response = ApiResponse::success(transactions.clone());
            info!("API响应成功: 高级搜索交易 - 查询条件: {:?}, 返回交易数: {}", 
                  query, transactions.len());
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            let response = ApiResponse::<Vec<String>>::error(&e.to_string());
            error!("API响应错误: 高级搜索交易 - 查询条件: {:?}, error: {}", query, e);
            Ok(warp::reply::json(&response))
        }
    }
} 