/**
 * 文件描述: API服务器实现，提供区块链数据查询RESTful接口
 * 功能概述:
 * - 提供代币余额查询API
 * - 提供交易历史查询API
 * - 提供账户信息查询API
 * - 提供数据统计API
 * - 支持多代币并发查询
 * 
 * 主要组件:
 * - transaction_to_bson函数 (第18-76行): 将交易对象转换为BSON格式
 * - ApiServer结构体 (第82-91行): API服务器主类
 * - QueryParams结构体 (第97-105行): 查询参数定义
 * - ApiResponse结构体 (第111-156行): 统一API响应格式
 * - ApiServer实现 (第158-360行): API服务器方法实现，包括:
 *   - new: 创建新实例
 *   - start: 启动API服务器
 *   - build_routes: 构建API路由
 * - 各API处理函数 (第426-951行): 实现不同API端点的具体业务逻辑
 */

use std::sync::Arc;
use warp::{Filter, Rejection, Reply};
use warp::filters::BoxedFilter;
use mongodb::bson::{doc, Document};
use serde::{Serialize, Deserialize};
use log::{info, error, debug};
use futures::stream::StreamExt;
use crate::db::DbConnection;
use crate::api;
use crate::models::Transaction;
use crate::error::{ApiError, handle_rejection, map_db_error};

/// 辅助函数：将Transaction对象转换为BSON Document
/// 
/// # 参数
/// * `tx` - 要转换的交易对象
/// * `token_symbol` - 代币符号
/// * `token_name` - 代币名称
/// 
/// # 返回
/// 转换后的BSON文档
fn transaction_to_bson(tx: &Transaction, token_symbol: &str, token_name: &str) -> Document {
    let mut doc = Document::new();
    
    // 基本字段
    if let Some(index) = tx.index {
        doc.insert("index", mongodb::bson::Bson::Int64(index as i64));
    } else {
        doc.insert("index", mongodb::bson::Bson::Int64(0));
    }
    doc.insert("timestamp", mongodb::bson::Bson::Int64(tx.timestamp as i64));
    doc.insert("kind", &tx.kind);
    doc.insert("token", token_symbol);
    doc.insert("token_name", token_name);
    
    // 添加ISO格式的日期时间，方便前端显示
    // 时间戳是秒级，需要转换为毫秒级
    let datetime = chrono::DateTime::from_timestamp(tx.timestamp as i64, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();
    doc.insert("datetime", datetime);
    
    // 根据交易类型添加特定字段
    if tx.kind == "transfer" {
        if let Some(transfer) = &tx.transfer {
            doc.insert("from", transfer.from.to_string());
            doc.insert("to", transfer.to.to_string());
            doc.insert("amount", transfer.amount.to_string());
            
            if let Some(fee) = &transfer.fee {
                doc.insert("fee", fee.to_string());
            } else {
                doc.insert("fee", "0");
            }
            
            if let Some(memo) = &transfer.memo {
                let hex_memo = memo.iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();
                doc.insert("memo", hex_memo);
                
                // 尝试将十六进制转换为UTF-8文本
                if let Ok(text) = String::from_utf8(memo.clone()) {
                    if !text.trim().is_empty() && text.chars().all(|c| !c.is_control() || c == '\n' || c == '\t') {
                        doc.insert("memo_text", text);
                    }
                }
            }
        }
    } else if tx.kind == "mint" {
        if let Some(mint) = &tx.mint {
            doc.insert("to", mint.to.to_string());
            doc.insert("amount", mint.amount.to_string());
        }
    } else if tx.kind == "burn" {
        if let Some(burn) = &tx.burn {
            doc.insert("from", burn.from.to_string());
            doc.insert("amount", burn.amount.to_string());
        }
    }
    
    doc
}

/// API服务器结构体
/// 
/// 提供了REST API接口用于查询账户余额、交易历史等信息。
/// 支持同时处理多种代币，通过token查询参数可以指定要查询的代币。
pub struct ApiServer {
    /// 数据库连接
    db_conn: Arc<DbConnection>,
    /// 支持的代币配置列表
    tokens: Vec<crate::models::TokenConfig>,
}

/// API查询参数
/// 
/// 支持分页和代币选择的查询参数
#[derive(Debug, Deserialize, Clone)]
pub struct QueryParams {
    /// 返回结果的最大条目数（可选，默认值根据不同API而异）
    pub limit: Option<i64>,
    /// 跳过的条目数，用于分页（可选，默认为0）
    pub skip: Option<i64>,
    /// 要查询的代币符号（可选，默认使用配置的第一个代币）
    pub token: Option<String>,
}

/// 通用API响应结构
/// 
/// 用于统一API返回格式，包含状态码、数据和错误信息
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    /// HTTP状态码
    pub code: u16,
    /// 响应数据，成功时包含实际结果
    pub data: Option<T>,
    /// 错误信息，失败时包含错误详情
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    /// 创建一个成功的响应，包含数据
    pub fn success(data: T) -> Self {
        Self {
            code: 200,
            data: Some(data),
            error: None,
        }
    }

    /// 创建一个错误响应，包含错误信息
    pub fn error(msg: &str) -> Self {
        Self {
            code: 400,
            data: None,
            error: Some(msg.to_string()),
        }
    }
    
    /// 创建一个自定义状态码的错误响应
    pub fn error_with_code(code: u16, msg: &str) -> Self {
        Self {
            code,
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

impl ApiServer {
    /// 创建新的API服务器实例
    /// 
    /// # 参数
    /// * `db_conn` - 数据库连接实例
    /// * `tokens` - 支持的代币配置列表
    /// 
    /// # 返回
    /// 返回一个新的ApiServer实例
    pub fn new(db_conn: DbConnection, tokens: Vec<crate::models::TokenConfig>) -> Self {
        Self {
            db_conn: Arc::new(db_conn),
            tokens,
        }
    }

    /// 启动API服务器
    /// 
    /// # 参数
    /// * `port` - 监听的端口号
    /// 
    /// # 返回
    /// 服务器正常运行将不会返回，如果出现错误则返回错误信息
    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        info!("启动API服务器，端口: {}", port);

        // 构建API路由
        let api_routes = self.build_routes();

        // 添加CORS支持
        let cors = warp::cors()
            .allow_any_origin()
            .allow_methods(vec!["GET", "POST", "OPTIONS"])
            .allow_headers(vec!["Content-Type", "Authorization", "Accept"]);

        // 整合所有路由
        let routes = api_routes
            .with(cors)
            .with(warp::log("api"))
            .recover(handle_rejection);  // 添加统一错误处理

        // 启动服务器
        warp::serve(routes)
            .run(([0, 0, 0, 0], port))
            .await;

        Ok(())
    }

    /// 构建API路由
    pub fn build_routes(&self) -> BoxedFilter<(impl Reply,)> {
        let db_conn = self.db_conn.clone();
        let tokens = self.tokens.clone();

        // 获取已支持的代币列表
        let supported_tokens = warp::path!("api" / "tokens")
            .and(warp::get())
            .map(move || {
                let token_list: Vec<_> = tokens.iter().map(|t| {
                    doc! {
                        "symbol": &t.symbol,
                        "name": &t.name,
                        "decimals": (t.decimals.unwrap_or(8) as i32),
                        "canister_id": t.canister_id.to_string(),
                    }
                }).collect();
                let response = ApiResponse::success(token_list);
                warp::reply::json(&response)
            });

        // 获取账户余额
        let tokens_for_balance = self.tokens.clone();
        let balance = warp::path!("api" / "balance" / String)
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_balance.clone()))
            .and_then(|account, params, db, tokens| async move {
                handle_get_balance(account, params, db, tokens).await
            });

        // 获取账户交易历史
        let tokens_for_transactions = self.tokens.clone();
        let transactions = warp::path!("api" / "transactions" / String)
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_transactions.clone()))
            .and_then(|account, params, db, tokens| async move {
                handle_get_account_transactions(account, params, db, tokens).await
            });

        // 获取特定交易详情
        let tokens_for_transaction = self.tokens.clone();
        let transaction = warp::path!("api" / "transaction" / u64)
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_transaction.clone()))
            .and_then(|index, params, db, tokens| async move {
                handle_get_transaction(index, params, db, tokens).await
            });

        // 获取最新交易
        let tokens_for_latest = self.tokens.clone();
        let latest_transactions = warp::path!("api" / "latest_transactions")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_latest.clone()))
            .and_then(|params, db, tokens| async move {
                handle_get_transaction_count(params, db, tokens).await
            });

        // 获取交易总数
        let tokens_for_count = self.tokens.clone();
        let tx_count = warp::path!("api" / "tx_count")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_count.clone()))
            .and_then(|params, db, tokens| async move {
                handle_get_transaction_count(params, db, tokens).await
            });

        // 获取账户总数
        let tokens_for_accounts = self.tokens.clone();
        let account_count = warp::path!("api" / "account_count")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_accounts.clone()))
            .and_then(|params, db, tokens| async move {
                handle_get_account_count(params, db, tokens).await
            });

        // 获取代币总供应量
        let tokens_for_supply = self.tokens.clone();
        let total_supply = warp::path!("api" / "total_supply")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_supply.clone()))
            .and_then(|params, db, tokens| async move {
                handle_get_total_supply(params, db, tokens).await
            });

        // 获取账户列表
        let tokens_for_account_list = self.tokens.clone();
        let accounts = warp::path!("api" / "accounts")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_account_list.clone()))
            .and_then(|params, db, tokens| async move {
                handle_get_accounts(params, db, tokens).await
            });

        // 获取活跃账户
        let tokens_for_active = self.tokens.clone();
        let active_accounts = warp::path!("api" / "active_accounts")
            .and(warp::get())
            .and(warp::query::<QueryParams>())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_active.clone()))
            .and_then(|params, db, tokens| async move {
                handle_get_active_accounts(params, db, tokens).await
            });

        // 高级搜索
        let tokens_for_search = self.tokens.clone();
        let search = warp::path!("api" / "search")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_db(db_conn.clone()))
            .and(warp::any().map(move || tokens_for_search.clone()))
            .and_then(|query, db, tokens| async move {
                handle_search_transactions(query, db, tokens).await
            });

        // 合并所有路由
        supported_tokens
            .or(balance)
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

/// 辅助函数：将数据库连接注入到处理函数
/// 
/// 该函数用于在Warp过滤器链中注入数据库连接
fn with_db(db_conn: Arc<DbConnection>) -> impl Filter<Extract = (Arc<DbConnection>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || db_conn.clone())
}

/// 辅助函数：查找指定符号的代币或使用默认代币
/// 
/// # 参数
/// * `tokens` - 可用的代币配置列表
/// * `token_symbol` - 要查找的代币符号，None表示使用默认（第一个）代币
/// 
/// # 返回
/// 成功找到代币配置或返回错误
fn find_token<'a>(
    tokens: &'a [crate::models::TokenConfig], 
    token_symbol: Option<&str>
) -> Result<&'a crate::models::TokenConfig, Rejection> {
    if tokens.is_empty() {
        return Err(warp::reject::custom(
            ApiError::TokenError("系统未配置任何代币".to_string())
        ));
    }
    
    match token_symbol {
        Some(symbol) => {
            let token = tokens.iter().find(|t| t.symbol == symbol);
            match token {
                Some(t) => Ok(t),
                None => Err(warp::reject::custom(
                    ApiError::TokenError(format!("未找到指定的代币: {}", symbol))
                ))
            }
        },
        None => Ok(&tokens[0]) // 使用第一个代币作为默认值
    }
}

// 辅助函数：将代币列表注入到处理函数
#[allow(dead_code)]
fn with_tokens(tokens: Vec<crate::models::TokenConfig>) -> impl Filter<Extract = (Vec<crate::models::TokenConfig>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || tokens.clone())
}

/// 处理函数：获取账户余额
///
/// # 参数
/// * `account` - 要查询余额的账户ID
/// * `params` - 查询参数，包括可选的token
/// * `db_conn` - 数据库连接
/// * `tokens` - 代币配置列表
///
/// # 返回
/// 成功时返回账户余额，失败时返回错误信息
async fn handle_get_balance(
    account: String,
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API请求: 获取账户余额 - account: {}, token: {:?}", account, params.token);
    
    if account.trim().is_empty() {
        return Err(warp::reject::custom(
            ApiError::InvalidQuery("账户ID不能为空".to_string())
        ));
    }
    
    // 获取查询参数中的token或者默认第一个代币
    let token = find_token(&tokens, params.token.as_deref())?;
    debug!("使用代币: {}", token.symbol);
    
    // 从数据库中获取该代币的集合
    let collections = db_conn.collections.get(&token.symbol)
        .ok_or_else(|| warp::reject::custom(
            ApiError::TokenError(format!("未找到代币 {} 的数据库集合", token.symbol))
        ))?;
    
    match api::get_account_balance(&collections.balances_col, &account).await {
        Ok(balance) => {
            let response = ApiResponse::success(doc! {
                "account": account.clone(),
                "balance": balance.clone(),
                "token": token.symbol.clone(),
                "token_name": token.name.clone(),
                "decimals": token.decimals.unwrap_or(8) as i32,
            });
            info!("API响应成功: 获取账户余额 - account: {}, balance: {}, token: {}", 
                  account, balance, token.symbol);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            error!("API响应错误: 获取账户余额 - account: {}, error: {}", account, e);
            Err(warp::reject::custom(map_db_error(e)))
        }
    }
}

// 处理函数：获取账户交易历史
async fn handle_get_account_transactions(
    account: String,
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API响应: 获取账户交易历史 - account: {}, limit: {:?}, skip: {:?}", 
           account, params.limit, params.skip);
    
    // 获取查询参数中的token或者默认第一个代币
    let token_symbol = params.token.as_deref();
    let token = match token_symbol {
        Some(symbol) => tokens.iter().find(|t| t.symbol == symbol),
        None => tokens.first()
    };
    
    // 如果找不到代币，返回错误
    let token = match token {
        Some(t) => t,
        None => {
            let msg = match token_symbol {
                Some(s) => format!("未找到指定的代币: {}", s),
                None => "系统未配置任何代币".to_string()
            };
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Vec<String>>::error(&msg)));
        }
    };
    
    // 从数据库中获取该代币的集合
    // 从数据库中获取该代币的集合
    let collections = db_conn.collections.get(&token.symbol)
        .ok_or_else(|| warp::reject::custom(
            ApiError::TokenError(format!("未找到代币 {} 的数据库集合", token.symbol))
        ))?;
    
    match api::get_account_transactions(
        &collections.accounts_col,
        &collections.tx_col,
        &account,
        params.limit,
        params.skip,
    ).await {
        Ok(transactions) => {
            // 将交易数据转换为可序列化的格式
            let tx_docs = transactions.iter()
                .map(|tx| transaction_to_bson(tx, &token.symbol, &token.name))
                .collect::<Vec<_>>();
            
            let meta = doc! {
                "total": tx_docs.len() as i32,
                "account": account.clone(),
                "token": token.symbol.clone(),
                "limit": params.limit.unwrap_or(50),
                "skip": params.skip.unwrap_or(0),
            };
            
            let response_data = doc! {
                "transactions": tx_docs,
                "meta": meta
            };
            
            let response = ApiResponse::success(response_data);
            info!("API响应成功: 获取账户交易历史 - account: {}, count: {}, token: {}", 
                 account, transactions.len(), token.symbol);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            error!("API响应错误: 获取账户交易历史 - account: {}, error: {}", account, e);
            Err(warp::reject::custom(map_db_error(e)))
        }
    }
}

/// 处理函数：获取最新交易列表
///
/// # 参数
/// * `params` - 查询参数，包括可选的token和limit
/// * `db_conn` - 数据库连接
/// * `tokens` - 代币配置列表
///
/// # 返回
/// 成功时返回最新交易列表，失败时返回错误信息
async fn handle_get_latest_transactions(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    // 查找对应的代币配置
    let token = find_token(&tokens, params.token.as_deref())?;
    
    // 获取交易集合
    let tx_col = db_conn.get_transactions_collection(&token.symbol);
    
    // 设置分页参数
    let limit = params.limit.unwrap_or(20).min(100); // 最多返回100条记录
    
    // 构建查询过滤器和选项
    let filter = doc! {};
    let options = mongodb::options::FindOptions::builder()
        .sort(doc! { "index": -1 }) // 按索引降序排序
        .limit(limit)
        .build();
    
    // 执行查询
    let mut cursor = tx_col.find(filter, options).await
        .map_err(|e| warp::reject::custom(
            ApiError::Database(format!("查询交易失败: {}", e))
        ))?;
    
    // 收集结果
    let mut transactions = Vec::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => transactions.push(doc),
            Err(e) => {
                error!("解析交易文档失败: {}", e);
                return Err(warp::reject::custom(
                    ApiError::Database(format!("解析交易文档失败: {}", e))
                ));
            }
        }
    }
    
    // 构建响应
    let response = ApiResponse::success(transactions);
    Ok(warp::reply::json(&response))
}

/// 处理函数：获取特定交易详情
///
/// # 参数
/// * `index` - 交易索引
/// * `params` - 查询参数，包括可选的token
/// * `db_conn` - 数据库连接
/// * `tokens` - 代币配置列表
///
/// # 返回
/// 成功时返回交易详情，失败时返回错误信息
async fn handle_get_transaction(
    index: u64,
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API请求: 获取交易详情 - index: {}, token: {:?}", index, params.token);
    
    // 获取查询参数中的token或者默认第一个代币
    let token = find_token(&tokens, params.token.as_deref())?;
    debug!("使用代币: {}", token.symbol);
    
    // 从数据库中获取该代币的集合
    let collections = db_conn.collections.get(&token.symbol)
        .ok_or_else(|| warp::reject::custom(
            ApiError::TokenError(format!("未找到代币 {} 的数据库集合", token.symbol))
        ))?;
    
    match api::get_transaction_by_index(&collections.tx_col, index).await {
        Ok(transaction) => {
            // 检查交易是否存在
            match transaction {
                Some(tx) => {
                    // 将Transaction对象转换为可序列化的文档
                    let tx_doc = transaction_to_bson(&tx, &token.symbol, &token.name);
                    
                    let response = ApiResponse::success(tx_doc);
                    info!("API响应成功: 获取交易详情 - index: {}, token: {}", index, token.symbol);
                    Ok(warp::reply::json(&response))
                },
                None => {
                    let msg = format!("未找到指定的交易: {} (token: {})", index, token.symbol);
                    error!("API错误: {}", msg);
                    Err(warp::reject::custom(ApiError::NotFound(msg)))
                }
            }
        },
        Err(e) => {
            error!("API响应错误: 获取交易详情 - index: {}, error: {}", index, e);
            Err(warp::reject::custom(map_db_error(e)))
        }
    }
}

/// 处理函数：获取交易总数
///
/// # 参数
/// * `params` - 查询参数，包括可选的token
/// * `db_conn` - 数据库连接
/// * `tokens` - 代币配置列表
///
/// # 返回
/// 成功时返回交易总数，失败时返回错误信息
async fn handle_get_transaction_count(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API请求: 获取交易总数 - token: {:?}", params.token);
    
    // 获取查询参数中的token或者默认第一个代币
    let token = find_token(&tokens, params.token.as_deref())?;
    debug!("使用代币: {}", token.symbol);
    
    // 从数据库中获取该代币的集合
    let collections = db_conn.collections.get(&token.symbol)
        .ok_or_else(|| warp::reject::custom(
            ApiError::TokenError(format!("未找到代币 {} 的数据库集合", token.symbol))
        ))?;
    
    match api::get_transaction_count(&collections.tx_col).await {
        Ok(count) => {
            let response_data = doc! {
                "count": count as i64,
                "token": token.symbol.clone(),
                "token_name": token.name.clone(),
            };
            
            let response = ApiResponse::success(response_data);
            info!("API响应成功: 获取交易总数 - count: {}, token: {}", count, token.symbol);
            Ok(warp::reply::json(&response))
        },
        Err(e) => {
            error!("API响应错误: 获取交易总数 - error: {}", e);
            Err(warp::reject::custom(map_db_error(e)))
        }
    }
}

// 处理函数：获取账户总数
async fn handle_get_account_count(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API响应: 获取账户总数");
    
    // 获取查询参数中的token或者默认第一个代币
    let token_symbol = params.token.as_deref();
    let token = match token_symbol {
        Some(symbol) => tokens.iter().find(|t| t.symbol == symbol),
        None => tokens.first()
    };
    
    // 如果找不到代币，返回错误
    let token = match token {
        Some(t) => t,
        None => {
            let msg = match token_symbol {
                Some(s) => format!("未找到指定的代币: {}", s),
                None => "系统未配置任何代币".to_string()
            };
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<u64>::error(&msg)));
        }
    };
    
    // 从数据库中获取该代币的集合
    let collections = match db_conn.collections.get(&token.symbol) {
        Some(cols) => cols,
        None => {
            let msg = format!("未找到代币 {} 的数据库集合", token.symbol);
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<u64>::error(&msg)));
        }
    };
    
    match api::get_account_count(&collections.accounts_col).await {
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
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API响应: 获取代币总供应量");
    
    // 获取查询参数中的token或者默认第一个代币
    let token_symbol = params.token.as_deref();
    let token = match token_symbol {
        Some(symbol) => tokens.iter().find(|t| t.symbol == symbol),
        None => tokens.first()
    };
    
    // 如果找不到代币，返回错误
    let token = match token {
        Some(t) => t,
        None => {
            let msg = match token_symbol {
                Some(s) => format!("未找到指定的代币: {}", s),
                None => "系统未配置任何代币".to_string()
            };
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<String>::error(&msg)));
        }
    };
    
    // 从数据库中获取该代币的集合
    let collections = match db_conn.collections.get(&token.symbol) {
        Some(cols) => cols,
        None => {
            let msg = format!("未找到代币 {} 的数据库集合", token.symbol);
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<String>::error(&msg)));
        }
    };
    
    match api::get_total_supply(&collections.total_supply_col).await {
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
async fn handle_get_accounts(
    params: QueryParams,
    db_conn: Arc<DbConnection>,
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API响应: 获取账户列表 - limit: {:?}, skip: {:?}", params.limit, params.skip);
    
    // 获取查询参数中的token或者默认第一个代币
    let token_symbol = params.token.as_deref();
    let token = match token_symbol {
        Some(symbol) => tokens.iter().find(|t| t.symbol == symbol),
        None => tokens.first()
    };
    
    // 如果找不到代币，返回错误
    let token = match token {
        Some(t) => t,
        None => {
            let msg = match token_symbol {
                Some(s) => format!("未找到指定的代币: {}", s),
                None => "系统未配置任何代币".to_string()
            };
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Vec<String>>::error(&msg)));
        }
    };
    
    // 从数据库中获取该代币的集合
    let collections = match db_conn.collections.get(&token.symbol) {
        Some(cols) => cols,
        None => {
            let msg = format!("未找到代币 {} 的数据库集合", token.symbol);
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Vec<String>>::error(&msg)));
        }
    };
    
    match api::get_all_accounts(&collections.accounts_col, params.limit, params.skip).await {
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
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    info!("API响应: 获取活跃账户 - limit: {:?}", params.limit);
    
    // 获取查询参数中的token或者默认第一个代币
    let token_symbol = params.token.as_deref();
    let token = match token_symbol {
        Some(symbol) => tokens.iter().find(|t| t.symbol == symbol),
        None => tokens.first()
    };
    
    // 如果找不到代币，返回错误
    let token = match token {
        Some(t) => t,
        None => {
            let msg = match token_symbol {
                Some(s) => format!("未找到指定的代币: {}", s),
                None => "系统未配置任何代币".to_string()
            };
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Vec<String>>::error(&msg)));
        }
    };
    
    // 从数据库中获取该代币的集合
    let collections = match db_conn.collections.get(&token.symbol) {
        Some(cols) => cols,
        None => {
            let msg = format!("未找到代币 {} 的数据库集合", token.symbol);
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Vec<String>>::error(&msg)));
        }
    };
    
    match api::get_active_accounts(&collections.tx_col, params.limit).await {
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
    tokens: Vec<crate::models::TokenConfig>,
) -> Result<impl Reply, Rejection> {
    // 创建默认查询参数
    let params = QueryParams {
        limit: Some(50),
        skip: Some(0),
        token: None,
    };
    info!("API响应: 高级搜索交易 - 查询条件: {:?}", query);
    
    // 默认限制和偏移量
    let limit = params.limit.or_else(|| Some(50));
    let skip = params.skip.or_else(|| Some(0));
    
    // 获取查询参数中的token或者默认第一个代币
    let token_symbol = params.token.as_deref();
    let token = match token_symbol {
        Some(symbol) => tokens.iter().find(|t| t.symbol == symbol),
        None => tokens.first()
    };
    
    // 如果找不到代币，返回错误
    let token = match token {
        Some(t) => t,
        None => {
            let msg = match token_symbol {
                Some(s) => format!("未找到指定的代币: {}", s),
                None => "系统未配置任何代币".to_string()
            };
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Document>::error(&msg)));
        }
    };
    
    // 从数据库中获取该代币的集合
    let collections = match db_conn.collections.get(&token.symbol) {
        Some(cols) => cols,
        None => {
            let msg = format!("未找到代币 {} 的数据库集合", token.symbol);
            error!("API错误: {}", msg);
            return Ok(warp::reply::json(&ApiResponse::<Document>::error(&msg)));
        }
    };

    match api::search_transactions(&collections.tx_col, query.clone(), limit, skip).await {
        Ok(transactions) => {
            // 将Transaction对象转换为可序列化的文档
            let tx_docs: Vec<Document> = transactions.iter()
                .map(|tx| transaction_to_bson(tx, &token.symbol, &token.name))
                .collect();
            
            let response = ApiResponse::success(doc! {
                "query": query.clone(),
                "transactions": tx_docs,
                "count": (transactions.len() as i64),
            });
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