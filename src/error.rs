/**
 * 文件描述: 错误处理模块，定义应用错误类型和处理函数
 * 功能概述:
 * - 定义API服务的自定义错误类型
 * - 处理HTTP请求拒绝和错误响应
 * - 提供错误映射函数
 * 
 * 主要组件:
 * - ApiError枚举 (第6-17行): 定义不同类型的API错误
 * - fmt::Display实现 (第19-30行): 错误消息格式化
 * - handle_rejection函数 (第37-62行): 将错误转换为HTTP响应
 * - map_error函数 (第65-67行): 将标准错误转换为API错误
 * - map_db_error函数 (第70-74行): 将数据库错误转换为API错误
 */

use std::fmt;
use warp::reject::Reject;

/// 自定义错误类型，用于API服务
#[derive(Debug)]
pub enum ApiError {
    /// 数据库操作错误
    Database(String),
    /// 无效的查询参数
    InvalidQuery(String),
    /// 资源未找到
    NotFound(String),
    /// 代币相关错误
    TokenError(String),
    /// 内部服务器错误
    Internal(String),
    /// 序列化/反序列化错误
    SerializationError(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ApiError::Database(msg) => write!(f, "数据库错误: {}", msg),
            ApiError::InvalidQuery(msg) => write!(f, "无效的查询参数: {}", msg),
            ApiError::NotFound(msg) => write!(f, "资源未找到: {}", msg),
            ApiError::TokenError(msg) => write!(f, "代币错误: {}", msg),
            ApiError::Internal(msg) => write!(f, "内部服务器错误: {}", msg),
            ApiError::SerializationError(msg) => write!(f, "序列化错误: {}", msg),
        }
    }
}

impl std::error::Error for ApiError {}

impl Reject for ApiError {}

/// 创建自定义拒绝响应
pub async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, std::convert::Infallible> {
    use warp::http::StatusCode;
    use warp::reply::json;
    use crate::api_server::ApiResponse;

    log::error!("请求处理异常: {:?}", err);

    let (code, message) = if err.is_not_found() {
        (StatusCode::NOT_FOUND, "未找到请求的资源".to_string())
    } else if let Some(e) = err.find::<ApiError>() {
        match e {
            ApiError::Database(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::InvalidQuery(_) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            ApiError::TokenError(_) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::SerializationError(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    } else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
        (StatusCode::BAD_REQUEST, format!("无效的请求数据: {}", e))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, "内部服务器错误".to_string())
    };

    let json_response = ApiResponse::<()>::error(&message);
    Ok(warp::reply::with_status(json(&json_response), code))
}

/// 从标准错误转换为API错误
pub fn map_error<E: std::error::Error>(err: E) -> ApiError {
    ApiError::Internal(err.to_string())
}

/// 从数据库错误转换为API错误
pub fn map_db_error<E>(err: E) -> ApiError 
where E: std::fmt::Display
{
    ApiError::Database(err.to_string())
}
