/**
 * 文件描述: 配置管理模块，负责加载和处理应用配置
 * 功能概述:
 * - 从配置文件加载应用设置
 * - 解析命令行参数
 * - 创建IC网络连接
 * - 获取代币信息
 * 
 * 主要组件:
 * - load_config函数 (第26-61行): 从config.toml文件加载应用配置
 * - parse_args函数 (第63-69行): 解析命令行参数(如--reset)
 * - get_token_decimals函数 (第71-122行): 从IC网络获取代币小数位数
 * - create_agent函数 (第124-138行): 创建IC网络连接代理
 * - parse_canister_id函数 (第140-149行): 解析Canister ID为Principal类型
 */

use std::error::Error;
use config as config_rs;
use ic_agent::export::Principal;
use candid::{Encode, Decode};
use ic_agent::Agent;
use log::{info, error, warn};
use crate::models::{Config as AppConfig, DEFAULT_DECIMALS};
use crate::utils::create_error;

/// 加载应用配置
pub async fn load_config() -> Result<AppConfig, Box<dyn Error>> {
    // 使用TOML配置文件
    let settings = match config_rs::Config::builder()
        .add_source(config_rs::File::with_name("config").required(false))
        .build() {
        Ok(config) => config,
        Err(e) => {
            return Err(create_error(&format!("配置文件错误: {}", e)));
        }
    };
    
    // 如果没有找到任何配置文件，返回错误
    if settings.get_string("mongodb_url").is_err() {
        return Err(create_error("未找到配置文件。请创建config.toml"));
    }
    
    let cfg: AppConfig = match settings.try_deserialize() {
        Ok(c) => c,
        Err(e) => {
            return Err(create_error(&format!("配置解析错误: {}", e)));
        }
    };
    
    // 验证代币配置
    if cfg.tokens.is_empty() {
        return Err(create_error("配置文件中没有发现代币配置，请至少配置一个代币"));
    }
    
    info!("已加载 {} 个代币的配置", cfg.tokens.len());
    for token in &cfg.tokens {
        info!("代币配置: {} ({}) canister_id: {}", token.name, token.symbol, token.canister_id);
    }

    Ok(cfg)
}

/// 解析命令行参数
pub async fn parse_args(args: &crate::models::AppArgs) -> Result<(), Box<dyn Error>> {
    if args.reset {
        info!("检测到重置参数 --reset");
    }
    Ok(())
}

/// 查询代币小数位数
pub async fn get_token_decimals(
    agent: &Agent,
    canister_id: &Principal,
    token_symbol: &str,
) -> Result<u8, Box<dyn Error>> {
    info!("{}: 查询代币小数位数...", token_symbol);
    
    // 调用icrc1_decimals方法
    let empty_args = ();
    let arg_bytes = match Encode!(&empty_args) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("{}: 编码参数失败: {}", token_symbol, e);
            return Err(create_error(&format!("{}: 参数编码失败: {}", token_symbol, e)));
        }
    };
    
    // 添加重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        match agent.query(canister_id, "icrc1_decimals")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
                match Decode!(&response, u8) {
                    Ok(decimals) => {
                        info!("{}: 代币小数位数: {}", token_symbol, decimals);
                        return Ok(decimals);
                    },
                    Err(e) => {
                        warn!("{}: 解析decimals响应失败: {}, 使用默认值{}", token_symbol, e, DEFAULT_DECIMALS);
                        return Ok(DEFAULT_DECIMALS);
                    }
                }
            },
            Err(e) => {
                retry_count += 1;
                let wait_time = std::time::Duration::from_secs(2 * retry_count);
                warn!("{}: 查询decimals失败 (尝试 {}/{}): {}, 等待 {:?} 后重试", 
                    token_symbol, retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    warn!("{}: 查询decimals达到最大重试次数，使用默认值{}", token_symbol, DEFAULT_DECIMALS);
    Ok(DEFAULT_DECIMALS)
}

/// 创建IC连接代理
pub fn create_agent(ic_url: &str) -> Result<Agent, Box<dyn Error>> {
    match Agent::builder()
        .with_url(ic_url)
        .build() {
        Ok(a) => {
            info!("IC网络连接创建成功: {}", ic_url);
            Ok(a)
        },
        Err(e) => {
            error!("IC网络连接创建失败: {} - 错误: {}", ic_url, e);
            Err(Box::new(e))
        }
    }
}

/// 解析Canister ID
pub fn parse_canister_id(canister_id_text: &str) -> Result<Principal, Box<dyn Error>> {
    match Principal::from_text(canister_id_text) {
        Ok(id) => Ok(id),
        Err(e) => {
            error!("无效的Canister ID格式: {}", e);
            Err(create_error(&format!("无效的Canister ID: {}", e)))
        }
    }
}
