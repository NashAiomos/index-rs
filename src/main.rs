#![allow(unused_variables)]
#![allow(improper_ctypes_definitions)]
#![allow(improper_ctypes)]
#![allow(non_camel_case_types)]
#![allow(type_alias_bounds)]
#![allow(dead_code)]

mod models;
mod utils;
mod config;
mod blockchain;
mod db;
mod sync;
mod api;
mod api_server;
mod error;

use std::error::Error;
use std::collections::HashMap;
use std::fs;
use tokio;
use tokio::time::Duration;
use log::{info, error, warn, debug, LevelFilter};
use crate::db::balances;
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Config as LogConfig, Root};
use log4rs::filter::threshold::ThresholdFilter;
use crate::config::{load_config, parse_args, parse_canister_id, create_agent, get_token_decimals};
use crate::db::{init_db, create_indexes};
use crate::sync::{sync_ledger_transactions, sync_archive_transactions};
use crate::sync::admin::reset_and_sync_all_transactions;
use crate::db::balances::calculate_incremental_balances;
use crate::db::sync_status::{get_sync_status, set_incremental_mode};
use crate::db::transactions::get_latest_transaction_index;
use chrono;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 读取配置文件（不使用日志记录）
    let cfg = match load_config().await {
        Ok(config) => config,
        Err(e) => {
            eprintln!("配置加载失败: {}", e);
            return Err(e);
        }
    };
    
    // 初始化日志系统
    if let Err(e) = setup_logger(&cfg) {
        eprintln!("警告: 无法设置日志系统: {}", e);
        // 继续执行，但日志会输出到标准错误
    }
    
    info!("=======================================");
    info!("==============  服务启动  ==============");
    info!("=======================================");
    
    info!("正在启动区块链索引服务...");
    
    // 设置全局错误捕获
    let result = run_application(cfg).await;
    
    // 处理顶层错误
    if let Err(e) = &result {
        error!("程序执行过程中发生错误: {}", e);
        let error_details = format!("{:?}", e);
        error!("详细错误信息: {}", error_details);
        
        if error_details.contains("mongodb") || error_details.contains("connection") {
            error!("可能是数据库连接问题，请检查MongoDB服务是否正常运行以及连接配置是否正确");
        } else if error_details.contains("canister") || error_details.contains("agent") || error_details.contains("ic") {
            error!("可能是IC网络连接问题，请检查网络连接以及canister ID配置是否正确");
        } else if error_details.contains("permission") || error_details.contains("access") {
            error!("可能是文件或资源访问权限问题，请检查程序运行权限");
        }
        
        error!("建议尝试以下恢复步骤:");
        error!("1. 检查配置文件中的参数设置");
        error!("2. 确认网络连接状态");
        error!("3. 验证数据库服务是否可用");
        error!("4. 使用 --reset 参数重新启动尝试完全同步");
    }
    
    result
}

/// 根据配置设置日志系统
fn setup_logger(cfg: &models::Config) -> Result<(), Box<dyn Error>> {
    // 获取日志配置
    let log_cfg = match &cfg.log {
        Some(log_config) => log_config,
        None => {
            // 没有日志配置，创建默认文件日志
            eprintln!("未找到日志配置，使用默认配置");
            // 确保日志目录存在
            let log_dir = std::path::Path::new("logs");
            if !log_dir.exists() {
                fs::create_dir_all(log_dir)?;
            }
            
            // 指定UTF-8编码
            let encoder = PatternEncoder::new("[{d(%Y-%m-%d %H:%M:%S)}] [{l}] - {m}{n}");
            
            let file = FileAppender::builder()
                .encoder(Box::new(encoder))
                .build("logs/index-rs.log")?;
                
            let config = LogConfig::builder()
                .appender(Appender::builder().build("file", Box::new(file)))
                .build(Root::builder().appender("file").build(LevelFilter::Info))?;
                
            log4rs::init_config(config)?;
            eprintln!("日志系统已初始化，使用默认配置");
            return Ok(());
        }
    };
    
    // 设置日志级别
    let log_level = match log_cfg.level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };
    
    // 设置控制台日志级别
    let console_level = match log_cfg.console_level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Error, // 设置为Error级别，减少控制台输出
    };
    
    // 创建编码器（指定UTF-8）
    let pattern = "[{d(%Y-%m-%d %H:%M:%S)}] [{l}] - {m}{n}";
    let encoder = PatternEncoder::new(pattern);
    
    // 创建控制台输出（后面如果关掉日志，就会打印在控制台）
    let stdout = ConsoleAppender::builder()
        .target(Target::Stdout)
        .encoder(Box::new(encoder.clone()))
        .build();
    
    // 确保日志目录存在
    if log_cfg.file_enabled {
        let log_dir = std::path::Path::new(&log_cfg.file).parent()
            .ok_or("无效的日志文件路径")?;
        
        if !log_dir.exists() {
            fs::create_dir_all(log_dir)?;
        }
    }
    
    // 构建日志配置
    let mut config_builder = LogConfig::builder();
    let mut root_builder = Root::builder();
    
    // 如果启用了文件日志，添加文件输出
    if log_cfg.file_enabled {
        let file = FileAppender::builder()
            .encoder(Box::new(encoder.clone()))
            .build(&log_cfg.file)?;
        
        config_builder = config_builder.appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(log_level)))
                .build("file", Box::new(file))
        );
        
        root_builder = root_builder.appender("file");
        
        // 仅当控制台级别低于ERROR时才添加控制台输出
        if console_level < LevelFilter::Error {
            config_builder = config_builder.appender(
                Appender::builder()
                    .filter(Box::new(ThresholdFilter::new(console_level)))
                    .build("stdout", Box::new(stdout))
            );
            
            root_builder = root_builder.appender("stdout");
        }
    } else {
        // 如果文件日志未启用，回退到控制台
        config_builder = config_builder.appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(log_level)))
                .build("stdout", Box::new(stdout))
        );
        
        root_builder = root_builder.appender("stdout");
    }
    
    // 应用日志配置
    let log_config = config_builder
        .build(root_builder.build(log_level))?;
    
    // 初始化日志系统
    log4rs::init_config(log_config)?;
    
    if log_cfg.file_enabled {
        eprintln!("日志系统已初始化，日志文件：{}", log_cfg.file);
    } else {
        eprintln!("日志系统已初始化，使用控制台输出");
    }
    
    Ok(())
}

// 将主要应用逻辑移到独立函数，便于错误处理
async fn run_application(cfg: models::Config) -> Result<(), Box<dyn Error>> {
    info!("启动索引服务...");
    
    // 获取命令行参数
    let args = models::AppArgs { reset: std::env::args().any(|arg| arg == "--reset") };
    let _ = parse_args(&args).await?;
    let reset_mode = args.reset;
    
    // 初始化 MongoDB
    let db_conn = init_db(&cfg.mongodb_url, &cfg.database, &cfg.tokens).await?;
    
    // 初始化IC Agent
    let agent = create_agent(&cfg.ic_url)?;

    // 获取并验证所有代币的canister ID和小数位数
    for token in &cfg.tokens {
        // 解析Canister ID
        info!("{}: 解析canister ID: {}", token.symbol, token.canister_id);
        let _canister_id = parse_canister_id(&token.canister_id)?;
        
        // 获取代币小数位数 (如果未在配置中指定)
        if token.decimals.is_none() {
            // 仅显示信息，不执行查询，查询将在实际同步时进行
            info!("{}: 配置中未指定代币小数位，将在同步时从canister查询", token.symbol);
        } else {
            info!("{}: 使用配置文件中指定的代币小数位: {}", token.symbol, token.decimals.unwrap());
        }
    }

    // 创建索引以提高查询性能
    create_indexes(&db_conn).await?;

    // 如果是重置模式，执行完整的数据库重置和重新同步
    if reset_mode && !cfg.tokens.is_empty() {
        info!("开始执行数据库重置和重新同步操作...");
        // 单例模式：只有一个代币时，直接重置和同步
        let first_token = &cfg.tokens[0];
        // 解析canister ID
        let canister_id = parse_canister_id(&first_token.canister_id)?;
        reset_and_sync_all_transactions(&agent, &canister_id, &db_conn, first_token).await?;
        info!("数据库重置和重新同步成功完成！");
        return Ok(());
    }
    
    // 根据重置模式确定同步策略
    if reset_mode {
        info!("重置模式已启用，将对所有代币进行全量同步");
        // 重置模式下，对所有代币进行全量同步
        for token in &cfg.tokens {
            info!("开始代币 {} 的数据库重置和重新同步操作...", token.symbol);
            
            // 获取该代币的集合
            let _collections = match db_conn.collections.get(&token.symbol) {
                Some(cols) => cols,
                None => {
                    error!("没有找到代币 {} 的集合", token.symbol);
                    continue;
                }
            };
            
            // 从canister_id获取 Principal
            let canister_id = match parse_canister_id(&token.canister_id) {
                Ok(id) => id,
                Err(e) => {
                    error!("{}: 解析canister ID失败: {}", token.symbol, e);
                    continue;
                }
            };
            
            // 调用reset_and_sync_all_transactions函数同步该代币的所有交易
            match reset_and_sync_all_transactions(&agent, &canister_id, &db_conn, &token).await {
                Ok(_) => {
                    info!("{}: 重置和同步运行成功", token.symbol);
                },
                Err(e) => {
                    error!("{}: 重置和同步失败: {}", token.symbol, e);
                }
            }
        }
        return Ok(());
    }
    
    // 判断每个代币是否需要初始同步
    let mut tokens_sync_status = HashMap::new();
    for token in &cfg.tokens {
        let sync_status = get_sync_status(&db_conn.sync_status_col, &token.symbol).await;
        let needs_initial_sync = match &sync_status {
            Ok(Some(status)) => {
                if status.sync_mode == "incremental" && status.last_synced_index > 0 {
                    info!("{}: 检测到有效的同步状态，上次同步索引：{}，上次同步时间：{}，将继续增量同步", 
                          token.symbol, status.last_synced_index, 
                          chrono::DateTime::from_timestamp(status.updated_at, 0)
                             .unwrap_or_else(|| chrono::Utc::now())
                             .format("%Y-%m-%d %H:%M:%S"));
                    false
                } else {
                    info!("{}: 同步状态无效或为全量模式，需要进行初始同步", token.symbol);
                    true
                }
            },
            _ => {
                info!("{}: 未找到同步状态记录，将进行初始同步", token.symbol);
                true
            }
        };
        tokens_sync_status.insert(token.symbol.clone(), (sync_status, needs_initial_sync));
    }
    
    // 为需要初始同步的代币执行全量同步流程
    for token in &cfg.tokens {
        let (sync_status, needs_initial_sync) = match tokens_sync_status.get(&token.symbol) {
            Some(status) => status,
            None => {
                error!("{}: 缺失同步状态信息", token.symbol);
                continue;
            }
        };
        
        // 获取该代币的集合
        let collections = match db_conn.collections.get(&token.symbol) {
            Some(cols) => cols,
            None => {
                error!("{}: 没有找到代币的集合", token.symbol);
                continue;
            }
        };
        
        // 解析Canister ID
        let canister_id = parse_canister_id(&token.canister_id)?;
        
        // 获取代币小数位数
        let token_decimals = match token.decimals {
            Some(decimals) => decimals,
            None => get_token_decimals(&agent, &canister_id, &token.symbol).await?
        };
        
        if *needs_initial_sync {
            // 正常模式：先同步所有交易，再统一计算余额
            info!("{}: 阶段1：同步所有交易数据...", token.symbol);
            
            // 先同步归档数据
            let _archives_result = sync_archive_transactions(
                &agent,
                &canister_id,
                &collections.tx_col,
                &collections.accounts_col,
                &collections.balances_col,
                &collections.total_supply_col,
                token_decimals,
                false // 不计算余额
            ).await?;
            
            // 同步主账本数据
            info!("{}: 开始同步ledger交易...", token.symbol);
            let ledger_txs = if let Ok(txs) = sync_ledger_transactions(
                &agent,
                &canister_id,
                &collections.tx_col,
                &collections.accounts_col,
                &collections.balances_col,
                &collections.total_supply_col,
                &token,
                false // 不计算余额
            ).await {
                txs
            } else {
                error!("{}: 同步ledger交易时发生错误，继续执行后续逻辑", token.symbol);
                Vec::new()
            };
            
            // 阶段2：根据账户交易记录计算余额
            info!("{}: 阶段2：根据账户交易记录统一计算余额...", token.symbol);
            // 调用余额计算函数，传递代币配置
            if let Err(e) = balances::calculate_all_balances(
                &collections.accounts_col,
                &collections.tx_col,
                &collections.balances_col,
                &collections.total_supply_col,
                &collections.balance_anomalies_col,
                &token
            ).await {
                error!("{}: 计算余额时出错: {}", token.symbol, e);
            }
            
            // 设置增量同步模式
            if !ledger_txs.is_empty() {
                if let Some(last_tx) = ledger_txs.last() {
                    if let Some(index) = last_tx.index {
                        info!("{}: 设置增量同步起点为最后一笔交易索引: {}", token.symbol, index);
                        set_incremental_mode(
                            &db_conn.sync_status_col,
                            &token.symbol,
                            index,
                            last_tx.timestamp
                        ).await?;
                    }
                }
            }
            
            info!("{}: 初始同步和余额计算完成", token.symbol);
        } else if let Ok(Some(status)) = sync_status {
            // 检查是否需要验证同步状态的完整性
            info!("{}: 从断点继续同步，验证同步状态的完整性...", token.symbol);
            
            // 检查数据库中最新交易索引与同步状态是否一致
            match get_latest_transaction_index(&collections.tx_col).await {
                Ok(Some(db_latest_index)) => {
                    if db_latest_index < status.last_synced_index {
                        warn!("{}: 数据库最新交易索引 ({}) 小于同步状态记录的索引 ({}), 可能有数据丢失", 
                             token.symbol, db_latest_index, status.last_synced_index);
                        info!("{}: 将从数据库最新索引开始重新同步...", token.symbol);
                        
                        // 更新同步状态为数据库的最新索引
                        if let Err(e) = set_incremental_mode(
                            &db_conn.sync_status_col,
                            &token.symbol,
                            db_latest_index,
                            status.last_synced_timestamp
                        ).await {
                            error!("{}: 更新同步状态失败: {}", token.symbol, e);
                        }
                    } else if db_latest_index > status.last_synced_index {
                        info!("{}: 数据库最新交易索引 ({}) 大于同步状态记录的索引 ({}), 将更新同步状态", 
                              token.symbol, db_latest_index, status.last_synced_index);
                        
                        // 更新同步状态为数据库的最新索引
                        if let Err(e) = set_incremental_mode(
                            &db_conn.sync_status_col,
                            &token.symbol,
                            db_latest_index,
                            status.last_synced_timestamp
                        ).await {
                            error!("{}: 更新同步状态失败: {}", token.symbol, e);
                        }
                    } else {
                        info!("{}: 同步状态与数据库记录一致，索引: {}", token.symbol, db_latest_index);
                    }
                },
                _ => {
                    warn!("{}: 无法获取数据库最新交易索引，将使用同步状态记录的索引", token.symbol);
                }
            }
            
            info!("{}: 跳过初始同步，直接进入增量同步模式", token.symbol);
        }
    }
    
    // 启动API服务器（如果配置中启用）
    if let Some(api_config) = &cfg.api_server {
        if api_config.enabled {
            info!("配置中启用了API服务器，即将启动...");
            // 克隆数据库连接和端口到新的变量，避免借用 cfg
            let db_conn_clone = db_conn.clone();
            let port = api_config.port;
            let tokens_clone = cfg.tokens.clone();

            // 创建异步任务启动API服务器
            tokio::spawn(async move {
                let api_server = api_server::ApiServer::new(db_conn_clone, tokens_clone);
                if let Err(e) = api_server.start(port).await {
                    log::error!("API服务器启动失败: {}", e);
                }
            });

            info!("API服务器已在后台启动，端口: {}", port);
        } else {
            info!("API服务器在配置中被禁用，不会启动API服务");
        }
    } else {
        info!("未找到API服务器配置，不会启动API服务");
    }
    
    // 定时增量同步
    info!("开始实时监控多代币的新交易");
    let mut consecutive_errors = HashMap::new();
    let max_consecutive_errors = 5;
    let token_rotation_delay = Duration::from_secs(2); // 不同代币同步间隔
    
    // 当没有代币时直接返回
    if cfg.tokens.is_empty() {
        error!("没有配置代币，结束同步");
        return Ok(());
    }
    
    // 初始化每个代币的错误计数
    for token in &cfg.tokens {
        consecutive_errors.insert(token.symbol.clone(), 0);
    }
    
    // 创建代币列表循环器
    let tokens_cycle = std::iter::repeat(cfg.tokens.clone()).flatten();
    let mut token_iter = tokens_cycle.enumerate();
    
    loop {
        // 获取当前要同步的代币
        let (index, token) = token_iter.next().unwrap();
        
        // 如果不是第一个代币，等待2秒再同步
        if index > 0 {
            tokio::time::sleep(token_rotation_delay).await;
        }
        
        debug!("{}: 执行定时增量同步...", token.symbol);
        
        // 获取该代币的集合
        let collections = match db_conn.collections.get(&token.symbol) {
            Some(cols) => cols,
            None => {
                error!("{}: 没有找到代币的集合", token.symbol);
                continue;
            }
        };
        
        // 解析Canister ID
        let canister_id = match parse_canister_id(&token.canister_id) {
            Ok(id) => id,
            Err(e) => {
                error!("{}: 解析canister ID失败: {}", token.symbol, e);
                continue;
            }
        };
        
        // 获取代币小数位数
        let token_decimals = match token.decimals {
            Some(decimals) => decimals,
            None => {
                match get_token_decimals(&agent, &canister_id, &token.symbol).await {
                    Ok(decimals) => decimals,
                    Err(e) => {
                        error!("{}: 获取代币小数位失败: {}", token.symbol, e);
                        continue;
                    }
                }
            }
        };
        
        // 访问或初始化该代币的连续错误计数
        let error_count = consecutive_errors.entry(token.symbol.clone()).or_insert(0);
        
        // 增量同步交易数据
        match sync_ledger_transactions(
            &agent, 
            &canister_id, 
            &collections.tx_col, 
            &collections.accounts_col, 
            &collections.balances_col, 
            &collections.total_supply_col,
            &token,
            false // 增量同步时不再实时计算余额
        ).await {
            Ok(new_transactions) => {
                // 同步完成后，只计算新交易相关账户的余额
                if !new_transactions.is_empty() {
                    info!("{}: 增量同步获取到 {} 笔新交易，计算相关账户余额...", token.symbol, new_transactions.len());
                    match calculate_incremental_balances(
                        &new_transactions,
                        &collections.tx_col,
                        &collections.accounts_col,
                        &collections.balances_col,
                        &collections.total_supply_col,
                        &collections.balance_anomalies_col,
                        &token
                    ).await {
                        Ok((success, error)) => {
                            info!("{}: 增量余额计算完成: 更新了 {} 个账户, 失败 {} 个账户", token.symbol, success, error);
                            *error_count = 0; // 重置错误计数
                        },
                        Err(e) => {
                            error!("{}: 增量计算余额时出错: {}", token.symbol, e);
                            *error_count += 1;
                        }
                    }
                } else {
                    debug!("{}: 没有获取到新交易，跳过余额计算", token.symbol);
                    *error_count = 0; // 重置错误计数
                }
            },
            Err(e) => {
                *error_count += 1;
                error!("{}: 定时增量同步出错 ({}/{}): {}", token.symbol, error_count, max_consecutive_errors, e);
                
                if *error_count >= max_consecutive_errors {
                    error!("{}: 连续错误次数达到上限 ({}), 对该代币等待更长时间后继续...", token.symbol, max_consecutive_errors);
                    // 发生多次连续错误时，等待更长时间再重试，但继续处理其他代币
                    *error_count = 0; // 重置计数
                }
            }
        }
    }
}

