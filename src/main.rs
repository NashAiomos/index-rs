mod models;
mod utils;
mod config;
mod blockchain;
mod db;
mod sync;

use std::error::Error;
use std::fs;
use tokio::time::interval;
use tokio::time::Duration;
use log::{info, error, warn, debug, LevelFilter};
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Config as LogConfig, Root};
use log4rs::filter::threshold::ThresholdFilter;
use crate::config::{load_config, parse_args, parse_canister_id, create_agent, get_token_decimals};
use crate::db::{init_db, create_indexes};
use crate::sync::{sync_ledger_transactions, sync_archive_transactions};
use crate::sync::admin::{reset_and_sync_all_transactions, calculate_all_balances};
use crate::db::balances::calculate_incremental_balances;
use crate::db::sync_status::{get_sync_status, set_incremental_mode};

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
    
    info!("正在启动区块链索引服务...");
    
    // 设置全局错误捕获
    let result = run_application(cfg).await;
    
    // 处理顶层错误
    if let Err(e) = &result {
        error!("程序执行过程中发生错误: {}", e);
        // 可以在这里添加额外的错误处理逻辑，如发送警报通知等
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
    
    // 创建控制台输出（如果需要）
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
    let db_conn = init_db(&cfg.mongodb_url, &cfg.database).await?;
    
    // 初始化IC Agent
    let agent = create_agent(&cfg.ic_url)?;

    // 解析Canister ID
    let canister_id = parse_canister_id(&cfg.ledger_canister_id)?;
    
    // 获取代币小数位数
    let token_decimals = match cfg.token_decimals {
        Some(decimals) => {
            info!("使用配置文件中指定的代币小数位: {}", decimals);
            decimals
        },
        None => {
            // 尝试从canister查询小数位数
            match get_token_decimals(&agent, &canister_id).await {
                Ok(decimals) => {
                    info!("从canister查询到代币小数位: {}", decimals);
                    decimals
                },
        Err(e) => {
                    warn!("查询代币小数位失败: {}, 使用默认值{}", e, models::DEFAULT_DECIMALS);
                    models::DEFAULT_DECIMALS
                }
            }
        }
    };
    info!("代币小数位设置为: {}", token_decimals);

    // 创建索引以提高查询性能
    create_indexes(&db_conn).await?;

    // 如果是重置模式，执行完整的数据库重置和重新同步
    if reset_mode {
        info!("开始执行数据库重置和重新同步操作...");
        reset_and_sync_all_transactions(&agent, &canister_id, &db_conn, token_decimals).await?;
        info!("数据库重置和重新同步成功完成！");
        return Ok(());
    }
    
    // 检查同步状态，确定是否需要初始同步
    let needs_initial_sync = match get_sync_status(&db_conn.sync_status_col).await {
        Ok(Some(status)) => {
            if status.sync_mode == "incremental" && status.last_synced_index > 0 {
                info!("检测到有效的同步状态，上次同步索引：{}，将继续增量同步", status.last_synced_index);
                false
            } else {
                info!("同步状态无效或为全量模式，需要进行初始同步");
                true
            }
        },
        _ => {
            info!("未找到同步状态记录，将进行初始同步");
            true
        }
    };
    
    // 如果需要初始同步，执行全量同步流程
    if needs_initial_sync {
        // 正常模式：先同步所有交易，再统一计算余额
        info!("阶段1：同步所有交易数据...");
        
        // 先同步归档数据
        let _archives_result = sync_archive_transactions(
            &agent,
            &canister_id, 
            &db_conn.tx_col, 
            &db_conn.accounts_col, 
            &db_conn.balances_col, 
            token_decimals,
            false // 不计算余额
        ).await?;
        
        // 同步主账本数据
        info!("开始同步ledger交易...");
        let ledger_txs = if let Ok(txs) = sync_ledger_transactions(
            &agent,
            &canister_id, 
            &db_conn.tx_col, 
            &db_conn.accounts_col, 
            &db_conn.balances_col, 
            token_decimals,
            false // 不计算余额
        ).await {
            txs
        } else {
            error!("同步ledger交易时发生错误，继续执行后续逻辑");
            Vec::new()
        };
        
        // 阶段2：使用新算法根据账户交易记录计算余额
        info!("阶段2：根据账户交易记录统一计算余额...");
        calculate_all_balances(&db_conn, token_decimals).await?;
        
        // 设置增量同步模式
        if !ledger_txs.is_empty() {
            if let Some(last_tx) = ledger_txs.last() {
                if let Some(index) = last_tx.index {
                    info!("设置增量同步起点为最后一笔交易索引: {}", index);
                    set_incremental_mode(
                        &db_conn.sync_status_col, 
                        index, 
                        last_tx.timestamp
                    ).await?;
                }
            }
        }
        
        info!("初始同步和余额计算完成，将开始实时监控新交易");
    } else {
        info!("跳过初始同步，直接进入增量同步模式");
    }
    
    // 定时增量同步
    info!("开始实时监控新交易");
    let mut interval = interval(Duration::from_secs(5));
    let mut consecutive_errors = 0;
    let max_consecutive_errors = 5;
    
    loop {
        interval.tick().await;
        
        debug!("执行定时增量同步...");
        
        // 增量同步交易数据
        match sync_ledger_transactions(
            &agent, 
            &canister_id, 
            &db_conn.tx_col, 
            &db_conn.accounts_col, 
            &db_conn.balances_col, 
            token_decimals,
            false // 增量同步时不再实时计算余额
        ).await {
            Ok(new_transactions) => {
                // 同步完成后，只计算新交易相关账户的余额
                if !new_transactions.is_empty() {
                    info!("增量同步获取到 {} 笔新交易，计算相关账户余额...", new_transactions.len());
                    match calculate_incremental_balances(
                        &new_transactions,
                        &db_conn.tx_col,
                        &db_conn.accounts_col,
                        &db_conn.balances_col,
                        token_decimals
                    ).await {
                        Ok((success, error)) => {
                            info!("增量余额计算完成: 更新了 {} 个账户, 失败 {} 个账户", success, error);
                            consecutive_errors = 0; // 重置错误计数
                        },
                        Err(e) => {
                            error!("增量计算余额时出错: {}", e);
                            consecutive_errors += 1;
                        }
                    }
                } else {
                    debug!("没有获取到新交易，跳过余额计算");
                    consecutive_errors = 0; // 重置错误计数
                }
            },
            Err(e) => {
                consecutive_errors += 1;
                error!("定时增量同步出错 ({}/{}): {}", consecutive_errors, max_consecutive_errors, e);
                
                if consecutive_errors >= max_consecutive_errors {
                    error!("连续错误次数达到上限 ({}), 等待更长时间后继续...", max_consecutive_errors);
                    // 发生多次连续错误时，等待更长时间再重试
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    consecutive_errors = 0; // 重置计数
                }
            }
        }
    }
}

