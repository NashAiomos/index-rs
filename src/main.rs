mod models;
mod utils;
mod config;
mod blockchain;
mod db;
mod sync;

use std::error::Error;
use tokio::time::interval;
use tokio::time::Duration;
use crate::config::{load_config, parse_args, parse_canister_id, create_agent, get_token_decimals};
use crate::db::{init_db, create_indexes};
use crate::sync::{sync_ledger_transactions, sync_archive_transactions};
use crate::sync::admin::{reset_and_sync_all_transactions, calculate_all_balances};
use crate::db::balances::calculate_incremental_balances;
use crate::models::Transaction;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 设置全局错误捕获
    let result = run_application().await;
    
    // 处理顶层错误
    if let Err(e) = &result {
        eprintln!("程序执行过程中发生错误: {}", e);
        // 可以在这里添加额外的错误处理逻辑，如发送警报通知等
    }
    
    result
}

// 将主要应用逻辑移到独立函数，便于错误处理
async fn run_application() -> Result<(), Box<dyn Error>> {
    println!("启动索引服务...");
    
    // 获取命令行参数
    let reset_mode = parse_args();
    
    // 读取配置文件
    let cfg = load_config().await?;

    // 初始化 MongoDB
    let db_conn = init_db(&cfg.mongodb_url, &cfg.database).await?;
    
    // 初始化IC Agent
    let agent = create_agent(&cfg.ic_url)?;

    // 解析Canister ID
    let canister_id = parse_canister_id(&cfg.ledger_canister_id)?;
    
    // 获取代币小数位数
    let token_decimals = match cfg.token_decimals {
        Some(decimals) => {
            println!("使用配置文件中指定的代币小数位: {}", decimals);
            decimals
        },
        None => {
            // 尝试从canister查询小数位数
            match get_token_decimals(&agent, &canister_id).await {
                Ok(decimals) => {
                    println!("从canister查询到代币小数位: {}", decimals);
                    decimals
                },
        Err(e) => {
                    println!("查询代币小数位失败: {}, 使用默认值{}", e, models::DEFAULT_DECIMALS);
                    models::DEFAULT_DECIMALS
                }
            }
        }
    };
    println!("代币小数位设置为: {}", token_decimals);

    // 创建索引以提高查询性能
    create_indexes(&db_conn).await?;

    // 如果是重置模式，执行完整的数据库重置和重新同步
    if reset_mode {
        println!("开始执行数据库重置和重新同步操作...");
        reset_and_sync_all_transactions(&agent, &canister_id, &db_conn, token_decimals).await?;
        println!("数据库重置和重新同步成功完成！");
        return Ok(());
    }
    
    // 正常模式：先同步所有交易，再统一计算余额
    println!("阶段1：同步所有交易数据...");
    
    // 先同步归档数据
    let archives_result = sync_archive_transactions(
        &agent,
        &canister_id, 
        &db_conn.tx_col, 
        &db_conn.accounts_col, 
        &db_conn.balances_col, 
        token_decimals,
        false // 不计算余额
    ).await?;
    
    // 同步主账本数据
    println!("开始同步ledger交易...");
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
        eprintln!("同步ledger交易时发生错误，继续执行后续逻辑");
        Vec::new()
    };
    
    // 阶段2：使用新算法根据账户交易记录计算余额
    println!("阶段2：根据账户交易记录统一计算余额...");
    calculate_all_balances(&db_conn, token_decimals).await?;
    
    println!("初始同步和余额计算完成，开始实时监控新交易");
    
    // 定时增量同步
    let mut interval = interval(Duration::from_secs(5));
    let mut consecutive_errors = 0;
    let max_consecutive_errors = 5;
    
    loop {
        interval.tick().await;
        
        println!("\n执行定时增量同步...");
        
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
                    println!("增量同步获取到 {} 笔新交易，计算相关账户余额...", new_transactions.len());
                    match calculate_incremental_balances(
                        &new_transactions,
                        &db_conn.tx_col,
                        &db_conn.accounts_col,
                        &db_conn.balances_col,
                        token_decimals
                    ).await {
                        Ok((success, error)) => {
                            println!("增量余额计算完成: 更新了 {} 个账户, 失败 {} 个账户", success, error);
                            consecutive_errors = 0; // 重置错误计数
                        },
                        Err(e) => {
                            eprintln!("增量计算余额时出错: {}", e);
                            consecutive_errors += 1;
                        }
                    }
                } else {
                    println!("没有获取到新交易，跳过余额计算");
                    consecutive_errors = 0; // 重置错误计数
                }
            },
            Err(e) => {
                consecutive_errors += 1;
                eprintln!("定时增量同步出错 ({}/{}): {}", consecutive_errors, max_consecutive_errors, e);
                
                if consecutive_errors >= max_consecutive_errors {
                    eprintln!("连续错误次数达到上限 ({}), 等待更长时间后继续...", max_consecutive_errors);
                    // 发生多次连续错误时，等待更长时间再重试
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    consecutive_errors = 0; // 重置计数
                }
            }
        }
    }
}
