use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use log::{info, error, warn};
use crate::db::transactions::clear_transactions;
use crate::db::accounts::clear_accounts;
use crate::db::balances::{clear_balances, calculate_all_balances as calc_balances};
use crate::db::sync_status::{clear_sync_status, set_full_sync_mode, set_incremental_mode};
use crate::db::create_indexes;
use crate::db::DbConnection;
use crate::sync::archive::sync_archive_transactions;
use crate::sync::ledger::sync_ledger_transactions;
use crate::blockchain::get_first_transaction_index;

/// 重置数据库并完全重新同步所有交易
/// 
/// 注意：此函数只能通过命令行参数 --reset 触发，属于管理员功能
pub async fn reset_and_sync_all_transactions(
    agent: &Agent,
    canister_id: &Principal,
    db_conn: &DbConnection,
    token_decimals: u8,
) -> Result<(), Box<dyn Error>> {
    info!("开始重置数据库并重新同步所有交易数据...");
    
    // 清空集合
    info!("清空交易集合...");
    clear_transactions(&db_conn.tx_col).await?;
    
    info!("清空账户-交易关系集合...");
    clear_accounts(&db_conn.accounts_col).await?;
    
    info!("清空余额集合...");
    clear_balances(&db_conn.balances_col).await?;
    
    info!("清空同步状态集合...");
    clear_sync_status(&db_conn.sync_status_col).await?;
    
    // 设置为全量同步模式
    set_full_sync_mode(&db_conn.sync_status_col).await?;
    
    // 重新创建索引
    info!("重新创建索引...");
    create_indexes(db_conn).await?;
    
    // 第一阶段：同步交易数据
    info!("\n第一阶段：同步所有交易数据到数据库...");
    
    // 先同步归档数据
    info!("\n同步归档交易...");
    let _archive_result = sync_archive_transactions(
        agent,
        canister_id,
        &db_conn.tx_col,
        &db_conn.accounts_col,
        &db_conn.balances_col,
        &db_conn.total_supply_col,
        token_decimals,
        false // 不计算余额，只保存交易
    ).await?;
    
    // 同步ledger的交易
    info!("\n同步ledger交易...");
    
    // 尝试获取区块链初始索引
    match get_first_transaction_index(agent, canister_id).await {
        Ok(first_index) => {
            info!("获取到区块链初始索引: {}", first_index);
        },
        Err(e) => {
            warn!("获取区块链初始索引失败: {}，尝试从0开始", e);
        }
    }
    
    // 从当前索引开始同步ledger数据
    let ledger_transactions = sync_ledger_transactions(
        agent,
        canister_id,
        &db_conn.tx_col,
        &db_conn.accounts_col,
        &db_conn.balances_col,
        &db_conn.total_supply_col,
        token_decimals,
        false // 不计算余额，只保存交易
    ).await?;
    
    // 第二阶段：计算余额
    info!("\n第二阶段：根据账户信息计算余额...");
    calculate_all_balances(db_conn, token_decimals).await?;
    
    // 获取最新交易索引和时间戳，用于设置增量同步起点
    let mut latest_index = 0;
    let mut latest_timestamp = 0;
    
    if !ledger_transactions.is_empty() {
        if let Some(last_tx) = ledger_transactions.last() {
            if let Some(index) = last_tx.index {
                latest_index = index;
            }
            latest_timestamp = last_tx.timestamp;
        }
    }
    
    // 设置为增量同步模式，并保存最新同步状态
    if latest_index > 0 {
        info!("重置完成：设置增量同步模式，最新索引: {}, 时间戳: {}", 
              latest_index, latest_timestamp);
        set_incremental_mode(&db_conn.sync_status_col, latest_index, latest_timestamp).await?;
    } else {
        warn!("重置完成但未找到有效交易，保持全量同步模式");
        // 即使没有交易，也将同步模式设置为增量，避免重复全量同步
        set_incremental_mode(&db_conn.sync_status_col, 0, 0).await?;
    }
    
    info!("数据库重置和交易同步完成，所有账户余额已根据交易记录重新计算！");
    info!("下次运行将从索引 {} 开始增量同步", latest_index + 1);
    
    Ok(())
}

/// 从数据库读取所有账户关联的交易，计算每个账户的余额
pub async fn calculate_all_balances(
    db_conn: &DbConnection,
    token_decimals: u8,
) -> Result<(), Box<dyn Error>> {
    info!("开始使用新算法计算所有账户余额...");
    
    match calc_balances(
        &db_conn.accounts_col,
        &db_conn.tx_col,
        &db_conn.balances_col,
        &db_conn.total_supply_col,
        &db_conn.balance_anomalies_col,
        token_decimals
    ).await {
        Ok((success, error)) => {
            info!("余额计算完成: 成功处理 {} 个账户, 失败 {} 个账户", success, error);
        },
        Err(e) => {
            error!("余额计算过程中发生错误: {}", e);
            return Err(e);
        }
    }
    
    info!("所有账户的余额计算已完成");
    Ok(())
}

