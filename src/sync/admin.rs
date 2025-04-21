use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use crate::db::transactions::clear_transactions;
use crate::db::accounts::clear_accounts;
use crate::db::balances::clear_balances;
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
    println!("开始重置数据库并重新同步所有交易数据...");
    
    // 清空交易集合
    println!("清空交易集合...");
    clear_transactions(&db_conn.tx_col).await?;
    
    // 清空账户-交易关系集合
    println!("清空账户-交易关系集合...");
    clear_accounts(&db_conn.accounts_col).await?;
    
    // 清空余额集合
    println!("清空余额集合...");
    clear_balances(&db_conn.balances_col).await?;
    
    // 重新创建索引
    println!("重新创建索引...");
    create_indexes(db_conn).await?;
    
    // 先同步归档数据
    println!("\n开始同步归档交易...");
    sync_archive_transactions(
        agent,
        canister_id,
        &db_conn.tx_col,
        &db_conn.accounts_col,
        &db_conn.balances_col,
        token_decimals
    ).await?;
    
    // 同步ledger的交易
    println!("\n开始同步ledger交易...");
    
    // 尝试获取区块链初始索引
    match get_first_transaction_index(agent, canister_id).await {
        Ok(first_index) => {
            println!("获取到区块链初始索引: {}", first_index);
        },
        Err(e) => {
            println!("获取区块链初始索引失败: {}，尝试从0开始", e);
        }
    }
    
    // 从当前索引开始同步ledger数据
    sync_ledger_transactions(
        agent,
        canister_id,
        &db_conn.tx_col,
        &db_conn.accounts_col,
        &db_conn.balances_col,
        token_decimals
    ).await?;
    
    println!("数据库重置和交易同步完成！");
    Ok(())
} 