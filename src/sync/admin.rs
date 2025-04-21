use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::Cursor;
use futures::StreamExt;
use crate::db::transactions::clear_transactions;
use crate::db::accounts::clear_accounts;
use crate::db::balances::clear_balances;
use crate::db::create_indexes;
use crate::db::DbConnection;
use crate::sync::archive::sync_archive_transactions;
use crate::sync::ledger::sync_ledger_transactions;
use crate::blockchain::get_first_transaction_index;
use crate::models::Transaction;
use crate::db::balances::process_batch_balances;

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
    
    // 第一阶段：先同步所有交易数据，不计算余额
    println!("\n第一阶段：同步所有交易数据到数据库...");
    
    // 先同步归档数据
    println!("\n同步归档交易...");
    sync_archive_transactions(
        agent,
        canister_id,
        &db_conn.tx_col,
        &db_conn.accounts_col,
        &db_conn.balances_col,
        token_decimals,
        false // 不计算余额，只保存交易
    ).await?;
    
    // 同步ledger的交易
    println!("\n同步ledger交易...");
    
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
        token_decimals,
        false // 不计算余额，只保存交易
    ).await?;
    
    // 第二阶段：从数据库读取所有交易，按索引排序后进行余额计算
    println!("\n第二阶段：按交易索引顺序计算余额...");
    calculate_all_balances(db_conn, token_decimals).await?;
    
    println!("数据库重置和交易同步完成，所有账户余额已根据交易索引顺序重新计算！");
    Ok(())
}

/// 从数据库读取所有交易，按索引排序后统一计算余额
pub async fn calculate_all_balances(
    db_conn: &DbConnection,
    token_decimals: u8,
) -> Result<(), Box<dyn Error>> {
    // 清空当前余额集合
    println!("清空当前余额集合，准备按索引顺序重新计算...");
    clear_balances(&db_conn.balances_col).await?;
    
    // 从数据库中按索引顺序读取所有交易
    println!("从数据库读取所有交易...");
    let mut cursor = db_conn.tx_col.find(doc! {}, None).await?;
    
    let mut transactions = Vec::new();
    
    // 将所有交易加载到内存
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => {
                match mongodb::bson::from_document::<Transaction>(doc) {
                    Ok(tx) => {
                        transactions.push(tx);
                    },
                    Err(e) => {
                        println!("解析交易文档失败: {}", e);
                    }
                }
            },
            Err(e) => {
                println!("读取交易文档失败: {}", e);
            }
        }
    }
    
    println!("读取到 {} 笔交易，开始按索引排序...", transactions.len());
    
    // 确保按索引顺序排序
    transactions.sort_by_key(|tx| tx.index.unwrap_or(0));
    
    // 分批处理余额计算，避免单次处理交易过多
    const BATCH_SIZE: usize = 5000;
    let total_batches = (transactions.len() + BATCH_SIZE - 1) / BATCH_SIZE;
    
    println!("将分 {} 批次处理余额计算，每批 {} 笔交易", total_batches, BATCH_SIZE);
    
    let mut total_processed = 0;
    
    for (batch_idx, chunk) in transactions.chunks(BATCH_SIZE).enumerate() {
        println!("处理余额计算批次 {}/{}，交易索引范围: {}-{}", 
                batch_idx + 1, 
                total_batches,
                chunk.first().map_or(0, |tx| tx.index.unwrap_or(0)),
                chunk.last().map_or(0, |tx| tx.index.unwrap_or(0)));
        
        match process_batch_balances(&db_conn.balances_col, chunk, token_decimals).await {
            Ok((success, error)) => {
                println!("批次余额更新完成: 成功处理 {} 笔交易, 失败 {} 笔", success, error);
                total_processed += success;
            },
            Err(e) => {
                println!("批次余额处理失败: {}", e);
            }
        }
    }
    
    println!("所有交易的余额计算完成，共处理 {} 笔交易", total_processed);
    Ok(())
} 