use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use tokio::time::Duration;
use num_traits::ToPrimitive;
use mongodb::{Collection, bson::Document};
use crate::blockchain::{fetch_archives, fetch_archive_transactions};
use crate::db::transactions::save_transaction;
use crate::db::accounts::save_account_transaction;
use crate::db::balances::process_batch_balances;
use crate::utils::group_transactions_by_account;
use crate::models::{ArchiveInfo, ARCHIVE_BATCH_SIZE};

/// 同步归档canister的交易数据
pub async fn sync_archive_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    token_decimals: u8,
    calculate_balance: bool, // 是否计算余额
) -> Result<(), Box<dyn Error>> {
    println!("获取归档信息...");
    let archives = match fetch_archives(&agent, &canister_id).await {
        Ok(a) => a,
        Err(e) => {
            println!("获取归档信息失败: {}，跳过归档处理", e);
            return Ok(());
        }
    };
    
    if archives.is_empty() {
        println!("没有找到归档信息");
        println!("交易都存在 ledger canister 里。跳过归档canister，直接查询ledger canister。");
        return Ok(());
    }
    
    // 打印归档信息
    println!("打印归档信息:");
    for archive in &archives {
        println!("找到归档信息: canister_id={}, 范围: {}-{}",
            archive.canister_id, 
            archive.block_range_start.0, 
            archive.block_range_end.0
        );
    }
    
    // 按区块范围起始位置对归档进行排序，确保按时间顺序处理
    let mut sorted_archives = archives.clone();
    sorted_archives.sort_by(|a, b| {
        a.block_range_start.0.cmp(&b.block_range_start.0)
    });
    
    // 处理所有找到的归档
    for (idx, archive_info) in sorted_archives.iter().enumerate() {
        process_single_archive(
            agent,
            archive_info,
            idx + 1,
            sorted_archives.len(),
            tx_col,
            accounts_col,
            balances_col,
            token_decimals,
            calculate_balance
        ).await?;
    }
    
    println!("\n所有归档处理完毕");
    Ok(())
}

/// 处理单个归档canister
async fn process_single_archive(
    agent: &Agent,
    archive_info: &ArchiveInfo,
    index: usize,
    total: usize,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    token_decimals: u8,
    calculate_balance: bool, // 是否计算余额
) -> Result<(), Box<dyn Error>> {
    println!("\n处理归档 {}/{}: canister_id={}", index, total, archive_info.canister_id);
    let archive_canister_id = &archive_info.canister_id;
    let block_range_start = archive_info.block_range_start.0.to_u64().unwrap_or(0);
    let block_range_end = archive_info.block_range_end.0.to_u64().unwrap_or(0);
    
    println!("归档范围: {}-{}", block_range_start, block_range_end);
    
    // 先测试单个交易的解码
    match fetch_archive_transactions(
        &agent,
        archive_canister_id,
        block_range_start,
        1
    ).await {
        Ok(test_transactions) => {
            if test_transactions.is_empty() {
                println!("无法从归档canister获取交易，跳过此归档");
                return Ok(());
            }
            
            println!("测试获取交易成功，开始批量同步...");
            println!("使用批量大小: {} 笔交易/批次", ARCHIVE_BATCH_SIZE);
            
            // 分批处理归档交易
            let mut current_start = block_range_start;
            let mut error_count = 0;
            let max_consecutive_errors = 3;
            
            while current_start <= block_range_end && error_count < max_consecutive_errors {
                let current_length = std::cmp::min(ARCHIVE_BATCH_SIZE, 
                              block_range_end.saturating_sub(current_start) + 1);
                              
                if current_length == 0 {
                    println!("计算出的批次长度为0，停止处理此归档");
                    break;
                }
                
                println!("获取归档交易批次: {}-{}", current_start, 
                        current_start + current_length - 1);
                
                match fetch_archive_transactions(
                    &agent,
                    archive_canister_id,
                    current_start,
                    current_length
                ).await {
                    Ok(transactions) => {
                        let num_fetched = transactions.len();
                        error_count = 0; // 重置错误计数
                        
                        if num_fetched == 0 {
                            println!("批次内无交易，跳到下一批次");
                            current_start += current_length;
                            if current_start > block_range_end {
                                println!("已达到归档范围末尾");
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        
                        println!("获取到 {} 笔交易，保存到数据库", num_fetched);
                        
                        // 按交易索引对交易进行排序，确保按时间顺序处理
                        let mut sorted_transactions = transactions.clone();
                        sorted_transactions.sort_by_key(|tx| tx.index.unwrap_or(0));
                        
                        // 保存交易到数据库
                        let mut success_count = 0;
                        let mut error_count = 0;
                        
                        for tx in &sorted_transactions {
                            match save_transaction(tx_col, tx).await {
                                Ok(_) => {
                                    success_count += 1;
                                    
                                    let index = tx.index.unwrap_or(0);
                                    let tx_clone = tx.clone();
                                    let tx_array = vec![tx_clone];
                                    let account_txs = group_transactions_by_account(&tx_array);
                                    
                                    for (account, _) in &account_txs {
                                        if let Err(e) = save_account_transaction(&accounts_col, account, index).await {
                                            println!("保存账户-交易关系失败: {}", e);
                                            error_count += 1;
                                        }
                                    }
                                },
                                Err(e) => {
                                    println!("保存交易失败: {}", e);
                                    error_count += 1;
                                }
                            }
                        }
                        
                        println!("保存结果: 成功={}, 失败={}", success_count, error_count);
                        
                        // 根据参数决定是否处理余额
                        if calculate_balance {
                            println!("处理余额更新...");
                            // 处理这批交易的余额更新
                            match process_batch_balances(&balances_col, &sorted_transactions, token_decimals).await {
                                Ok((success, error)) => {
                                    println!("余额更新: 成功处理 {} 笔交易, 失败 {} 笔", success, error);
                                },
                                Err(e) => {
                                    println!("批量处理余额更新失败: {}", e);
                                }
                            }
                        } else {
                            println!("跳过余额计算");
                        }
                        
                        // 推进索引，确保即使获取数量少于请求数量也能正确前进
                        current_start += num_fetched as u64;
                        
                        // 检查是否已到达归档末尾
                        if (num_fetched as u64) < current_length {
                            println!("获取的交易数量少于请求数量，可能已达归档末尾");
                            break;
                        }
                    },
                    Err(e) => {
                        error_count += 1;
                        println!("获取归档交易失败 ({}/{}): {}", 
                            error_count, max_consecutive_errors, e);
                        
                        if error_count >= max_consecutive_errors {
                            println!("连续错误次数达到上限，中止此归档处理");
                            break;
                        }
                        
                        println!("短暂休眠后尝试跳过此批次...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        current_start += current_length;
                    }
                }
                
                // 在批次之间添加短暂延迟
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            
            println!("归档 {} 处理完成", archive_canister_id);
            Ok(())
        },
        Err(e) => {
            println!("测试获取归档交易失败: {}, 跳过此归档", e);
            Ok(())
        }
    }
} 