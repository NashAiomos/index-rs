use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use tokio::time::Duration;
use mongodb::{Collection, bson::Document};
use crate::db::transactions::get_latest_transaction_index;
use crate::blockchain::{get_first_transaction_index, fetch_ledger_transactions};
use crate::db::transactions::save_transaction;
use crate::db::accounts::save_account_transaction;
use crate::db::balances::process_batch_balances;
use crate::utils::group_transactions_by_account;
use crate::models::BATCH_SIZE;

/// 直接使用已知的交易起点和偏移量查询数据
pub async fn sync_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    token_decimals: u8,
) -> Result<(), Box<dyn Error>> {
    // 获取数据库里面最新的交易索引
    let latest_index = match get_latest_transaction_index(tx_col).await {
        Ok(Some(index)) => {
            println!("数据库中最新的交易索引: {}", index);
            println!("从索引 {} 开始同步新交易", index + 1);
            index
        },
        Ok(None) | Err(_) => {
            println!("数据库中没有找到交易索引，将从区块链上的第一笔交易开始同步");
            
            // 先尝试获取ledger的状态，得到first_index
            println!("获取区块链初始索引...");
            match get_first_transaction_index(agent, canister_id).await {
                Ok(first_index) => {
                    println!("从区块链获取的初始索引为: {}", first_index);
                    // 返回比first_index小1的值，这样current_index会从first_index开始
                    first_index.saturating_sub(1)
                },
                Err(e) => {
                    println!("获取区块链初始索引失败: {}，尝试直接查询交易", e);
                    // 如果获取失败，尝试从0开始查询
                    0
                }
            }
        }
    };
    
    // 使用增量同步方式查询新交易
    let mut current_index = latest_index + 1;
    let mut retry_count = 0;
    let max_retries = 3;
    let mut consecutive_empty = 0;
    let max_consecutive_empty = 2; // 连续空结果次数阈值
    
    // 尝试同步交易，每次获取一批
    while retry_count < max_retries && consecutive_empty < max_consecutive_empty {
        let length = BATCH_SIZE;
        println!("查询交易批次: {}-{}", current_index, current_index + length - 1);
        
        match fetch_ledger_transactions(agent, canister_id, current_index, length).await {
            Ok((transactions, first_index, log_length)) => {
                // 如果first_index大于current_index，说明有交易被跳过，应该从first_index开始查询
                if first_index > current_index {
                    println!("检测到first_index ({}) 大于 current_index ({}), 调整查询索引", 
                        first_index, current_index);
                    current_index = first_index;
                    continue;
                }
                
                // 如果是第一次查询且初始索引为0，但first_index不是0，则使用first_index
                if current_index == 1 && first_index > 0 {
                    println!("首次查询，调整初始索引为区块链上的first_index: {}", first_index);
                    current_index = first_index;
                    continue;
                }
                
                if transactions.is_empty() {
                    consecutive_empty += 1;
                    println!("没有获取到新交易 ({}/{}), 可能已到达链上最新状态或索引有误", 
                        consecutive_empty, max_consecutive_empty);
                    
                    // 尝试跳到下一个可能的索引位置
                    if log_length > current_index {
                        println!("日志长度 ({}) 大于当前索引 ({}), 尝试从新位置查询", log_length, current_index);
                        current_index = log_length;
                        consecutive_empty = 0; // 重置连续空计数
                    } else {
                        // 如果没有明确的新位置，小幅度向前尝试
                        current_index += BATCH_SIZE / 10; 
                        println!("尝试从新位置 {} 查询", current_index);
                    }
                    
                    // 短暂等待避免过快查询
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue; // 继续下一个循环迭代
                }
                
                // 获取到新交易，重置计数
                consecutive_empty = 0;
                println!("获取到 {} 笔交易", transactions.len());
                
                // 保存交易到数据库
                let mut success_count = 0;
                let mut error_count = 0;
                for tx in &transactions {
                    // 保存交易
                    match save_transaction(tx_col, tx).await {
                        Ok(_) => {
                            success_count += 1;
                            // 更新账户-交易关系
                            let index = tx.index.unwrap_or(0);
                            let tx_clone = tx.clone();
                            let tx_array = vec![tx_clone];
                            let account_txs = group_transactions_by_account(&tx_array);
                            
                            for (account, _) in &account_txs {
                                if let Err(e) = save_account_transaction(accounts_col, account, index).await {
                                    println!("保存账户-交易关系失败 (账户: {}, 交易索引: {}): {}", account, index, e);
                                    error_count += 1;
                                }
                            }
                        },
                        Err(e) => {
                            println!("保存交易失败 (索引: {}): {}", tx.index.unwrap_or(0), e);
                            error_count += 1;
                        }
                    }
                }
                
                println!("成功保存 {} 笔交易，失败 {} 笔", success_count, error_count);
                
                // 处理这批交易的余额更新
                match process_batch_balances(balances_col, &transactions, token_decimals).await {
                    Ok((success, error)) => {
                        println!("余额更新: 成功处理 {} 笔交易, 失败 {} 笔", success, error);
                    },
                    Err(e) => {
                        println!("批量处理余额更新失败: {}", e);
                    }
                }
                
                // 更新当前索引并重置重试计数
                current_index += transactions.len() as u64;
                retry_count = 0;
                
                // 当前批次处理完成后，短暂休息以减轻系统负担
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
            Err(e) => {
                println!("获取交易失败: {}，重试 {}/{}", e, retry_count + 1, max_retries);
                retry_count += 1;
                
                // 错误恢复策略
                if retry_count >= max_retries {
                    println!("达到最大重试次数，尝试跳过当前批次...");
                    current_index += BATCH_SIZE / 2; // 跳过部分索引，尝试继续
                    retry_count = 0;
                    consecutive_empty = 0;
                } else {
                    // 指数退避
                    let wait_time = Duration::from_secs(2u64.pow(retry_count as u32));
                    println!("等待 {:?} 后重试", wait_time);
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }
    
    if consecutive_empty >= max_consecutive_empty {
        println!("连续 {} 次获取空结果，认为已达到链上最新状态", consecutive_empty);
    }
    
    println!("交易同步完成，当前索引: {}", current_index - 1);
    Ok(())
} 