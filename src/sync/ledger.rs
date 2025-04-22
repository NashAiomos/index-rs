use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use tokio::time::Duration;
use mongodb::{Collection, bson::Document};
use log::{info, error, warn, debug};
use crate::db::transactions::get_latest_transaction_index;
use crate::blockchain::{get_first_transaction_index, fetch_ledger_transactions};
use crate::db::transactions::save_transaction;
use crate::db::accounts::save_account_transaction;
use crate::db::sync_status::{get_sync_status, set_incremental_mode};
use crate::utils::group_transactions_by_account;
use crate::models::{Transaction, BATCH_SIZE};

/// 直接使用已知的交易起点和偏移量查询数据
pub async fn sync_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    _token_decimals: u8,
    _calculate_balance: bool, // 是否计算余额
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    // 首先检查同步状态
    let mut start_from_sync_status = false;
    let mut sync_status_index = 0;
    
    // balances_col实际上是错误的，但目前API不方便修改
    // 此处假设API中第5个参数是sync_status_col
    let sync_status_col = balances_col;
    
    if let Ok(Some(status)) = get_sync_status(sync_status_col).await {
        if status.sync_mode == "incremental" && status.last_synced_index > 0 {
            info!("从同步状态恢复，上次同步到索引: {}", status.last_synced_index);
            start_from_sync_status = true;
            sync_status_index = status.last_synced_index;
        } else {
            info!("同步状态显示为全量同步模式或起始状态");
        }
    }
    
    // 获取数据库里面最新的交易索引
    let latest_index = if start_from_sync_status {
        info!("使用同步状态中的索引: {}", sync_status_index);
        sync_status_index
    } else {
        match get_latest_transaction_index(tx_col).await {
            Ok(Some(index)) => {
                info!("数据库中最新的交易索引: {}", index);
                info!("从索引 {} 开始同步新交易", index + 1);
                index
            },
            Ok(None) | Err(_) => {
                info!("数据库中没有找到交易索引，将从区块链上的第一笔交易开始同步");
                
                // 先尝试获取ledger的状态，得到first_index
                info!("获取区块链初始索引...");
                match get_first_transaction_index(agent, canister_id).await {
                    Ok(first_index) => {
                        info!("从区块链获取的初始索引为: {}", first_index);
                        // 返回比first_index小1的值，这样current_index会从first_index开始
                        first_index.saturating_sub(1)
                    },
                    Err(e) => {
                        warn!("获取区块链初始索引失败: {}，尝试直接查询交易", e);
                        // 如果获取失败，尝试从0开始查询
                        0
                    }
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
    
    // 收集所有同步到的新交易
    let mut all_new_transactions = Vec::new();
    
    // 跟踪最新的交易索引和时间戳
    let mut latest_tx_index = latest_index;
    let mut latest_tx_timestamp = 0;
    
    // 尝试同步交易，每次获取一批
    while retry_count < max_retries && consecutive_empty < max_consecutive_empty {
        let length = BATCH_SIZE;
        debug!("查询交易批次: {}-{}", current_index, current_index + length - 1);
        
        match fetch_ledger_transactions(agent, canister_id, current_index, length).await {
            Ok((transactions, first_index, log_length)) => {
                // 如果first_index大于current_index，说明有交易被跳过，应该从first_index开始查询
                if first_index > current_index {
                    info!("检测到first_index ({}) 大于 current_index ({}), 调整查询索引", 
                        first_index, current_index);
                    current_index = first_index;
                    continue;
                }
                
                // 如果是第一次查询且初始索引为0，但first_index不是0，则使用first_index
                if current_index == 1 && first_index > 0 {
                    info!("首次查询，调整初始索引为区块链上的first_index: {}", first_index);
                    current_index = first_index;
                    continue;
                }
                
                if transactions.is_empty() {
                    consecutive_empty += 1;
                    debug!("没有获取到新交易 ({}/{}), 可能已到达链上最新状态或索引有误", 
                        consecutive_empty, max_consecutive_empty);
                    
                    // 尝试跳到下一个可能的索引位置
                    if log_length > current_index {
                        info!("日志长度 ({}) 大于当前索引 ({}), 尝试从新位置查询", log_length, current_index);
                        current_index = log_length;
                        consecutive_empty = 0; // 重置连续空计数
                    } else {
                        // 如果没有明确的新位置，小幅度向前尝试
                        current_index += BATCH_SIZE / 10; 
                        debug!("尝试从新位置 {} 查询", current_index);
                    }
                    
                    // 短暂等待避免过快查询
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue; // 继续下一个循环迭代
                }
                
                // 获取到新交易，重置计数
                consecutive_empty = 0;
                info!("获取到 {} 笔交易", transactions.len());
                
                // 确保交易按索引排序
                let mut sorted_transactions = transactions.clone();
                sorted_transactions.sort_by_key(|tx| tx.index.unwrap_or(0));
                
                // 保存交易到数据库并收集成功保存的交易
                let mut success_count = 0;
                let mut error_count = 0;
                
                for tx in &sorted_transactions {
                    // 更新最新的交易索引和时间戳
                    if let Some(index) = tx.index {
                        if index > latest_tx_index {
                            latest_tx_index = index;
                            latest_tx_timestamp = tx.timestamp;
                        }
                    }
                    
                    // 保存交易
                    match save_transaction(tx_col, tx).await {
                        Ok(_) => {
                            success_count += 1;
                            // 收集成功保存的交易，用于后续余额计算
                            let tx_clone = tx.clone();
                            all_new_transactions.push(tx_clone);
                            
                            // 更新账户-交易关系
                            let index = tx.index.unwrap_or(0);
                            let tx_array = vec![tx.clone()];
                            let account_txs = group_transactions_by_account(&tx_array);
                            
                            for (account, _) in &account_txs {
                                if let Err(e) = save_account_transaction(accounts_col, account, index).await {
                                    error!("保存账户-交易关系失败 (账户: {}, 交易索引: {}): {}", account, index, e);
                                    error_count += 1;
                                }
                            }
                        },
                        Err(e) => {
                            error!("保存交易失败 (索引: {}): {}", tx.index.unwrap_or(0), e);
                            error_count += 1;
                        }
                    }
                }
                
                info!("成功保存 {} 笔交易，失败 {} 笔", success_count, error_count);
                
                // 不再需要在此处计算余额，由新算法统一计算
                debug!("跳过余额计算（将使用增量余额计算算法）");
                
                // 更新当前索引并重置重试计数
                current_index += transactions.len() as u64;
                retry_count = 0;
                
                // 定期更新同步状态
                if latest_tx_index > latest_index && all_new_transactions.len() % 1000 == 0 {
                    if let Err(e) = set_incremental_mode(sync_status_col, latest_tx_index, latest_tx_timestamp).await {
                        error!("更新同步状态失败: {}", e);
                    } else {
                        info!("同步状态已更新至索引: {}", latest_tx_index);
                    }
                }
                
                // 当前批次处理完成后，短暂休息以减轻系统负担
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
            Err(e) => {
                warn!("获取交易失败: {}，重试 {}/{}", e, retry_count + 1, max_retries);
                retry_count += 1;
                
                // 错误恢复策略
                if retry_count >= max_retries {
                    warn!("达到最大重试次数，尝试跳过当前批次...");
                    current_index += BATCH_SIZE / 2; // 跳过部分索引，尝试继续
                    retry_count = 0;
                    consecutive_empty = 0;
                } else {
                    // 指数退避
                    let wait_time = Duration::from_secs(2u64.pow(retry_count as u32));
                    debug!("等待 {:?} 后重试", wait_time);
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }
    
    if consecutive_empty >= max_consecutive_empty {
        info!("连续 {} 次获取空结果，认为已达到链上最新状态", consecutive_empty);
    }
    
    // 完成同步后，更新同步状态
    if latest_tx_index > latest_index {
        if let Err(e) = set_incremental_mode(sync_status_col, latest_tx_index, latest_tx_timestamp).await {
            error!("最终更新同步状态失败: {}", e);
        } else {
            info!("同步状态已更新至最新索引: {}", latest_tx_index);
        }
    }
    
    info!("交易同步完成，当前索引: {}, 共同步 {} 笔新交易", current_index - 1, all_new_transactions.len());
    Ok(all_new_transactions)
} 