/**
 * 文件描述: 归档数据同步模块，负责从归档canister同步历史交易
 * 功能概述:
 * - 获取归档canister信息
 * - 同步归档交易数据
 * - 处理不同格式的交易数据
 * - 保存交易到数据库
 * 
 * 主要组件:
 * - sync_archive_transactions函数 (第14-182行): 主要同步函数，协调整体同步流程
 *   - 获取归档canister信息 (第20-40行)
 *   - 按批次获取归档交易 (第76-170行)
 *   - 处理和保存交易数据 (第101-125行)
 * - process_single_archive函数 (第185-316行): 处理单个归档canister的函数
 *   - 测试归档canister可用性 (第196-216行)
 *   - 分批处理归档交易 (第218-268行)
 *   - 保存交易到数据库 (第270-295行)
 */

use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use tokio::time::Duration;
use num_traits::ToPrimitive;
use mongodb::{Collection, bson::Document};
use crate::blockchain::{fetch_archives, fetch_archive_transactions, test_archive_transactions};
use crate::db::transactions::save_transaction;
use crate::db::accounts::save_account_transaction;
use crate::utils::group_transactions_by_account;
use crate::models::{ArchiveInfo, Transaction, ARCHIVE_BATCH_SIZE};
use log::{info, debug, error, warn};

/// 同步归档canister的交易数据
pub async fn sync_archive_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    _balances_col: &Collection<Document>,
    _supply_col: &Collection<Document>,
    _token_decimals: u8,
    calculate_balance: bool,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    info!("获取归档信息...");
    
    // 获取所有归档canister信息
    let mut archives = match fetch_archives(agent, canister_id).await {
        Ok(archives) => archives,
        Err(e) => {
            error!("获取归档信息失败: {}", e);
            return Err(e);
        }
    };
    
    // 按照block_range_start排序
    archives.sort_by_key(|a| a.block_range_start.0.clone());
    
    debug!("打印归档信息:");
    for archive in &archives {
        debug!("找到归档信息: canister_id={}, 范围: {}-{}", 
            archive.canister_id,
            archive.block_range_start.0,
            archive.block_range_end.0);
    }
    
    if archives.is_empty() {
        info!("未找到归档canister，跳过归档同步");
        return Ok(Vec::new());
    }
    
    // 返回值，收集所有同步到的交易
    let mut all_transactions: Vec<Transaction> = Vec::new();
    let mut archive_count = 1;
    
    for archive in &archives {
        let start = archive.block_range_start.0.to_u64().unwrap_or(0);
        let end = archive.block_range_end.0.to_u64().unwrap_or(0);
        
        info!("处理归档 {}/{}: canister_id={}", archive_count, archives.len(), archive.canister_id);
        debug!("归档范围: {}-{}", start, end);
        archive_count += 1;
        
        // 先尝试获取1笔交易，测试归档canister是否可用
        match test_archive_transactions(agent, &archive.canister_id, start, 1).await {
            Ok(test_txs) => {
                if test_txs.is_empty() {
                    warn!("测试获取交易失败，归档 {} 可能无法访问，跳过", archive.canister_id);
                    continue;
                }
                debug!("测试获取交易成功，开始批量同步...");
            },
            Err(e) => {
                error!("测试获取归档交易失败: {}，跳过归档 {}", e, archive.canister_id);
                continue;
            }
        }
        
        // 确定一次拉取的批次大小
        let batch_size = ARCHIVE_BATCH_SIZE;
        debug!("使用批量大小: {} 笔交易/批次", batch_size);
        
        // 分批次获取归档交易
        let mut current = start;
        
        while current <= end {
            let length = if current + batch_size > end {
                end - current + 1
            } else {
                batch_size
            };
            
            debug!("获取归档交易批次: {}-{}", current, current + length - 1);
            
            // 获取交易
            match fetch_archive_transactions(agent, &archive.canister_id, current, length).await {
                Ok(transactions) => {
                    let tx_count = transactions.len();
                    if tx_count > 0 {
                        debug!("获取到 {} 笔交易，保存到数据库", tx_count);
                        
                        // 保存交易
                        let mut success = 0;
                        let mut fail = 0;
                        
                        for tx in &transactions {
                            match save_transaction(tx_col, tx).await {
                                Ok(_) => {
                                    success += 1;
                                    
                                    // 更新账户-交易关系
                                    if let Some(index) = tx.index {
                                        let tx_array = vec![tx.clone()];
                                        let account_txs = group_transactions_by_account(&tx_array);
                                        
                                        for (account, _) in &account_txs {
                                            if let Err(e) = save_account_transaction(accounts_col, account, index).await {
                                                debug!("保存账户-交易关系失败 (账户: {}, 交易索引: {}): {}", 
                                                    account, index, e);
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    warn!("保存交易失败 (索引: {}): {}", tx.index.unwrap_or(0), e);
                                    fail += 1;
                                }
                            }
                        }
                        
                        debug!("保存结果: 成功={}, 失败={}", success, fail);
                        
                        if calculate_balance {
                            debug!("执行余额计算...");
                            // [余额计算代码省略]
                            // 注意：新算法中，不在这里计算余额，而是在完成所有交易同步后统一计算
                        } else {
                            debug!("跳过余额计算（将使用增量余额计算算法）");
                        }
                        
                        // 收集成功保存的交易
                        all_transactions.extend_from_slice(&transactions);
                    } else {
                        debug!("批次 {}-{} 未获取到交易", current, current + length - 1);
                    }
                    
                    current += length;
                },
                Err(e) => {
                    error!("获取归档交易失败: {}", e);
                    // 尝试跳过当前批次，继续下一批次
                    current += length / 2;
                    if current <= end {
                        warn!("跳过批次 {}-{}，尝试从 {} 继续", 
                            current - length / 2, current - 1, current);
                    }
                }
            }
        }
    }
    
    info!("归档同步完成，共同步 {} 笔归档交易", all_transactions.len());
    Ok(all_transactions)
}

#[allow(dead_code)]
/// 处理单个归档canister
async fn process_single_archive(
    agent: &Agent,
    archive_info: &ArchiveInfo,
    index: usize,
    total: usize,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    _token_decimals: u8,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    info!("\n处理归档 {}/{}: canister_id={}", index, total, archive_info.canister_id);
    let archive_canister_id = &archive_info.canister_id;
    let block_range_start = archive_info.block_range_start.0.to_u64().unwrap_or(0);
    let block_range_end = archive_info.block_range_end.0.to_u64().unwrap_or(0);
    
    info!("归档范围: {}-{}", block_range_start, block_range_end);
    
    // 用于收集同步到的交易
    let mut synced_transactions = Vec::new();
    
    // 先测试单个交易的解码
    match test_archive_transactions(
        &agent,
        archive_canister_id,
        block_range_start,
        1
    ).await {
        Ok(test_transactions) => {
            if test_transactions.is_empty() {
                warn!("无法从归档canister获取交易，跳过此归档");
                return Ok(Vec::new());
            }
            
            info!("测试获取交易成功，开始批量同步...");
            debug!("使用批量大小: {} 笔交易/批次", ARCHIVE_BATCH_SIZE);
            
            // 分批处理归档交易
            let mut current_start = block_range_start;
            let mut error_count = 0;
            let max_consecutive_errors = 3;
            
            while current_start <= block_range_end && error_count < max_consecutive_errors {
                let current_length = std::cmp::min(ARCHIVE_BATCH_SIZE, 
                              block_range_end.saturating_sub(current_start) + 1);
                              
                if current_length == 0 {
                    warn!("计算出的批次长度为0，停止处理此归档");
                    break;
                }
                
                debug!("获取归档交易批次: {}-{}", current_start, 
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
                            debug!("批次内无交易，跳到下一批次");
                            current_start += current_length;
                            if current_start > block_range_end {
                                debug!("已达到归档范围末尾");
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        
                        info!("获取到 {} 笔交易，保存到数据库", num_fetched);
                        
                        // 按交易索引对交易进行排序，确保按时间顺序处理
                        let mut sorted_transactions = transactions.clone();
                        sorted_transactions.sort_by_key(|tx| tx.index.unwrap_or(0));
                        
                        // 保存交易到数据库
                        let mut success_count = 0;
                        let mut save_error_count = 0;
                        
                        for tx in &sorted_transactions {
                            match save_transaction(tx_col, tx).await {
                                Ok(_) => {
                                    success_count += 1;
                                    
                                    // 收集成功保存的交易，用于后续余额计算
                                    synced_transactions.push(tx.clone());
                                    
                                    let index = tx.index.unwrap_or(0);
                                    let tx_clone = tx.clone();
                                    let tx_array = vec![tx_clone];
                                    let account_txs = group_transactions_by_account(&tx_array);
                                    
                                    for (account, _) in &account_txs {
                                        if let Err(e) = save_account_transaction(accounts_col, account, index).await {
                                            error!("保存账户-交易关系失败: {}", e);
                                            save_error_count += 1;
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("保存交易失败: {}", e);
                                    save_error_count += 1;
                                }
                            }
                        }
                        
                        info!("保存结果: 成功={}, 失败={}", success_count, save_error_count);
                        
                        // 不再需要在此处计算余额，由新算法统一计算
                        debug!("跳过余额计算（将使用增量余额计算算法）");
                        
                        // 推进索引，确保即使获取数量少于请求数量也能正确前进
                        current_start += num_fetched as u64;
                        
                        // 减轻系统负担，短暂休息
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    },
                    Err(e) => {
                        error!("获取归档交易失败: {}", e);
                        error_count += 1;
                        
                        if error_count >= max_consecutive_errors {
                            warn!("连续错误次数达到上限，跳过剩余部分");
                            break;
                        }
                        
                        // 指数退避等待
                        let wait_time = Duration::from_secs(2u64.pow(error_count as u32));
                        debug!("等待 {:?} 后重试", wait_time);
                        tokio::time::sleep(wait_time).await;
                    }
                }
            }
        },
        Err(e) => {
            error!("测试访问归档canister失败: {}", e);
            return Ok(Vec::new());
        }
    }
    
    info!("归档 {} 处理完成，同步了 {} 笔交易", archive_info.canister_id, synced_transactions.len());
    Ok(synced_transactions)
}

