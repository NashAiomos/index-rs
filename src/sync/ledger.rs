use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use tokio::time::Duration;
use mongodb::{Collection, bson::{doc, Document}};
use log::{info, error, warn, debug};
use crate::db::transactions::get_latest_transaction_index;
use crate::blockchain::{get_first_transaction_index, fetch_ledger_transactions};
use crate::db::transactions::save_transaction;
use crate::db::accounts::save_account_transaction;
use crate::db::sync_status::{get_sync_status, set_incremental_mode};
use crate::utils::{group_transactions_by_account};
use crate::models::{Transaction, BATCH_SIZE};

/// 打印交易详细信息到日志
fn log_transaction_details(tx: &Transaction) {
    let index_str = match tx.index {
        Some(idx) => idx.to_string(),
        None => "未知".to_string(),
    };
    
    // 将时间戳转换为可读时间格式
    let timestamp = tx.timestamp;
    let datetime = chrono::DateTime::from_timestamp(
        (timestamp / 1_000_000_000) as i64, 
        (timestamp % 1_000_000_000) as u32
    ).unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
    
    let time_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
    
    info!("📝 同步新交易 [索引: {}] [时间: {}] [类型: {}]", index_str, time_str, tx.kind);
    
    match tx.kind.as_str() {
        "transfer" => {
            if let Some(transfer) = &tx.transfer {
                info!("   ↪ 转账明细: {} → {}", transfer.from, transfer.to);
                info!("   ↪ 金额: {}", transfer.amount);
                if let Some(fee) = &transfer.fee {
                    info!("   ↪ 手续费: {}", fee);
                }
                if let Some(spender) = &transfer.spender {
                    info!("   ↪ 授权者: {}", spender);
                }
                if let Some(memo) = &transfer.memo {
                    if !memo.is_empty() {
                        let memo_str = if memo.iter().all(|&b| b.is_ascii() && !b.is_ascii_control()) {
                            String::from_utf8_lossy(memo).to_string()
                        } else {
                            format!("0x{}", hex::encode(memo))
                        };
                        info!("   ↪ 备注: {}", memo_str);
                    }
                }
            }
        },
        "mint" => {
            if let Some(mint) = &tx.mint {
                info!("   ↪ 铸币明细: → {}", mint.to);
                info!("   ↪ 金额: {}", mint.amount);
                if let Some(memo) = &mint.memo {
                    if !memo.is_empty() {
                        let memo_str = if memo.iter().all(|&b| b.is_ascii() && !b.is_ascii_control()) {
                            String::from_utf8_lossy(memo).to_string()
                        } else {
                            format!("0x{}", hex::encode(memo))
                        };
                        info!("   ↪ 备注: {}", memo_str);
                    }
                }
            }
        },
        "burn" => {
            if let Some(burn) = &tx.burn {
                info!("   ↪ 销毁明细: {} →", burn.from);
                info!("   ↪ 金额: {}", burn.amount);
                if let Some(spender) = &burn.spender {
                    info!("   ↪ 授权者: {}", spender);
                }
                if let Some(memo) = &burn.memo {
                    if !memo.is_empty() {
                        let memo_str = if memo.iter().all(|&b| b.is_ascii() && !b.is_ascii_control()) {
                            String::from_utf8_lossy(memo).to_string()
                        } else {
                            format!("0x{}", hex::encode(memo))
                        };
                        info!("   ↪ 备注: {}", memo_str);
                    }
                }
            }
        },
        "approve" => {
            if let Some(approve) = &tx.approve {
                info!("   ↪ 授权明细: {} → {}", approve.from, approve.spender);
                info!("   ↪ 授权额度: {}", approve.amount);
                if let Some(fee) = &approve.fee {
                    info!("   ↪ 手续费: {}", fee);
                }
                if let Some(expires_at) = approve.expires_at {
                    let expire_dt = chrono::DateTime::from_timestamp(
                        (expires_at / 1_000_000_000) as i64, 
                        (expires_at % 1_000_000_000) as u32
                    ).unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    let expire_str = expire_dt.format("%Y-%m-%d %H:%M:%S").to_string();
                    info!("   ↪ 过期时间: {}", expire_str);
                }
                if let Some(memo) = &approve.memo {
                    if !memo.is_empty() {
                        let memo_str = if memo.iter().all(|&b| b.is_ascii() && !b.is_ascii_control()) {
                            String::from_utf8_lossy(memo).to_string()
                        } else {
                            format!("0x{}", hex::encode(memo))
                        };
                        info!("   ↪ 备注: {}", memo_str);
                    }
                }
            }
        },
        _ => {
            info!("   ↪ 未知交易类型");
        }
    }
    
    info!("--------------------------------------------------------");
}

/// 验证同步点附近交易的完整性
/// 检查上次同步的最新交易和前几笔交易是否存在，如果不存在可能需要从早一点的位置重新同步
async fn verify_synced_transactions(
    tx_col: &Collection<Document>,
    _sync_status_col: &Collection<Document>,
    _token_symbol: &str,
    last_synced_index: u64,
    verification_range: u64,
) -> Result<(bool, u64), Box<dyn Error>> {
    info!("验证同步点附近交易的完整性，从索引 {} 开始检查 {} 条记录", 
          last_synced_index.saturating_sub(verification_range), verification_range);
    
    // 验证最后同步的交易是否存在
    let last_tx_exists = tx_col
        .find_one(doc! { "index": last_synced_index as i64 }, None)
        .await?
        .is_some();
    
    if !last_tx_exists {
        warn!("最后同步的交易(索引:{})在数据库中不存在，可能需要从更早的位置重新同步", last_synced_index);
        
        // 查找最近的有效交易
        let start_from = last_synced_index.saturating_sub(verification_range);
        let mut valid_point = start_from;
        let mut found_valid = false;
        
        for i in start_from..last_synced_index {
            let tx_exists = tx_col
                .find_one(doc! { "index": i as i64 }, None)
                .await?
                .is_some();
            
            if tx_exists {
                valid_point = i;
                found_valid = true;
                info!("找到最近的有效交易点: {}", valid_point);
                break;
            }
        }
        
        if !found_valid {
            warn!("未找到 {} 到 {} 范围内的有效交易点，将重置到 {}", 
                 start_from, last_synced_index, start_from);
            valid_point = start_from;
        }
        
        // 返回验证失败和推荐的起始点
        return Ok((false, valid_point));
    }
    
    // 检查连续性 - 从最后同步点往前验证一定数量的交易
    let mut continuity_valid = true;
    let check_limit = verification_range.min(last_synced_index);
    let mut missing_indices = Vec::new();
    
    for i in 1..=check_limit {
        let index = last_synced_index - i;
        let tx_exists = tx_col
            .find_one(doc! { "index": index as i64 }, None)
            .await?
            .is_some();
        
        if !tx_exists {
            continuity_valid = false;
            missing_indices.push(index);
        }
    }
    
    if !continuity_valid {
        warn!("同步点附近发现不连续的交易，缺失的索引: {:?}", missing_indices);
        
        // 找到最近的连续点
        let mut valid_point = last_synced_index;
        for i in 1..=check_limit {
            let index = last_synced_index - i;
            if missing_indices.contains(&index) {
                valid_point = index.saturating_sub(1);
            } else {
                break;
            }
        }
        
        return Ok((false, valid_point));
    }
    
    info!("同步点附近交易验证成功，数据完整性正常");
    Ok((true, last_synced_index))
}

/// 直接使用已知的交易起点和偏移量查询数据
pub async fn sync_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    _balances_col: &Collection<Document>,
    _supply_col: &Collection<Document>,
    token_config: &crate::models::TokenConfig,
    _calculate_balance: bool,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    // 从配置中提取代币符号和小数位数
    let token_symbol = &token_config.symbol;
    let _token_decimals = token_config.decimals.unwrap_or(8);
    // 兼容现有API，第5个参数是sync_status_col
    let sync_status_col = _balances_col;
    
    // 首先检查同步状态
    let mut start_from_sync_status = false;
    let mut sync_status_index = 0;
    
    if let Ok(Some(status)) = get_sync_status(sync_status_col, token_symbol).await {
        if status.sync_mode == "incremental" && status.last_synced_index > 0 {
            info!("从同步状态恢复，上次同步到索引: {}", status.last_synced_index);
            start_from_sync_status = true;
            sync_status_index = status.last_synced_index;
            
            // 验证同步点附近交易的完整性
            let verification_range = 20; // 验证前20笔交易
            match verify_synced_transactions(tx_col, sync_status_col, token_symbol, sync_status_index, verification_range).await {
                Ok((valid, recommended_point)) => {
                    if !valid {
                        warn!("同步点验证失败，将从索引 {} 重新开始同步", recommended_point);
                        sync_status_index = recommended_point;
                        
                        // 更新同步状态
                        if let Err(e) = set_incremental_mode(sync_status_col, token_symbol, recommended_point, 0).await {
                            error!("更新同步状态失败: {}", e);
                        } else {
                            info!("已更新同步状态到索引 {}", recommended_point);
                        }
                    } else {
                        info!("同步点验证成功，将从索引 {} 继续同步", sync_status_index + 1);
                    }
                },
                Err(e) => {
                    warn!("验证同步点时发生错误: {}，将使用原始同步点", e);
                }
            }
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
    let max_retries = 5;  // 增加最大重试次数
    let mut consecutive_empty = 0;
    let max_consecutive_empty = 3;  // 增加连续空结果阈值
    
    // 收集所有同步到的新交易
    let mut all_new_transactions = Vec::new();
    
    // 跟踪最新的交易索引和时间戳
    let mut latest_tx_index = latest_index;
    let mut latest_tx_timestamp = 0;
    
    // 记录上次更新同步状态的索引
    let mut last_status_update_index = latest_index;
    let status_update_frequency: usize = 100;  // 每同步100笔交易更新一次状态
    
    info!("🚀 开始增量同步交易数据，从索引 {} 开始", current_index);
    info!("=====================================================");
    
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
                    
                    // 检查是否应该更新同步状态 - 如果有新交易同步过
                    if latest_tx_index > last_status_update_index {
                        if let Err(e) = set_incremental_mode(sync_status_col, token_symbol, latest_tx_index, latest_tx_timestamp).await {
                            warn!("连续空结果时更新同步状态失败: {}", e);
                        } else {
                            info!("已更新同步状态索引: {} -> {}", last_status_update_index, latest_tx_index);
                            last_status_update_index = latest_tx_index;
                        }
                    }
                    
                    // 短暂等待避免过快查询
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue; // 继续下一个循环迭代
                }
                
                // 获取到新交易，重置计数
                consecutive_empty = 0;
                info!("获取到 {} 笔交易", transactions.len());
                info!("🔄 开始处理交易批次: {}～{}", current_index, current_index + transactions.len() as u64 - 1);
                
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
                    
                    // 保存交易之前打印交易详细信息
                    log_transaction_details(tx);
                    
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
                info!("✅ 交易批次处理完成: {}～{}", current_index, current_index + transactions.len() as u64 - 1);
                
                // 不再需要在此处计算余额，由新算法统一计算
                debug!("跳过余额计算（将使用增量余额计算算法）");
                
                // 更新当前索引并重置重试计数
                current_index += transactions.len() as u64;
                retry_count = 0;
                
                // 更频繁地更新同步状态
                if latest_tx_index > last_status_update_index && 
                   ((latest_tx_index - last_status_update_index) as usize >= status_update_frequency || 
                    all_new_transactions.len() % status_update_frequency == 0) {
                    if let Err(e) = set_incremental_mode(sync_status_col, token_symbol, latest_tx_index, latest_tx_timestamp).await {
                        warn!("更新同步状态失败: {}", e);
                    } else {
                        info!("已更新同步状态索引: {} -> {}", last_status_update_index, latest_tx_index);
                        last_status_update_index = latest_tx_index;
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
                    // 检查是否有已获取的交易记录
                    if latest_tx_index > last_status_update_index {
                        warn!("达到最大重试次数但已有部分交易，将保存当前同步状态后重试...");
                        
                        // 保存当前同步状态
                        if let Err(status_err) = set_incremental_mode(sync_status_col, token_symbol, latest_tx_index, latest_tx_timestamp).await {
                            error!("错误恢复时保存同步状态失败: {}", status_err);
                        } else {
                            info!("错误恢复：已保存同步状态至索引 {}", latest_tx_index);
                            last_status_update_index = latest_tx_index;
                        }
                        
                        warn!("尝试跳过当前批次继续同步...");
                        current_index += BATCH_SIZE / 4; // 跳过部分索引，尝试继续
                        retry_count = 0; // 重置重试计数
                        consecutive_empty = 0; // 重置连续空计数
                        
                        // 等待较长时间后重试
                        let wait_time = Duration::from_secs(5);
                        info!("等待 {:?} 后继续同步", wait_time);
                        tokio::time::sleep(wait_time).await;
                    } else {
                        warn!("达到最大重试次数，尝试跳过当前批次...");
                        current_index += BATCH_SIZE / 4; // 跳过部分索引，尝试继续
                        retry_count = 0;
                        consecutive_empty = 0;
                        
                        // 指数退避
                        let wait_time = Duration::from_secs(5);
                        debug!("等待 {:?} 后重试", wait_time);
                        tokio::time::sleep(wait_time).await;
                    }
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
        if let Err(e) = set_incremental_mode(sync_status_col, token_symbol, latest_tx_index, latest_tx_timestamp).await {
            error!("最终更新同步状态失败: {}", e);
        } else {
            info!("同步状态已更新至最新索引: {} (共同步 {} 笔新交易)", latest_tx_index, all_new_transactions.len());
        }
    } else {
        info!("无新交易，保持同步状态在索引: {}", latest_index);
    }
    
    info!("=====================================================");
    info!("🏁 交易同步完成，当前索引: {}, 共同步 {} 笔新交易", current_index - 1, all_new_transactions.len());
    Ok(all_new_transactions)
}
