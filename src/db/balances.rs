use std::error::Error;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;
use mongodb::{Collection};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::FindOptions;
use tokio::time::Duration;
use candid::Nat;
use num_traits::Zero;
use log::{info, error, warn, debug};
use crate::models::{Transaction, BalanceAnomaly};
use crate::utils::{create_error, format_token_amount};
use crate::db::supply;

// 全局账户锁映射
lazy_static::lazy_static! {
    static ref ACCOUNT_LOCKS: Mutex<HashMap<String, Arc<Mutex<()>>>> = Mutex::new(HashMap::new());
}

/// 获取账户锁
async fn get_account_lock(account: &str) -> Arc<Mutex<()>> {
    let mut locks = ACCOUNT_LOCKS.lock().await;
    locks.entry(account.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// 获取账户余额
#[allow(dead_code)]
pub async fn get_account_balance(
    balances_col: &Collection<Document>,
    account: &str,
) -> Result<String, Box<dyn Error>> {
    // 规范化账户格式
    let normalized_account = normalize_account_id(account);
    
    if let Some(doc) = balances_col
        .find_one(doc! { "account": &normalized_account }, None)
        .await?
    {
        if let Ok(balance) = doc.get_str("balance") {
            return Ok(balance.to_string());
        }
    }
    
    Ok("0".to_string()) // 默认返回0余额
}

/// 清空余额集合
pub async fn clear_balances(balances_col: &Collection<Document>) -> Result<u64, Box<dyn Error>> {
    match balances_col.delete_many(doc! {}, None).await {
        Ok(result) => {
            info!("已清除 {} 条余额记录", result.deleted_count);
            Ok(result.deleted_count)
        },
        Err(e) => {
            error!("清除余额集合失败: {}", e);
            Err(create_error(&format!("清除余额集合失败: {}", e)))
        }
    }
}

/// 计算并保存账户余额 - 新算法
/// 在所有交易同步完成后调用，根据accounts数据和transactions集合计算每个账户的余额
pub async fn calculate_all_balances(
    accounts_col: &Collection<Document>,
    tx_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    supply_col: &Collection<Document>,
    anomalies_col: &Collection<Document>,
    token_decimals: u8,
) -> Result<(u64, u64), Box<dyn Error>> {
    info!("开始计算所有账户余额...");
    
    // 首先清空余额集合
    clear_balances(balances_col).await?;
    
    // 查询所有账户
    let mut accounts_cursor = accounts_col.find(doc! {}, None).await?;
    
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    let mut total_anomalies = 0u64;
    
    // 遍历所有账户
    while accounts_cursor.advance().await? {
        let raw_doc = accounts_cursor.current();
        // 转换为Document类型
        let account_doc = Document::try_from(raw_doc.to_owned())?;
        
        let account = match account_doc.get_str("account") {
            Ok(acc) => acc.to_string(),
            Err(e) => {
                error!("无法获取账户信息: {}", e);
                error_count += 1;
                continue;
            }
        };
        
        // 获取该账户的所有交易索引
        let tx_indices: Vec<i64> = if let Some(indices) = account_doc.get("transaction_indices") {
            if let Bson::Array(arr) = indices {
                arr.iter().filter_map(|b| match b {
                    Bson::Int64(i) => Some(*i),
                    Bson::Int32(i) => Some(i64::from(*i)),
                    _ => None,
                }).collect()
            } else {
                error!("账户 {} 的交易索引不是数组格式", account);
                error_count += 1;
                continue;
            }
        } else {
            error!("无法获取账户 {} 的交易索引", account);
            error_count += 1;
            continue;
        };
        
        if tx_indices.is_empty() {
            debug!("账户 {} 没有交易记录", account);
            continue;
        }
        
        // 计算该账户的余额
        match calculate_account_balance(&account, &tx_indices, tx_col, token_decimals, anomalies_col).await {
            Ok((balance, has_anomalies)) => {
                // 更新余额记录
                match save_account_balance(balances_col, &account, &balance).await {
                    Ok(_) => {
                        success_count += 1;
                        if has_anomalies {
                            total_anomalies += 1;
                            info!("账户 {} 在余额计算中检测到异常，已记录详细信息", account);
                        }
                    },
                    Err(e) => {
                        error!("保存账户 {} 余额失败: {}", account, e);
                        error_count += 1;
                    }
                }
            },
            Err(e) => {
                error!("计算账户 {} 余额失败: {}", account, e);
                error_count += 1;
            }
        }
    }
    
    info!("全量余额计算完成: 处理 {} 个账户, 失败 {} 个账户, 检测到 {} 个余额异常", 
          success_count, error_count, total_anomalies);

    // 重新计算并保存总供应量
    supply::recalculate_total_supply(balances_col, supply_col).await?;

    Ok((success_count, error_count))
}

/// 增量计算余额 - 只处理新同步的交易
/// 计算新交易对相关账户余额的影响，而不是重新计算所有账户的余额
pub async fn calculate_incremental_balances(
    new_transactions: &[Transaction],
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    supply_col: &Collection<Document>,
    anomalies_col: &Collection<Document>,
    token_decimals: u8,
) -> Result<(u64, u64), Box<dyn Error>> {
    if new_transactions.is_empty() {
        debug!("没有新交易需要计算余额");
        return Ok((0, 0));
    }
    
    info!("开始增量计算余额，共 {} 笔新交易", new_transactions.len());
    
    // 收集所有涉及的账户
    let mut affected_accounts = std::collections::HashSet::new();
    
    // 从交易中提取所有相关账户
    for tx in new_transactions {
        match tx.kind.as_str() {
            "transfer" => {
                if let Some(ref transfer) = tx.transfer {
                    affected_accounts.insert(transfer.from.to_string());
                    affected_accounts.insert(transfer.to.to_string());
                    // 处理transferFrom的代理地址
                    if let Some(ref spender) = transfer.spender {
                        affected_accounts.insert(spender.to_string());
                    }
                }
            },
            "mint" => {
                if let Some(ref mint) = tx.mint {
                    affected_accounts.insert(mint.to.to_string());
                }
            },
            "burn" => {
                if let Some(ref burn) = tx.burn {
                    affected_accounts.insert(burn.from.to_string());
                    // 处理授权销毁的代理地址
                    if let Some(ref spender) = burn.spender {
                        affected_accounts.insert(spender.to_string());
                    }
                }
            },
            "approve" => {
                if let Some(ref approve) = tx.approve {
                    affected_accounts.insert(approve.from.to_string());
                    affected_accounts.insert(approve.spender.to_string());
                }
            },
            "notify" => {
                // ICRC-3通知事件处理
                debug!("检测到通知事件，但ICRC-3实现尚未完成");
            },
            _ => {
                warn!("未知交易类型: {}, 跳过账户提取", tx.kind);
            }
        }
    }
    
    debug!("找到 {} 个受影响的账户需要更新余额", affected_accounts.len());
    
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    let mut total_anomalies = 0u64;
    
    // 顺序处理每个受影响的账户，但使用账户锁确保并发安全
    
    // 处理每个受影响的账户
    for account in affected_accounts {
        // 获取账户锁
        let account_lock = get_account_lock(&account).await;
        
        // 获取账户锁，确保在更新余额期间只有一个线程操作此账户
        let _guard = account_lock.lock().await;
        debug!("获取账户 {} 的锁", account);
        
        // 查询账户交易索引
        let account_doc = match accounts_col.find_one(doc! { "account": &account }, None).await {
            Ok(Some(doc)) => doc,
            Ok(None) => {
                error!("找不到账户 {} 的记录", account);
                error_count += 1;
                continue;
            },
            Err(e) => {
                error!("查询账户 {} 时出错: {}", account, e);
                error_count += 1;
                continue;
            }
        };
        
        // 获取该账户的所有交易索引
        let tx_indices: Vec<i64> = if let Some(indices) = account_doc.get("transaction_indices") {
            if let Bson::Array(arr) = indices {
                arr.iter().filter_map(|b| match b {
                    Bson::Int64(i) => Some(*i),
                    Bson::Int32(i) => Some(i64::from(*i)),
                    _ => None,
                }).collect()
            } else {
                error!("账户 {} 的交易索引不是数组格式", account);
                error_count += 1;
                continue;
            }
        } else {
            error!("无法获取账户 {} 的交易索引", account);
            error_count += 1;
            continue;
        };
        
        if tx_indices.is_empty() {
            debug!("账户 {} 没有交易记录", account);
            continue;
        }
        
        // 计算该账户的余额
        match calculate_account_balance(&account, &tx_indices, tx_col, token_decimals, anomalies_col).await {
            Ok((balance, has_anomalies)) => {
                // 更新余额记录
                match save_account_balance(balances_col, &account, &balance).await {
                    Ok(_) => {
                        success_count += 1;
                        if has_anomalies {
                            total_anomalies += 1;
                            info!("账户 {} 在余额计算中检测到异常，已记录详细信息", account);
                        }
                    },
                    Err(e) => {
                        error!("保存账户 {} 余额失败: {}", account, e);
                        error_count += 1;
                    }
                }
            },
            Err(e) => {
                error!("计算账户 {} 余额失败: {}", account, e);
                error_count += 1;
            }
        }
    }
    
    info!("增量余额计算完成: 更新 {} 个账户, 失败 {} 个账户, 检测到 {} 个余额异常", 
          success_count, error_count, total_anomalies);
    
    // 重新计算并保存总供应量
    supply::recalculate_total_supply(balances_col, supply_col).await?;
   
    Ok((success_count, error_count))
}

/// 计算单个账户的余额
async fn calculate_account_balance(
    account: &str,
    tx_indices: &[i64],
    tx_col: &Collection<Document>,
    token_decimals: u8,
    anomalies_col: &Collection<Document>,
) -> Result<(Nat, bool), Box<dyn Error>> {
    // 规范化账户ID
    let normalized_account = normalize_account_id(account);
    let mut balance = Nat::from(0u64);
    let mut processed_count = 0u64;
    let mut has_anomalies = false;
    
    // 查询与该账户相关的所有交易
    let filter = doc! { 
        "index": { "$in": tx_indices }
    };
    
    let options = FindOptions::builder()
        .sort(doc! { "index": 1 }) // 按交易索引排序，确保按时间顺序处理
        .build();
    
    let mut tx_cursor = tx_col.find(filter, options).await?;
    
    // 遍历处理每一笔交易
    while tx_cursor.advance().await? {
        let raw_doc = tx_cursor.current();
        // 转换为Document类型
        let tx_doc = Document::try_from(raw_doc.to_owned())?;
        
        // 反序列化为交易对象 - 使用克隆避免所有权移动
        let tx: Transaction = match mongodb::bson::from_document(tx_doc.clone()) {
            Ok(transaction) => transaction,
            Err(e) => {
                error!("反序列化交易失败: {}", e);
                continue;
            }
        };
        
        // 获取交易索引，用于记录异常
        let tx_index = tx.index.unwrap_or(0);
        
        // 检查交易状态 - 如果存在status字段且不是"COMPLETED"或"SUCCESS"，则跳过
        if let Some(status) = tx_doc.get_str("status").ok() {
            if status != "COMPLETED" && status != "SUCCESS" {
                let index = tx.index.unwrap_or(0);
                debug!("跳过未完成的交易 [索引:{}] [状态:{}]", index, status);
                
                // 记录交易类型以便更好地分析
                match tx.kind.as_str() {
                    "transfer" => {
                        if let Some(ref transfer) = tx.transfer {
                            debug!("  - 跳过的转账交易: {} -> {} [金额:{}]",
                                transfer.from, transfer.to, transfer.amount.0);
                        }
                    },
                    "mint" => {
                        if let Some(ref mint) = tx.mint {
                            debug!("  - 跳过的铸币交易: 接收方:{} [金额:{}]",
                                mint.to, mint.amount.0);
                        }
                    },
                    "burn" => {
                        if let Some(ref burn) = tx.burn {
                            debug!("  - 跳过的销毁交易: 发送方:{} [金额:{}]",
                                burn.from, burn.amount.0);
                        }
                    },
                    "approve" => {
                        if let Some(ref approve) = tx.approve {
                            debug!("  - 跳过的授权交易: {} 授权给 {} [金额:{}]",
                                approve.from, approve.spender, approve.amount.0);
                        }
                    },
                    _ => {
                        debug!("  - 跳过的未知类型交易: {}", tx.kind);
                    }
                }
                continue;
            }
        }
        
        // 检查账户格式是否包含子账户
        let account_parts: Vec<&str> = normalized_account.split(':').collect();
        // 添加前导下划线避免未使用变量警告
        let _principal_id = account_parts[0];
        let _subaccount_hex = if account_parts.len() > 1 { Some(account_parts[1]) } else { None };
        
        // 根据交易类型和账户角色计算余额变化
        match tx.kind.as_str() {
            "transfer" => {
                if let Some(ref transfer) = tx.transfer {
                    let from_account = transfer.from.to_string();
                    let to_account = transfer.to.to_string();
                    
                    // 验证账户匹配，考虑子账户
                    let is_from = account_match(&from_account, &normalized_account);
                    let is_to = account_match(&to_account, &normalized_account);
                    
                    // 检查是否是transferFrom操作 (当spender字段存在时)
                    let is_spender = if let Some(ref spender) = transfer.spender {
                        account_match(&spender.to_string(), &normalized_account)
                    } else {
                        false
                    };
                    
                    // 如果是发送方，减少余额
                    if is_from {
                        // 先创建错误消息，避免借用冲突
                        let error_msg = format!("账户 {} 的余额不足，当前余额: {}, 转账金额: {}", 
                                              normalized_account, balance.0, transfer.amount.0);
                        // 安全扣减余额，确保不会变成负数
                        if let Ok(anomaly) = safe_subtract_balance_with_logging(
                            &mut balance, 
                            &transfer.amount, 
                            &error_msg,
                            &normalized_account,
                            tx_index,
                            "transfer",
                            anomalies_col
                        ).await {
                            has_anomalies = has_anomalies || anomaly;
                        }
                        
                        // 减去手续费
                        if let Some(ref fee) = transfer.fee {
                            if !fee.0.is_zero() {
                                let fee_error_msg = format!("账户 {} 的余额不足以支付手续费，当前余额: {}, 手续费: {}", 
                                                         normalized_account, balance.0, fee.0);
                                if let Ok(anomaly) = safe_subtract_balance_with_logging(
                                    &mut balance, 
                                    fee, 
                                    &fee_error_msg,
                                    &normalized_account,
                                    tx_index,
                                    "transfer_fee",
                                    anomalies_col
                                ).await {
                                    has_anomalies = has_anomalies || anomaly;
                                }
                            }
                        }
                    }
                    
                    // 如果是接收方，增加余额
                    if is_to {
                        balance = balance + transfer.amount.clone();
                    }
                    
                    // 如果是spender (转账授权代理)，则不直接影响余额
                    if is_spender {
                        debug!("账户 {} 作为授权代理执行了从 {} 到 {} 的转账，金额: {}", 
                                normalized_account, from_account, to_account, transfer.amount.0);
                    }
                }
            },
            "mint" => {
                if let Some(ref mint) = tx.mint {
                    let to_account = mint.to.to_string();
                    
                    // 如果是接收方，增加余额
                    if account_match(&to_account, &normalized_account) {
                        balance = balance + mint.amount.clone();
                    }
                }
            },
            "burn" => {
                if let Some(ref burn) = tx.burn {
                    let from_account = burn.from.to_string();
                    
                    // 检查是否是授权销毁
                    let is_spender = if let Some(ref spender) = burn.spender {
                        account_match(&spender.to_string(), &normalized_account)
                    } else {
                        false
                    };
                    
                    // 如果是发送方，减少余额
                    if account_match(&from_account, &normalized_account) {
                        let error_msg = format!("账户 {} 的余额不足，当前余额: {}, 销毁金额: {}", 
                                              normalized_account, balance.0, burn.amount.0);
                        if let Ok(anomaly) = safe_subtract_balance_with_logging(
                            &mut balance, 
                            &burn.amount, 
                            &error_msg,
                            &normalized_account,
                            tx_index,
                            "burn",
                            anomalies_col
                        ).await {
                            has_anomalies = has_anomalies || anomaly;
                        }
                    }
                    
                    // 记录spender操作
                    if is_spender {
                        debug!("账户 {} 作为授权代理执行了从 {} 销毁代币的操作，金额: {}", 
                                normalized_account, from_account, burn.amount.0);
                    }
                }
            },
            "approve" => {
                // approve操作不直接影响余额，只是授权
                // 但如果有手续费，需要从发送方扣除
                if let Some(ref approve) = tx.approve {
                    let from_account = approve.from.to_string();
                    
                    if account_match(&from_account, &normalized_account) {
                        if let Some(ref fee) = approve.fee {
                            if !fee.0.is_zero() {
                                let fee_error_msg = format!("账户 {} 的余额不足以支付授权手续费，当前余额: {}, 手续费: {}", 
                                                         normalized_account, balance.0, fee.0);
                                if let Ok(anomaly) = safe_subtract_balance_with_logging(
                                    &mut balance, 
                                    fee, 
                                    &fee_error_msg,
                                    &normalized_account,
                                    tx_index,
                                    "approve_fee",
                                    anomalies_col
                                ).await {
                                    has_anomalies = has_anomalies || anomaly;
                                }
                            }
                        }
                    }
                }
            },
            "notify" => {
                // 处理ICRC-3标准的通知事件
                debug!("处理通知事件 (索引:{}), 目前通知事件不影响余额", tx.index.unwrap_or(0));
            },
            _ => {
                warn!("未知交易类型: {}, 跳过余额计算 (索引:{})", tx.kind, tx.index.unwrap_or(0));
            }
        }
        
        processed_count += 1;
    }
    
    // 使用更精简的日志格式
    debug!("已完成 {} 余额计算，共 {} 笔交易，余额：{} ({} 代币)", 
           normalized_account, processed_count, balance.0, format_token_amount(&balance, token_decimals));
           
    if has_anomalies {
        info!("账户 {} 在余额计算中检测到异常，已记录详细信息", normalized_account);
    }
    
    Ok((balance, has_anomalies))
}

/// 安全减少余额，确保不会变成负数
/// 如果余额不足，将记录异常情况
async fn safe_subtract_balance_with_logging(
    balance: &mut Nat,
    amount: &Nat,
    warning_msg: &str,
    account: &str,
    tx_index: u64,
    tx_type: &str,
    anomalies_col: &Collection<Document>
) -> Result<bool, Box<dyn Error>> {
    let mut anomaly_detected = false;
    
    if *balance >= *amount {
        *balance = balance.clone() - amount.clone();
    } else {
        warn!("警告: {}", warning_msg);
        
        // 记录余额异常
        let anomaly = BalanceAnomaly {
            account: account.to_string(),
            tx_index,
            tx_type: tx_type.to_string(),
            anomaly_type: "insufficient_balance".to_string(),
            balance: balance.0.to_string(),
            amount: amount.0.to_string(),
            description: warning_msg.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };
        
        // 将异常记录保存到数据库
        if let Err(e) = log_balance_anomaly(anomalies_col, &anomaly).await {
            error!("记录余额异常失败: {}", e);
        } else {
            anomaly_detected = true;
        }
        
        *balance = Nat::from(0u64);
    }
    
    Ok(anomaly_detected)
}

/// 保存余额异常记录到数据库
async fn log_balance_anomaly(
    anomalies_col: &Collection<Document>,
    anomaly: &BalanceAnomaly
) -> Result<(), Box<dyn Error>> {
    let anomaly_doc = mongodb::bson::to_document(anomaly)?;
    
    match anomalies_col.insert_one(anomaly_doc, None).await {
        Ok(_) => {
            debug!("已记录账户 {} 的余额异常 (交易索引: {})", anomaly.account, anomaly.tx_index);
            Ok(())
        },
        Err(e) => {
            error!("保存余额异常记录失败: {}", e);
            Err(create_error(&format!("保存余额异常记录失败: {}", e)))
        }
    }
}

/// 安全减法：从余额中减去金额，如果余额不足则记录警告并设为0
#[allow(dead_code)]
fn safe_subtract_balance(balance: &mut Nat, amount: &Nat, warning_msg: &str) {
    if *balance >= *amount {
        *balance = balance.clone() - amount.clone();
    } else {
        warn!("警告: {}", warning_msg);
        *balance = Nat::from(0u64);
    }
}

/// 检查两个账户是否匹配，考虑子账户
fn account_match(account1: &str, account2: &str) -> bool {
    if account1 == account2 {
        return true;
    }
    
    // 拆分账户字符串，检查principal和子账户
    let parts1: Vec<&str> = account1.split(':').collect();
    let parts2: Vec<&str> = account2.split(':').collect();
    
    // 检查principal是否一致
    if parts1[0] != parts2[0] {
        return false;
    }
    
    // 检查子账户是否匹配
    let sub1 = if parts1.len() > 1 { Some(parts1[1]) } else { None };
    let sub2 = if parts2.len() > 1 { Some(parts2[1]) } else { None };
    
    match (sub1, sub2) {
        // 两者都没有子账户
        (None, None) => true,
        
        // 一方有子账户，另一方没有子账户
        (Some(s), None) => is_default_subaccount(s),
        (None, Some(s)) => is_default_subaccount(s),
        
        // 两方都有子账户，直接比较
        (Some(s1), Some(s2)) => s1 == s2 || (is_default_subaccount(s1) && is_default_subaccount(s2)),
    }
}

/// 检查子账户是否为默认子账户（全0）
fn is_default_subaccount(subaccount: &str) -> bool {
    // 去掉"0x"前缀，检查剩余部分是否全为0
    let subaccount = subaccount.trim_start_matches("0x");
    // 子账户标准长度为32字节(64个十六进制字符)
    if subaccount.len() != 64 {
        return false;
    }
    
    subaccount.chars().all(|c| c == '0')
}

/// 保存账户余额到数据库
async fn save_account_balance(
    balances_col: &Collection<Document>,
    account: &str,
    balance: &Nat,
) -> Result<(), Box<dyn Error>> {
    // 规范化账户格式
    let normalized_account = normalize_account_id(account);
    
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        // 更新余额
        match balances_col.update_one(
            doc! { "account": &normalized_account },
            doc! {
                "$set": {
                    "account": &normalized_account,
                    "balance": balance.0.to_string(),
                    "last_updated": (chrono::Utc::now().timestamp() as i64),
                }
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        ).await {
            Ok(_) => {
                // 如果账户被规范化了，记录一下
                if normalized_account != account {
                    debug!("账户 {} 已规范化为 {}", account, normalized_account);
                }
                return Ok(());
            },
            Err(e) => {
                retry_count += 1;
                let wait_time = Duration::from_millis(500 * retry_count);
                warn!("更新账户余额失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&format!("更新账户 {} 余额失败，已重试 {} 次", normalized_account, max_retries)))
}

/// 规范化账户ID，去除全0子账户
pub fn normalize_account_id(account: &str) -> String {
    // 拆分账户字符串，检查principal和子账户
    let parts: Vec<&str> = account.split(':').collect();
    
    // 如果没有子账户部分，直接返回
    if parts.len() <= 1 {
        return account.to_string();
    }
    
    // 检查子账户是否为默认子账户（全0）
    if is_default_subaccount(parts[1]) {
        // 只返回principal部分
        return parts[0].to_string();
    }
    
    // 其他情况，保持原样
    account.to_string()
}
