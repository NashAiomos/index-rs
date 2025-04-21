use std::error::Error;
use mongodb::{Collection};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::FindOptions;
use tokio::time::Duration;
use candid::Nat;
use num_traits::Zero;
use crate::models::Transaction;
use crate::utils::{create_error, format_token_amount};

/// 查询账户余额
pub async fn get_account_balance(
    balances_col: &Collection<Document>,
    account: &str,
) -> Result<String, Box<dyn Error>> {
    if let Some(doc) = balances_col
        .find_one(doc! { "account": account }, None)
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
            println!("已清除 {} 条余额记录", result.deleted_count);
            Ok(result.deleted_count)
        },
        Err(e) => {
            println!("清除余额集合失败: {}", e);
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
    token_decimals: u8,
) -> Result<(u64, u64), Box<dyn Error>> {
    println!("开始计算所有账户余额...");
    
    // 首先清空余额集合
    clear_balances(balances_col).await?;
    
    // 查询所有账户
    let mut accounts_cursor = accounts_col.find(doc! {}, None).await?;
    
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    
    // 遍历所有账户
    while accounts_cursor.advance().await? {
        let raw_doc = accounts_cursor.current();
        // 转换为Document类型
        let account_doc = Document::try_from(raw_doc.to_owned())?;
        
        let account = match account_doc.get_str("account") {
            Ok(acc) => acc.to_string(),
            Err(e) => {
                println!("无法获取账户信息: {}", e);
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
                println!("账户 {} 的交易索引不是数组格式", account);
                error_count += 1;
                continue;
            }
        } else {
            println!("无法获取账户 {} 的交易索引", account);
            error_count += 1;
            continue;
        };
        
        if tx_indices.is_empty() {
            println!("账户 {} 没有交易记录", account);
            continue;
        }
        
        println!("正在计算账户 {} 的余额，共有 {} 笔交易", account, tx_indices.len());
        
        // 计算该账户的余额
        match calculate_account_balance(&account, &tx_indices, tx_col, token_decimals).await {
            Ok(balance) => {
                // 更新余额记录
                match save_account_balance(balances_col, &account, &balance).await {
                    Ok(_) => {
                        println!("账户 {} 余额计算完成: {} ({} 代币)", 
                                account, balance.0, format_token_amount(&balance, token_decimals));
                        success_count += 1;
                    },
                    Err(e) => {
                        println!("保存账户 {} 余额失败: {}", account, e);
                        error_count += 1;
                    }
                }
            },
            Err(e) => {
                println!("计算账户 {} 余额失败: {}", account, e);
                error_count += 1;
            }
        }
    }
    
    println!("余额计算完成: 成功 {} 个账户, 失败 {} 个账户", success_count, error_count);
    Ok((success_count, error_count))
}

/// 增量计算余额 - 只处理新同步的交易
/// 计算新交易对相关账户余额的影响，而不是重新计算所有账户的余额
pub async fn calculate_incremental_balances(
    new_transactions: &[Transaction],
    tx_col: &Collection<Document>,
    accounts_col: &Collection<Document>,
    balances_col: &Collection<Document>,
    token_decimals: u8,
) -> Result<(u64, u64), Box<dyn Error>> {
    if new_transactions.is_empty() {
        println!("没有新交易需要计算余额");
        return Ok((0, 0));
    }
    
    println!("开始增量计算余额，共 {} 笔新交易", new_transactions.len());
    
    // 收集所有涉及的账户
    let mut affected_accounts = std::collections::HashSet::new();
    
    // 从交易中提取所有相关账户
    for tx in new_transactions {
        match tx.kind.as_str() {
            "transfer" => {
                if let Some(ref transfer) = tx.transfer {
                    affected_accounts.insert(transfer.from.to_string());
                    affected_accounts.insert(transfer.to.to_string());
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
                }
            },
            "approve" => {
                if let Some(ref approve) = tx.approve {
                    affected_accounts.insert(approve.from.to_string());
                }
            },
            _ => {}
        }
    }
    
    println!("找到 {} 个受影响的账户需要更新余额", affected_accounts.len());
    
    let mut success_count = 0u64;
    let mut error_count = 0u64;
    
    // 处理每个受影响的账户
    for account in affected_accounts {
        let account_doc = match accounts_col.find_one(doc! { "account": &account }, None).await? {
            Some(doc) => doc,
            None => {
                println!("找不到账户 {} 的记录", account);
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
                println!("账户 {} 的交易索引不是数组格式", account);
                error_count += 1;
                continue;
            }
        } else {
            println!("无法获取账户 {} 的交易索引", account);
            error_count += 1;
            continue;
        };
        
        if tx_indices.is_empty() {
            println!("账户 {} 没有交易记录", account);
            continue;
        }
        
        println!("正在重新计算账户 {} 的余额，共有 {} 笔交易", account, tx_indices.len());
        
        // 计算该账户的余额
        match calculate_account_balance(&account, &tx_indices, tx_col, token_decimals).await {
            Ok(balance) => {
                // 更新余额记录
                match save_account_balance(balances_col, &account, &balance).await {
                    Ok(_) => {
                        println!("账户 {} 余额更新完成: {} ({} 代币)", 
                                account, balance.0, format_token_amount(&balance, token_decimals));
                        success_count += 1;
                    },
                    Err(e) => {
                        println!("保存账户 {} 余额失败: {}", account, e);
                        error_count += 1;
                    }
                }
            },
            Err(e) => {
                println!("计算账户 {} 余额失败: {}", account, e);
                error_count += 1;
            }
        }
    }
    
    println!("增量余额计算完成: 成功更新 {} 个账户, 失败 {} 个账户", success_count, error_count);
    Ok((success_count, error_count))
}

/// 计算单个账户的余额
async fn calculate_account_balance(
    account: &str,
    tx_indices: &[i64],
    tx_col: &Collection<Document>,
    _token_decimals: u8,
) -> Result<Nat, Box<dyn Error>> {
    let mut balance = Nat::from(0u64);
    let mut processed_count = 0u64;
    
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
        
        // 反序列化为交易对象
        let tx: Transaction = match mongodb::bson::from_document(tx_doc) {
            Ok(transaction) => transaction,
            Err(e) => {
                println!("反序列化交易失败: {}", e);
                continue;
            }
        };
        
        // 根据交易类型和账户角色计算余额变化
        match tx.kind.as_str() {
            "transfer" => {
                if let Some(ref transfer) = tx.transfer {
                    let from_account = transfer.from.to_string();
                    let to_account = transfer.to.to_string();
                    
                    // 如果是发送方，减少余额
                    if from_account == account {
                        // 减去转账金额
                        if balance >= transfer.amount {
                            balance = balance - transfer.amount.clone();
                        } else {
                            println!("警告: 账户 {} 的余额不足，当前余额: {}, 转账金额: {}", 
                                    account, balance.0, transfer.amount.0);
                            balance = Nat::from(0u64);
                        }
                        
                        // 减去手续费
                        if let Some(ref fee) = transfer.fee {
                            if !fee.0.is_zero() {
                                if balance >= *fee {
                                    balance = balance - fee.clone();
                                } else {
                                    println!("警告: 账户 {} 的余额不足以支付手续费，当前余额: {}, 手续费: {}", 
                                            account, balance.0, fee.0);
                                    balance = Nat::from(0u64);
                                }
                            }
                        }
                    }
                    
                    // 如果是接收方，增加余额
                    if to_account == account {
                        balance = balance + transfer.amount.clone();
                    }
                }
            },
            "mint" => {
                if let Some(ref mint) = tx.mint {
                    let to_account = mint.to.to_string();
                    
                    // 如果是接收方，增加余额
                    if to_account == account {
                        balance = balance + mint.amount.clone();
                    }
                }
            },
            "burn" => {
                if let Some(ref burn) = tx.burn {
                    let from_account = burn.from.to_string();
                    
                    // 如果是发送方，减少余额
                    if from_account == account {
                        if balance >= burn.amount {
                            balance = balance - burn.amount.clone();
                        } else {
                            println!("警告: 账户 {} 的余额不足，当前余额: {}, 销毁金额: {}", 
                                    account, balance.0, burn.amount.0);
                            balance = Nat::from(0u64);
                        }
                    }
                }
            },
            "approve" => {
                // approve操作不直接影响余额，只是授权
                // 但如果有手续费，需要从发送方扣除
                if let Some(ref approve) = tx.approve {
                    let from_account = approve.from.to_string();
                    
                    if from_account == account {
                        if let Some(ref fee) = approve.fee {
                            if !fee.0.is_zero() {
                                if balance >= *fee {
                                    balance = balance - fee.clone();
                                } else {
                                    println!("警告: 账户 {} 的余额不足以支付授权手续费，当前余额: {}, 手续费: {}", 
                                            account, balance.0, fee.0);
                                    balance = Nat::from(0u64);
                                }
                            }
                        }
                    }
                }
            },
            _ => {
                println!("未知交易类型: {}, 跳过余额计算", tx.kind);
            }
        }
        
        processed_count += 1;
    }
    
    println!("账户 {} 处理了 {} 笔交易，最终余额: {}", account, processed_count, balance.0);
    Ok(balance)
}

/// 保存账户余额到数据库
async fn save_account_balance(
    balances_col: &Collection<Document>,
    account: &str,
    balance: &Nat,
) -> Result<(), Box<dyn Error>> {
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        // 更新余额
        match balances_col.update_one(
            doc! { "account": account },
            doc! {
                "$set": {
                    "account": account,
                    "balance": balance.0.to_string(),
                    "last_updated": (chrono::Utc::now().timestamp() as i64),
                }
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        ).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                retry_count += 1;
                let wait_time = Duration::from_millis(500 * retry_count);
                println!("更新账户余额失败 (尝试 {}/{}): {}，等待 {:?} 后重试",
                    retry_count, max_retries, e, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&format!("更新账户 {} 余额失败，已重试 {} 次", account, max_retries)))
}

