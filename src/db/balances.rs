use std::error::Error;
use std::str::FromStr;
use mongodb::{Collection, bson::doc};
use mongodb::bson::Document;
use tokio::time::Duration;
use candid::Nat;
use num_traits::Zero;
use crate::models::Transaction;
use crate::utils::{create_error, format_token_amount};

/// 更新账户余额
pub async fn update_account_balance(
    balances_col: &Collection<Document>,
    account: &str,
    amount: &Nat,
    timestamp: u64,
    tx_index: u64,
    is_credit: bool, // true表示增加余额，false表示减少余额
    token_decimals: u8, // 代币小数位数
) -> Result<(), Box<dyn Error>> {
    if account.trim().is_empty() {
        println!("账户为空，跳过更新余额");
        return Ok(());
    }
    
    // 设置重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        // 首先获取当前余额
        let current_balance_doc = balances_col
            .find_one(doc! { "account": account }, None)
            .await?;
        
        let new_balance = if let Some(doc) = current_balance_doc {
            // 已存在余额记录
            let current_balance_str = doc.get_str("balance").unwrap_or("0").to_string();
            let current_balance = Nat::from_str(&current_balance_str).unwrap_or(Nat::from(0u64));
            
            if is_credit {
                // 增加余额
                current_balance + amount.clone()
            } else {
                // 减少余额，确保不会出现负数
                if current_balance >= *amount {
                    current_balance - amount.clone()
                } else {
                    // 如果当前余额小于要减去的金额，记录一个警告并设为0
                    println!("警告: 账户 {} 的余额 {} ({} 代币) 小于要减去的金额 {} ({} 代币), 设置为0", 
                        account, current_balance.0, format_token_amount(&current_balance, token_decimals),
                        amount.0, format_token_amount(amount, token_decimals));
                    Nat::from(0u64)
                }
            }
        } else {
            // 如果是新账户且金额为正，创建新记录
            if is_credit {
                amount.clone()
            } else {
                // 如果是扣款操作但账户不存在，记录警告并设为0
                println!("警告: 试图从不存在的账户 {} 扣除 {} ({} 代币), 设置为0", 
                    account, amount.0, format_token_amount(amount, token_decimals));
                Nat::from(0u64)
            }
        };
        
        // 更新余额
        match balances_col.update_one(
            doc! { "account": account },
            doc! {
                "$set": {
                    "account": account,
                    "balance": new_balance.0.to_string(),
                    "last_updated": (timestamp as i64),
                    "last_tx_index": (tx_index as i64)
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

/// 处理单个交易并更新相关账户的余额
pub async fn process_transaction_balance(
    balances_col: &Collection<Document>,
    tx: &Transaction,
    token_decimals: u8,
) -> Result<(), Box<dyn Error>> {
    let index = tx.index.unwrap_or(0);
    let timestamp = tx.timestamp;
    
    match tx.kind.as_str() {
        "transfer" => {
            if let Some(ref transfer) = tx.transfer {
                // 从发送方减少余额（包括转账金额）
                let from_account = transfer.from.to_string();
                update_account_balance(
                    balances_col,
                    &from_account,
                    &transfer.amount,
                    timestamp,
                    index,
                    false, // 减少余额
                    token_decimals,
                ).await?;
                
                // 给接收方增加余额
                let to_account = transfer.to.to_string();
                update_account_balance(
                    balances_col,
                    &to_account,
                    &transfer.amount,
                    timestamp,
                    index,
                    true, // 增加余额
                    token_decimals,
                ).await?;
                
                // 如果有手续费，从发送方额外扣除
                if let Some(ref fee) = transfer.fee {
                    if !fee.0.is_zero() {
                        update_account_balance(
                            balances_col,
                            &from_account,
                            fee,
                            timestamp,
                            index,
                            false, // 减少余额
                            token_decimals,
                        ).await?;
                    }
                }
            }
        },
        "mint" => {
            if let Some(ref mint) = tx.mint {
                // 铸币操作，增加接收方余额
                let to_account = mint.to.to_string();
                update_account_balance(
                    balances_col,
                    &to_account,
                    &mint.amount,
                    timestamp,
                    index,
                    true, // 增加余额
                    token_decimals,
                ).await?;
            }
        },
        "burn" => {
            if let Some(ref burn) = tx.burn {
                // 销毁操作，减少发送方余额
                let from_account = burn.from.to_string();
                update_account_balance(
                    balances_col,
                    &from_account,
                    &burn.amount,
                    timestamp,
                    index,
                    false, // 减少余额
                    token_decimals,
                ).await?;
            }
        },
        "approve" => {
            // approve操作不影响余额，只是授权其他账户可以代表发送方转账
            // 但如果有手续费，需要扣除
            if let Some(ref approve) = tx.approve {
                if let Some(ref fee) = approve.fee {
                    if !fee.0.is_zero() {
                        let from_account = approve.from.to_string();
                        update_account_balance(
                            balances_col,
                            &from_account,
                            fee,
                            timestamp,
                            index,
                            false, // 减少余额
                            token_decimals,
                        ).await?;
                    }
                }
            }
        },
        _ => {
            println!("未知交易类型: {}, 跳过余额更新", tx.kind);
        }
    }
    
    Ok(())
}

/// 处理一批交易并更新相关账户余额
pub async fn process_batch_balances(
    balances_col: &Collection<Document>,
    transactions: &[Transaction],
    token_decimals: u8,
) -> Result<(u32, u32), Box<dyn Error>> {
    let mut success_count = 0;
    let mut error_count = 0;
    
    // 按交易索引排序，确保按照时间顺序处理
    let mut ordered_transactions = transactions.to_vec();
    ordered_transactions.sort_by_key(|tx| tx.index.unwrap_or(0));
    
    for tx in &ordered_transactions {
        match process_transaction_balance(balances_col, tx, token_decimals).await {
            Ok(_) => {
                success_count += 1;
            },
            Err(e) => {
                println!("处理交易(索引:{})的余额更新失败: {}", tx.index.unwrap_or(0), e);
                error_count += 1;
            }
        }
    }
    
    Ok((success_count, error_count))
}

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
    
    // 如果没有找到账户或者余额字段，返回0
    Ok("0".to_string())
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