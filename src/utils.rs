use std::error::Error;
use std::collections::HashMap;
use candid::Nat;
use crate::models::{Transaction};

/// 格式化代币金额为人类可读形式
pub fn format_token_amount(amount: &Nat, decimals: u8) -> String {
    let amount_str = amount.0.to_string();
    
    if decimals == 0 {
        return amount_str;
    }
    
    // 确保金额字符串长度足够
    let padded_amount = if amount_str.len() <= decimals as usize {
        format!("{:0>width$}", amount_str, width = decimals as usize + 1)
    } else {
        amount_str.clone()
    };
    
    // 插入小数点
    let len = padded_amount.len();
    let decimal_pos = len - decimals as usize;
    
    if decimal_pos == 0 {
        format!("0.{}", padded_amount)
    } else {
        let (integer_part, decimal_part) = padded_amount.split_at(decimal_pos);
        format!("{}.{}", integer_part, decimal_part)
    }
}

/// 将交易按账户分组
pub fn group_transactions_by_account(transactions: &[Transaction]) -> HashMap<String, Vec<&Transaction>> {
    let mut map: HashMap<String, Vec<&Transaction>> = HashMap::new();
    for tx in transactions {
        // 收集所有相关账户
        let mut accounts = Vec::new();
        if let Some(ref transfer) = tx.transfer {
            accounts.push(transfer.from.to_string());
            accounts.push(transfer.to.to_string());
            if let Some(ref spender) = transfer.spender {
                accounts.push(spender.to_string());
            }
        }
        if let Some(ref mint) = tx.mint {
            accounts.push(mint.to.to_string());
        }
        if let Some(ref approve) = tx.approve {
            accounts.push(approve.from.to_string());
            accounts.push(approve.spender.to_string());
        }
        if let Some(ref burn) = tx.burn {
            accounts.push(burn.from.to_string());
            if let Some(ref spender) = burn.spender {
                accounts.push(spender.to_string());
            }
        }
        // 去重
        accounts.sort();
        accounts.dedup();
        for acc in accounts {
            map.entry(acc).or_default().push(tx);
        }
    }
    map
}

/// 创建错误
pub fn create_error(message: &str) -> Box<dyn Error> {
    Box::new(std::io::Error::new(std::io::ErrorKind::Other, message))
}
