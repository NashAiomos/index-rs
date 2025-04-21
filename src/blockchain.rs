use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use candid::{Encode, Decode};
use num_traits::ToPrimitive;
use crate::models::{
    ArchivesResult, ArchiveInfo, GetTransactionsArg, Transaction, 
    LedgerGetTransactionsResult, SimpleTransactionRange,
    TransactionList
};
use crate::utils::create_error;
use tokio::time::Duration;

/// 查询archives方法获取归档信息
pub async fn fetch_archives(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<Vec<ArchiveInfo>, Box<dyn Error>> {
    let empty_tuple = ();
    let arg_bytes = Encode!(&empty_tuple)?;
    let response = agent.query(canister_id, "archives")
        .with_arg(arg_bytes)
        .call()
        .await?;
    
    let archives_result: ArchivesResult = Decode!(&response, ArchivesResult)?;
    Ok(archives_result.0)
}

/// 从归档canister获取交易
pub async fn fetch_archive_transactions(
    agent: &Agent,
    archive_canister_id: &Principal,
    start: u64,
    length: u64,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    println!("从归档canister获取交易: start={}, length={}", start, length);
    
    if length == 0 {
        println!("请求长度为0，返回空交易列表");
        return Ok(Vec::new());
    }
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("编码参数失败: {}", e);
            return Err(create_error(&format!("参数编码失败: {}", e)));
        }
    };
    
    println!("调用归档canister: {}", archive_canister_id);
    
    // 添加重试逻辑
    let max_retries = 3;
    let mut retry_count = 0;
    let mut last_error = None;
    
    while retry_count < max_retries {
        match agent.query(archive_canister_id, "get_transactions")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
                println!("收到归档canister响应，长度: {} 字节", response.len());
                
                // 尝试多种可能的结构解码方式
                
                // 1. 首先尝试解码为SimpleTransactionRange（调整顺序，优先尝试）
                println!("尝试解码为SimpleTransactionRange...");
                if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
                    println!("成功解码为SimpleTransactionRange，交易数量: {}", range.transactions.len());
                    
                    // 给交易添加索引信息
                    let mut indexed_transactions = Vec::new();
                    for (i, mut tx) in range.transactions.into_iter().enumerate() {
                        let index = start + i as u64;
                        tx.index = Some(index);
                        indexed_transactions.push(tx);
                    }
                    
                    return Ok(indexed_transactions);
                }
                
                // 2. 尝试解码为TransactionList(Vec<Transaction>)
                println!("尝试解码为TransactionList...");
                if let Ok(list) = Decode!(&response, TransactionList) {
                    println!("成功解码为TransactionList，交易数量: {}", list.0.len());
                    
                    // 给交易添加索引信息
                    let mut indexed_transactions = Vec::new();
                    for (i, mut tx) in list.0.into_iter().enumerate() {
                        let index = start + i as u64;
                        tx.index = Some(index);
                        indexed_transactions.push(tx);
                    }
                    
                    return Ok(indexed_transactions);
                }
                
                // 3. 尝试直接解码为Vec<Transaction>
                println!("尝试解码为Vec<Transaction>...");
                if let Ok(transactions) = Decode!(&response, Vec<Transaction>) {
                    println!("成功解码为Vec<Transaction>，交易数量: {}", transactions.len());
                    
                    // 给交易添加索引信息
                    let mut indexed_transactions = Vec::new();
                    for (i, mut tx) in transactions.into_iter().enumerate() {
                        let index = start + i as u64;
                        tx.index = Some(index);
                        indexed_transactions.push(tx);
                    }
                    
                    return Ok(indexed_transactions);
                }
                
                // 所有解码方法都失败，但API调用成功了，重试可能没用
                println!("所有解码方法都失败，返回空交易列表");
                return Ok(Vec::new());
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = Duration::from_secs(2 * retry_count); // 指数退避
                println!("调用归档canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    println!("达到最大重试次数 ({}), 调用归档canister失败", max_retries);
    Err(create_error(&format!("调用归档canister失败，已重试 {} 次: {}", 
            max_retries, last_error.unwrap())))
}

/// 获取主 canister 交易
pub async fn fetch_ledger_transactions(
    agent: &Agent,
    canister_id: &Principal,
    start: u64,
    length: u64,
) -> Result<(Vec<Transaction>, u64, u64), Box<dyn Error>> {
    println!("查询ledger交易: start={}, length={}", start, length);
    
    // 验证参数
    if length == 0 {
        println!("请求长度为0，返回空交易列表");
        return Ok((Vec::new(), start, start));
    }
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("编码参数失败: {}", e);
            return Err(create_error(&format!("ledger参数编码失败: {}", e)));
        }
    };
    
    // 实现重试机制
    let max_retries = 3;
    let mut retry_count = 0;
    let mut last_error = None;
    
    while retry_count < max_retries {
        match agent.query(canister_id, "get_transactions")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
                println!("收到ledger响应，长度: {} 字节", response.len());
                
                // 尝试使用LedgerGetTransactionsResult解析
                match Decode!(&response, LedgerGetTransactionsResult) {
                    Ok(result) => {
                        println!("成功解码为LedgerGetTransactionsResult");
                        println!("first_index: {}, log_length: {}, 交易数: {}, 归档交易数: {}", 
                            result.first_index.0, 
                            result.log_length.0,
                            result.transactions.len(),
                            result.archived_transactions.len());
                        
                        let first_index = result.first_index.0.to_u64().unwrap_or(0);
                        let log_length = result.log_length.0.to_u64().unwrap_or(0);
                        
                        // 给交易添加索引信息
                        let mut transactions = Vec::new();
                        for (i, mut tx) in result.transactions.into_iter().enumerate() {
                            let index = first_index + i as u64;
                            tx.index = Some(index);
                            transactions.push(tx);
                        }
                        
                        return Ok((transactions, first_index, log_length));
                    },
                    Err(e) => {
                        println!("解析ledger响应失败，尝试备用解码方法: {}", e);
                        
                        // 尝试使用SimpleTransactionRange解析
                        if let Ok(_range) = Decode!(&response, SimpleTransactionRange) {
                            println!("使用SimpleTransactionRange成功解析");
                            // 由于SimpleTransactionRange没有first_index信息，假设为0
                            return Ok((Vec::new(), start, start));
                        }
                        
                        // 如果两种解码方法都失败，但API调用成功，返回空结果
                        println!("所有解码方法都失败，返回空交易列表");
                        return Ok((Vec::new(), start, start));
                    }
                }
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = Duration::from_secs(2 * retry_count); // 指数退避
                println!("调用ledger canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    println!("达到最大重试次数 ({}), 调用ledger canister失败", max_retries);
    Err(create_error(&format!("调用ledger canister失败，已重试 {} 次: {}", 
            max_retries, last_error.unwrap())))
}

/// 获取区块链上的第一个交易索引
pub async fn get_first_transaction_index(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<u64, Box<dyn Error>> {
    println!("尝试获取区块链上的第一个交易索引...");
    
    // 查询第一个交易，主要目的是获取first_index
    let arg = GetTransactionsArg {
        start: candid::Nat::from(0u64),
        length: candid::Nat::from(1u64),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("编码参数失败: {}", e);
            return Err(create_error(&format!("参数编码失败: {}", e)));
        }
    };
    
    // 实现重试机制
    let max_retries = 3;
    let mut retry_count = 0;
    let mut last_error: Option<String> = None;
    
    while retry_count < max_retries {
        match agent.query(canister_id, "get_transactions")
            .with_arg(arg_bytes.clone())
            .call()
            .await {
            Ok(response) => {
                // 尝试解码响应
                match Decode!(&response, LedgerGetTransactionsResult) {
                    Ok(result) => {
                        let first_index = result.first_index.0.to_u64().unwrap_or(0);
                        println!("成功获取区块链初始索引: {}", first_index);
                        return Ok(first_index);
                    },
                    Err(e) => {
                        println!("解析ledger响应失败: {}", e);
                        
                        // 尝试使用SimpleTransactionRange解析
                        if let Ok(_range) = Decode!(&response, SimpleTransactionRange) {
                            println!("使用SimpleTransactionRange成功解析");
                            // 由于SimpleTransactionRange没有first_index信息，假设为0
                            return Ok(0);
                        }
                        
                        // 如果所有解码方法都失败，可能需要重试
                        retry_count += 1;
                        last_error = Some(format!("无法解析响应: {}", e));
                    }
                }
            },
            Err(e) => {
                retry_count += 1;
                let error_msg = format!("调用canister失败: {}", e);
                last_error = Some(error_msg.clone());
                let wait_time = Duration::from_secs(2 * retry_count); // 指数退避
                println!("调用canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, error_msg, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    Err(create_error(&last_error.unwrap_or_else(|| "尝试获取区块链初始索引失败，达到最大重试次数".to_string())))
} 