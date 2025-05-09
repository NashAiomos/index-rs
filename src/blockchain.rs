use std::error::Error;
use ic_agent::Agent;
use ic_agent::export::Principal;
use candid::{Encode, Decode};
use num_traits::ToPrimitive;
use log::{info, error, warn, debug};
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
    info!("获取归档信息...");
    
    let empty_tuple = ();
    let arg_bytes = Encode!(&empty_tuple)?;
    let response = agent.query(canister_id, "archives")
        .with_arg(arg_bytes)
        .call()
        .await?;
    
    let archives_result: ArchivesResult = Decode!(&response, ArchivesResult)?;
    
    if !archives_result.0.is_empty() {
        info!("发现 {} 个归档 canister，将依次同步", archives_result.0.len());
    } else {
        info!("未发现任何归档 canister");
    }
    
    Ok(archives_result.0)
}

/// 从归档canister获取交易
pub async fn fetch_archive_transactions(
    agent: &Agent,
    archive_canister_id: &Principal,
    start: u64,
    length: u64,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    debug!("从归档canister获取交易: start={}, length={}", start, length);
    
    if length == 0 {
        debug!("请求长度为0，返回空交易列表");
        return Ok(Vec::new());
    }
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("编码参数失败: {}", e);
            return Err(create_error(&format!("参数编码失败: {}", e)));
        }
    };
    
    debug!("调用归档canister: {}", archive_canister_id);
    
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
                debug!("收到归档canister响应，长度: {} 字节", response.len());
                
                // 尝试多种可能的结构解码方式
                
                // 1. 首先尝试解码为SimpleTransactionRange（调整顺序，优先尝试）
                debug!("尝试解码为SimpleTransactionRange...");
                if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
                    let tx_count = range.transactions.len();
                    debug!("成功解码为SimpleTransactionRange，交易数量: {}", tx_count);
                    
                    // 输出精简信息到命令行
                    if tx_count > 0 {
                        let end = start + tx_count as u64 - 1;
                        info!("成功获取到归档交易批次：{}-{}，使用SimpleTransactionRange解码，已保存到数据库", start, end);
                    }
                    
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
                debug!("尝试解码为TransactionList...");
                if let Ok(list) = Decode!(&response, TransactionList) {
                    let tx_count = list.0.len();
                    debug!("成功解码为TransactionList，交易数量: {}", tx_count);
                    
                    // 输出精简信息到命令行
                    if tx_count > 0 {
                        let end = start + tx_count as u64 - 1;
                        info!("成功获取到归档交易批次：{}-{}，使用TransactionList解码，已保存到数据库", start, end);
                    }
                    
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
                debug!("尝试解码为Vec<Transaction>...");
                if let Ok(transactions) = Decode!(&response, Vec<Transaction>) {
                    let tx_count = transactions.len();
                    debug!("成功解码为Vec<Transaction>，交易数量: {}", tx_count);
                    
                    if tx_count > 0 {
                        let end = start + tx_count as u64 - 1;
                        info!("成功获取到归档交易批次：{}-{}，使用Vec<Transaction>解码，已保存到数据库", start, end);
                    }
                    
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
                debug!("所有解码方法都失败，返回空交易列表");
                error!("解码错误：归档交易批次 {}-{} 所有解码方式均失败，API调用成功但无法解析响应数据，已跳过此批次", 
                      start, start + length - 1);
                return Ok(Vec::new());
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = Duration::from_secs(2 * retry_count); // 指数退避
                warn!("网络错误：调用归档canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    error!("网络错误：达到最大重试次数 ({}), 调用归档canister {} 失败，无法获取交易批次 {}-{}", 
          max_retries, archive_canister_id, start, start + length - 1);
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
    debug!("查询ledger交易: start={}, length={}", start, length);
    
    // 验证参数
    if length == 0 {
        debug!("请求长度为0，返回空交易列表");
        return Ok((Vec::new(), start, start));
    }
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("编码参数失败: {}", e);
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
                debug!("收到ledger响应，长度: {} 字节", response.len());
                
                // 尝试使用LedgerGetTransactionsResult解析
                match Decode!(&response, LedgerGetTransactionsResult) {
                    Ok(result) => {
                        debug!("成功解码为LedgerGetTransactionsResult");
                        debug!("first_index: {}, log_length: {}, 交易数: {}, 归档交易数: {}", 
                            result.first_index.0, 
                            result.log_length.0,
                            result.transactions.len(),
                            result.archived_transactions.len());
                        
                        let first_index = result.first_index.0.to_u64().unwrap_or(0);
                        let log_length = result.log_length.0.to_u64().unwrap_or(0);
                        let tx_count = result.transactions.len();
                        
                        // 输出精简信息到命令行
                        if tx_count > 0 {
                            let end = first_index + tx_count as u64 - 1;
                            info!("成功获取到主账本交易批次：{}-{}，使用LedgerGetTransactionsResult解码，已保存到数据库", first_index, end);
                        } else {
                            debug!("主账本未返回任何交易");
                        }
                        
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
                        debug!("解析ledger响应失败，尝试备用解码方法: {}", e);
                        
                        // 尝试使用SimpleTransactionRange解析
                        if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
                            debug!("使用SimpleTransactionRange成功解析");
                            let tx_count = range.transactions.len();
                            
                            if tx_count > 0 {
                                let end = start + tx_count as u64 - 1;
                                info!("成功获取到主账本交易批次：{}-{}，使用SimpleTransactionRange解码，已保存到数据库", start, end);
                                
                                // 给交易添加索引信息
                                let mut transactions = Vec::new();
                                for (i, mut tx) in range.transactions.into_iter().enumerate() {
                                    let index = start + i as u64;
                                    tx.index = Some(index);
                                    transactions.push(tx);
                                }
                                
                                return Ok((transactions, start, start + tx_count as u64));
                            }
                            
                            // 由于SimpleTransactionRange没有first_index信息，假设为start
                            return Ok((Vec::new(), start, start));
                        }
                        
                        // 如果两种解码方法都失败，但API调用成功，返回空结果
                        debug!("所有解码方法都失败，返回空交易列表");
                        error!("解码错误：主账本交易批次 {}-{} 所有解码方式均失败，API调用成功但无法解析响应数据，已跳过此批次", 
                              start, start + length - 1);
                        return Ok((Vec::new(), start, start));
                    }
                }
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = Duration::from_secs(2 * retry_count); // 指数退避
                warn!("网络错误：调用主账本canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    error!("网络错误：达到最大重试次数 ({}), 调用主账本canister失败，无法获取交易批次 {}-{}", 
          max_retries, start, start + length - 1);
    Err(create_error(&format!("调用ledger canister失败，已重试 {} 次: {}", 
            max_retries, last_error.unwrap())))
}

/// 获取区块链上的第一个交易索引
pub async fn get_first_transaction_index(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<u64, Box<dyn Error>> {
    debug!("尝试获取区块链上的第一个交易索引...");
    
    // 查询第一个交易，主要目的是获取first_index
    let arg = GetTransactionsArg {
        start: candid::Nat::from(0u64),
        length: candid::Nat::from(1u64),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("编码参数失败: {}", e);
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
                        info!("获取到区块链初始索引: {}", first_index);
                        return Ok(first_index);
                    },
                    Err(e) => {
                        debug!("解析ledger响应失败: {}", e);
                        
                        // 尝试使用SimpleTransactionRange解析
                        if let Ok(_range) = Decode!(&response, SimpleTransactionRange) {
                            debug!("使用SimpleTransactionRange成功解析");
                            info!("区块链初始索引默认为0");
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
                warn!("调用canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, error_msg, wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    warn!("无法获取区块链初始索引，将使用默认值0");
    Err(create_error(&last_error.unwrap_or_else(|| "尝试获取区块链初始索引失败，达到最大重试次数".to_string())))
}

/// 测试归档canister可用性，不记录普通交易日志
pub async fn test_archive_transactions(
    agent: &Agent,
    archive_canister_id: &Principal,
    start: u64,
    length: u64,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    debug!("测试归档canister可用性: start={}, length={}", start, length);
    
    if length == 0 {
        debug!("请求长度为0，返回空交易列表");
        return Ok(Vec::new());
    }
    
    let arg = GetTransactionsArg {
        start: candid::Nat::from(start),
        length: candid::Nat::from(length),
    };
    
    let arg_bytes = match Encode!(&arg) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("编码参数失败: {}", e);
            return Err(create_error(&format!("参数编码失败: {}", e)));
        }
    };
    
    debug!("测试调用归档canister: {}", archive_canister_id);
    
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
                debug!("收到归档canister测试响应，长度: {} 字节", response.len());
                
                // 尝试多种可能的结构解码方式
                
                // 1. 首先尝试解码为SimpleTransactionRange（调整顺序，优先尝试）
                debug!("尝试解码为SimpleTransactionRange...");
                if let Ok(range) = Decode!(&response, SimpleTransactionRange) {
                    let tx_count = range.transactions.len();
                    debug!("测试成功解码为SimpleTransactionRange，交易数量: {}", tx_count);
                    
                    // 给交易添加索引信息
                    let mut indexed_transactions = Vec::new();
                    for (i, mut tx) in range.transactions.into_iter().enumerate() {
                        let index = start + i as u64;
                        tx.index = Some(index);
                        indexed_transactions.push(tx);
                    }
                    
                    debug!("归档canister测试成功");
                    return Ok(indexed_transactions);
                }
                
                // 2. 尝试解码为TransactionList(Vec<Transaction>)
                debug!("尝试解码为TransactionList...");
                if let Ok(list) = Decode!(&response, TransactionList) {
                    let tx_count = list.0.len();
                    debug!("测试成功解码为TransactionList，交易数量: {}", tx_count);
                    
                    // 给交易添加索引信息
                    let mut indexed_transactions = Vec::new();
                    for (i, mut tx) in list.0.into_iter().enumerate() {
                        let index = start + i as u64;
                        tx.index = Some(index);
                        indexed_transactions.push(tx);
                    }
                    
                    debug!("归档canister测试成功");
                    return Ok(indexed_transactions);
                }
                
                // 3. 尝试直接解码为Vec<Transaction>
                debug!("尝试解码为Vec<Transaction>...");
                if let Ok(transactions) = Decode!(&response, Vec<Transaction>) {
                    let tx_count = transactions.len();
                    debug!("测试成功解码为Vec<Transaction>，交易数量: {}", tx_count);
                    
                    // 给交易添加索引信息
                    let mut indexed_transactions = Vec::new();
                    for (i, mut tx) in transactions.into_iter().enumerate() {
                        let index = start + i as u64;
                        tx.index = Some(index);
                        indexed_transactions.push(tx);
                    }
                    
                    debug!("归档canister测试成功");
                    return Ok(indexed_transactions);
                }
                
                // 所有解码方法都失败，但API调用成功了，重试可能没用
                debug!("测试解码失败，返回空交易列表");
                error!("解码错误：测试归档canister {} 所有解码方式均失败，API调用成功但无法解析响应数据", 
                      archive_canister_id);
                return Ok(Vec::new());
            },
            Err(e) => {
                retry_count += 1;
                last_error = Some(e);
                let wait_time = Duration::from_secs(2 * retry_count); // 指数退避
                warn!("网络错误：测试调用归档canister失败 (尝试 {}/{}): {}，等待 {:?} 后重试", 
                    retry_count, max_retries, last_error.as_ref().unwrap(), wait_time);
                tokio::time::sleep(wait_time).await;
            }
        }
    }
    
    // 如果达到最大重试次数仍然失败
    error!("网络错误：达到最大重试次数 ({}), 测试调用归档canister {} 失败", 
          max_retries, archive_canister_id);
    Err(create_error(&format!("测试调用归档canister失败，已重试 {} 次: {}", 
            max_retries, last_error.unwrap())))
}

