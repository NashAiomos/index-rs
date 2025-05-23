/**
 * 文件描述: 数据模型定义，包含应用中使用的各种数据结构
 * 功能概述:
 * - 定义交易相关数据结构
 * - 定义配置相关数据结构
 * - 定义账户和余额相关结构
 * - 实现数据格式转换
 * 
 * 主要组件:
 * - 常量定义 (第28-31行): 定义批处理大小和默认小数位数
 * - 账户结构体 (第51-72行): 定义账户数据结构及其显示格式
 * - 交易相关结构体 (第75-124行): 
 *   - Transfer (第75-84行): 转账交易
 *   - Mint (第86-92行): 铸币交易
 *   - Approve (第94-104行): 授权交易
 *   - Burn (第106-113行): 销毁交易
 *   - Transaction (第115-128行): 综合交易结构体
 * - 配置结构体 (第201-207行): 应用配置数据结构
 * - TokenConfig (第218-226行): 代币配置结构
 * - BalanceAnomaly (第229-246行): 余额异常记录结构
 */

use ic_agent::export::Principal;
use candid::{CandidType};
use serde::{Deserialize, Serialize};
use std::fmt;

// 常量定义
pub const BATCH_SIZE: u64 = 2000;
pub const ARCHIVE_BATCH_SIZE: u64 = 2000;
pub const DEFAULT_DECIMALS: u8 = 8;

// 参数结构体
#[derive(CandidType, Deserialize)]
pub struct GetTransactionsArg {
    pub start: candid::Nat,
    pub length: candid::Nat,
}

// Archives 查询的返回类型
#[derive(CandidType, Deserialize, Debug, Clone)]
pub struct ArchiveInfo {
    pub block_range_end: candid::Nat,
    pub canister_id: Principal,
    pub block_range_start: candid::Nat,
}

#[derive(CandidType, Deserialize, Debug)]
pub struct ArchivesResult(pub Vec<ArchiveInfo>);

// 账户结构体
#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
pub struct Account {
    pub owner: Principal,
    pub subaccount: Option<Vec<u8>>,
}

impl fmt::Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let owner_str = self.owner.to_text();
        let sub_str = match &self.subaccount {
            Some(sub) => {
                if sub.is_empty() {
                    "".to_string()
                } else if sub.iter().all(|&b| b == 0) {
                    // 如果子账户是全0，则视为默认子账户，不显示
                    "".to_string()
                } else {
                    format!("0x{}", hex::encode(sub))
                }
            }
            None => "".to_string(),
        };
        if sub_str.is_empty() {
            write!(f, "{}", owner_str)
        } else {
            write!(f, "{}:{}", owner_str, sub_str)
        }
    }
}

// 交易类型定义
#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
pub struct Transfer {
    pub to: Account,
    pub fee: Option<candid::Nat>,
    pub from: Account,
    pub memo: Option<Vec<u8>>,
    pub created_at_time: Option<u64>,
    pub amount: candid::Nat,
    pub spender: Option<Account>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
pub struct Mint {
    pub to: Account,
    pub amount: candid::Nat,
    pub memo: Option<Vec<u8>>,
    pub created_at_time: Option<u64>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
pub struct Approve {
    pub from: Account,
    pub spender: Account,
    pub amount: candid::Nat,
    pub fee: Option<candid::Nat>,
    pub memo: Option<Vec<u8>>,
    pub created_at_time: Option<u64>,
    pub expected_allowance: Option<candid::Nat>,
    pub expires_at: Option<u64>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
pub struct Burn {
    pub from: Account,
    pub amount: candid::Nat,
    pub memo: Option<Vec<u8>>,
    pub created_at_time: Option<u64>,
    pub spender: Option<Account>,
}

#[derive(CandidType, Deserialize, Debug, Clone, Serialize)]
pub struct Transaction {
    #[serde(rename = "kind")]
    pub kind: String,
    #[serde(rename = "timestamp")]
    pub timestamp: u64,
    #[serde(rename = "transfer")]
    pub transfer: Option<Transfer>,
    #[serde(rename = "mint")]
    pub mint: Option<Mint>,
    #[serde(rename = "burn")]
    pub burn: Option<Burn>,
    #[serde(rename = "approve")]
    pub approve: Option<Approve>,
    // 索引字段用于唯一标识交易
    #[serde(rename = "index", skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
}

#[derive(CandidType, Deserialize, Debug)]
pub struct ArchivedTransaction {
    pub callback: Principal,
    pub start: candid::Nat,
    pub length: candid::Nat,
}

#[derive(CandidType, Deserialize, Debug)]
pub struct GetTransactionsResult {
    pub first_index: candid::Nat,
    pub log_length: candid::Nat,
    pub transactions: Vec<Transaction>,
    pub archived_transactions: Vec<ArchivedTransaction>,
}

// 归档交易结构体，用于ledger canister接口
#[derive(CandidType, Deserialize, Debug)]
pub struct LedgerArchivedTransaction {
    #[serde(rename = "callback")]
    pub callback_canister_id: Principal,
    pub start: candid::Nat,
    pub length: candid::Nat,
}

// GetTransactionsResult，用于ledger canister
#[derive(CandidType, Deserialize, Debug)]
pub struct LedgerGetTransactionsResult {
    pub first_index: candid::Nat,
    pub log_length: candid::Nat,
    pub transactions: Vec<Transaction>,
    pub archived_transactions: Vec<LedgerArchivedTransaction>,
}

// TransactionRange结构体
#[derive(CandidType, Deserialize, Debug)]
pub struct SimpleTransactionRange {
    pub transactions: Vec<Transaction>,
}

// Transaction结构体，适应可能的不同格式
#[derive(CandidType, Deserialize, Debug, Clone)]
pub struct SimpleTransaction {
    pub kind: String,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer: Option<Transfer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint: Option<Mint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burn: Option<Burn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approve: Option<Approve>,
}

// 交易数组
#[derive(CandidType, Deserialize, Debug)]
pub struct TransactionList(pub Vec<Transaction>);

// 账户余额记录结构体
#[derive(Debug, Serialize, Deserialize)]
pub struct BalanceRecord {
    pub account: String,
    pub balance: String,  // 使用字符串存储，因为余额可能很大
    pub last_updated: u64, // 最后更新时间戳
    pub last_tx_index: u64, // 最后处理的交易索引
}

// 日志配置结构体
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct LogConfig {
    pub level: String,            // 日志级别: error, warn, info, debug, trace
    pub file: String,             // 日志文件路径
    pub console_level: String,    // 控制台日志级别
    pub file_enabled: bool,       // 是否启用文件日志
    pub max_size: u64,            // 日志文件最大大小(MB)
    pub max_files: u32,           // 保留的历史日志文件数量
}

// 配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub mongodb_url: String,
    pub database: String,
    pub ic_url: String,
    pub tokens: Vec<TokenConfig>,  // 多代币配置
    pub log: Option<LogConfig>,    // 日志配置
    pub api_server: Option<ApiServerConfig>, // API服务器配置
}

// API服务器配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct ApiServerConfig {
    pub enabled: bool,       // 是否启用API服务器
    pub port: u16,           // API服务器监听端口
    #[allow(dead_code)]
    pub cors_enabled: bool,  // 是否启用CORS
}

// 命令行参数结构体
#[derive(Debug, Clone)]
pub struct AppArgs {
    pub reset: bool,
}

/// 代币配置结构体
#[derive(Debug, Deserialize, Clone)]
pub struct TokenConfig {
    /// 代币标识符，用于数据库中区分不同代币
    pub symbol: String,
    /// 代币名称
    pub name: String,
    /// 代币的canister ID
    pub canister_id: String,
    /// 代币小数位数
    pub decimals: Option<u8>,
}

/// 余额异常记录
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BalanceAnomaly {
    /// 账户ID
    pub account: String,
    /// 交易索引
    pub tx_index: u64,
    /// 交易类型
    pub tx_type: String,
    /// 异常类型
    pub anomaly_type: String,
    /// 当时账户余额
    pub balance: String,
    /// 尝试操作的金额
    pub amount: String,
    /// 异常描述
    pub description: String,
    /// 记录时间
    pub timestamp: i64,
}
