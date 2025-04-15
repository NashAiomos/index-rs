use candid::{Decode, Encode, Principal, Nat, CandidType};
use ic_agent::Agent;
use mongodb::{
    bson::doc,
    options::{ClientOptions, IndexOptions},
    Client, Collection, IndexModel,
};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};
use std::error::Error;
use log::{info, error};
use num_traits::cast::ToPrimitive;

// 本地交易结构体，用于存储到 MongoDB
#[derive(Debug, Serialize, Deserialize)]
struct LocalTransaction {
    tx_index: u64,       // 交易索引
    timestamp: u64,      // 时间戳
    from_account: String, // 发送方账户
    to_account: String,   // 接收方账户
    amount: u64,         // 交易金额
}

// get_transactions 的参数结构体
#[derive(CandidType, Deserialize)]
struct GetTransactionsArgs {
    start: Nat,  // 起始交易索引
    length: Nat, // 获取的交易数量
}

// 账户结构体，映射 Motoko 的 { owner: principal; subaccount: opt vec nat8 }
#[derive(CandidType, Deserialize)]
struct Account {
    owner: Principal,         // 主账户
    subaccount: Option<Vec<u8>>, // 子账户（可选）
}

// 交易结构体，映射 Motoko 的 Transaction
#[derive(CandidType, Deserialize)]
struct Transaction {
    tx_index: Nat,  // 交易索引
    timestamp: Nat, // 时间戳
    from: Account,  // 发送方账户
    to: Account,    // 接收方账户
    amount: Nat,    // 交易金额
}

// get_transactions 的返回结果结构体
#[derive(CandidType, Deserialize)] // 添加 CandidType
struct GetTransactionsResult {
    transactions: Vec<Transaction>, // 交易列表
    total: Nat,                    // 交易总数
}

// 将 Account 转换为字符串格式
fn format_account(account: &Account) -> String {
    let subaccount_str = account
        .subaccount
        .as_ref()
        .map(|sa| hex::encode(sa))
        .unwrap_or_default();
    format!("{}:{}", account.owner.to_text(), subaccount_str)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 初始化日志系统
    env_logger::init();

    // 初始化 IC Agent
    let agent = Agent::builder()
        .with_url("HTTPS://ic0.app")
        .build()?;
    agent.fetch_root_key().await?;

    // 设置 ledger canister ID
    let ledger_canister_id = Principal::from_text("4c4fd-caaaa-aaaaq-aaa3a-cai")?;

    // 初始化 MongoDB 连接
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    let db = client.database("ic_data");
    let collection: Collection<LocalTransaction> = db.collection("transactions");

    // 创建唯一索引，按 tx_index 排序
    let index_model = IndexModel::builder()
        .keys(doc! { "tx_index": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    collection.create_index(index_model, None).await?;

    // 获取数据库中最大的 tx_index
    let last_doc = collection
        .find_one(
            doc! {},
            mongodb::options::FindOneOptions::builder()
                .sort(doc! { "tx_index": -1 })
                .build(),
        )
        .await?;
    let mut last_tx_index = last_doc.map_or(0, |doc| doc.tx_index + 1);

    // 主循环：每秒同步一次交易数据
    loop {
        // 构造查询参数
        let args = GetTransactionsArgs {
            start: Nat::from(last_tx_index),
            length: Nat::from(10_u64),
        };

        // 调用 get_transactions 函数
        let response = match agent
            .query(&ledger_canister_id, "get_transactions")
            .with_arg(Encode!(&args)?)
            .call()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("Ledger canister query error: {:?}", e);
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // 解码返回结果
        let transactions_result = match Decode!(response.as_slice(), GetTransactionsResult) {
            Ok(r) => r,
            Err(e) => {
                error!("Decode transactions error: {:?}", e);
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // 处理交易数据
        for tx in transactions_result.transactions.iter() {
            let transaction = LocalTransaction {
                tx_index: tx.tx_index.0.to_u64().unwrap(), // 需要 use num_traits::cast::ToPrimitive
                timestamp: tx.timestamp.0.to_u64().unwrap(),
                from_account: format_account(&tx.from),
                to_account: format_account(&tx.to),
                amount: tx.amount.0.to_u64().unwrap(),
            };

            // 插入数据库（upsert 方式）
            let filter = doc! { "tx_index": mongodb::bson::Bson::Int64(transaction.tx_index as i64) };
            let update = doc! { "$setOnInsert": mongodb::bson::to_document(&transaction)? };
            let res = collection
                .update_one(
                    filter,
                    update,
                    mongodb::options::UpdateOptions::builder().upsert(true).build(),
                )
                .await;
            match res {
                Ok(r) => {
                    if r.upserted_id.is_some() {
                        info!("Inserted tx_index {}", transaction.tx_index);
                    }
                }
                Err(e) => error!("Insert error: {:?}", e),
            }

            // 更新 last_tx_index
            last_tx_index = transaction.tx_index + 1;
        }

        // 等待 1 秒
        sleep(Duration::from_secs(1)).await;
    }
}
