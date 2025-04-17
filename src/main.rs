use ic_agent::{Agent};
use ic_agent::export::Principal;
use candid::{Encode, Decode, CandidType};
use serde::{Deserialize};
use std::error::Error;
use num_traits::ToPrimitive;
use std::fmt;

// 定义参数结构体
#[derive(CandidType, Deserialize)]
struct GetTransactionsArg {
    start: candid::Nat,
    length: candid::Nat,
}

// 定义返回结构体
#[derive(CandidType, Deserialize, Debug)]
struct Account {
    owner: Principal,
    subaccount: Option<Vec<u8>>,
}

impl fmt::Display for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let owner_str = self.owner.to_text();
        let sub_str = match &self.subaccount {
            Some(sub) => {
                if sub.is_empty() {
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

#[derive(CandidType, Deserialize, Debug)]
struct Transfer {
    to: Account,
    fee: Option<candid::Nat>,
    from: Account,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    amount: candid::Nat,
    spender: Option<Account>,
}

#[derive(CandidType, Deserialize, Debug)]
struct Mint {
    to: Account,
    amount: candid::Nat,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    // ...其他字段...
}

#[derive(CandidType, Deserialize, Debug)]
struct Approve {
    from: Account,
    spender: Account,
    amount: candid::Nat,
    fee: Option<candid::Nat>,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    expected_allowance: Option<candid::Nat>,
    expires_at: Option<u64>,
    // ...其他字段...
}

#[derive(CandidType, Deserialize, Debug)]
struct Burn {
    from: Account,
    amount: candid::Nat,
    memo: Option<Vec<u8>>,
    created_at_time: Option<u64>,
    spender: Option<Account>,
}

#[derive(CandidType, Deserialize, Debug)]
struct Transaction {
    #[serde(rename = "kind")]
    kind: String,
    #[serde(rename = "timestamp")]
    timestamp: u64,
    #[serde(rename = "transfer")]
    transfer: Option<Transfer>,
    #[serde(rename = "mint")]
    mint: Option<Mint>,
    #[serde(rename = "approve")]
    approve: Option<Approve>,
    #[serde(rename = "burn")]
    burn: Option<Burn>,
    // candid 里有的变体都要加上，字段名全部小写
    // ...其他字段...
}

#[derive(CandidType, Deserialize, Debug)]
struct ArchivedTransaction {
    callback: Principal,
    start: candid::Nat,
    length: candid::Nat,
}

#[derive(CandidType, Deserialize, Debug)]
struct GetTransactionsResult {
    first_index: candid::Nat,
    log_length: candid::Nat,
    transactions: Vec<Transaction>,
    archived_transactions: Vec<ArchivedTransaction>,
}

// 查询归档 canister 的交易
async fn fetch_archived_transaction_latest(
    agent: &Agent,
    archived: &ArchivedTransaction,
) -> Result<Option<Transaction>, Box<dyn Error>> {
    let archived_length: u64 = archived.length.0.to_u64().unwrap_or(0);
    if archived_length == 0 {
        return Ok(None);
    }
    let last_index = archived_length - 1;
    let start = archived.start.clone() + candid::Nat::from(last_index);
    let arg = GetTransactionsArg {
        start,
        length: candid::Nat::from(1u64),
    };
    let arg_bytes = Encode!(&arg)?;
    let response = agent
        .query(&archived.callback, "get_transactions")
        .with_arg(arg_bytes)
        .call()
        .await?;
    let archived_result: GetTransactionsResult = Decode!(&response, GetTransactionsResult)?;
    Ok(archived_result.transactions.into_iter().next())
}

fn print_transaction(tx: &Transaction) {
    println!("kind: {}", tx.kind);
    println!("timestamp: {}", tx.timestamp);
    if let Some(ref transfer) = tx.transfer {
        println!("-- Transfer --");
        println!("from: {}", transfer.from);
        println!("to: {}", transfer.to);
        println!("amount: {}", transfer.amount);
        println!("fee: {:?}", transfer.fee);
        println!("memo: {:?}", transfer.memo);
        println!("created_at_time: {:?}", transfer.created_at_time);
        println!("spender: {:?}", transfer.spender.as_ref().map(|a| a.to_string()));
    }
    if let Some(ref mint) = tx.mint {
        println!("-- Mint --");
        println!("to: {}", mint.to);
        println!("amount: {}", mint.amount);
        println!("memo: {:?}", mint.memo);
        println!("created_at_time: {:?}", mint.created_at_time);
    }
    if let Some(ref approve) = tx.approve {
        println!("-- Approve --");
        println!("from: {}", approve.from);
        println!("spender: {}", approve.spender);
        println!("amount: {}", approve.amount);
        println!("fee: {:?}", approve.fee);
        println!("memo: {:?}", approve.memo);
        println!("created_at_time: {:?}", approve.created_at_time);
        println!("expected_allowance: {:?}", approve.expected_allowance);
        println!("expires_at: {:?}", approve.expires_at);
    }
    if let Some(ref burn) = tx.burn {
        println!("-- Burn --");
        println!("from: {}", burn.from);
        println!("amount: {}", burn.amount);
        println!("memo: {:?}", burn.memo);
        println!("created_at_time: {:?}", burn.created_at_time);
        println!("spender: {:?}", burn.spender.as_ref().map(|a| a.to_string()));
    }
    if tx.transfer.is_none() && tx.mint.is_none() && tx.approve.is_none() && tx.burn.is_none() {
        println!("No details.");
    }
}

// 获取主 canister 和所有归档 canister的所有交易
async fn fetch_all_transactions(
    agent: &Agent,
    canister_id: &Principal,
) -> Result<Vec<Transaction>, Box<dyn Error>> {
    // 1. 获取主 canister 的所有交易
    let arg = GetTransactionsArg {
        start: candid::Nat::from(0u64),
        length: candid::Nat::from(u64::MAX),
    };
    let arg_bytes = Encode!(&arg)?;
    let response = agent.query(canister_id, "get_transactions")
        .with_arg(arg_bytes)
        .call()
        .await?;
    let result: GetTransactionsResult = Decode!(&response, GetTransactionsResult)?;

    let mut all_transactions = result.transactions;

    // 2. 获取所有归档 canister 的交易
    for archived in &result.archived_transactions {
        let archived_length: u64 = archived.length.0.to_u64().unwrap_or(0);
        if archived_length == 0 {
            continue;
        }
        let arg = GetTransactionsArg {
            start: archived.start.clone(),
            length: archived.length.clone(),
        };
        let arg_bytes = Encode!(&arg)?;
        let response = agent
            .query(&archived.callback, "get_transactions")
            .with_arg(arg_bytes)
            .call()
            .await?;
        let archived_result: GetTransactionsResult = Decode!(&response, GetTransactionsResult)?;
        all_transactions.extend(archived_result.transactions);
    }

    Ok(all_transactions)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let agent = Agent::builder()
        .with_url("https://icp0.io")
        .build()?;

    let canister_id = Principal::from_text("4x2jw-rqaaa-aaaak-qufjq-cai")?;

    let all_transactions = fetch_all_transactions(&agent, &canister_id).await?;

    if all_transactions.is_empty() {
        println!("No transactions found.");
    } else {
        println!("All transactions ({}):", all_transactions.len());
        for (i, tx) in all_transactions.iter().enumerate() {
            println!("--- Transaction {} ---", i);
            print_transaction(tx);
        }
    }
    Ok(())
}
