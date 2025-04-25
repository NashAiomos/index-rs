use std::error::Error;
use mongodb::Collection;
use mongodb::bson::{doc, Document};
use candid::Nat;
use log::info;
use futures::stream::TryStreamExt;

/// 重新计算并保存总供应量
pub async fn recalculate_total_supply(
    balances_col: &Collection<Document>,
    supply_col: &Collection<Document>,
) -> Result<Nat, Box<dyn Error>> {
    let mut total = Nat::from(0u64);

    // 遍历余额集合求和
    let mut cursor = balances_col.find(doc! {}, None).await?;
    while let Some(doc) = cursor.try_next().await? {
        if let Ok(balance_str) = doc.get_str("balance") {
            if let Ok(balance_nat) = Nat::parse(balance_str.as_bytes()) {
                total += balance_nat;
            }
        }
    }

    // 更新或插入总供应量文档
    supply_col
        .update_one(
            doc! { "id": "total_supply" },
            doc! { "$set": { "id": "total_supply", "value": total.to_string() } },
            mongodb::options::UpdateOptions::builder().upsert(true).build(),
        )
        .await?;

    info!("已重新计算并更新总供应量: {}", total.to_string());
    Ok(total)
}

/// 获取当前存储的总供应量
pub async fn get_stored_total_supply(
    supply_col: &Collection<Document>,
) -> Result<Option<String>, Box<dyn Error>> {
    if let Some(doc) = supply_col.find_one(doc! { "id": "total_supply" }, None).await? {
        if let Ok(value) = doc.get_str("value") {
            return Ok(Some(value.to_string()));
        }
    }
    Ok(None)
} 