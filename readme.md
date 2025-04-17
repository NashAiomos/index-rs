# index-rs
`index-rs` 是一个用于同步和索引 Internet Computer (IC) 账本 Canister 交易的 Rust 工具。它会定期拉取主账本及归档 Canister 的所有交易，并按账户分组存储到 MongoDB 数据库中，便于后续查询和分析。

## 功能特性

- 支持同步主账本和所有归档 Canister 的交易数据
- 按账户分组索引交易，便于高效查询
- 增量同步，自动检测并存储新交易
- 支持通过 MongoDB 查询指定账户的所有交易

## 构建与运行

1. **安装依赖**
   ```bash
   cargo build
   ```

2. **启动 MongoDB**
   - 默认连接到 `mongodb://localhost:27017`，数据库名为 `ledger_sync`。

3. **运行项目**
   ```bash
   cargo run
   ```

   程序会自动同步所有交易并持续增量更新。

## 查询接口示例

可通过 `get_account_transactions` 函数查询某账户的所有交易：

```rust
let txs = get_account_transactions(&accounts_col, "principal:subaccount").await?;
```

## 配置说明

- 默认同步的 Canister ID 为 `4x2jw-rqaaa-aaaak-qufjq-cai`，如需更换请在 `main.rs` 中修改。
- MongoDB 连接字符串可在 `main.rs` 顶部修改。

## 主要结构说明

- `main.rs`：核心逻辑，包括交易同步、分组、存储与查询。
- 结构体定义：
  - `Account`、`Transfer`、`Mint`、`Approve`、`Burn`、`Transaction` 等，分别对应账本的不同交易类型。
- 主要函数：
  - `fetch_all_transactions`：拉取主账本及归档所有交易
  - `group_transactions_by_account`：按账户分组交易
  - `get_account_transactions`：查询某账户下的所有交易

## MongoDB 数据结构

每个账户在 `accounts` 集合中有一条文档，结构如下：

```json
{
  "account": "principal:subaccount",
  "transactions": [ ... ] // 这个账户相关的所有交易
}
```

## 依赖环境

- Rust 2021
- MongoDB 数据库（本地或远程）
- 依赖库见 `Cargo.toml`，主要包括：
  - `ic-agent`、`candid`（与 IC 通信）
  - `mongodb`（数据库操作）
  - `tokio`、`futures`（异步运行时）
  - `serde`、`hex`、`num-traits` 