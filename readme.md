# index-rs

`index-rs` 是一个用于同步和索引 Internet Computer (IC) 账本 Canister 交易的 Rust 工具。

它会每秒拉取主账本及归档 Canister 的所有交易，然后按账户分组存储到 MongoDB 数据库中，便于后续查询和分析。

## 构建与运行

1. **安装依赖**

   ```bash
   cargo build
   ```

2. **启动 MongoDB**

   - 默认连接 `mongodb://localhost:27017`，数据库名 `ledger_sync`。

3. **运行项目**

   ```bash
   cargo run
   ```

## 功能特性

1. **初始化数据库和网络连接**

   连接本地 MongoDB，准备两个集合：accounts（账户-交易关系）和 transactions（交易详情）。

   为 index 字段（交易索引）和 account 字段建立索引，提高查询效率。

   初始化 IC 网络代理（Agent），指定主 Ledger Canister 的 ID。

2. **获取归档信息**

   调用主 Ledger Canister 的 archives 方法，获取所有归档 Canister 的信息（每个归档包含其 canister_id 及区块范围）。

   如果没有归档信息，直接退出。

3. **同步归档 Canister 的历史交易**

   选取第一个归档 canister，获取其区块范围。

   先尝试获取一笔交易，测试解码是否成功（兼容多种可能的数据结构）。

   如果能成功解码，按批次（BATCH_SIZE）循环获取归档区块范围内的所有交易：

   每批获取后，保存到 transactions 集合。

   对每笔交易，提取相关账户，更新 accounts 集合，记录账户与交易索引的关系。

4. **同步主 Ledger Canister 的新交易**

   查询本地数据库中已同步的最大交易索引，从下一个索引开始增量同步。

   每次批量获取新交易，保存到数据库，并更新账户-交易关系。

   如果遇到错误，最多重试 3 次。

5. **定时增量同步**

   初始同步完成后，进入循环，每隔 5 秒自动增量同步主 Ledger Canister 的新交易，保持本地数据库与链上数据实时同步。

6. **辅助功能**

   支持多种交易结构的解码，兼容不同版本的 canister。

   提供按账户查询交易、保存单笔交易、获取最新交易索引等数据库操作。

**流程**：初始化 → 2. 获取归档信息 → 3. 同步归档历史交易 → 4. 同步主账本新交易 → 5. 定时增量同步

## MongoDB 数据结构

每个账户在 `accounts` 集合中有一条文档，结构如下：

```
{
  "account": "principal:subaccount",
  "transactions": [ ... ] // 这个账户相关的所有交易
}
```
