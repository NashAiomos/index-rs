# index-rs

`index-rs` 是一个用于同步和索引 Internet Computer (IC) 账本 Canister 交易的 Rust 工具。

它会每秒拉取主账本及归档 Canister 的所有交易，然后按账户分组存储到 MongoDB 数据库中，便于后续查询和分析。同时，还会自动计算和更新每个账户的实时余额。

## 项目结构

```
src/
├── main.rs             # 主程序入口
├── config.rs           # 配置加载和处理
├── models.rs           # 数据模型定义
├── utils.rs            # 工具函数集合
├── blockchain.rs       # 区块链交互功能
├── db/
│   ├── mod.rs          # 数据库模块入口
│   ├── transactions.rs # 交易数据操作
│   ├── accounts.rs     # 账户数据操作
│   ├── balances.rs     # 余额数据操作
├── sync/
│   ├── mod.rs          # 同步模块入口
│   ├── ledger.rs       # 主账本同步
│   ├── archive.rs      # 归档同步
│   ├── admin.rs        # 管理员功能(包含reset)
```

## 数据库集合

程序维护三个主要集合：

1. **transactions**: 存储所有交易记录
2. **accounts**: 记录账户与交易的关系
3. **balances**: 存储每个账户的最新余额信息

## 配置文件

配置文件 `config.toml` 包含以下配置项：

```toml
mongodb_url = "mongodb://localhost:27017"  # MongoDB连接地址
database = "ledger"                       # 数据库名称
ic_url = "https://icp0.io"                # IC网络地址
ledger_canister_id = "ryjl3-tyaaa-..."    # 账本Canister ID
token_decimals = 8                        # 可选：代币小数位，如不指定会自动查询
```

## 构建与运行

1. **安装依赖**

   确保已安装 Rust 工具链及 MongoDB 数据库。

2. **构建项目**

   ```bash
   cargo build --release
   ```

3. **启动 MongoDB**

4. **运行项目**

   ```bash
   cargo run
   ```

5. **重置数据库并重新同步所有交易**

   如果需要忽略现有数据库内容，完全重新同步所有交易数据：

   ```bash
   cargo run -- --reset
   ```

## 功能特性

1. **代币小数位自动识别**
   
   程序会自动查询账本 Canister 的 `icrc1_decimals` 方法，获取代币小数位，使余额显示更准确。

2. **归档同步**
   
   程序会先从主账本 Canister 获取归档信息，然后顺序处理每个归档 Canister 中的历史交易。

3. **主账本同步**
   
   完成归档同步后，从主账本 Canister 获取最新交易，保持数据库与链上状态一致。

4. **实时余额计算**
   
   针对每笔交易，程序会实时更新相关账户的余额状态，支持转账、铸币、销毁和授权等操作。

5. **定时增量同步**
   
   每 5 秒自动检查一次主账本是否有新交易，并同步到数据库中。

6. **丰富的日志信息**
   
   程序运行过程中记录详细的日志，便于追踪同步进度和问题诊断。

## 管理员功能

1. **数据库重置**
   
   通过 `--reset` 参数触发完整重置和重新同步，仅限管理员使用。
   
   ```bash
   cargo run -- --reset
   ```
   
2. **错误恢复**
   
   即使遇到错误，程序也会尝试自动恢复和继续同步，确保数据完整性。
