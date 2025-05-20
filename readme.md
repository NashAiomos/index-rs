# Blockchain Index API

一个区块链索引服务，用于同步和索引区块链上的交易数据，并提供查询 API。支持多代币同时索引与查询。

## 功能特点

- 同步区块链交易数据
- 计算账户余额
- 提供 RESTful API 接口查询交易和账户信息
- 支持增量同步和全量重置
- 支持归档数据同步
- 支持多代币同时索引与查询

## 项目结构

```
src/
├── main.rs              # 主程序入口
├── api.rs               # API 功能模块，包含所有查询功能
├── api_server.rs        # HTTP API 服务器实现
├── models.rs            # 数据模型定义
├── blockchain.rs        # 区块链交互功能
├── utils.rs             # 通用工具函数
├── config.rs            # 配置加载功能
├── error.rs             # 错误处理模块
├── db/                  # 数据库相关功能
│   ├── mod.rs           # 数据库模块入口
│   ├── transactions.rs  # 交易数据库操作
│   ├── accounts.rs      # 账户数据库操作
│   ├── balances.rs      # 余额数据库操作
│   ├── supply.rs        # 总供应量数据库操作
│   └── sync_status.rs   # 同步状态数据库操作
└── sync/                # 同步功能
    ├── mod.rs           # 同步模块入口
    ├── archive.rs       # 归档历史数据
    ├── ledger.rs        # 账本处理功能
    └── admin.rs         # 管理员功能（重置等）
```

## 数据库集合

程序为每个代币维护以下集合，前缀为代币符号（例如：`ICP_transactions`）：

1. **transactions**: 存储所有交易记录
2. **accounts**: 记录账户与交易的关系
3. **balances**: 存储每个账户的最新余额信息
4. **total_supply**: 记录代币的总供应量
5. **balance_anomalies**: 记录余额计算过程中的异常情况

此外，系统还维护一个全局集合：

6. **sync_status**: 保存各代币的同步状态，支持增量同步

## 构建与运行

1. **安装依赖**

   确保已安装 Rust 工具链及 MongoDB 数据库。

2. **构建项目**

   ```bash
   cargo build --release
   ```

3. **启动 MongoDB**

4. **运行项目**

   正常启动（增量同步）

   ```bash
   cargo run
   ```

## 配置说明

使用 TOML 格式的配置文件。在启动前，请先创建 `config.toml` 配置文件

配置项包括：

```toml
# MongoDB连接地址
mongodb_url = "mongodb://localhost:27017"
# 数据库名称
database = "ledger"
# IC网络地址
ic_url = "https://ic0.app"

# 代币配置列表
[[tokens]]
# 代币标识符 (用于在数据库中区分不同代币)
symbol = "ICP"
# 代币名称
name = "Internet Computer"
# 代币canister_id
canister_id = "ryjl3-tyaaa-aaaaa-aaaba-cai"
# 代币小数位数（可选，如果不设置会自动查询）
decimals = 8

# 可以添加更多代币配置
[[tokens]]
symbol = "LIKE"
name = "LIKE"
canister_id = "spdsf-5yaaa-aaaam-adcnq-cai"
decimals = 6

# 日志配置
[log]
# 日志级别: error, warn, info, debug, trace
level = "info"
# 日志文件路径
file = "logs/index-rs.log"
# 控制台日志级别 
console_level = "info"
# 是否启用文件日志
file_enabled = true
# 日志文件最大大小(MB)
max_size = 10
# 保留的历史日志文件数量
max_files = 5

# API服务器配置
[api_server]
# 是否启用API服务器
enabled = true
# API服务器端口
port = 6017
# 是否启用CORS支持
cors_enabled = true
```

## 功能特性

1. **多代币支持**

   可同时索引和查询多个代币的交易和账户数据，每个代币使用独立的数据库集合。

2. **代币小数位自动识别**
   
   程序会自动查询账本 Canister 的 `icrc1_decimals` 方法，获取代币小数位，使余额显示更准确。

3. **归档同步**
   
   程序会先从主账本 Canister 获取归档信息，然后顺序处理每个归档 Canister 中的历史交易。

4. **主账本同步**
   
   完成归档同步后，从主账本 Canister 获取最新交易，保持数据库与链上状态一致。

5. **实时余额计算**
   
   针对每笔交易，程序会实时更新相关账户的余额状态，支持转账、铸币、销毁和授权等操作。

6. **定时增量同步**
   
   每 5 秒自动检查一次主账本是否有新交易，并同步到数据库中。

7. **同步状态保存**
   
   程序为每个代币保存同步状态，确保重启后能从上次同步点继续，避免重复处理交易。

8. **完善的日志记录**
   
   支持多级别、多目标的日志记录，方便监控和问题排查。控制台仅显示重要信息，详细日志保存到文件。

## 管理员功能

1. **数据库重置**
   
   通过 `--reset` 参数触发完整重置和重新同步，仅限管理员使用。
   
   ```bash
   cargo run -- --reset
   ```
   
2. **错误恢复**
   
   即使遇到错误，程序也会尝试自动恢复和继续同步，确保数据完整性。


## API接口列表

以下所有接口路径均以 `/api` 为前缀，响应均遵循下面的 [API响应格式](#API响应格式)。

所有接口均支持通过查询参数 `token` 来指定要查询的代币，例如 `?token=VUSD`。如果不指定，则使用配置的第一个代币作为默认值。

### 代币相关

#### GET /api/tokens
- 描述：获取系统支持的所有代币列表及其详情
- 响应数据：代币列表，每个代币包含 symbol、name、decimals 和 canister_id 字段
- 示例请求：
  ```
  GET /api/tokens
  ```
- 示例响应：
  ```json
  {
    "code": 200,
    "data": [
      {
        "symbol": "ICP",
        "name": "Internet Computer",
        "decimals": 8,
        "canister_id": "ryjl3-tyaaa-aaaaa-aaaba-cai"
      },
      {
        "symbol": "LIKE",
        "name": "LIKE",
        "decimals": 6,
        "canister_id": "spdsf-5yaaa-aaaam-adcnq-cai"
      }
    ],
    "error": null
  }
  ```

#### GET /api/total_supply
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
- 描述：获取指定代币的总供应量
- 示例请求：
  ```
  GET /api/total_supply?token=ICP
  ```
- 示例响应：
  ```json
  {
    "code": 200,
    "data": "469213174432378925",
    "error": null
  }
  ```

### 账户相关

#### GET /api/balance/{account}
- 路径参数：
  - `account` (String)：账户标识，格式 `owner` 或 `owner:subaccount`
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
- 描述：查询指定账户的当前余额，返回字符串形式的余额数值
- 示例请求：
  ```
  GET /api/balance/5667a-dzhlm-w6u3z-fq2o5-lmjho-yrkdy-idhr6-6n3jx-gg4u7-fmbqg-4qe?token=VUSD
  ```
- 示例响应：
  ```json
  {
    "code": 200,
    "data": {
        "account": "5667a-dzhlm-w6u3z-fq2o5-lmjho-yrkdy-idhr6-6n3jx-gg4u7-fmbqg-4qe",
        "balance": "53457",
        "token": "VUSD",
        "token_name": "VUSD",
        "decimals": 6
    },
    "error": null
  }
  ```

#### GET /api/transactions/{account}
- 路径参数：
  - `account` (String)：账户标识
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
  - `limit` (i64)：返回记录数，默认 `50`
  - `skip` (i64)：跳过前 N 条记录，默认 `0`
- 描述：分页查询指定账户的交易历史，按交易索引倒序排列
- 示例请求：
  ```
  GET /api/transactions/ryjl3-tyaaa-aaaaa-aaaba-cai?limit=10&skip=0&token=VUSD
  ```

#### GET /api/accounts
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
  - `limit` (i64)：返回最大账户数，默认 `100`
  - `skip` (i64)：跳过前 N 个账户，默认 `0`
- 描述：分页获取所有账户列表，按账户字符串正序排列
- 示例请求：
  ```
  GET /api/accounts?limit=20&skip=0&token=VUSD
  ```

#### GET /api/active_accounts
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
  - `limit` (i64)：返回最近活跃账户数，默认 `1000`
- 描述：获取最近交易中活跃的唯一账户列表，按最新交易时间倒序
- 示例请求：
  ```
  GET /api/active_accounts?limit=20&token=VUSD
  ```

#### GET /api/account_count
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
- 描述：获取特定代币的账户总数
- 示例请求：
  ```
  GET /api/account_count?token=VUSD
  ```

### 交易相关

#### GET /api/transaction/{index}
- 路径参数：
  - `index` (u64)：交易索引
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
- 描述：查询指定索引交易的完整详情
- 示例请求：
  ```
  GET /api/transaction/1024?token=VUSD
  ```

#### GET /api/latest_transactions
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
  - `limit` (i64)：返回最新交易数，默认 `20`
- 描述：获取按索引倒序排列的最新交易列表
- 示例请求：
  ```
  GET /api/latest_transactions?limit=5&token=VUSD
  ```

#### GET /api/tx_count
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
- 描述：获取特定代币的交易总数
- 示例请求：
  ```
  GET /api/tx_count?token=VUSD
  ```

#### POST /api/search
- 请求头：
  - `Content-Type: application/json`
- 查询参数（可选）：
  - `token` (String)：代币符号，默认为配置的第一个代币
  - `limit` (i64)：返回记录数，默认 `50`
  - `skip` (i64)：跳过前 N 条记录，默认 `0`
- 请求体（JSON）：
  - 任意符合 BSON 格式的查询条件，如：
    ```json
    {
      "kind": "transfer",
      "timestamp": { "$gte": 1620000000 }
    }
    ```
- 描述：根据条件高级搜索交易，返回匹配结果列表
- 示例请求：
  ```
  POST /api/search?token=VUSD&limit=20
  Content-Type: application/json

  {
    "kind": "transfer"
  }
  ```

## API响应格式

所有 API 响应都使用统一的 JSON 格式：

```json
{
  "code": 200,
  "data": { ... },
  "error": null
}
```

失败时：

```json
{
  "code": 400,
  "data": null,
  "error": "错误信息"
}
```
