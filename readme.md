# Blockchain Index API

一个区块链索引服务，用于同步和索引区块链上的交易数据，并提供查询 API 。

## 功能特点

- 同步区块链交易数据
- 计算账户余额
- 提供 RESTful API 接口查询交易和账户信息
- 支持增量同步和全量重置
- 支持归档数据同步

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

程序维护四个主要集合：

1. **transactions**: 存储所有交易记录
2. **accounts**: 记录账户与交易的关系
3. **balances**: 存储每个账户的最新余额信息
4. **sync_status**: 保存同步状态，支持增量同步

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
database = "token_index"
# 要索引的账本Canister ID
ledger_canister_id = "你的Canister ID"
# IC网络地址
ic_url = "https://ic0.app"
# 代币小数位数（可选，如果不设置会自动查询）
token_decimals = 8

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
port = 3000
# 是否启用CORS支持
cors_enabled = true
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

6. **同步状态保存**
   
   程序会保存同步状态，确保重启后能从上次同步点继续，避免重复处理交易。

7. **完善的日志记录**
   
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

以下是可用的 API 接口:

### 账户相关

- `GET /api/balance/{account}` - 查询账户余额
- `GET /api/transactions/{account}?limit={limit}&skip={skip}` - 查询账户交易历史
- `GET /api/accounts?limit={limit}&skip={skip}` - 获取所有账户列表
- `GET /api/active_accounts?limit={limit}&skip={skip}` - 获取活跃账户列表
- `GET /api/account_count` - 获取账户总数

### 交易相关

- `GET /api/transaction/{index}` - 查询特定交易详情
- `GET /api/latest_transactions?limit={limit}&skip={skip}` - 获取最新交易
- `GET /api/tx_count` - 获取交易总数
- `POST /api/search` - 高级交易搜索（请求体支持 JSON 格式的过滤条件）

### 代币相关

- `GET /api/total_supply` - 获取代币总供应量

## API响应格式

所有 API 响应都使用统一的 JSON 格式：

```json
{
  "success": true,
  "data": { ... },
  "error": null
}
```

失败时：

```json
{
  "success": false,
  "data": null,
  "error": "错误信息"
}
```

## 示例

### 查询账户余额

```
GET /api/balance/ryjl3-tyaaa-aaaaa-aaaba-cai
```

响应:

```json
{
  "success": true,
  "data": "1000000000",
  "error": null
}
```

### 查询账户交易历史

```
GET /api/transactions/ryjl3-tyaaa-aaaaa-aaaba-cai?limit=2&skip=0
```

响应:

```json
{
  "success": true,
  "data": [
    {
      "kind": "transfer",
      "timestamp": 1677721600000000000,
      "transfer": {
        "from": {"owner": "ryjl3-tyaaa-aaaaa-aaaba-cai"},
        "to": {"owner": "aaaaa-aa"},
        "amount": "100000000"
      },
      "index": 1024
    },
    {
      "kind": "mint",
      "timestamp": 1677721500000000000,
      "mint": {
        "to": {"owner": "ryjl3-tyaaa-aaaaa-aaaba-cai"},
        "amount": "1000000000"
      },
      "index": 1023
    }
  ],
  "error": null
}
```

### 查询特定交易详情

```
GET /api/transaction/1024
```

响应:

```json
{
  "success": true,
  "data": {
    "kind": "transfer",
    "timestamp": 1677721600000000000,
    "transfer": {
      "from": {"owner": "ryjl3-tyaaa-aaaaa-aaaba-cai"},
      "to": {"owner": "bbbb5-xxxxx"},
      "amount": "50000000"
    },
    "index": 1024
  },
  "error": null
}
```

### 获取最新交易

```
GET /api/latest_transactions?limit=3&skip=0
```

响应:

```json
{
  "success": true,
  "data": [
    {
      "kind": "transfer",
      "timestamp": 1677721700000000000,
      "transfer": {...},
      "index": 1030
    },
    {
      "kind": "mint",
      "timestamp": 1677721650000000000,
      "mint": {...},
      "index": 1029
    },
    {
      "kind": "burn",
      "timestamp": 1677721600000000000,
      "burn": {
        "from": {"owner": "cccc6-xxxxx"},
        "amount": "1000000"
      },
      "index": 1028
    }
  ],
  "error": null
}
```

### 获取交易总数

```
GET /api/tx_count
```

响应:

```json
{
  "success": true,
  "data": 2048,
  "error": null
}
```

### 获取账户总数

```
GET /api/account_count
```

响应:

```json
{
  "success": true,
  "data": 512,
  "error": null
}
```

### 获取代币总供应量

```
GET /api/total_supply
```

响应:

```json
{
  "success": true,
  "data": "1000000000000",
  "error": null
}
```

### 获取所有账户列表

```
GET /api/accounts?limit=2&skip=0
```

响应:

```json
{
  "success": true,
  "data": [
    "ryjl3-tyaaa-aaaaa-aaaba-cai",
    "aaaaa-aa"
  ],
  "error": null
}
```

### 获取活跃账户列表

```
GET /api/active_accounts?limit=2&skip=0
```

响应:

```json
{
  "success": true,
  "data": [
    "ryjl3-tyaaa-aaaaa-aaaba-cai",
    "bbbb5-xxxxx"
  ],
  "error": null
}
```

### 高级搜索交易

```
POST /api/search
Content-Type: application/json

{
  "from": "ryjl3-tyaaa-aaaaa-aaaba-cai",
  "kind": "transfer"
}
```

响应:

```json
{
  "success": true,
  "data": [
    {
      "kind": "transfer",
      "timestamp": 1677721600000000000,
      "transfer": {
        "from": {"owner": "ryjl3-tyaaa-aaaaa-aaaba-cai"},
        "to": {"owner": "aaaaa-aa"},
        "amount": "100000000"
      },
      "index": 1024
    },
    {
      "kind": "transfer",
      "timestamp": 1677721580000000000,
      "transfer": {
        "from": {"owner": "ryjl3-tyaaa-aaaaa-aaaba-cai"},
        "to": {"owner": "bbbb5-xxxxx"},
        "amount": "20000000"
      },
      "index": 1022
    }
  ],
  "error": null
}
```
