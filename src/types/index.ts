// 代币类型定义
export interface Token {
  symbol: string;
  name: string;
  decimals: number;
  canister_id: string;
}

// 代币统计信息类型
export interface TokenStats {
  transactionVolume: string;
  transactions24h: string;
  totalAddresses: string;
}

// 交易类型定义
export interface Transaction {
  hash: string;
  time: string;
  from: string;
  to: string;
  value: string;
  token: string;
}

// API响应类型
export interface ApiResponse<T> {
  code: number;
  data: T;
  error: string | null;
} 