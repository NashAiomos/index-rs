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

// VUSD交易详情类型定义
export interface VUSDTransaction {
  _id: {
    $oid: string;
  };
  index: number;
  kind: string;
  timestamp: number;
  transfer?: {
    to: {
      owner: string;
      subaccount: string | null;
    };
    fee: any[];
    from: {
      owner: string;
      subaccount: string | null;
    };
    memo: string | null;
    created_at_time: string | null;
    amount: number[];
    spender: string | null;
  };
  approve?: any;
  burn?: any;
  mint?: any;
}

// API响应类型
export interface ApiResponse<T> {
  code: number;
  data: T;
  error: string | null;
} 