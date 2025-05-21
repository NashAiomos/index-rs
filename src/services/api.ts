import axios from 'axios';
import { ApiResponse, Token } from '../types';

// 设置API基础URL
const API_BASE_URL = 'http://localhost:6017/api';

// 创建axios实例
const api = axios.create({
  baseURL: API_BASE_URL
});

// 获取所有支持的代币
export const getTokens = async (): Promise<Token[]> => {
  const response = await api.get<ApiResponse<Token[]>>('/tokens');
  return response.data.data;
};

// 获取代币的总供应量
export const getTotalSupply = async (token?: string): Promise<string> => {
  const response = await api.get<ApiResponse<string>>('/total_supply', {
    params: { token }
  });
  return response.data.data;
};

// 获取账户余额
export const getBalance = async (account: string, token?: string) => {
  const response = await api.get(`/balance/${account}`, {
    params: { token }
  });
  return response.data.data;
};

// 获取最新交易
export const getLatestTransactions = async (token?: string, limit: number = 20) => {
  const response = await api.get('/latest_transactions', {
    params: { token, limit }
  });
  return response.data.data;
};

// 获取交易总数
export const getTransactionCount = async (token?: string) => {
  const response = await api.get('/tx_count', {
    params: { token }
  });
  return response.data.data;
};

// 获取账户总数
export const getAccountCount = async (token?: string) => {
  const response = await api.get('/account_count', {
    params: { token }
  });
  return response.data.data;
};

// 搜索交易或账户
export const search = async (query: string, token?: string) => {
  // 如果是有效的账户地址格式，查询余额
  if (query.match(/^[a-zA-Z0-9-]{10,}$/)) {
    try {
      const balance = await getBalance(query, token);
      return { type: 'account', data: balance };
    } catch (error) {
      // 如果不是账户，尝试作为交易哈希查询
    }
  }
  
  // 如果是有效的交易哈希格式，查询交易
  if (query.match(/^0x[a-fA-F0-9]{64}$/)) {
    try {
      const response = await api.get(`/transaction/${query}`, {
        params: { token }
      });
      return { type: 'transaction', data: response.data.data };
    } catch (error) {
      return { type: 'error', message: '未找到结果' };
    }
  }
  
  return { type: 'error', message: '搜索格式无效' };
}; 