import axios from 'axios';
import { ApiResponse, Token } from '../types';

// 设置API基础URL
const API_BASE_URL = 'https://index-service.zkid.app/api';

// 创建axios实例
const api = axios.create({
  baseURL: API_BASE_URL
});

// 获取所有支持的代币
export const getTokens = async (): Promise<Token[]> => {
  try {
    const response = await api.get<ApiResponse<Token[]>>('/tokens');
    return response.data.data;
  } catch (error) {
    console.error('获取代币列表失败:', error);
    return [];
  }
};

// 获取代币的总供应量
export const getTotalSupply = async (token?: string): Promise<string> => {
  const paramToken = token?.toUpperCase();
  try {
    const response = await api.get<ApiResponse<string>>('/total_supply', {
      params: { token: paramToken }
    });
    
    // 处理返回的格式，移除下划线使其适合展示
    if (response.data.data) {
      return response.data.data.replace(/_/g, ',');
    }
    return '0';
  } catch (error) {
    console.error(`获取${token || ''}总供应量失败:`, error);
    throw error;
  }
};

// 获取账户余额
export const getBalance = async (account: string, token?: string) => {
  try {
    const response = await api.get(`/balance/${account}`, {
      params: { token }
    });
    return response.data.data;
  } catch (error) {
    console.error(`获取账户${account}余额失败:`, error);
    throw error;
  }
};

// 获取最新交易
export const getLatestTransaction = async (token?: string) => {
  const paramToken = token?.toUpperCase();
  try {
    const response = await api.get('/latest_transactions', {
      params: { token: paramToken, limit: 1 }
    });
    return response.data.data?.[0] || null;
  } catch (error) {
    console.error(`获取${token || ''}最新交易失败:`, error);
    return null;
  }
};

// 获取最新交易列表
export const getLatestTransactions = async (token?: string, limit: number = 20, offset: number = 0) => {
  const paramToken = token?.toUpperCase();
  try {
    const response = await api.get('/latest_transactions', {
      params: { token: paramToken, limit, offset }
    });
    return response.data.data || [];
  } catch (error) {
    console.error(`获取${token || ''}最新交易列表失败:`, error);
    return [];
  }
};

// 获取交易总数
export const getTransactionCount = async (token?: string) => {
  const paramToken = token?.toUpperCase();
  try {
    const response = await api.get('/tx_count', {
      params: { token: paramToken }
    });
    return response.data.data || '0';
  } catch (error) {
    console.error(`获取${token || ''}交易总数失败:`, error);
    throw error;
  }
};

// 获取账户总数
export const getAccountCount = async (token?: string) => {
  const paramToken = token?.toUpperCase();
  try {
    const response = await api.get('/account_count', {
      params: { token: paramToken }
    });
    return response.data.data || '0';
  } catch (error) {
    console.error(`获取${token || ''}账户总数失败:`, error);
    throw error;
  }
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
      console.error('搜索账户失败，尝试作为交易哈希查询');
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
      console.error('搜索交易失败:', error);
      return { type: 'error', message: '未找到结果' };
    }
  }
  
  return { type: 'error', message: '搜索格式无效' };
};

// 按范围获取交易
export const getTransactionsByRange = async (start: number, end: number, token?: string) => {
  const paramToken = token?.toUpperCase();
  try {
    const response = await api.get(`/transactions_by_range/${start}/${end}`, {
      params: { token: paramToken }
    });
    return response.data.data || { transactions: [] };
  } catch (error) {
    console.error(`获取${token || ''}交易范围数据失败:`, error);
    return { transactions: [] };
  }
}; 