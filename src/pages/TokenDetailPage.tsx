import React, { useEffect, useState, useCallback, useRef } from 'react';
import { useParams, Link } from 'react-router-dom';
import Header from '../components/Header';
import { getTransactionsByRange, getTotalSupply, getAccountCount, getTransactionCount, getLatestTransaction } from '../services/api';
import { Transaction, VUSDTransaction } from '../types';

// 格式化地址显示，截取前6位和后4位
const formatAddress = (address: string) => {
  if (!address) return '';
  return address.length > 10 
    ? `${address.substring(0, 12)}...${address.substring(address.length - 10)}`
    : address;
};

// 格式化代币金额显示，考虑小数位数
const formatTokenValue = (value: string, symbol: string) => {
  console.log('格式化代币金额:', { value, symbol, valueType: typeof value });
  
  if (!value) return '0';
  
  // 确保value是字符串，并移除可能的非数字字符（如下划线）
  const strValue = String(value).replace(/[^0-9.-]/g, '');
  console.log('清理后的字符串值:', strValue);
  
  // VUSD有6位小数
  if (symbol.toUpperCase() === 'VUSD') {
    try {
      // 转换为数字进行计算
      const numValue = Number(strValue);
      console.log('转换为数字:', numValue);
      
      if (isNaN(numValue)) {
        console.log('数值转换失败，返回0');
        return '0';
      }
      
      // VUSD始终使用6位小数，直接除以1,000,000
      const formattedValue = (numValue / 1000000).toFixed(6);
      
      // 移除尾部不必要的0，但保留小数点前的0
      const finalValue = formattedValue.replace(/\.?0+$/, '') || '0';
      
      console.log('最终格式化结果:', finalValue);
      return finalValue;
    } catch (error) {
      console.error('转换VUSD金额出错:', error);
      return '0';
    }
  }
  
  return strValue;
};

const TokenDetailPage: React.FC = () => {
  const { symbol } = useParams<{ symbol: string }>();
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [currentStartIndex, setCurrentStartIndex] = useState<number | null>(null);
  const [batchSize] = useState(300); // 每次加载300条交易
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);
  
  // 代币统计信息初始状态
  const [tokenStats, setTokenStats] = useState({
    totalSupply: '',
    holdingAccounts: '',
    transactions24h: ''
  });
  
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(20);
  const [dateFilter, setDateFilter] = useState('');
  
  // 用于存储最新数据的引用，避免在刷新时丢失
  const latestDataRef = useRef({
    transactions: [] as Transaction[],
    tokenStats: {
      totalSupply: '',
      holdingAccounts: '',
      transactions24h: ''
    }
  });

  // 获取数据的函数
  const fetchData = useCallback(async (loadMore = false) => {
    try {
      const token = symbol || 'LIKE';
      
      if (loadMore) {
        setLoadingMore(true);
      } else {
        setLoading(true);
      }
      setError(null);

      // 获取代币统计数据
      const [totalSupply, accountCount, txCount] = await Promise.allSettled([
        getTotalSupply(token),
        getAccountCount(token),
        getTransactionCount(token)
      ]);
      
      // 处理总供应量
      if (totalSupply.status === 'fulfilled') {
        latestDataRef.current.tokenStats.totalSupply = totalSupply.value;
      } else {
        console.warn('获取总供应量失败:', totalSupply.reason);
      }
      
      // 处理账户数
      if (accountCount.status === 'fulfilled') {
        latestDataRef.current.tokenStats.holdingAccounts = Number(accountCount.value).toLocaleString();
      } else {
        console.warn('获取账户数量失败:', accountCount.reason);
      }
      
      // 处理交易数
      if (txCount.status === 'fulfilled') {
        latestDataRef.current.tokenStats.transactions24h = Number(txCount.value).toLocaleString();
      } else {
        console.warn('获取交易数量失败:', txCount.reason);
      }
      
      // 更新状态
      setTokenStats(latestDataRef.current.tokenStats);
    } catch (error) {
      console.error('获取代币统计数据失败:', error);
      setError('获取代币数据失败，请刷新页面重试');
    } finally {
      if (!loadMore) {
        setLoading(false);
      }
    }
  }, [symbol]);

  // 初始加载数据
  useEffect(() => {
    // 重置状态
    setTransactions([]);
    setCurrentStartIndex(null);
    setHasMore(true);
    setCurrentPage(1);
    
    // 获取基本信息
    fetchData();
    
    // 获取初始交易数据
    fetchInitialTransactions();
  }, [symbol]);
  
  // 添加监视器查看交易数据结构
  useEffect(() => {
    if (transactions.length > 0) {
      console.log('当前交易数据结构示例:', transactions[0]);
      // 获取当前页的交易数据
      const currentPageTransactions = transactions
        .filter(tx => {
          if (!dateFilter) return true;
          try {
            const txDate = new Date(tx.time).toISOString().split('T')[0];
            return txDate === dateFilter;
          } catch {
            return true;
          }
        })
        .slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);
      
      console.log('当前页面交易数据:', currentPageTransactions);
    }
  }, [transactions, currentPage, itemsPerPage, dateFilter]);
  
  // 获取初始交易数据
  const fetchInitialTransactions = async (): Promise<void> => {
    try {
      setLoading(true);
      setError(null);
      
      const token = symbol || 'LIKE';
      
      // 1. 首先获取最新的一条交易，获取最新索引
      let latestIndex = 25775; // 默认值，以防API调用失败
      
      try {
        // 获取最新的一条交易
        const latestTx = await getLatestTransaction(token);
        if (latestTx && latestTx.index) {
          latestIndex = latestTx.index;
          console.log('获取到最新交易索引:', latestIndex);
        } else {
          console.warn('未获取到最新交易索引，使用默认值');
        }
      } catch (error) {
        console.warn('获取最新交易失败，使用默认值:', error);
      }
      
      // 2. 使用批量获取交易API获取300条最新交易
      const start = latestIndex;
      const end = Math.max(0, latestIndex - batchSize + 1);
      
      try {
        console.log(`获取交易范围: ${start} 到 ${end}`);
        // 获取批量交易数据
        const result = await getTransactionsByRange(start, end, token);
        
        if (result && Array.isArray(result.transactions) && result.transactions.length > 0) {
          // 处理交易数据
          const formattedTxs: Transaction[] = formatTransactions(result.transactions, token);
          
          // 更新状态
          setTransactions(formattedTxs);
          latestDataRef.current.transactions = formattedTxs;
          setCurrentStartIndex(end - 1); // 下一批的开始索引
          
          // 检查是否还有更多数据
          if (end <= 0) {
            setHasMore(false);
          }
        } else {
          throw new Error('返回的交易数据为空');
        }
      } catch (error) {
        console.error('获取交易数据失败:', error);
        setError('获取交易数据失败，请刷新重试');
        setHasMore(false);
      }
    } finally {
      setLoading(false);
    }
  };
  
  // 获取更多交易数据
  const fetchMoreTransactions = useCallback(async (): Promise<void> => {
    if (!hasMore || currentStartIndex === null || currentStartIndex < 0 || loadingMore) {
      return;
    }
    
    try {
      setLoadingMore(true);
      setError(null);
      
      const token = symbol || 'LIKE';
      
      // 计算起始和结束索引
      const start = currentStartIndex;
      const end = Math.max(0, start - batchSize + 1);
      
      try {
        // 获取下一批交易数据
        const result = await getTransactionsByRange(start, end, token);
        
        if (result && Array.isArray(result.transactions) && result.transactions.length > 0) {
          // 处理交易数据
          const formattedTxs: Transaction[] = formatTransactions(result.transactions, token);
          
          // 更新状态
          setTransactions(prevTxs => [...prevTxs, ...formattedTxs]);
          latestDataRef.current.transactions = [...latestDataRef.current.transactions, ...formattedTxs];
          
          // 更新下一批的开始索引
          setCurrentStartIndex(end - 1);
          
          // 检查是否还有更多数据
          if (end <= 0) {
            setHasMore(false);
          }
        } else {
          throw new Error('返回的交易数据为空');
        }
      } catch (error) {
        console.error('获取更多交易数据失败:', error);
        setError('加载更多数据失败，请重试');
        setHasMore(false);
      }
    } finally {
      setLoadingMore(false);
    }
  }, [hasMore, currentStartIndex, loadingMore, symbol, batchSize]);
  
  // 加载更多交易
  const loadMoreTransactions = useCallback(() => {
    fetchMoreTransactions();
  }, [fetchMoreTransactions]);

  // 格式化交易数据的辅助函数
  const formatTransactions = (transactions: any[], token: string): Transaction[] => {
    if (!Array.isArray(transactions)) return [];
    
    console.log('开始格式化交易数据，原始数据:', JSON.stringify(transactions[0], null, 2));
    
    // 针对VUSD的特殊处理
    if (token.toUpperCase() === 'VUSD') {
      return transactions.map((tx: any, index) => {
        console.log(`处理VUSD交易 ${index}:`, JSON.stringify(tx, null, 2));
        
        // 正确提取时间戳
        let timestamp = 0;
        if (tx.timestamp) {
          if (String(tx.timestamp).length > 13) {
            timestamp = Math.floor(Number(tx.timestamp) / 1000000);
          } else {
            timestamp = Number(tx.timestamp);
          }
        }
        
        // 直接检查和解构交易对象结构
        // 提取from和to地址 - 处理各种可能的数据结构
        let fromAddress = '';
        let toAddress = '';
        let value = '';  // 使用空字符串初始化，而不是'0'，以便于调试
        
        // 详细检查交易数据结构，提取value
        console.log('检查交易数据结构以提取value:', tx);
        
        if (tx.transfer) {
          // 通用处理转账交易
          console.log('交易含有transfer字段:', tx.transfer);
          
          if (tx.transfer.from && tx.transfer.from.owner) {
            fromAddress = tx.transfer.from.owner;
          }
          
          if (tx.transfer.to && tx.transfer.to.owner) {
            toAddress = tx.transfer.to.owner;
          }
          
          // 检查amount字段的数据类型和结构
          console.log('检查amount字段:', tx.transfer.amount, 
                     '类型:', Array.isArray(tx.transfer.amount) ? 'Array' : typeof tx.transfer.amount);
          
          if (tx.transfer.amount) {
            if (Array.isArray(tx.transfer.amount) && tx.transfer.amount.length > 0) {
              console.log('amount是数组:', tx.transfer.amount);
              const amountValue = tx.transfer.amount[0];
              console.log('提取的amount[0]值:', amountValue, '类型:', typeof amountValue);
              value = amountValue !== undefined && amountValue !== null ? 
                     String(amountValue) : '';
            } else {
              console.log('amount不是数组:', tx.transfer.amount);
              value = String(tx.transfer.amount);
            }
          }
        } else if (tx.kind === 'transfer') {
          // 处理另一种可能的转账结构
          console.log('交易kind为transfer:', tx);
          
          if (tx.from) {
            fromAddress = typeof tx.from === 'string' ? tx.from : 
                         (tx.from as any).owner ? (tx.from as any).owner : '';
          }
          
          if (tx.to) {
            toAddress = typeof tx.to === 'string' ? tx.to : 
                       (tx.to as any).owner ? (tx.to as any).owner : '';
          }
          
          // 检查amount字段
          console.log('检查amount字段:', tx.amount, '类型:', typeof tx.amount);
          
          if (tx.amount !== undefined) {
            value = String(tx.amount);
          }
        }
        
        // 确保value值有效
        if (value === undefined || value === null) {
          console.log('Value值无效，设置为空字符串');
          value = '';
        }
        
        console.log('提取的value值:', value, '类型:', typeof value);
        
        // 生成返回对象
        const result = {
          hash: tx.index?.toString() || '',
          time: timestamp ? new Date(timestamp).toISOString() : '',
          from: fromAddress,
          to: toAddress,
          value: value,
          token: 'VUSD'
        };
        
        console.log('格式化后的VUSD交易:', result);
        return result;
      });
    } else {
      // 转换为前端所需格式 - 非VUSD代币
      return transactions.map((tx: any, index) => {
        console.log(`处理普通交易 ${index}:`, JSON.stringify(tx, null, 2));
        
        // 处理可能的不同数据格式
        const txHash = tx.hash || tx.index?.toString() || '';
        
        // 处理时间戳
        let timestamp = 0;
        if (tx.timestamp) {
          if (String(tx.timestamp).length > 13) {
            timestamp = Math.floor(Number(tx.timestamp) / 1000000);
          } else {
            timestamp = Number(tx.timestamp);
          }
        }
        const time = timestamp ? new Date(timestamp).toISOString() : '';
        
        // 处理嵌套的from和to字段 - 考虑多种可能的数据结构
        let fromAddress = '';
        let toAddress = '';
        
        // 处理各种可能的from结构
        if (tx.transfer && tx.transfer.from && tx.transfer.from.owner) {
          fromAddress = tx.transfer.from.owner;
        } else if (tx.from) {
          if (typeof tx.from === 'string') {
            fromAddress = tx.from;
          } else if ((tx.from as any).owner) {
            fromAddress = (tx.from as any).owner;
          }
        }
        
        // 处理各种可能的to结构
        if (tx.transfer && tx.transfer.to && tx.transfer.to.owner) {
          toAddress = tx.transfer.to.owner;
        } else if (tx.to) {
          if (typeof tx.to === 'string') {
            toAddress = tx.to;
          } else if ((tx.to as any).owner) {
            toAddress = (tx.to as any).owner;
          }
        }
        
        // 处理金额 - 考虑多种可能的数据结构
        let value = '0';
        if (tx.value !== undefined) {
          value = tx.value.toString();
        } else if (tx.amount !== undefined) {
          value = tx.amount.toString();
        } else if (tx.transfer && tx.transfer.amount) {
          if (Array.isArray(tx.transfer.amount) && tx.transfer.amount.length > 0) {
            value = tx.transfer.amount[0]?.toString() || '0';
          } else {
            value = tx.transfer.amount.toString();
          }
        }
        
        // 生成返回对象
        const result = {
          hash: txHash,
          time: time,
          from: fromAddress,
          to: toAddress,
          value: value,
          token: tx.token || token
        };
        
        console.log('格式化后的普通交易:', result);
        return result;
      });
    }
  };

  // 处理页码变化
  const handlePageChange = (page: number) => {
    setCurrentPage(page);
  };

  // 处理每页显示数量变化
  const handleItemsPerPageChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setItemsPerPage(Number(e.target.value));
    setCurrentPage(1); // 重置到第一页
  };

  // 处理日期筛选
  const handleDateFilterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setDateFilter(e.target.value);
  };

  // 计算总页数
  const totalPages = Math.ceil(transactions.length / itemsPerPage);

  // 获取当前页的交易
  const currentTransactions = transactions
    .filter(tx => {
      if (!dateFilter) return true;
      try {
        const txDate = new Date(tx.time).toISOString().split('T')[0];
        return txDate === dateFilter;
      } catch {
        return true;
      }
    })
    .slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

  return (
    <div className="min-h-screen bg-gray-50">
      <Header />
      
      <main className="container mx-auto py-8 px-4">
        {/* 代币信息卡片 */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <div className="flex flex-wrap items-center gap-2 mb-4">
            <h1 className="text-2xl font-bold text-gray-800">{symbol}</h1>
          </div>
          
          <div className="flex gap-40">
            <div>
              <h3 className="text-gray-500 text-sm mb-1">Total Supply</h3>
              <p className="text-xl font-bold">{tokenStats.totalSupply || '加载中...'}</p>
            </div>
            <div>
              <h3 className="text-gray-500 text-sm mb-1">Total Addresses</h3>
              <p className="text-xl font-bold">{tokenStats.holdingAccounts || '加载中...'}</p>
            </div>
          </div>
        </div>
        
        {/* 错误信息显示 */}
        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4 relative">
            <span className="block sm:inline">{error}</span>
            <button 
              className="px-4 py-2 bg-red-500 text-white rounded ml-4"
              onClick={() => {
                setError(null);
                fetchInitialTransactions();
              }}
            >
              重试
            </button>
          </div>
        )}
        
        {/* 交易列表 */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Transactions</h2>
          
          {/* 日期筛选 */}
          <div className="mb-4">
            <input
              type="date"
              className="px-4 py-2 border border-gray-300 rounded-md"
              value={dateFilter}
              onChange={handleDateFilterChange}
              placeholder="yyyy / mm / dd"
            />
          </div>
          
          {loading ? (
            <div className="animate-pulse">
              {[...Array(5)].map((_, i) => (
                <div key={i} className="border-b border-gray-100 py-4">
                  <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                  <div className="h-4 bg-gray-200 rounded w-1/2"></div>
                </div>
              ))}
            </div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Transaction Hash
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Time
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        From
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        To
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Value
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Token
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {currentTransactions.length > 0 ? (
                      currentTransactions.map((tx, index) => {
                        // 计算相对时间
                        const minutesAgo = tx.time ? 
                          Math.floor((Date.now() - new Date(tx.time).getTime()) / 60000) : 0;
                        
                        let timeDisplay = '';
                        if (minutesAgo < 1) timeDisplay = 'just now';
                        else if (minutesAgo < 60) timeDisplay = `${minutesAgo} mins ago`;
                        else timeDisplay = `${Math.floor(minutesAgo / 60)} hrs ago`;
                        
                        return (
                          <tr key={index} className="hover:bg-gray-50">
                            <td className="px-4 py-3 whitespace-nowrap text-sm">
                              <Link to={`/transaction/${tx.hash}`} className="text-blue-500 hover:text-blue-700 flex items-center">
                                {tx.hash}
                                <svg className="w-4 h-4 ml-1 text-gray-400" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg">
                                  <path d="M7 9a2 2 0 012-2h6a2 2 0 012 2v6a2 2 0 01-2 2H9a2 2 0 01-2-2V9z"></path>
                                  <path d="M5 3a2 2 0 00-2 2v6a2 2 0 002 2V5h8a2 2 0 00-2-2H5z"></path>
                                </svg>
                              </Link>
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                              {timeDisplay}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm">
                              {tx.from ? (
                                <Link to={`/address/${tx.from}`} className="text-blue-500 hover:text-blue-700">
                                  {formatAddress(tx.from)}
                                </Link>
                              ) : (
                                <span className="text-gray-400">-</span>
                              )}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm">
                              {tx.to ? (
                                <Link to={`/address/${tx.to}`} className="text-blue-500 hover:text-blue-700">
                                  {formatAddress(tx.to)}
                                </Link>
                              ) : (
                                <span className="text-gray-400">-</span>
                              )}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">
                              {(() => {
                                // 在渲染前输出调试信息
                                console.log('渲染Value值:', {
                                  value: tx.value, 
                                  type: typeof tx.value,
                                  token: tx.token
                                });
                                
                                // 格式化后的值
                                const formattedValue = tx.value 
                                  ? formatTokenValue(tx.value, tx.token) 
                                  : '0';
                                
                                return (
                                  <div>
                                    <span>{formattedValue}</span>
                                  </div>
                                );
                              })()}
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">
                              {tx.token}
                            </td>
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td colSpan={6} className="px-4 py-5 text-center text-gray-500">
                          No transactions found
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>

              {/* 分页控制 */}
              <div className="flex items-center justify-between mt-4">
                <div className="flex items-center">
                  <span className="mr-2 text-sm text-gray-700">Items per page:</span>
                  <select
                    className="border border-gray-300 rounded-md px-2 py-1 text-sm"
                    value={itemsPerPage}
                    onChange={handleItemsPerPageChange}
                  >
                    <option value="20">20</option>
                    <option value="30">30</option>
                    <option value="50">50</option>
                    <option value="60">60</option>
                  </select>
                </div>
                
                <div className="flex space-x-1">
                  {/* 上一页按钮 */}
                  <button
                    onClick={() => handlePageChange(currentPage - 1)}
                    disabled={currentPage === 1}
                    className={`px-3 py-1 rounded-md ${
                      currentPage === 1 
                        ? 'text-gray-400 cursor-not-allowed' 
                        : 'text-gray-700 hover:bg-gray-100'
                    }`}
                  >
                    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg">
                      <path fillRule="evenodd" d="M12.707 5.293a1 1 0 010 1.414L9.414 10l3.293 3.293a1 1 0 01-1.414 1.414l-4-4a1 1 0 010-1.414l4-4a1 1 0 011.414 0z" clipRule="evenodd"></path>
                    </svg>
                  </button>
                  
                  {/* 页码按钮 - 限制显示 */}
                  {[...Array(Math.min(5, totalPages))].map((_, i) => {
                    // 根据当前页计算显示哪些页码
                    let pageNum: number;
                    if (totalPages <= 5) {
                      // 总页数小于等于5，显示所有页码
                      pageNum = i + 1;
                    } else if (currentPage <= 3) {
                      // 当前页靠近开始，显示前5页
                      pageNum = i + 1;
                    } else if (currentPage >= totalPages - 2) {
                      // 当前页靠近结束，显示最后5页
                      pageNum = totalPages - 4 + i;
                    } else {
                      // 当前页在中间，显示当前页及其前后两页
                      pageNum = currentPage - 2 + i;
                    }
                    
                    return (
                      <button
                        key={i}
                        onClick={() => handlePageChange(pageNum)}
                        className={`px-3 py-1 rounded-md ${
                          currentPage === pageNum
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-700 hover:bg-gray-100'
                        }`}
                      >
                        {pageNum}
                      </button>
                    );
                  })}
                  
                  {/* 下一页或更多按钮 */}
                  {currentPage < totalPages ? (
                    <button
                      onClick={() => handlePageChange(currentPage + 1)}
                      className="px-3 py-1 rounded-md text-gray-700 hover:bg-gray-100"
                    >
                      <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg">
                        <path fillRule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clipRule="evenodd"></path>
                      </svg>
                    </button>
                  ) : hasMore ? (
                    <button
                      onClick={loadMoreTransactions}
                      disabled={loadingMore}
                      className="px-3 py-1 rounded-md bg-blue-600 text-white hover:bg-blue-700"
                    >
                      {loadingMore ? '加载中...' : '更多'}
                    </button>
                  ) : null}
                </div>
              </div>
            </>
          )}
        </div>
      </main>
    </div>
  );
};

export default TokenDetailPage; 
