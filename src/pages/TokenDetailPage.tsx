import React, { useEffect, useState, useCallback, useRef } from 'react';
import { useParams, Link } from 'react-router-dom';
import Header from '../components/Header';
import { getLatestTransactions, getTotalSupply, getAccountCount, getTransactionCount } from '../services/api';
import { Transaction } from '../types';

// 格式化地址显示，截取前6位和后4位
const formatAddress = (address: string) => {
  if (!address) return '';
  return address.length > 10 
    ? `${address.substring(0, 6)}...${address.substring(address.length - 4)}`
    : address;
};

const TokenDetailPage: React.FC = () => {
  const { symbol } = useParams<{ symbol: string }>();
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  
  // 代币统计信息初始状态
  const [tokenStats, setTokenStats] = useState({
    totalSupply: '',
    holdingAccounts: '',
    transactions24h: ''
  });
  
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(5);
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
  const fetchData = useCallback(async () => {
    try {
      const token = symbol || 'LIKE';
      
      // 获取代币统计数据
      const [totalSupply, accountCount, txCount, latestTxs] = await Promise.allSettled([
        getTotalSupply(token),
        getAccountCount(token),
        getTransactionCount(token),
        getLatestTransactions(token, 20)
      ]);
      
      // 处理总供应量
      if (totalSupply.status === 'fulfilled') {
        latestDataRef.current.tokenStats.totalSupply = totalSupply.value;
      }
      
      // 处理账户数
      if (accountCount.status === 'fulfilled') {
        latestDataRef.current.tokenStats.holdingAccounts = Number(accountCount.value).toLocaleString();
      }
      
      // 处理交易数
      if (txCount.status === 'fulfilled') {
        latestDataRef.current.tokenStats.transactions24h = Number(txCount.value).toLocaleString();
      }
      
      // 处理交易列表
      if (latestTxs.status === 'fulfilled' && Array.isArray(latestTxs.value)) {
        // 转换为前端所需格式
        const formattedTxs = latestTxs.value.map((tx: any) => ({
          hash: tx.hash || tx.index?.toString() || '',
          time: tx.timestamp ? new Date(tx.timestamp * 1000).toISOString() : '',
          from: tx.from || '',
          to: tx.to || '',
          value: tx.value || tx.amount || '0',
          token: tx.token || token
        }));
        
        latestDataRef.current.transactions = formattedTxs;
      }
      
      // 更新状态
      setTokenStats(latestDataRef.current.tokenStats);
      setTransactions(latestDataRef.current.transactions);
    } catch (error) {
      console.error('Failed to fetch data:', error);
      // 保持使用现有数据，不在UI中显示错误
    } finally {
      setLoading(false);
    }
  }, [symbol]);

  // 初始加载数据
  useEffect(() => {
    fetchData();
  }, [fetchData]);
  
  // 设置定时刷新
  useEffect(() => {
    const refreshInterval = setInterval(() => {
      fetchData();
    }, 7000); // 每7秒刷新一次
    
    return () => clearInterval(refreshInterval);
  }, [fetchData]);

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
              <p className="text-xl font-bold">{tokenStats.totalSupply}</p>
            </div>
            <div>
              <h3 className="text-gray-500 text-sm mb-1">Total Addresses</h3>
              <p className="text-xl font-bold">{tokenStats.holdingAccounts}</p>
            </div>
          </div>
        </div>
        
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
                              <Link to={`/address/${tx.from}`} className="text-blue-500 hover:text-blue-700">
                                {tx.from}
                              </Link>
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm">
                              <Link to={`/address/${tx.to}`} className="text-blue-500 hover:text-blue-700">
                                {tx.to}
                              </Link>
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">
                              {tx.value}
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
                    <option value="5">5</option>
                    <option value="10">10</option>
                    <option value="20">20</option>
                    <option value="50">50</option>
                  </select>
                </div>
                
                <div className="flex space-x-1">
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
                  
                  {/* 页码按钮 */}
                  {[...Array(totalPages)].map((_, i) => (
                    <button
                      key={i}
                      onClick={() => handlePageChange(i + 1)}
                      className={`px-3 py-1 rounded-md ${
                        currentPage === i + 1
                          ? 'bg-blue-600 text-white'
                          : 'text-gray-700 hover:bg-gray-100'
                      }`}
                    >
                      {i + 1}
                    </button>
                  ))}
                  
                  <button
                    onClick={() => handlePageChange(currentPage + 1)}
                    disabled={currentPage === totalPages || totalPages === 0}
                    className={`px-3 py-1 rounded-md ${
                      currentPage === totalPages || totalPages === 0
                        ? 'text-gray-400 cursor-not-allowed'
                        : 'text-gray-700 hover:bg-gray-100'
                    }`}
                  >
                    <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg">
                      <path fillRule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clipRule="evenodd"></path>
                    </svg>
                  </button>
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