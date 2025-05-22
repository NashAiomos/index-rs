import React, { useEffect, useState, useCallback, useRef } from 'react';
import Header from '../components/Header';
import TokenCard from '../components/TokenCard';
import TransactionList from '../components/TransactionList';
import { getLatestTransactions, getAccountCount, getTransactionCount, getTotalSupply } from '../services/api';
import { Transaction, VUSDTransaction } from '../types';
import { Link } from 'react-router-dom';

const HomePage: React.FC = () => {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  
  // LIKE代币统计信息初始状态
  const [likeStats, setLikeStats] = useState({
    totalSupply: '',
    totalAddresses: ''
  });
  
  // vUSD代币统计信息初始状态
  const [vusdStats, setVusdStats] = useState({
    totalSupply: '',
    totalAddresses: ''
  });

  // 用于存储最新数据的引用，避免在刷新时丢失
  const latestDataRef = useRef({
    transactions: [] as Transaction[],
    likeStats: {
      totalSupply: '',
      totalAddresses: ''
    },
    vusdStats: {
      totalSupply: '',
      totalAddresses: ''
    }
  });

  // 获取数据的函数
  const fetchData = useCallback(async () => {
    try {
      // 获取LIKE代币数据
      const [likeTotalSupply, likeAccounts, likeTxCount, likeTxs] = await Promise.allSettled([
        getTotalSupply('LIKE'),
        getAccountCount('LIKE'),
        getTransactionCount('LIKE'),
        getLatestTransactions('LIKE', 5)
      ]);
      
      // 处理LIKE数据
      if (likeTotalSupply.status === 'fulfilled') {
        latestDataRef.current.likeStats.totalSupply = likeTotalSupply.value;
      }
      
      if (likeAccounts.status === 'fulfilled') {
        latestDataRef.current.likeStats.totalAddresses = Number(likeAccounts.value).toLocaleString();
      }
      
      // 获取vUSD代币数据
      const [vusdTotalSupply, vusdAccounts, vusdTxCount] = await Promise.allSettled([
        getTotalSupply('vUSD'),
        getAccountCount('vUSD'),
        getTransactionCount('vUSD')
      ]);
      
      // 处理vUSD数据
      if (vusdTotalSupply.status === 'fulfilled') {
        latestDataRef.current.vusdStats.totalSupply = vusdTotalSupply.value;
      }
      
      if (vusdAccounts.status === 'fulfilled') {
        latestDataRef.current.vusdStats.totalAddresses = Number(vusdAccounts.value).toLocaleString();
      }
      
      // 处理交易列表
      if (likeTxs.status === 'fulfilled' && Array.isArray(likeTxs.value)) {
        // 处理LIKE交易数据
        const formattedTxs = likeTxs.value.map((tx: any) => {
          let timeString = '';
          try {
            // 检查时间戳格式，避免无效值导致的错误
            if (tx.timestamp) {
              const timestamp = Number(tx.timestamp);
              if (!isNaN(timestamp) && isFinite(timestamp)) {
                timeString = new Date(timestamp * 1000).toISOString();
              }
            }
          } catch (e) {
            console.warn('Invalid timestamp format:', tx.timestamp);
          }
          
          return {
            hash: tx.hash || tx.index?.toString() || '',
            time: timeString,
            from: tx.from || '',
            to: tx.to || '',
            value: tx.value || tx.amount || '0',
            token: tx.token || 'LIKE'
          };
        });
        
        latestDataRef.current.transactions = formattedTxs;
      }
      
      // 更新状态
      setLikeStats(latestDataRef.current.likeStats);
      setVusdStats(latestDataRef.current.vusdStats);
      setTransactions(latestDataRef.current.transactions);
    } catch (error) {
      console.error('Failed to fetch data:', error);
      // 保持使用现有数据，不在UI中显示错误
    } finally {
      setLoading(false);
    }
  }, []);

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

  return (
    <div className="min-h-screen bg-gray-50">
      <Header />
      
      <main className="container mx-auto py-8">
        <div className="token-card-flex">
          <Link to="/token/LIKE">
            <TokenCard 
              symbol="LIKE"
              totalSupply={likeStats.totalSupply}
              totalAddresses={likeStats.totalAddresses}
            />
          </Link>
          <Link to="/token/vUSD">
            <TokenCard 
              symbol="vUSD"
              totalSupply={vusdStats.totalSupply}
              totalAddresses={vusdStats.totalAddresses}
            />
          </Link>
        </div>
        
        <TransactionList transactions={transactions} loading={loading} />
      </main>
    </div>
  );
};

export default HomePage; 