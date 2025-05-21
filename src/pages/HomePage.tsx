import React, { useEffect, useState } from 'react';
import Header from '../components/Header';
import TokenCard from '../components/TokenCard';
import TransactionList from '../components/TransactionList';
import { getLatestTransactions, getAccountCount, getTransactionCount } from '../services/api';
import { Transaction } from '../types';

const HomePage: React.FC = () => {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  
  const [likeStats, setLikeStats] = useState({
    transactionVolume: '1,234,567',
    transactions24h: '45,678',
    totalAddresses: '12,345'
  });
  
  const [vusdStats, setVusdStats] = useState({
    transactionVolume: '2,345,678',
    transactions24h: '34,567',
    totalAddresses: '9,876'
  });

  useEffect(() => {
    const fetchData = async () => {
      try {
        // 获取最新交易数据
        const latestTxs = await getLatestTransactions('LIKE', 5);
        
        // 转换为前端所需格式
        const formattedTxs = latestTxs.map((tx: any) => ({
          hash: tx.hash || tx.index?.toString() || '',
          time: tx.timestamp ? new Date(tx.timestamp * 1000).toISOString() : '',
          from: tx.from || '',
          to: tx.to || '',
          value: tx.value || tx.amount || '0',
          token: tx.token || 'LIKE'
        }));
        
        setTransactions(formattedTxs);
        
        // 获取LIKE代币统计数据
        const likeAccounts = await getAccountCount('LIKE');
        const likeTxCount = await getTransactionCount('LIKE');
        
        // 假设每天交易量是总交易量的5%
        const likeTx24h = Math.floor(Number(likeTxCount) * 0.05).toLocaleString();
        
        setLikeStats({
          transactionVolume: '1,234,567', // 可从API获取实际数据
          transactions24h: likeTx24h,
          totalAddresses: Number(likeAccounts).toLocaleString()
        });
        
        // 获取vUSD代币统计数据
        const vusdAccounts = await getAccountCount('vUSD');
        const vusdTxCount = await getTransactionCount('vUSD');
        
        // 假设每天交易量是总交易量的5%
        const vusdTx24h = Math.floor(Number(vusdTxCount) * 0.05).toLocaleString();
        
        setVusdStats({
          transactionVolume: '2,345,678', // 可从API获取实际数据
          transactions24h: vusdTx24h,
          totalAddresses: Number(vusdAccounts).toLocaleString()
        });
      } catch (error) {
        console.error('Failed to fetch data:', error);
        // 使用默认数据
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  return (
    <div className="min-h-screen bg-gray-50">
      <Header />
      
      <main className="container mx-auto py-8">
        <div className="token-card-flex">
          <TokenCard 
            symbol="LIKE"
            transactionVolume={likeStats.transactionVolume}
            transactions24h={likeStats.transactions24h}
            totalAddresses={likeStats.totalAddresses}
          />
          
          <TokenCard 
            symbol="vUSD"
            transactionVolume={vusdStats.transactionVolume}
            transactions24h={vusdStats.transactions24h}
            totalAddresses={vusdStats.totalAddresses}
          />
        </div>
        
        <TransactionList transactions={transactions} loading={loading} />
      </main>
    </div>
  );
};

export default HomePage; 