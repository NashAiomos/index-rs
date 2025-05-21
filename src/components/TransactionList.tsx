import React from 'react';
import { Link } from 'react-router-dom';
import { Transaction } from '../types';

interface TransactionListProps {
  transactions: Transaction[];
  loading: boolean;
}

const TransactionList: React.FC<TransactionListProps> = ({ transactions, loading }) => {
  // 格式化地址显示，截取前6位和后4位
  const formatAddress = (address: string) => {
    if (!address) return '';
    return address.length > 10 
      ? `${address.substring(0, 6)}...${address.substring(address.length - 4)}`
      : address;
  };

  // 格式化时间显示
  const formatTime = (time: string) => {
    try {
      const minutes = Math.floor((Date.now() - new Date(time).getTime()) / 60000);
      if (minutes < 1) return 'just now';
      if (minutes < 60) return `${minutes} mins ago`;
      
      const hours = Math.floor(minutes / 60);
      if (hours < 24) return `${hours} hrs ago`;
      
      return new Date(time).toLocaleDateString();
    } catch (e) {
      return time;
    }
  };

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6 w-full">
        <h2 className="text-xl font-semibold text-gray-800 mb-4">Latest Transactions</h2>
        <div className="animate-pulse">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="border-b border-gray-100 py-4">
              <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
              <div className="h-4 bg-gray-200 rounded w-1/2"></div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow-md p-6 w-full">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold text-gray-800">Latest Transactions</h2>
        <Link to="/transactions" className="text-blue-500 hover:text-blue-700 text-sm">
          View All Transactions
        </Link>
      </div>
      
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
            {transactions.map((tx, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-4 py-3 whitespace-nowrap">
                  <Link to={`/transaction/${tx.hash}`} className="text-blue-500 hover:text-blue-700">
                    {formatAddress(tx.hash)}
                  </Link>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                  {formatTime(tx.time)}
                </td>
                <td className="px-4 py-3 whitespace-nowrap">
                  <Link to={`/address/${tx.from}`} className="text-blue-500 hover:text-blue-700">
                    {formatAddress(tx.from)}
                  </Link>
                </td>
                <td className="px-4 py-3 whitespace-nowrap">
                  <Link to={`/address/${tx.to}`} className="text-blue-500 hover:text-blue-700">
                    {formatAddress(tx.to)}
                  </Link>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 font-medium">
                  {tx.value}
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                  {tx.token}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default TransactionList; 