import React from 'react';

interface TokenCardProps {
  symbol: string;
  totalSupply: string;
  totalAddresses: string;
}

const TokenCard: React.FC<TokenCardProps> = ({
  symbol,
  totalSupply,
  totalAddresses
}) => {
  return (
    <div 
      className="bg-white rounded-lg shadow-md p-6 w-full cursor-pointer hover:shadow-lg transition-shadow duration-300"
    >
      <div className="flex items-center mb-4">
        <div className="bg-blue-100 p-2 rounded-full mr-3">
          <svg className="w-6 h-6 text-blue-500" fill="currentColor" viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg">
            <path d="M4 4a2 2 0 00-2 2v1h16V6a2 2 0 00-2-2H4z"></path>
            <path fillRule="evenodd" d="M18 9H2v5a2 2 0 002 2h12a2 2 0 002-2V9zM4 13a1 1 0 011-1h1a1 1 0 110 2H5a1 1 0 01-1-1zm5-1a1 1 0 100 2h1a1 1 0 100-2H9z" clipRule="evenodd"></path>
          </svg>
        </div>
        <h2 className="text-xl font-semibold text-gray-800">{symbol}</h2>
      </div>
      
      <div className="grid grid-cols-2 gap-4">
        <div className="text-center">
          <p className="text-sm text-gray-500 mb-1">Total Supply</p>
          <p className="text-xl font-bold text-gray-800">{totalSupply}</p>
        </div>
        
        <div className="text-center">
          <p className="text-sm text-gray-500 mb-1">Total Addresses</p>
          <p className="text-xl font-bold text-gray-800">{totalAddresses}</p>
        </div>
      </div>
    </div>
  );
};

export default TokenCard; 