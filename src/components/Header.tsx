import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

const Header: React.FC = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const navigate = useNavigate();

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      navigate(`/search?q=${encodeURIComponent(searchQuery.trim())}`);
    }
  };

  return (
    <header className="bg-white shadow-sm">
      <div className="container mx-auto px-4 py-4">
        <div className="flex flex-col md:flex-row justify-between items-center">
          <h1
            className="text-2xl font-bold text-gray-800 mb-4 md:mb-0 cursor-pointer"
            onClick={() => navigate('/')}
          >
            Vly Explorer
          </h1>
          
          <form onSubmit={handleSearch} className="w-full md:w-3/5">
            <div className="relative">
              <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                <svg className="w-4 h-4 text-gray-400" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 20 20">
                  <path stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="m19 19-4-4m0-7A7 7 0 1 1 1 8a7 7 0 0 1 14 0Z" />
                </svg>
              </div>
              <input
                type="search"
                id="search"
                className="block w-full p-3 pl-10 text-sm text-gray-900 border border-gray-200 rounded-lg bg-gray-50 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Search by Address / TxN Hash"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                required
              />
              <button
                type="submit"
                className="absolute right-2 bottom-2 top-2 bg-blue-600 hover:bg-blue-700 text-white px-4 rounded-md"
              >
                Search
              </button>
            </div>
          </form>
        </div>
      </div>
    </header>
  );
};

export default Header; 