import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';
import TokenDetailPage from './pages/TokenDetailPage';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/token/:symbol" element={<TokenDetailPage />} />
      </Routes>
    </Router>
  );
}

export default App;
