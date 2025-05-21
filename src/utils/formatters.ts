// 格式化地址显示，截取前几位和后几位
export const formatAddress = (address: string, prefixLength = 6, suffixLength = 4) => {
  if (!address) return '';
  if (address.length <= prefixLength + suffixLength) return address;
  
  return `${address.substring(0, prefixLength)}...${address.substring(address.length - suffixLength)}`;
};

// 格式化时间为相对时间
export const formatRelativeTime = (timestamp: number | string) => {
  if (!timestamp) return '';
  
  const date = typeof timestamp === 'number' 
    ? new Date(timestamp * 1000) 
    : new Date(timestamp);
  
  const now = new Date();
  const diffInSeconds = Math.floor((now.getTime() - date.getTime()) / 1000);
  
  if (diffInSeconds < 60) return 'just now';
  
  const diffInMinutes = Math.floor(diffInSeconds / 60);
  if (diffInMinutes < 60) return `${diffInMinutes} min${diffInMinutes > 1 ? 's' : ''} ago`;
  
  const diffInHours = Math.floor(diffInMinutes / 60);
  if (diffInHours < 24) return `${diffInHours} hr${diffInHours > 1 ? 's' : ''} ago`;
  
  const diffInDays = Math.floor(diffInHours / 24);
  if (diffInDays < 30) return `${diffInDays} day${diffInDays > 1 ? 's' : ''} ago`;
  
  return date.toLocaleDateString();
};

// 格式化数字，添加千位分隔符
export const formatNumber = (num: number | string) => {
  if (num === undefined || num === null) return '0';
  
  const value = typeof num === 'string' ? parseFloat(num) : num;
  
  return new Intl.NumberFormat().format(value);
};

// 格式化代币金额，考虑小数位
export const formatTokenAmount = (amount: string | number, decimals: number = 8) => {
  if (!amount) return '0';
  
  const value = typeof amount === 'string' ? amount : amount.toString();
  
  // 如果金额小于等于9位数，直接返回
  if (value.length <= decimals) {
    const padded = value.padStart(decimals + 1, '0');
    const result = `0.${padded.slice(0, decimals).replace(/0+$/, '')}`.replace(/\.$/, '');
    return result === '' ? '0' : result;
  }
  
  // 如果金额大于9位数，需要在适当位置添加小数点
  const integerPart = value.slice(0, value.length - decimals);
  const decimalPart = value.slice(value.length - decimals);
  
  return `${formatNumber(integerPart)}.${decimalPart.replace(/0+$/, '')}`.replace(/\.$/, '');
}; 