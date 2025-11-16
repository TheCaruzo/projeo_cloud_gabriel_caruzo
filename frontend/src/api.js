import axios from 'axios';

// Use environment variable REACT_APP_API_URL when provided (for dev/prod),
// otherwise default to local backend running on port 8000.
const baseURL = process.env.REACT_APP_API_URL || 'http://127.0.0.1:8000';
export const api = axios.create({ baseURL });
export const fetchAssets = async (filters) => {
  const res = await api.get('/assets', { params: filters });
  return res.data;
};
