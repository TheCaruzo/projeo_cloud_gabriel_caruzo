import React from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';
import { Paper } from '@mui/material';

export default function AssetChart({ data }) {
  return (
    <Paper sx={{ p: 2, height: 320 }}>
      <ResponsiveContainer width="100%" height="100%"> 
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="dataPregao" />
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="precoFechamento" stroke="#1976d2" />
        </LineChart>
      </ResponsiveContainer>
    </Paper>
  );
}
