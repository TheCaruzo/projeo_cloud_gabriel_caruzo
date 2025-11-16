import React from 'react';
import { Table, TableHead, TableRow, TableCell, TableBody, Paper, TableContainer } from '@mui/material';

export default function AssetTable({ items }) {
  return (
    <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
      <Table stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Nome</TableCell>
            <TableCell>Data</TableCell>
            <TableCell align="right">Abertura</TableCell>
            <TableCell align="right">Fechamento</TableCell>
            <TableCell align="right">Volume</TableCell>
            <TableCell align="right">Preço Médio</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {items.map(row => (
            <TableRow key={row.id}>
              <TableCell>{row.nome}</TableCell>
              <TableCell>{row.dataPregao}</TableCell>
              <TableCell align="right">{row.precoAbertura}</TableCell>
              <TableCell align="right">{row.precoFechamento}</TableCell>
              <TableCell align="right">{row.volumeDiario}</TableCell>
              <TableCell align="right">{row.precoMedio}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}
