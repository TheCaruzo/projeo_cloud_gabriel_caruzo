import React from 'react';
import { TextField, Button, Box } from '@mui/material';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';

export default function Filters({ filters, setFilters, onSearch }) {
  return (
    <Box display="flex" gap={2} alignItems="center" mb={2}>
      <TextField label="Buscar ativo" value={filters.q || ''}
        onChange={e => setFilters({ ...filters, q: e.target.value })} />
      <DatePicker label="De" value={filters.from || null}
        onChange={v => setFilters({ ...filters, from: v ? v.toISOString().slice(0,10) : null })} />
      <DatePicker label="Até" value={filters.to || null}
        onChange={v => setFilters({ ...filters, to: v ? v.toISOString().slice(0,10) : null })} />
      <Button variant="contained" onClick={onSearch}>Filtrar</Button>
    </Box>
  );
}
