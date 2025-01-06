// src/components/Reports.js
import React, { useEffect, useState } from 'react';
import MaterialTable from '@material-table/core';
import {  Box, Button, Container, Typography, Paper, MenuItem, FormControl, InputLabel, Select, Grid, styled } from '@mui/material';
import axios from 'axios';
import { saveAs } from 'file-saver';
import * as XLSX from 'xlsx';
import { MaterialReactTable, type MRT_ColumnDef } from 'material-react-table';

import FileDownloadIcon from '@mui/icons-material/FileDownload';
import { mkConfig, generateCsv, download } from 'export-to-csv';


// Styling the container using styled components from MUI v5
const StyledContainer = styled(Container)(({ theme }) => ({
  marginTop: theme.spacing(4),
  padding: theme.spacing(3),
  '& .MuiFormControl-root': {
    margin: theme.spacing(1,0),
    minWidth: 120,
    maxWidth: 300
  },
  '& .MuiPaper-root': {
    border: '1px solid #e0e0e0',
    boxShadow: 'none'
  }
}));

const StyledTypography = styled(Typography)(({ theme }) => ({
  color: theme.palette.primary.main,
  marginBottom: theme.spacing(2)
}));

const Reports = () => {
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [filter1, setFilter1] = useState('');
  const [filter2, setFilter2] = useState('');
  const [filter3, setFilter3] = useState('');
  const [filter4, setFilter4] = useState('');
  const [tickerSymbols, setTickerSymbols] = useState([]);
  const [expiryDates, setExpiryDates] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const fetchData = async () => {
	  setData([]);
	  if( filter2 ==  '')
	  {
		  //return;
	  }
	   setIsLoading(true);
    try {
 	const data = {"ticker_symbol": filter1, "instrument_type": filter4,"expiry_date":filter2,"percentage_active": filter3} 
      const response = await axios.post('https://intranet.cytrion.com/trading-api/get-instruments-data',data);
      setData(response.data);
      if (response.data && response.data.length > 0) {
        const columnHeaders = Object.keys(response.data[0]).map(key => ({
          header: key.charAt(0).toUpperCase() + key.slice(1),
          accessorKey: key
        }));
        setColumns(columnHeaders);
      }
    } catch (error) {
      console.error("Error fetching data: ", error);
    } finally {
    setIsLoading(false);  
    }
  };

 const fetchTickerSymbols = async () => {
    try {
      const response = await axios.get('https://intranet.cytrion.com/trading-api/get-ticker-symbols', {
      });
      setData(response.data);
      if (response.data && response.data.length > 0) {
      	setTickerSymbols(response.data);
      }
    } catch (error) {
      console.error("Error fetching data: ", error);
    }
  };
  
const fetchExpiryDates = async () => {
    try {
      const response = await axios.get('https://intranet.cytrion.com/trading-api/get-instruments-expiry-dates', {
      });
      setData(response.data);
      if (response.data && response.data.length > 0) {
      	setExpiryDates(response.data);
      }
    } catch (error) {
      console.error("Error fetching data: ", error);
    }
  };

  useEffect(() => {
    fetchData();
  }, [filter1, filter2, filter3, filter4]);

  useEffect(() => {
	fetchTickerSymbols();
	fetchExpiryDates();
  },[]);
 
 const handleExportData = () => {
    const csv = generateCsv(csvConfig)(data);
    download(csvConfig)(csv);
  };
 
 const csvConfig = mkConfig({
  fieldSeparator: ',',
  decimalSeparator: '.',
  useKeysAsHeaders: true,
});

	const handleExportRows = (rows: MRT_Row<Person>[]) => {
    const rowData = rows.map((row) => row.original);
    const csv = generateCsv(csvConfig)(rowData);
    download(csvConfig)(csv);
  };
 const headerStyle = {
    backgroundColor: '#f5f5f5', // Light grey background color
    color: '#333', // Dark text color
    fontWeight: 'bold',
  };
  return (
    <StyledContainer>
      <StyledTypography variant="h4">
        OI Loading Reports
      </StyledTypography>
      <Grid container spacing={2}>
        <Grid item xs={12} sm={3}>
          <FormControl fullWidth>
            <InputLabel>Ticker Symbol</InputLabel>
            <Select value={filter1} label="Ticker Symbl" onChange={e => setFilter1(e.target.value)}>
              <MenuItem value="">None</MenuItem>
           	{tickerSymbols.map((option) => (
                <MenuItem key={option} value={option}>
                    {option}
                </MenuItem>
            ))} 
	   </Select>
          </FormControl>
        </Grid>
        <Grid item xs={12} sm={3}>
          <FormControl fullWidth>
            <InputLabel>Expiry Date</InputLabel>
            <Select value={filter2} label="Expiry Date" onChange={e => setFilter2(e.target.value)}>
              <MenuItem value="">None</MenuItem>
              {expiryDates.map((option) => (
                <MenuItem key={option} value={option}>
                    {option}
                </MenuItem>
            ))}
	    </Select>
          </FormControl>
        </Grid>
        <Grid item xs={12} sm={3}>
          <FormControl fullWidth>
            <InputLabel>Activity Threshold (%)</InputLabel>
            <Select value={filter3} label="Activity Threshold" onChange={e => setFilter3(e.target.value)}>
              <MenuItem value="">None</MenuItem>
              <MenuItem value="100">100 %</MenuItem>
	      <MenuItem value="90">90 %</MenuItem>
              <MenuItem value="80">80 %</MenuItem>
              <MenuItem value="70">70 %</MenuItem>
              <MenuItem value="60">60 %</MenuItem>
              <MenuItem value="50">50 %</MenuItem>
              <MenuItem value="40">40 %</MenuItem>
              <MenuItem value="30">30 %</MenuItem>
              <MenuItem value="20">20 %</MenuItem>
              <MenuItem value="10">10 %</MenuItem>

            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={12} sm={3}>
          <FormControl fullWidth>
            <InputLabel>Instrument Type</InputLabel>
            <Select value={filter4} label="Instrument Type" onChange={e => setFilter4(e.target.value)}>
                <MenuItem value="">None</MenuItem>
		<MenuItem value="STO">STO</MenuItem>
	      	<MenuItem value="STF">STF</MenuItem>
            </Select>
          </FormControl>
        </Grid>
      </Grid>
	<link
  		rel="stylesheet"
  		href="https://fonts.googleapis.com/icon?family=Material+Icons"
	/>
	  <MaterialReactTable 
	  columns={columns} 
	  data={data}
	  state = { {isLoading : isLoading }}
	  muiCircularProgressProps={{
        	color: 'secondary',
        	thickness: 5,
        	size: 55,
      	  }}
      	  muiSkeletonProps={{
        	animation: 'pulse',
        	height: 28,
      	 }}
	 muiTableContainerProps={{ sx: { minHeight: '500px' } }}

	 enableStickyHeader
	 muiTableHeadCellProps={{
         	style: headerStyle,
         }}

         initialState =   {{ density: 'compact' ,pagination: { pageSize: 50 }} }
         muiTableBodyProps= {{
                sx: {
                        //stripe the rows, make odd rows a darker color
                        '& tr:nth-of-type(odd) > td': {
                                backgroundColor: '#f5f5f5',
                        },
                },
                } }

	  muiPaginationProps = {{
    		rowsPerPageOptions: [5, 10, 20,50],
    		showFirstButton: false,
    		showLastButton: false,
  	 }}
	 enableRowSelection
	 renderTopToolbarCustomActions={({ table }) => (
        <Box
          sx={{
            display: 'flex',
            gap: '16px',
            padding: '8px',
            flexWrap: 'wrap',
          }}
        >
          <Button
            //export all data that is currently in the table (ignore pagination, sorting, filtering, etc.)
            onClick={handleExportData}
            startIcon={<FileDownloadIcon />}
          >
            Export All Data
          </Button>
          <Button
            disabled={table.getRowModel().rows.length === 0}
            //export all rows as seen on the screen (respects pagination, sorting, filtering, etc.)
            onClick={() => handleExportRows(table.getRowModel().rows)}
            startIcon={<FileDownloadIcon />}
          >
            Export Page Rows
          </Button>
          <Button
            disabled={
              !table.getIsSomeRowsSelected() && !table.getIsAllRowsSelected()
            }
            //only export selected rows
            onClick={() => handleExportRows(table.getSelectedRowModel().rows)}
            startIcon={<FileDownloadIcon />}
          >
            Export Selected Rows
          </Button>
        </Box>
      )}

	  />
    </StyledContainer>
  );
};

export default Reports;

