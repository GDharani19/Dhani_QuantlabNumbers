// src/components/Reports.js
import React, { useEffect, useState } from 'react';
import MaterialTable from '@material-table/core';
import { Box, Button, Container, Typography, Paper, MenuItem, FormControl, InputLabel, Select, Grid, styled } from '@mui/material';
import axios from 'axios';
import { saveAs } from 'file-saver';
import * as XLSX from 'xlsx';
import { MaterialReactTable, type MRT_ColumnDef } from 'material-react-table';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import dayjs from "dayjs";
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

const StockGenerator = () => {
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [filter1, setFilter1] = useState('');
  const [date, setDate] = useState('');


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

  const fetchData = async () => {
	  setData([]);
	  setIsLoading(true);
    try {
      const response = await axios.get('http://127.0.0.1:9009/get-report2',{
  params: {
	  date: filter1 ? dayjs(filter1).format("YYYY-MM-DD") : ''
  }});
      setData(response.data.data);
      if (response.data && response.data.data.length > 0) {
	      setDate(response.data.date);
        const columnHeaders = Object.keys(response.data.data[0]).map(key => ({
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


  useEffect(() => {
    fetchData();
  }, []);

  
  useEffect(() => {
    fetchData();
  }, [filter1]);


  const exportToExcel = () => {
    const worksheet = XLSX.utils.json_to_sheet(data);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Report');
    const buffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([buffer], { type: 'application/octet-stream' });
    saveAs(blob, 'report.xlsx');
  };
 const headerStyle = {
    backgroundColor: '#f5f5f5', // Light grey background color
    color: '#333', // Dark text color
    fontWeight: 'bold',
  };
  return (
    <StyledContainer>
      <StyledTypography variant="h4">
        Stock Generator
      </StyledTypography>
	   <Grid container spacing={2}>
        <Grid item xs={12} sm={3}>
          <FormControl fullWidth>
	  	<LocalizationProvider dateAdapter={AdapterDayjs}>
        <DatePicker label="Date" value={dayjs(date)} onChange={(newValue) => setFilter1(newValue)} format = "DD/MM/YYYY"/>
    </LocalizationProvider>
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
	  state={{ isLoading : isLoading}}
	  muiCircularProgressProps={{
        	color: 'secondary',
        	thickness: 5,
        	size: 55,
      	  }}
      	  muiSkeletonProps={{
        	animation: 'pulse',
        	height: 28,
      	 }}
	 enableStickyHeader
	    muiTableContainerProps={{ sx: { minHeight: '500px' } }}

	 muiTableHeadCellProps={{
	        style: headerStyle,
         }}
 	 
	 initialState =   {{ density: 'compact' }}
	 muiTableBodyProps= {{
    		sx: {
      			//stripe the rows, make odd rows a darker color
      			'& tr:nth-of-type(odd) > td': {
        			backgroundColor: '#f5f5f5',
      			},
    		},
  		} }
	 
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

export default StockGenerator;

