// src/index.js or where you render your root component
import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';

const theme = createTheme({
  // Customize your theme here
	 palette: {
    background: {
      default: '#f0f49f8' // Light grey background, change to '#e0f7fa' for light blue
    }
  }
});

ReactDOM.render(
  <ThemeProvider theme={theme}>
    <CssBaseline />  
    <App />
  </ThemeProvider>,
  document.getElementById('root')
);

