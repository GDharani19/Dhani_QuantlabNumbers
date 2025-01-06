// src/components/GlobalMenu.js
import React,  { useState } from 'react';
import { Popover, MenuItem, AppBar, Toolbar, Typography, Button } from '@mui/material';
import { Link } from 'react-router-dom';
import { useNavigate } from 'react-router-dom';

const GlobalMenu = () => {
  const isAuthenticated = localStorage.getItem('auth'); // Check if the user is authenticated
  const navigate = useNavigate();

const [anchorEl, setAnchorEl] = useState(null);

  const handlePopoverOpen = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handlePopoverClose = () => {
    setAnchorEl(null);
  };

  const open = Boolean(anchorEl);
	
  if (!isAuthenticated) {
    return null; // Do not render the menu if not authenticated
  }
const handleMenuItemClick = (path) => {
    handlePopoverClose();
    navigate(path);
  };
  return (
    <AppBar position="static" style={{ background: '#2E3B55' }}>
      <Toolbar>
        <Typography variant="h6" style={{ flexGrow: 1, textAlign: 'left' }}>
          Dashboard
        </Typography>
        <Button color="inherit" component={Link} to="/dashboard">Dashboard</Button>
	<div 
            onMouseEnter={handlePopoverOpen} 
            style={{ position: 'relative' }}
          >
            <Button color="inherit">Reports</Button>
            <Popover
              id="mouse-over-popover"
              sx={{
                pointerEvents: 'auto',
              }}
              open={open}
              anchorEl={anchorEl}
              anchorOrigin={{
                vertical: 'bottom',
                horizontal: 'left',
              }}
              transformOrigin={{
                vertical: 'top',
                horizontal: 'left',
              }}
              onClose={handlePopoverClose}
              disableRestoreFocus
            >
                <MenuItem onClick={() => handleMenuItemClick('/reports')}>OI Loading Report</MenuItem>
                <MenuItem onClick={() => handleMenuItemClick('/stock-generator')}>Stock Generator</MenuItem>
	  	<MenuItem onClick={() => handleMenuItemClick('/initial-series-stock-oi')}>Initial Series Stock OI</MenuItem>
	  	<MenuItem onClick={() => handleMenuItemClick('/month-high-low-range')}>Month High Low Range Report</MenuItem>
	  	<MenuItem onClick={() => handleMenuItemClick('/day-high-low-range')}>Day High Low Range Report</MenuItem>
		<MenuItem onClick={() => handleMenuItemClick('/adaptive-ema-calculations')}>Adaptive EMA Calculations</MenuItem>
	  	<MenuItem onClick={() => handleMenuItemClick('/all-data-csv-report')}>All CSV File Data</MenuItem>
	  </Popover>
          </div>
	<Button color="inherit" component={Link} to="/logout">Logout</Button>
      </Toolbar>
    </AppBar>
  );
};

export default GlobalMenu;

