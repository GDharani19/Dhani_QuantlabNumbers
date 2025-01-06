// src/components/Dashboard.js
import React from 'react';
import { Container, Typography, Paper } from '@mui/material';

const Dashboard = () => {
  return (
    <Container style={{ marginTop: 20 }}>
      <Paper style={{ padding: 16 }}>
        <Typography variant="h4">
          Dashboard
        </Typography>
        <Typography variant="subtitle1" style={{ marginTop: 20 }}>
          Welcome to your dashboard! Here's a quick overview of your operations.
        </Typography>
      </Paper>
    </Container>
  );
};

export default Dashboard;

