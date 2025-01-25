// src/components/Login.js
import React, { useState } from 'react';
import { Button, TextField, Container, Typography, Paper, Alert } from '@mui/material';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

const Login = ( { setAuth }) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('Oxalic_336');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleLogin = async (event) => {
    event.preventDefault();
    setError('');
    try {
      const response = await axios.post('http://127.0.0.1:9009/login', {
        email,
        password
      });
      if (response.data.data.accessToken) {
        localStorage.setItem('auth', 'true');
        localStorage.setItem('token', response.data.token);
            setAuth(true);  // Update the App state to reflect authenticated status

	      navigate('/dashboard');
      } else {
        setError('Login failed. Please check your credentials.');
      }
    } catch (err) {
      setError('Login failed. Please try again later.');
      console.error(err);
    }
  };

  return (
    <Container component="main" maxWidth="xs">
      <Paper elevation={6} style={{ padding: 16, marginTop: 100 }}>
        <Typography variant="h5" style={{ textAlign: 'center', margin: '20px 0' }}>
          Sign In
        </Typography>
        <form onSubmit={handleLogin}>
          <TextField
            variant="outlined"
            margin="normal"
            required
            fullWidth
            label="Email"
            autoFocus
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          <TextField
            variant="outlined"
            margin="normal"
            required
            fullWidth
            label="Password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          {error && <Alert severity="error" style={{ marginTop: 10 }}>{error}</Alert>}
          <Button type="submit" fullWidth variant="contained" color="primary" style={{ marginTop: 24 }}>
            Login
          </Button>
        </form>
      </Paper>
    </Container>
  );
};

export default Login;

