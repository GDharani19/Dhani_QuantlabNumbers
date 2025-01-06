// src/components/Logout.js
import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

const Logout = ({ setAuth }) => {
  const navigate = useNavigate();

  useEffect(() => {
    localStorage.removeItem('auth');
    setAuth(false);  // Update App state to reflect non-authenticated status
    navigate('/', { replace: true });
  }, [navigate, setAuth]);

  return null;  // Render nothing
};

export default Logout;

