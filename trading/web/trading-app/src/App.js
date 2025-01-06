// src/App.js
import React, { useEffect, useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import GlobalMenu from './components/GlobalMenu';
import Login from './components/Login';
import Dashboard from './components/Dashboard';
import Reports from './components/Reports';
import StockGenerator from './components/StockGenerator';
import InitialSeriesStockOi from './components/InitialSeriesStockOi';
import MonthHighLowRange from './components/MonthHighLowRange';
import DayHighLowRange from './components/DayHighLowRange';
import PrivateRoute from './components/PrivateRoute';
import AdaptiveEmaCalculations from './components/AdaptiveEmaCalculations';
import AllDataCSVReport from './components/AllDataCSVReport'
import './styles.css';
import Logout from './components/Logout';
function App() {
const [isAuthenticated, setIsAuthenticated] = useState(false);

  useEffect(() => {
    // Check the localStorage to see if user is authenticated
    const auth = localStorage.getItem('auth');
    setIsAuthenticated(!!auth);
  }, []);

return (
    <Router basename={process.env.REACT_APP_BASE_PATH}>
      <Routes>
        <Route path="/" element={<Login setAuth={setIsAuthenticated} />} />
	 {isAuthenticated && (
          <>
	<Route element={<PrivateRoute />}>
          <Route path="/" element={<><GlobalMenu /><Dashboard /></>} />
          <Route path="/dashboard" element={<><GlobalMenu /><Dashboard /></>} />
          <Route path="/reports" element={<><GlobalMenu /><Reports /></>} />
 	        <Route path="/stock-generator" element={<><GlobalMenu /><StockGenerator /> </>}/>
	        <Route path="/initial-series-stock-oi" element={<><GlobalMenu /><InitialSeriesStockOi/> </>}/>
	        <Route path="/month-high-low-range" element={<><GlobalMenu /><MonthHighLowRange/> </>}/>
          <Route path="/day-high-low-range" element={<><GlobalMenu /><DayHighLowRange/> </>}/>
	        <Route path="/adaptive-ema-calculations" element={<><GlobalMenu /><AdaptiveEmaCalculations/> </>}/>
	        <Route path="/all-data-csv-report" element={<><GlobalMenu /><AllDataCSVReport/> </>}/>

        </Route>
      <Route path="/logout" element={<Logout setAuth={setIsAuthenticated} />}/>
	 </>
	 )}
	        <Route path="*" element={<Navigate to="/" />} />
	</Routes> 
    </Router>
  );
}

export default App;

