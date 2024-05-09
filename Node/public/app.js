const express = require('express');
const cors = require('cors');
const mysql = require('mysql2');
const app = express();

// Serve static files from the 'public' folder
// Enable CORS for all routes
app.use(cors());
// Set up middleware to serve static files
app.use(express.static('public', { 
  setHeaders: (res, path, stat) => {
    if (path.endsWith('.css')) {
      res.setHeader('Content-Type', 'text/css');
    }
  }
}));

// Create a MySQL connection pool
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'twStock'
});

// Route to get all books
app.get('/stocks', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) throw err;

    const query = 'SELECT * FROM stock_data';
    connection.query(query, (err, results) => {
      connection.release();
      if (err) throw err;

      res.json(results);
    });
  });
});

// Route to get a specific book by ID
app.get('/stocks/:id', (req, res) => {
  pool.getConnection((err, connection) => {
    if (err) throw err;

    const stockCode = req.params.id;
    const query = 'SELECT * FROM stock_data WHERE stock_code = ?';
    connection.query(query, [stockCode], (err, results) => {
      connection.release();
      if (err) throw err;

      if (results.length === 0) {
        res.status(404).json({ error: 'Book not found' });
      } else {
        res.json(results);
      }
    });
  });
});

// Route to search for stock
app.get('/stocks_search', (req, res) => {
  const searchBy = Object.keys(req.query)[0];
  const searchValue = req.query[searchBy];
  console.log("searchBy = " + searchBy + ", searchValue = " + searchValue);

  pool.getConnection((err, connection) => {
    if (err) throw err;

    let query = 'SELECT * FROM stock_data WHERE ?? = ?';
    connection.query(query, [searchBy, searchValue], (err, results) => {
      connection.release();
      if (err) throw err;

      res.json(results);
    });
  });
});

// Route to search for stocks
app.get('/stocks_ma_search', (req, res) => {
  const searchBy = req.query['search-by'];
  const searchDirection = req.query['search-direction'];
  const searchValue = req.query['search-input'];
  console.log("searchBy = " + searchBy + ", searchDirection = " + searchDirection + ", searchValue = " + searchValue);

  pool.getConnection((err, connection) => {
    if (err) throw err;

    let query;
    if (searchDirection === 'above') {
      query = `SELECT a.stock_date, a.stock_code, a.closing_price, b.average_price FROM 
               (select * from stock_data WHERE stock_date = (SELECT MAX(stock_date) FROM stock_data)) a 
               JOIN stock_data_ma${searchBy} b 
               ON a.stock_code = b.stock_code AND a.stock_date = b.stock_date
               WHERE a.closing_price > b.average_price * (1 + ?/100)`;
      // console.log(query);
    } else if (searchDirection === 'below') {
      query = `SELECT a.stock_date, a.stock_code, a.closing_price, b.average_price FROM 
               (select * from stock_data WHERE stock_date = (SELECT MAX(stock_date) FROM stock_data)) a 
               JOIN stock_data_ma${searchBy} b 
               ON a.stock_code = b.stock_code AND a.stock_date = b.stock_date
               WHERE a.closing_price < b.average_price * (1 - ?/100)`;
      // console.log(query);
    } else {
      // Handle invalid search criteria
      res.status(400).json({ error: 'Invalid search criteria' });
      return;
    }

    connection.query(query, [searchValue], (err, results) => {
      connection.release();
      if (err) throw err;

      res.json(results);
    });
  });
});

// app.get('/', (req, res) => {
//     res.send('Welcome to Stock API!');
//   });

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});