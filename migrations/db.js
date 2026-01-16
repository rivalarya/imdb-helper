const { Client } = require('pg');
const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'imdb-performance-optimization',
    password: 'rival',
    port: 5432,
});

module.exports = client;
