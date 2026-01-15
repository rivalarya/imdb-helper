const { Client } = require('pg');
const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'rifall.osd',
    port: 5432,
});

module.exports = client;
