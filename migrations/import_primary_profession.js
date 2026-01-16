const fs = require('fs');
const client = require('./db');
const copyFrom = require('pg-copy-streams').from;

async function run() {
    await client.connect();

    const query = `
        COPY professions (name)
        FROM STDIN WITH (FORMAT text)
    `;

    const fileStream = fs.createReadStream('unique_professions.txt');
    const pgStream = client.query(copyFrom(query));

    fileStream.pipe(pgStream);

    pgStream.on('finish', () => {
        console.log('COPY finished');
        client.end();
    });

    pgStream.on('error', err => {
        console.error('COPY error:', err);
        client.end();
    });
}

run().catch(console.error);
