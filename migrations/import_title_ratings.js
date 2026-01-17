const fs = require('fs');
const split = require('split2');
const client = require('./db');
const copyFrom = require('pg-copy-streams').from;

async function run() {
    await client.connect();

    const query = `
    COPY title_ratings_temp (tconst, average_rating, num_votes)
    FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER E'\t')
`;

    const fileStream = fs.createReadStream('title.ratings.1m.tsv');

    const transform = split(line => {
        const cols = line.split('\t');

        return cols.map(col => col === '\\N' ? null : col).join('\t') + '\n';
    });

    const pgStream = client.query(copyFrom(query));

    fileStream
        .pipe(transform)
        .pipe(pgStream);

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
