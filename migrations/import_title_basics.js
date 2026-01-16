const fs = require('fs');
const split = require('split2');
const client = require('./db');
const copyFrom = require('pg-copy-streams').from;

async function run() {
    await client.connect();

    const typeResult = await client.query('SELECT id, name FROM title_type');
    const titleTypeMap = {};
    typeResult.rows.forEach(row => {
        titleTypeMap[row.name] = row.id;
    });

    const query = `
        COPY title_basics (tconst, title_type_id, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes)
        FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER E'\t')
    `;

    const fileStream = fs.createReadStream('title.basics.1m.tsv');

    const transform = split(line => {
        const cols = line.split('\t');

        const escapeValue = (val) => {
            if (val === '\\N') return null;
            if (val && val.includes('"')) {
                return '"' + val.replace(/"/g, '""') + '"';
            }
            return val;
        };

        const tconst = cols[0] === '\\N' ? null : cols[0];
        const titleTypeId = titleTypeMap[cols[1]] || null;
        const primaryTitle = escapeValue(cols[2]);
        const originalTitle = escapeValue(cols[3]);
        const isAdult = cols[4] === '\\N' ? null : cols[4];
        const startYear = cols[5] === '\\N' ? null : cols[5];
        const endYear = cols[6] === '\\N' ? null : cols[6];
        const runtimeMinutes = cols[7] === '\\N' ? null : cols[7];

        return [tconst, titleTypeId, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes].join('\t') + '\n';
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