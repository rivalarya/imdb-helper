const fs = require('fs');
const split = require('split2');
const client = require('./db');
const copyFrom = require('pg-copy-streams').from;

async function run() {
    await client.connect();

    const categoryResult = await client.query('SELECT id, name FROM principal_categories');
    const categoryMap = {};
    categoryResult.rows.forEach(row => {
        categoryMap[row.name] = row.id;
    });

    await client.query(`
        CREATE TEMP TABLE title_principals_temp (
            tconst TEXT,
            ordering SMALLINT,
            nconst TEXT,
            category_id SMALLINT,
            job TEXT,
            characters TEXT[]
        )
    `);

    const query = `
        COPY title_principals_temp (tconst, ordering, nconst, category_id, job, characters)
        FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER E'\t')
    `;

    const fileStream = fs.createReadStream('title.principals.1m.tsv');

    const transform = split(line => {
        const cols = line.split('\t');

        const escapeValue = (val) => {
            if (val === '\\N') return null;
            if (val && val.includes('"')) {
                return '"' + val.replace(/"/g, '""') + '"';
            }
            return val;
        };

        const parseArray = (val) => {
            if (val === '\\N') return null;
            if (!val) return null;

            try {
                const parsed = JSON.parse(val);
                const filtered = parsed.filter(item => item && typeof item === 'string' && item.trim() !== '');

                if (filtered.length === 0) return null;

                const escaped = filtered.map(item => {
                    return item.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
                });

                const arrayStr = `{${escaped.map(s => `"${s}"`).join(',')}}`;
                return `"${arrayStr.replace(/"/g, '""')}"`;
            } catch {
                return null;
            }
        };

        const tconst = cols[0] === '\\N' ? null : cols[0];
        const ordering = cols[1] === '\\N' ? null : cols[1];
        const nconst = cols[2] === '\\N' ? null : cols[2];
        const categoryId = categoryMap[cols[3]] || null;
        const job = escapeValue(cols[4]);
        const characters = parseArray(cols[5]);

        return [tconst, ordering, nconst, categoryId, job, characters].join('\t') + '\n';
    });

    const pgStream = client.query(copyFrom(query));

    fileStream
        .pipe(transform)
        .pipe(pgStream);

    pgStream.on('finish', async () => {
        console.log('COPY to temp table finished');
        
        try {
            await client.query(`
                INSERT INTO title_principals (tconst, ordering, nconst, category_id, job, characters)
                SELECT t.tconst, t.ordering, t.nconst, t.category_id, t.job, t.characters
                FROM title_principals_temp t
                WHERE EXISTS (SELECT 1 FROM name_basics WHERE nconst = t.nconst)
                  AND EXISTS (SELECT 1 FROM title_basics WHERE tconst = t.tconst)
                ON CONFLICT (tconst, ordering) DO NOTHING
            `);
            
            console.log('Insert from temp finished');
            
            await client.query('DROP TABLE title_principals_temp');
            
            console.log('Cleanup finished');
        } catch (err) {
            console.error('Error during insert/cleanup:', err);
        } finally {
            client.end();
        }
    });

    pgStream.on('error', err => {
        console.error('COPY error:', err);
        client.end();
    });
}

run().catch(console.error);