const fs = require('fs');
const readline = require('readline');
const client = require('./db');

const BATCH_SIZE = 1000;

async function getTitleIds() {
    const res = await client.query(
        'SELECT tconst FROM title_basics'
    );

    const set = new Set();
    for (const row of res.rows) {
        set.add(row.tconst);
    }

    return set;
}

async function insertBatch(batch) {
    if (batch.length === 0) return;

    const values = [];
    const placeholders = batch.map((row, i) => {
        const idx = i * 2;
        values.push(row.nconst, row.tconst);
        return `($${idx + 1}, $${idx + 2})`;
    }).join(',');

    const query = `
        INSERT INTO name_known_for_titles (nconst, tconst)
        VALUES ${placeholders}
        ON CONFLICT DO NOTHING
    `;

    await client.query(query, values);
}

async function run() {
    await client.connect();

    console.log('Loading title IDs...');
    const titleSet = await getTitleIds();

    console.log('Processing TSV...');
    const fileStream = fs.createReadStream('name.basics.1m.tsv');
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let batch = [];
    let isFirstLine = true;

    for await (const line of rl) {
        if (isFirstLine) {
            isFirstLine = false;
            continue;
        }

        const cols = line.split('\t');
        const nconst = cols[0];
        const knownForTitles = cols[5];

        if (!nconst || !knownForTitles || knownForTitles === '\\N') continue;

        for (const tconst of knownForTitles.split(',')) {
            if (!titleSet.has(tconst)) continue;

            batch.push({ nconst, tconst });

            if (batch.length >= BATCH_SIZE) {
                await insertBatch(batch);
                console.log(`${new Date().toISOString()} Inserted ${batch.length} rows...`);
                batch = [];
            }
        }
    }

    await insertBatch(batch);
    console.log('Done');
    await client.end();
}

run().catch(console.error);