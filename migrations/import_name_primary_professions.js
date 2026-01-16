const fs = require('fs');
const readline = require('readline');
const client = require('./db');

const BATCH_SIZE = 1000;

async function loadProfessions() {
    const res = await client.query(
        'SELECT id, name FROM professions'
    );

    const map = new Map();
    for (const row of res.rows) {
        map.set(row.name, row.id);
    }

    return map;
}

async function insertBatch(batch) {
    if (batch.length === 0) return;

    const values = [];
    const placeholders = batch.map((row, i) => {
        const idx = i * 2;
        values.push(row.nconst, row.profession_id);
        return `($${idx + 1}, $${idx + 2})`;
    }).join(',');

    const query = `
        INSERT INTO name_primary_professions (nconst, profession_id)
        VALUES ${placeholders}
        ON CONFLICT DO NOTHING
    `;

    await client.query(query, values);
}

async function run() {
    await client.connect();

    console.log('Loading professions...');
    const professionMap = await loadProfessions();

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
        const professions = cols[4];

        if (!nconst || !professions || professions === '\\N') continue;

        for (const name of professions.split(',')) {
            const professionId = professionMap.get(name);
            if (!professionId) continue;

            batch.push({
                nconst,
                profession_id: professionId
            });

            if (batch.length >= BATCH_SIZE) {
                await insertBatch(batch);
                console.log(`${new Date().toISOString()} Inserted ${batch.length} rows...`);
                batch = [];
            }
        }
    }

    // Insert remaining rows
    await insertBatch(batch);
    console.log('Done');
    await client.end();
}

run().catch(console.error);