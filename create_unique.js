const fs = require('fs');

const INPUT_FILE = 'name.basics.1m.tsv';
const OUTPUT_FILE = 'unique_professions.txt';

const fileContent = fs.readFileSync(INPUT_FILE, 'utf-8');
const rows = fileContent.trim().split('\n');

const uniqueProfessionSet = new Set();

// Skip header row (index 0)
for (let i = 1; i < rows.length; i++) {
    const columns = rows[i].split('\t');
    const professionsColumn = columns[4];

    if (professionsColumn && professionsColumn !== '\\N') {
        const professions = professionsColumn.split(',');
        for (const profession of professions) {
            uniqueProfessionSet.add(profession.trim());
        }
    }
}

const uniqueProfessions = Array.from(uniqueProfessionSet).sort();

fs.writeFileSync(OUTPUT_FILE, uniqueProfessions.join('\n'));
