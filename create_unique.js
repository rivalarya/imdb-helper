const fs = require('fs');

const data = fs.readFileSync('title.basics.100k.tsv', 'utf-8');
const rows = data.split('\n');

const allGenres = new Set();

for (let i = 1; i < rows.length; i++) {
    const columns = rows[i].split('\t');
    const genresColumn = columns[8];
    
    if (genresColumn && genresColumn !== '\\N') {
        const genres = genresColumn.split(',');
        for (const genre of genres) {
            allGenres.add(genre.trim());
        }
    }
}

const uniqueValues = Array.from(allGenres).sort();

fs.writeFileSync('unique_genres.txt', uniqueValues.join('\n'));