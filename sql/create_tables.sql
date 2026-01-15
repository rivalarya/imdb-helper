-- lookup tables
CREATE TABLE professions (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE genres (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE title_type (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE principal_categories (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- core tables
CREATE TABLE name_basics (
    nconst TEXT PRIMARY KEY,
    primary_name TEXT,
    birth_year SMALLINT,
    death_year SMALLINT
);

CREATE TABLE title_basics (
    tconst TEXT PRIMARY KEY,
    title_type_id SMALLINT NOT NULL,
    primary_title TEXT NOT NULL,
    original_title TEXT NOT NULL,
    is_adult BOOLEAN NOT NULL,
    start_year SMALLINT,
    end_year SMALLINT,
    runtime_minutes SMALLINT,

    CONSTRAINT fk_title_type
        FOREIGN KEY (title_type_id)
        REFERENCES title_type (id)
);

CREATE TABLE title_ratings (
    tconst TEXT PRIMARY KEY,
    average_rating REAL,
    num_votes INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT fk_ratings_title
        FOREIGN KEY (tconst)
        REFERENCES title_basics (tconst)
        ON DELETE CASCADE
);

CREATE TABLE title_principals (
    tconst TEXT NOT NULL,
    ordering SMALLINT NOT NULL,
    nconst TEXT NOT NULL,
    category_id SMALLINT NOT NULL,
    job TEXT,
    characters TEXT[],

    CONSTRAINT pk_title_principals
        PRIMARY KEY (tconst, ordering),

    CONSTRAINT fk_principals_title
        FOREIGN KEY (tconst)
        REFERENCES title_basics (tconst)
        ON DELETE CASCADE,

    CONSTRAINT fk_principals_name
        FOREIGN KEY (nconst)
        REFERENCES name_basics (nconst)
        ON DELETE CASCADE,

    CONSTRAINT fk_principals_category
        FOREIGN KEY (category_id)
        REFERENCES principal_categories (id)
);

-- junction tables
CREATE TABLE name_known_for_titles (
    nconst TEXT NOT NULL,
    tconst TEXT NOT NULL,

    CONSTRAINT pk_known_for
        PRIMARY KEY (nconst, tconst),

    CONSTRAINT fk_known_for_name
        FOREIGN KEY (nconst)
        REFERENCES name_basics (nconst)
        ON DELETE CASCADE,

    CONSTRAINT fk_known_for_title
        FOREIGN KEY (tconst)
        REFERENCES title_basics (tconst)
        ON DELETE CASCADE
);

CREATE TABLE name_primary_professions (
    nconst TEXT NOT NULL,
    profession_id SMALLINT NOT NULL,

    CONSTRAINT pk_name_professions
        PRIMARY KEY (nconst, profession_id),

    CONSTRAINT fk_name_professions_name
        FOREIGN KEY (nconst)
        REFERENCES name_basics (nconst)
        ON DELETE CASCADE,

    CONSTRAINT fk_name_professions_profession
        FOREIGN KEY (profession_id)
        REFERENCES professions (id)
);


CREATE TABLE title_genres (
    tconst TEXT NOT NULL,
    genre_id SMALLINT NOT NULL,

    CONSTRAINT pk_title_genres
        PRIMARY KEY (tconst, genre_id),

    CONSTRAINT fk_title_genres_title
        FOREIGN KEY (tconst)
        REFERENCES title_basics (tconst)
        ON DELETE CASCADE,

    CONSTRAINT fk_title_genres_genre
        FOREIGN KEY (genre_id)
        REFERENCES genres (id)
);

-- indexes
CREATE INDEX idx_title_principals_nconst
    ON title_principals (nconst);

CREATE INDEX idx_title_principals_category
    ON title_principals (category_id);

CREATE INDEX idx_title_genres_genre
    ON title_genres (genre_id);

CREATE INDEX idx_name_primary_professions_profession
    ON name_primary_professions (profession_id);