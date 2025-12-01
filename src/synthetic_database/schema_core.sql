-- IMDB-Core Schema
-- Types / dictionaries
CREATE TABLE kind_type (
  id   INTEGER PRIMARY KEY,
  kind TEXT NOT NULL
);

CREATE TABLE info_type (
  id   INTEGER PRIMARY KEY,
  info TEXT NOT NULL
);

CREATE TABLE role_type (
  id   INTEGER PRIMARY KEY,
  role TEXT NOT NULL
);

CREATE TABLE keyword (
  id      INTEGER PRIMARY KEY,
  keyword TEXT NOT NULL
);

-- Entities
CREATE TABLE title (
  id               INTEGER PRIMARY KEY,
  title            TEXT NOT NULL,
  kind_id          INTEGER NOT NULL REFERENCES kind_type(id),
  production_year  INTEGER
);

CREATE TABLE name (
  id     INTEGER PRIMARY KEY,
  name   TEXT NOT NULL,
  gender CHAR(1)
);

-- Typed attributes
CREATE TABLE movie_info (
  id            INTEGER PRIMARY KEY,
  movie_id      INTEGER NOT NULL REFERENCES title(id),
  info_type_id  INTEGER NOT NULL REFERENCES info_type(id),
  info          TEXT
);

-- Bridges
CREATE TABLE cast_info (
  id              INTEGER PRIMARY KEY,
  person_id       INTEGER NOT NULL REFERENCES name(id),
  movie_id        INTEGER NOT NULL REFERENCES title(id),
  person_role_id  INTEGER NOT NULL REFERENCES role_type(id),
  nr_order        INTEGER
);

CREATE TABLE movie_keyword (
  id         INTEGER PRIMARY KEY,
  movie_id   INTEGER NOT NULL REFERENCES title(id),
  keyword_id INTEGER NOT NULL REFERENCES keyword(id)
);
