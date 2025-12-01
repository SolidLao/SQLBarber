# IMDB Synthetic Database Generator

This directory contains scripts to generate simplified IMDB-derived databases (Core and Extended) from the complete IMDB database.

## Overview

The synthetic databases are created by:
1. Fixing a semantic backbone consisting of titles–cast–people tables
2. Expanding the join graph with additional satellite tables
3. Controlling column width through structured pruning (retaining key columns, predicate columns, removing decorative columns)
4. Maintaining foreign key integrity by keeping only rows whose referenced targets remain

## Database Schemas

### IMDB-Core (9 tables, 27 columns)

**Dictionary/Type Tables:**
- `kind_type` - Movie/title type classifications
- `info_type` - Information type classifications
- `role_type` - Actor/crew role types
- `keyword` - Keywords/tags

**Entity Tables:**
- `title` - Movies and TV shows (id, title, kind_id, production_year)
- `name` - People (id, name, gender)

**Relationship Tables:**
- `movie_info` - Additional movie information
- `cast_info` - Cast and crew assignments (with nr_order)
- `movie_keyword` - Movie-keyword associations

### IMDB-Extended (16 tables, 53 columns)

Includes all Core tables plus:

**Additional Entity Tables:**
- `company_name` - Production companies (id, name, country_code)
- `company_type` - Company type classifications

**Additional Relationship Tables:**
- `movie_info_idx` - Indexed movie information
- `movie_companies` - Movie-company associations (with note)
- `aka_title` - Alternative titles (with kind_id, production_year)
- `aka_name` - Alternative names
- `person_info` - Additional person information

## Files

- `schema_core.sql` - Schema definition for IMDB-Core
- `schema_extended.sql` - Schema definition for IMDB-Extended
- `create_databases.py` - Main script to create both databases
- `create_indexes.py` - Script to create secondary indexes (JOB benchmark compatible)
- `README.md` - This file

## Usage

### Prerequisites

- PostgreSQL server
- Complete IMDB database named 'imdb'
- Python 3 with psycopg2 installed
- Configuration file at `/home/SQLBarber/configs/postgres.ini` 
  - please change the `config_path` parameter based on your configuration file in `create_databases(indexes).py`

### Install Dependencies

```bash
pip install psycopg2-binary
```

### Create Databases

```bash
cd /home/synthetic_database
python3 create_databases.py
```

This will:
1. Drop existing `imdb_core` and `imdb_extended` databases (if they exist)
2. Create new empty databases
3. Create table schemas
4. Copy data from the complete IMDB database while maintaining referential integrity

### Create Indexes (JOB Benchmark Compatible)

After creating the databases, create secondary indexes to match the JOB benchmark structure:

```bash
cd /home/synthetic_database
python3 create_indexes.py
```

This will create:
- **IMDB-Core**: 8 secondary indexes (on foreign keys and join columns)
- **IMDB-Extended**: 18 secondary indexes (on foreign keys and join columns)

These indexes significantly improve query performance and match the original IMDB/JOB benchmark setup.

**Note**: Index creation may take several minutes depending on data volume (especially for large tables like `movie_info` and `cast_info`).

## Schema Complexity Comparison

| Database      | Tables | Columns | Avg Cols/Table | Join Range |
|---------------|--------|---------|----------------|------------|
| IMDB-Core     | 9      | 27      | 3.0            | 1-7        |
| IMDB-Extended | 16     | 53      | 3.3            | 1-12       |
| IMDB-Complete | 21     | 81      | 3.9            | 1-16       |