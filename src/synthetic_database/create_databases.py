#!/usr/bin/env python3
"""
Create IMDB-Core and IMDB-Extended databases from the complete IMDB database.
Maintains referential integrity by copying data in dependency order.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import configparser
import sys
from typing import Dict, List, Tuple


def read_config(config_path: str) -> Dict[str, str]:
    """Read PostgreSQL configuration from INI file."""
    config = configparser.ConfigParser()
    config.read(config_path)
    return {
        'host': 'localhost',
        'port': config['DATABASE']['port'],
        'user': config['DATABASE']['user'],
        'password': config['DATABASE']['password']
    }


def create_database(conn_params: Dict[str, str], db_name: str):
    """Create a new database if it doesn't exist."""
    # Connect to default postgres database
    conn = psycopg2.connect(**conn_params, database='postgres')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Check if database exists
    cur.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (db_name,)
    )

    if cur.fetchone():
        print(f"Database '{db_name}' already exists. Dropping...")
        # Terminate existing connections
        cur.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid <> pg_backend_pid()
        """)
        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))

    print(f"Creating database '{db_name}'...")
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    cur.close()
    conn.close()


def execute_schema(conn_params: Dict[str, str], db_name: str, schema_file: str):
    """Execute schema SQL file on the target database."""
    conn = psycopg2.connect(**conn_params, database=db_name)
    cur = conn.cursor()

    with open(schema_file, 'r') as f:
        schema_sql = f.read()

    print(f"Creating schema in '{db_name}'...")
    cur.execute(schema_sql)
    conn.commit()

    cur.close()
    conn.close()


def copy_table_data(
    source_conn,
    target_conn,
    table_name: str,
    columns: List[str],
    where_clause: str = None
):
    """Copy data from source to target table with specified columns."""
    source_cur = source_conn.cursor()
    target_cur = target_conn.cursor()

    # Build SELECT query
    columns_str = ', '.join(columns)
    query = f"SELECT {columns_str} FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"

    print(f"  Copying {table_name}...", end=' ')
    source_cur.execute(query)

    # Fetch and insert in batches
    batch_size = 10000
    total_rows = 0

    while True:
        rows = source_cur.fetchmany(batch_size)
        if not rows:
            break

        # Build INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        target_cur.executemany(insert_query, rows)
        total_rows += len(rows)

    target_conn.commit()
    print(f"{total_rows} rows")

    source_cur.close()
    target_cur.close()


def create_imdb_core(config_path: str):
    """Create IMDB-Core database."""
    print("\n" + "="*60)
    print("Creating IMDB-Core Database")
    print("="*60)

    conn_params = read_config(config_path)
    db_name = 'imdb_core'

    # Create database and schema
    create_database(conn_params, db_name)
    execute_schema(conn_params, db_name, '/home/synthetic_database/schema_core.sql')

    # Connect to source and target databases
    source_conn = psycopg2.connect(**conn_params, database='imdb')
    target_conn = psycopg2.connect(**conn_params, database=db_name)

    print("\nCopying data with referential integrity...")

    # Copy dictionary/type tables (no dependencies)
    copy_table_data(source_conn, target_conn, 'kind_type', ['id', 'kind'])
    copy_table_data(source_conn, target_conn, 'info_type', ['id', 'info'])
    copy_table_data(source_conn, target_conn, 'role_type', ['id', 'role'])
    copy_table_data(source_conn, target_conn, 'keyword', ['id', 'keyword'])

    # Copy entity tables
    # Title references kind_type
    copy_table_data(
        source_conn, target_conn, 'title',
        ['id', 'title', 'kind_id', 'production_year']
    )

    # Name is independent
    copy_table_data(source_conn, target_conn, 'name', ['id', 'name', 'gender'])

    # Copy relationship tables (depend on entity tables)
    # movie_info references title and info_type
    copy_table_data(
        source_conn, target_conn, 'movie_info',
        ['id', 'movie_id', 'info_type_id', 'info'],
        f"movie_id IN (SELECT id FROM title) AND info_type_id IN (SELECT id FROM info_type)"
    )

    # cast_info references name, title, and role_type
    copy_table_data(
        source_conn, target_conn, 'cast_info',
        ['id', 'person_id', 'movie_id', 'person_role_id', 'nr_order'],
        f"person_id IN (SELECT id FROM name) AND movie_id IN (SELECT id FROM title) AND person_role_id IN (SELECT id FROM role_type)"
    )

    # movie_keyword references title and keyword
    copy_table_data(
        source_conn, target_conn, 'movie_keyword',
        ['id', 'movie_id', 'keyword_id'],
        f"movie_id IN (SELECT id FROM title) AND keyword_id IN (SELECT id FROM keyword)"
    )

    source_conn.close()
    target_conn.close()

    print(f"\n✓ IMDB-Core database created successfully!")


def create_imdb_extended(config_path: str):
    """Create IMDB-Extended database."""
    print("\n" + "="*60)
    print("Creating IMDB-Extended Database")
    print("="*60)

    conn_params = read_config(config_path)
    db_name = 'imdb_extended'

    # Create database and schema
    create_database(conn_params, db_name)
    execute_schema(conn_params, db_name, '/home/synthetic_database/schema_extended.sql')

    # Connect to source and target databases
    source_conn = psycopg2.connect(**conn_params, database='imdb')
    target_conn = psycopg2.connect(**conn_params, database=db_name)

    print("\nCopying data with referential integrity...")

    # Copy core dictionary/type tables
    copy_table_data(source_conn, target_conn, 'kind_type', ['id', 'kind'])
    copy_table_data(source_conn, target_conn, 'info_type', ['id', 'info'])
    copy_table_data(source_conn, target_conn, 'role_type', ['id', 'role'])
    copy_table_data(source_conn, target_conn, 'keyword', ['id', 'keyword'])
    copy_table_data(source_conn, target_conn, 'company_type', ['id', 'kind'])

    # Copy entity tables
    copy_table_data(
        source_conn, target_conn, 'title',
        ['id', 'title', 'kind_id', 'production_year']
    )
    copy_table_data(source_conn, target_conn, 'name', ['id', 'name', 'gender'])
    copy_table_data(
        source_conn, target_conn, 'company_name',
        ['id', 'name', 'country_code']
    )

    # Copy core relationship tables
    copy_table_data(
        source_conn, target_conn, 'movie_info',
        ['id', 'movie_id', 'info_type_id', 'info'],
        f"movie_id IN (SELECT id FROM title) AND info_type_id IN (SELECT id FROM info_type)"
    )

    copy_table_data(
        source_conn, target_conn, 'cast_info',
        ['id', 'person_id', 'movie_id', 'person_role_id', 'nr_order'],
        f"person_id IN (SELECT id FROM name) AND movie_id IN (SELECT id FROM title) AND person_role_id IN (SELECT id FROM role_type)"
    )

    copy_table_data(
        source_conn, target_conn, 'movie_keyword',
        ['id', 'movie_id', 'keyword_id'],
        f"movie_id IN (SELECT id FROM title) AND keyword_id IN (SELECT id FROM keyword)"
    )

    # Copy extended relationship tables
    copy_table_data(
        source_conn, target_conn, 'movie_info_idx',
        ['id', 'movie_id', 'info_type_id', 'info'],
        f"movie_id IN (SELECT id FROM title) AND info_type_id IN (SELECT id FROM info_type)"
    )

    copy_table_data(
        source_conn, target_conn, 'movie_companies',
        ['id', 'movie_id', 'company_id', 'company_type_id', 'note'],
        f"movie_id IN (SELECT id FROM title) AND company_id IN (SELECT id FROM company_name) AND company_type_id IN (SELECT id FROM company_type)"
    )

    copy_table_data(
        source_conn, target_conn, 'aka_title',
        ['id', 'movie_id', 'title', 'kind_id', 'production_year'],
        f"movie_id IN (SELECT id FROM title)"
    )

    copy_table_data(
        source_conn, target_conn, 'aka_name',
        ['id', 'person_id', 'name'],
        f"person_id IN (SELECT id FROM name)"
    )

    copy_table_data(
        source_conn, target_conn, 'person_info',
        ['id', 'person_id', 'info_type_id', 'info'],
        f"person_id IN (SELECT id FROM name) AND info_type_id IN (SELECT id FROM info_type)"
    )

    source_conn.close()
    target_conn.close()

    print(f"\n✓ IMDB-Extended database created successfully!")


def main():
    config_path = '/home/SQLBarber/configs/postgres.ini'

    try:
        # Create both databases
        create_imdb_core(config_path)
        create_imdb_extended(config_path)

        print("\n" + "="*60)
        print("All databases created successfully!")
        print("="*60)
        print("\nDatabases created:")
        print("  - imdb_core (9 tables, ~27 columns)")
        print("  - imdb_extended (16 tables, ~53 columns)")
        print("\nYou can connect using:")
        print("  psql -h localhost -p 5600 -U postgres -d imdb_core")
        print("  psql -h localhost -p 5600 -U postgres -d imdb_extended")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
