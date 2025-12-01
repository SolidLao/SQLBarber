#!/usr/bin/env python3
"""
Create indexes for IMDB-Core and IMDB-Extended databases.
Matches the JOB benchmark index structure from the original IMDB database.
"""

import psycopg2
import configparser
import sys
from typing import Dict, List, Tuple
import time


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


def create_index(conn, index_name: str, table_name: str, column_name: str):
    """Create a single index."""
    cur = conn.cursor()

    # Check if index already exists
    cur.execute("""
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public'
        AND indexname = %s
    """, (index_name,))

    if cur.fetchone():
        print(f"  ✓ {index_name} already exists, skipping")
        cur.close()
        return

    sql = f"CREATE INDEX {index_name} ON {table_name} USING btree ({column_name})"
    print(f"  Creating {index_name} on {table_name}({column_name})...", end=' ', flush=True)

    start_time = time.time()
    cur.execute(sql)
    conn.commit()
    elapsed = time.time() - start_time

    print(f"✓ ({elapsed:.2f}s)")
    cur.close()


def create_core_indexes(config_path: str):
    """Create indexes for IMDB-Core database."""
    print("\n" + "="*60)
    print("Creating Indexes for IMDB-Core Database")
    print("="*60)

    conn_params = read_config(config_path)
    conn = psycopg2.connect(**conn_params, database='imdb_core')

    # Define indexes: (index_name, table_name, column_name)
    indexes = [
        # Title indexes
        ('kind_id_title', 'title', 'kind_id'),

        # Cast info indexes
        ('movie_id_cast_info', 'cast_info', 'movie_id'),
        ('person_id_cast_info', 'cast_info', 'person_id'),
        ('person_role_id_cast_info', 'cast_info', 'person_role_id'),

        # Movie info indexes
        ('movie_id_movie_info', 'movie_info', 'movie_id'),
        ('info_type_id_movie_info', 'movie_info', 'info_type_id'),

        # Movie keyword indexes
        ('movie_id_movie_keyword', 'movie_keyword', 'movie_id'),
        ('keyword_id_movie_keyword', 'movie_keyword', 'keyword_id'),
    ]

    total = len(indexes)
    print(f"\nCreating {total} indexes...\n")

    for idx, (index_name, table_name, column_name) in enumerate(indexes, 1):
        print(f"[{idx}/{total}]", end=' ')
        create_index(conn, index_name, table_name, column_name)

    conn.close()
    print(f"\n✓ All {total} indexes created for IMDB-Core!")


def create_extended_indexes(config_path: str):
    """Create indexes for IMDB-Extended database."""
    print("\n" + "="*60)
    print("Creating Indexes for IMDB-Extended Database")
    print("="*60)

    conn_params = read_config(config_path)
    conn = psycopg2.connect(**conn_params, database='imdb_extended')

    # Define indexes: (index_name, table_name, column_name)
    indexes = [
        # Core table indexes (same as Core)
        ('kind_id_title', 'title', 'kind_id'),
        ('movie_id_cast_info', 'cast_info', 'movie_id'),
        ('person_id_cast_info', 'cast_info', 'person_id'),
        ('person_role_id_cast_info', 'cast_info', 'person_role_id'),
        ('movie_id_movie_info', 'movie_info', 'movie_id'),
        ('info_type_id_movie_info', 'movie_info', 'info_type_id'),
        ('movie_id_movie_keyword', 'movie_keyword', 'movie_id'),
        ('keyword_id_movie_keyword', 'movie_keyword', 'keyword_id'),

        # Extended table indexes
        ('movie_id_movie_info_idx', 'movie_info_idx', 'movie_id'),
        ('info_type_id_movie_info_idx', 'movie_info_idx', 'info_type_id'),

        ('movie_id_movie_companies', 'movie_companies', 'movie_id'),
        ('company_id_movie_companies', 'movie_companies', 'company_id'),
        ('company_type_id_movie_companies', 'movie_companies', 'company_type_id'),

        ('movie_id_aka_title', 'aka_title', 'movie_id'),
        ('kind_id_aka_title', 'aka_title', 'kind_id'),

        ('person_id_aka_name', 'aka_name', 'person_id'),

        ('person_id_person_info', 'person_info', 'person_id'),
        ('info_type_id_person_info', 'person_info', 'info_type_id'),
    ]

    total = len(indexes)
    print(f"\nCreating {total} indexes...\n")

    for idx, (index_name, table_name, column_name) in enumerate(indexes, 1):
        print(f"[{idx}/{total}]", end=' ')
        create_index(conn, index_name, table_name, column_name)

    conn.close()
    print(f"\n✓ All {total} indexes created for IMDB-Extended!")


def verify_indexes(config_path: str):
    """Verify indexes were created successfully."""
    print("\n" + "="*60)
    print("Verification Summary")
    print("="*60)

    conn_params = read_config(config_path)

    # Check IMDB-Core
    conn = psycopg2.connect(**conn_params, database='imdb_core')
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM pg_indexes
        WHERE schemaname = 'public'
        AND indexname NOT LIKE '%_pkey'
    """)
    core_indexes = cur.fetchone()[0]
    cur.close()
    conn.close()

    # Check IMDB-Extended
    conn = psycopg2.connect(**conn_params, database='imdb_extended')
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM pg_indexes
        WHERE schemaname = 'public'
        AND indexname NOT LIKE '%_pkey'
    """)
    extended_indexes = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"\nIMDB-Core: {core_indexes} secondary indexes created")
    print(f"IMDB-Extended: {extended_indexes} secondary indexes created")
    print("\n✓ Index creation complete!")


def main():
    config_path = '/home/SQLBarber/configs/postgres.ini'

    try:
        start_time = time.time()

        # Create indexes for both databases
        create_core_indexes(config_path)
        create_extended_indexes(config_path)

        # Verify
        verify_indexes(config_path)

        elapsed = time.time() - start_time
        print(f"\nTotal time: {elapsed:.2f}s")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
