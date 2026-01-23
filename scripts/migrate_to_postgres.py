import sqlite3
import psycopg2
import os
import sys
from datetime import datetime

# CONFIGURATION
SQLITE_DB = "plumber.db"
# Default local postgres creds from installer script
PG_HOST = "localhost"
PG_DB = "plumber_db"
PG_USER = "plumber_user"
PG_PASS = "plumber_strong_password"

def migrate():
    print(f"🚀 Starting Migration: {SQLITE_DB} -> PostgreSQL...")
    
    if not os.path.exists(SQLITE_DB):
        print(f"❌ SQLite DB not found at {SQLITE_DB}")
        sys.exit(1)

    # 1. Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cur = sqlite_conn.cursor()

    # 2. Connect to Postgres
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )
        pg_cur = pg_conn.cursor()
    except Exception as e:
        print(f"❌ Could not connect to Postgres: {e}")
        print("Did you run ./scripts/install_production_stack.sh?")
        sys.exit(1)

    # 3. Get Tables
    sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row['name'] for row in sqlite_cur.fetchall() if row['name'] != 'sqlite_sequence']

    for table in tables:
        print(f"📦 Migrating table: {table}...")
        
        # Get data
        sqlite_cur.execute(f"SELECT * FROM {table}")
        rows = sqlite_cur.fetchall()
        
        if not rows:
            print(f"   Skipping (Empty)")
            continue
            
        # Get columns
        col_names = [description[0] for description in sqlite_cur.description]
        cols_str = ", ".join(col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        
        # Insert into Postgres
        # Note: We assume schema is already created via init_db logic or we create it dynamically.
        # For simplicity in this script, we assume the application 'init_db' has run against Postgres
        # effectively creating empty tables.
        
        count = 0
        for row in rows:
            try:
                pg_cur.execute(
                    f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    tuple(row)
                )
                count += 1
            except Exception as e:
                print(f"   ⚠️ Row Error: {e}")
        
        print(f"   ✅ Migrated {count} rows.")

    # Commit and Close
    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()
    print("✨ Migration Complete!")

if __name__ == "__main__":
    migrate()
