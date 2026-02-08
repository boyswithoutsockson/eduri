import sqlite3
import sys
import os

DB_FILE = os.environ.get("DATABASE_FILE", "db.sqlite3")

def run_sql(sql_file):
    conn = sqlite3.connect(DB_FILE)
    with open(sql_file, 'r') as f:
        sql = f.read()
    
    try:
        conn.executescript(sql)
        print(f"Executed {sql_file}")
    except sqlite3.Error as e:
        print(f"Error executing {sql_file}: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_sql.py <sql_file>")
        sys.exit(1)
    
    run_sql(sys.argv[1])
