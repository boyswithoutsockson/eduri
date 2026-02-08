import sqlite3
import os
import csv
import sys

# Increase CSV field size limit to handle large fields
csv.field_size_limit(sys.maxsize)

DB_FILE = os.environ.get("DATABASE_FILE", "db.sqlite3")


def get_connection():
    """Connects to a sqlite database"""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def bulk_insert(cursor, table, columns, file_obj, has_header=False):
    """
    Inserts data from a file object (CSV) into a table.
    
    Args:
        cursor: sqlite3 cursor
        table: Table name
        columns: List of column names
        file_obj: Open file object pointing to CSV data
        has_header: Boolean, if True skips the first row of the CSV file
    """
    reader = csv.reader(file_obj)
    if has_header:
        next(reader, None)
    
    # Convert empty strings to None (NULL)
    data = ([val if val != '' else None for val in row] for row in reader)
    
    placeholders = ', '.join(['?'] * len(columns))
    col_str = ', '.join(columns)
    sql = f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})"
    
    try:
        cursor.executemany(sql, data)
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        # We might want to re-raise or handle it, but for now print it.
        raise