import psycopg2
import os


def get_connection():
    """Connects to a postgres database"""
    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "db"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "postgres"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=os.environ.get("DATABASE_PASSWORD", "postgres"),
    )
