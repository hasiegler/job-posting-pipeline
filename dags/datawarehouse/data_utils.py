import os


try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn_cursor():
    supabase_host = os.getenv("SUPABASE_HOST")
    if supabase_host:
        port = int(os.getenv("SUPABASE_PORT", "5432"))
        conn = psycopg2.connect(
            host=supabase_host,
            port=port,
            dbname=os.getenv("SUPABASE_DB", "postgres"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD"),
            sslmode="require",
            cursor_factory=RealDictCursor,
        )
        cur = conn.cursor()
        return conn, cur
    hook = PostgresHook(postgres_conn_id="SUPABASE_DB", database="postgres")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()
