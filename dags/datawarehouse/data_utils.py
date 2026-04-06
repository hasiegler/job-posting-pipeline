import logging
import os
import time


try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

_MAX_RETRIES = 5
_BASE_DELAY = 2  # seconds


def get_conn_cursor(*, retries: int = _MAX_RETRIES, base_delay: float = _BASE_DELAY):
    """Return a (connection, cursor) pair with retry + exponential backoff.

    Supabase's connection pooler in Session mode has a hard pool_size limit.
    When many mapped Airflow tasks start simultaneously, some may get rejected
    with ``MaxClientsInSessionMode``.  Retrying with jitter-free backoff lets
    earlier tasks finish and release their slots.
    """
    supabase_host = os.getenv("SUPABASE_HOST")
    for attempt in range(1, retries + 1):
        try:
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
        except psycopg2.OperationalError as exc:
            if attempt == retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "Connection attempt %d/%d failed (%s), retrying in %.1fs …",
                attempt, retries, exc, delay,
            )
            time.sleep(delay)


def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()
