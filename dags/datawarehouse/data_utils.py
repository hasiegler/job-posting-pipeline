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

# Errors that indicate the underlying socket / pooler session has died and the
# connection is no longer usable.  Retrying on a *fresh* connection is safe.
_TRANSIENT_DB_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)


def get_conn_cursor(*, retries: int = _MAX_RETRIES, base_delay: float = _BASE_DELAY):
    """Return a (connection, cursor) pair with retry + exponential backoff.

    Supabase's connection pooler in Session mode has a hard pool_size limit.
    When many mapped Airflow tasks start simultaneously, some may get rejected
    with ``MaxClientsInSessionMode``.  Retrying with jitter-free backoff lets
    earlier tasks finish and release their slots.

    TCP keepalives are enabled so the pooler / NAT in front of Supabase does
    not silently drop a long-lived session, which previously surfaced as
    ``SSL connection has been closed unexpectedly`` mid-query.
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
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
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
    try:
        if cur is not None:
            cur.close()
    except Exception:
        pass
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


def run_with_db(fn, *, retries: int = 3, base_delay: float = 2.0):
    """Run ``fn(conn, cur)`` with retry on transient DB connection failures.

    Each attempt opens a fresh connection, runs ``fn``, and closes it.  If the
    connection dies mid-query (e.g. the Supabase pooler resets the SSL session
    or returns ``InterfaceError: connection already closed``), the broken
    connection is discarded and a brand new one is opened for the next try.

    This is safe because ``fn`` is expected to manage its own transaction —
    the bulk operations in ``data_modification`` either commit at the end or
    raise, so a retry restarts cleanly without leaving partial work behind.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        conn = None
        cur = None
        try:
            conn, cur = get_conn_cursor()
            result = fn(conn, cur)
            close_conn_cursor(conn, cur)
            return result
        except _TRANSIENT_DB_ERRORS as exc:
            last_exc = exc
            close_conn_cursor(conn, cur)
            if attempt == retries:
                logger.error(
                    "DB operation failed after %d attempts: %s", retries, exc,
                )
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "DB operation failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt, retries, exc, delay,
            )
            time.sleep(delay)
        except Exception:
            # Non-transient error: don't retry, but make sure we release the
            # connection back to the pooler before propagating.
            close_conn_cursor(conn, cur)
            raise

    # Defensive: loop should have either returned or re-raised above.
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("run_with_db exited without result or exception")
