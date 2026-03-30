import os
import sys
import threading

import psycopg2.extras
import psycopg2.pool

SYNC_DB_URL = os.environ.get("DATABASE_URL")
_sync_pool = None
_pool_lock = threading.Lock()


def get_sync_pool():
    global _sync_pool
    if _sync_pool is None:
        with _pool_lock:
            if _sync_pool is None:
                if not SYNC_DB_URL:
                    raise RuntimeError("Falta env var DATABASE_URL")
                _sync_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=10, dsn=SYNC_DB_URL, connect_timeout=5
                )
    return _sync_pool


def get_sync_conn():
    return get_sync_pool().getconn()


def release_sync_conn(conn):
    if conn:
        get_sync_pool().putconn(conn)


def tabla_vacia(tabla="ventas_items"):
    conn = get_sync_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {tabla} LIMIT 1)")
            tiene_datos = cur.fetchone()[0]
            return not tiene_datos
    finally:
        release_sync_conn(conn)


def upsert_items(records: list, tabla="ventas_items", batch_size=500):
    if not records:
        return 0

    columnas = list(records[0].keys())
    cols_str = ", ".join(columnas)

    update_cols = [c for c in columnas if c not in ("folio", "descripcion")]
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    query = f"""
        INSERT INTO {tabla} ({cols_str})
        VALUES %s
        ON CONFLICT (folio, descripcion) DO UPDATE SET {update_str}
    """

    conn = get_sync_conn()
    total = 0
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for start in range(0, len(records), batch_size):
                batch = records[start : start + batch_size]
                valores = [tuple(r[c] for c in columnas) for r in batch]
                psycopg2.extras.execute_values(cur, query, valores)
                total += len(batch)
                print(f"Upsert {start}–{start+len(batch)}: OK", file=sys.stderr)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_sync_conn(conn)

    return total

