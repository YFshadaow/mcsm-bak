import os
import sqlite3

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.mcsm_bak')


def open_db(label, instance):
    db_path = os.path.join(DB_DIR, label, instance)
    os.makedirs(db_path, exist_ok=True)
    conn = sqlite3.connect(os.path.join(db_path, 'cache.db'))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            path   TEXT PRIMARY KEY,
            mtime  REAL NOT NULL,
            size   INTEGER NOT NULL,
            sha256 TEXT NOT NULL
        )
    """)
    return conn


def load_cache(conn):
    cache = {}
    for row in conn.execute("SELECT path, mtime, size, sha256 FROM cache"):
        cache[row[0]] = {'mtime': row[1], 'size': row[2], 'sha256': row[3]}
    return cache


def write_entry(conn, path, file_meta):
    conn.execute(
        "INSERT OR REPLACE INTO cache (path, mtime, size, sha256) VALUES (?, ?, ?, ?)",
        (path, file_meta['mtime'], file_meta['size'], file_meta['sha256'])
    )
    conn.commit()
