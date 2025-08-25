# app/storage/sqlite_store.py
import sqlite3, os, json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterable, Dict, Any, List, Optional

ROW = Dict[str, Any]

class SQLiteStore:
    def __init__(self, path: str = "quickcap_results.db"):
        self.path = path
        self._init()

    @contextmanager
    def _conn(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield con
            con.commit()
        finally:
            con.close()

    def _init(self):
        with self._conn() as con:
            con.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              signal_type TEXT NOT NULL,    -- 'spot' | 'basis'
              venue TEXT NOT NULL,
              symbol TEXT NOT NULL,
              interval TEXT NOT NULL,
              side TEXT NOT NULL,
              price REAL NOT NULL,
              vwap REAL,
              rsi REAL,
              score REAL NOT NULL,
              triggers TEXT                  -- json array
            );
            CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts);

            CREATE TABLE IF NOT EXISTS executions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              venue TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              price REAL NOT NULL,
              score REAL NOT NULL,
              reason TEXT,
              is_paper INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_exec_ts ON executions(ts);

            CREATE TABLE IF NOT EXISTS signal_outcomes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              signal_id INTEGER NOT NULL,
              horizon_m INTEGER NOT NULL,
              entry_price REAL NOT NULL,
              exit_price REAL NOT NULL,
              ret REAL NOT NULL,       -- signed return for side
              max_fav REAL NOT NULL,   -- MFE
              max_adv REAL NOT NULL,   -- MAE
              UNIQUE(signal_id, horizon_m)
            );
            """)
    
    def insert_signal(self, row: ROW) -> int:
        with self._conn() as con:
            cur = con.execute("""
            INSERT INTO signals (ts, signal_type, venue, symbol, interval, side, price, vwap, rsi, score, triggers)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["ts"], row["signal_type"], row["venue"], row["symbol"], row["interval"],
                row["side"], row["price"], row.get("vwap"), row.get("rsi"), row["score"],
                json.dumps(row.get("triggers", [])),
            ))
            return int(cur.lastrowid)

    def insert_execution(self, row: ROW) -> int:
        with self._conn() as con:
            cur = con.execute("""
            INSERT INTO executions (ts, venue, symbol, side, price, score, reason, is_paper)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["ts"], row["venue"], row["symbol"], row["side"], row["price"], row["score"],
                row.get("reason"), int(row.get("is_paper", True)),
            ))
            return int(cur.lastrowid)

    def upsert_outcomes(self, rows: Iterable[ROW]) -> None:
        with self._conn() as con:
            con.executemany("""
            INSERT INTO signal_outcomes (signal_id, horizon_m, entry_price, exit_price, ret, max_fav, max_adv)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_id, horizon_m) DO UPDATE SET
                entry_price=excluded.entry_price,
                exit_price=excluded.exit_price,
                ret=excluded.ret,
                max_fav=excluded.max_fav,
                max_adv=excluded.max_adv
            """, [
                (r["signal_id"], r["horizon_m"], r["entry_price"], r["exit_price"], r["ret"], r["max_fav"], r["max_adv"])
                for r in rows
            ])
