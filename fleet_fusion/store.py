"""SQLite schema and low-level row helpers."""

from __future__ import annotations

import sqlite3
from typing import Optional, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS records(
  id TEXT PRIMARY KEY,
  vehicle TEXT NOT NULL,
  source TEXT NOT NULL,
  ts INTEGER NOT NULL,
  value INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_vst ON records(vehicle, source, ts);

CREATE TABLE IF NOT EXISTS anchors(
  speed_id TEXT PRIMARY KEY,
  vehicle TEXT NOT NULL,
  ts INTEGER NOT NULL,
  speed INTEGER NOT NULL,
  temp_id TEXT,
  temp_ts INTEGER,
  temp_val INTEGER,
  label TEXT NOT NULL,
  stress INTEGER,
  roll_sum INTEGER NOT NULL DEFAULT 0,
  roll_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_anchors_vt ON anchors(vehicle, ts);
CREATE INDEX IF NOT EXISTS idx_anchors_temp ON anchors(temp_id);
"""

MATCH_WINDOW_MS = 30_000
ROLL_WINDOW_MS = 300_000


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript(SCHEMA)
    return conn


def nearest_temp(
    conn: sqlite3.Connection, vehicle: str, ts: int
) -> Optional[Tuple[str, int, int, str]]:
    """Return (temp_id, temp_ts, temp_val, label) for the nearest temp record
    of `vehicle` within +/-MATCH_WINDOW_MS of `ts`; ties resolve to the
    earlier temp. None when no temp is inside the window."""
    pred = conn.execute(
        "SELECT id, ts, value FROM records"
        " WHERE vehicle=? AND source='temp' AND ts<=?"
        " ORDER BY ts DESC LIMIT 1",
        (vehicle, ts),
    ).fetchone()
    succ = conn.execute(
        "SELECT id, ts, value FROM records"
        " WHERE vehicle=? AND source='temp' AND ts>?"
        " ORDER BY ts ASC LIMIT 1",
        (vehicle, ts),
    ).fetchone()
    best = None
    if pred is not None and ts - pred[1] <= MATCH_WINDOW_MS:
        best = pred
    if succ is not None and succ[1] - ts <= MATCH_WINDOW_MS:
        if best is None or succ[1] - ts < ts - best[1]:
            best = succ
    if best is None:
        return None
    label = "exact" if best[1] == ts else "near"
    return (best[0], best[1], best[2], label)


def stress_of(speed: int, temp_val: Optional[int]) -> Optional[int]:
    if temp_val is None:
        return None
    return speed * max(0, temp_val - 180)
