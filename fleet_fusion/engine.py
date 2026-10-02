"""Incremental fusion engine.

Public API:
    Fusion(path)            open (or create) a fusion database
    ingest(records)         persist raw records and update affected derived state
    query(vehicle, s, e)    iterate anchors in [s, e) with match/stress/rolling
    explain(vehicle, sid)   match provenance + rolling lineage for one anchor
"""

from __future__ import annotations

import heapq
import sqlite3
from collections import defaultdict, deque
from typing import Any, Iterable, Iterator, Mapping, Optional, TypedDict

VERSION = "v1"
METHOD = "nearest_temp_within_30s_tie_earlier"

MATCH_WINDOW_MS = 30_000
ROLLING_WINDOW_MS = 300_000
TEMP_THRESHOLD_F = 180

_CHUNK = 2000

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    id       PRIMARY KEY,
    vehicle  TEXT    NOT NULL,
    source   TEXT    NOT NULL,
    ts       INTEGER NOT NULL,
    value    INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_records_vst ON records(vehicle, source, ts);

CREATE TABLE IF NOT EXISTS anchors (
    vehicle     TEXT    NOT NULL,
    speed_id    NOT NULL,
    ts          INTEGER NOT NULL,
    speed       INTEGER NOT NULL,
    temp_id     ,
    temp_ts     INTEGER,
    temp_value  INTEGER,
    label       TEXT    NOT NULL,
    stress      INTEGER,
    rolling_num INTEGER,
    rolling_den INTEGER,
    PRIMARY KEY (vehicle, speed_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_anchors_vt ON anchors(vehicle, ts);
"""


class Record(TypedDict):
    id: Any          # globally unique record id (str or int)
    vehicle: str
    source: str      # "speed" (mph) or "temp" (degF)
    ts: int          # epoch milliseconds
    value: int


class Fusion:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self.counters = {
            "records_read": 0,
            "anchors_scanned": 0,
            "anchors_written": 0,
        }

    def close(self) -> None:
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "Fusion":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ingest(self, records: Iterable[Mapping[str, Any]]) -> None:
        changed: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"speed": [], "temp_intervals": []}
        )
        chunk: list[Mapping[str, Any]] = []
        for rec in records:
            chunk.append(rec)
            if len(chunk) >= _CHUNK:
                self._upsert_chunk(chunk, changed)
                chunk = []
        if chunk:
            self._upsert_chunk(chunk, changed)
        for vehicle, points in changed.items():
            self._propagate(
                vehicle, points["speed"], points["temp_intervals"]
            )
        self._conn.commit()

    def _upsert_chunk(
        self,
        chunk: list[Mapping[str, Any]],
        changed: dict[str, dict[str, Any]],
    ) -> None:
        ids = [r["id"] for r in chunk]
        marks = ",".join("?" * len(ids))
        existing = {
            rid: val
            for rid, val in self._conn.execute(
                f"SELECT id, value FROM records WHERE id IN ({marks})", ids
            )
        }
        self.counters["records_read"] += len(chunk)
        dirty = [
            r for r in chunk
            if r["id"] not in existing or existing[r["id"]] != r["value"]
        ]
        if not dirty:
            return
        self._conn.executemany(
            "INSERT INTO records(id, vehicle, source, ts, value)"
            " VALUES(?,?,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET value=excluded.value",
            [(r["id"], r["vehicle"], r["source"], r["ts"], r["value"])
             for r in dirty],
        )
        temp_ts: dict[str, list[int]] = defaultdict(list)
        for r in dirty:
            if r["source"] == "speed":
                changed[r["vehicle"]]["speed"].append(r["ts"])
            else:
                temp_ts[r["vehicle"]].append(r["ts"])
        # Fold changed temp points into merged +-30s intervals per chunk so
        # memory stays proportional to affected regions, not record count.
        for vehicle, points in temp_ts.items():
            points.sort()
            intervals = _points_to_intervals(points, MATCH_WINDOW_MS)
            slot = changed[vehicle]["temp_intervals"]
            changed[vehicle]["temp_intervals"] = _union_intervals(
                slot, intervals
            )

    def _propagate(
        self, vehicle: str, speed_points: list[int],
        temp_intervals: list[list[int]],
    ) -> None:
        speed_points.sort()
        changed_stress: list[int] = []

        # Phase A1: (re)build anchors for every changed speed record.
        for ts in speed_points:
            sid, speed = self._conn.execute(
                "SELECT id, value FROM records"
                " WHERE vehicle=? AND source='speed' AND ts=?",
                (vehicle, ts),
            ).fetchone()
            match = self._compute_match(vehicle, ts, speed)
            self._write_anchor(vehicle, sid, ts, speed, match)

        # Phase A2: re-match anchors within +-30s of any changed temp.
        for lo, hi in temp_intervals:
            cur = self._conn.execute(
                "SELECT speed_id, ts, speed, temp_id, temp_ts, temp_value,"
                " label, stress FROM anchors"
                " WHERE vehicle=? AND ts>=? AND ts<? ORDER BY ts",
                (vehicle, lo, hi),
            )
            for row in cur:
                self.counters["anchors_scanned"] += 1
                sid, ts, speed = row[0], row[1], row[2]
                stored = tuple(row[3:])
                match = self._compute_match(vehicle, ts, speed)
                if match != stored:
                    self._write_anchor(vehicle, sid, ts, speed, match)
                    if match[4] != stored[4]:
                        changed_stress.append(ts)

        # Phase B: recompute rolling means over merged affected ranges.
        for lo, hi in _range_stream(
            speed_points, changed_stress, ROLLING_WINDOW_MS
        ):
            self._recompute_rolling(vehicle, lo, hi)

    def _match_temp(self, vehicle: str, ts: int):
        prev = self._conn.execute(
            "SELECT id, ts, value FROM records"
            " WHERE vehicle=? AND source='temp' AND ts<=?"
            " ORDER BY ts DESC LIMIT 1",
            (vehicle, ts),
        ).fetchone()
        nxt = self._conn.execute(
            "SELECT id, ts, value FROM records"
            " WHERE vehicle=? AND source='temp' AND ts>?"
            " ORDER BY ts ASC LIMIT 1",
            (vehicle, ts),
        ).fetchone()
        best = None
        if prev is not None and ts - prev[1] <= MATCH_WINDOW_MS:
            best = prev
        if nxt is not None and nxt[1] - ts <= MATCH_WINDOW_MS:
            if best is None or nxt[1] - ts < ts - best[1]:
                best = nxt
        return best

    def _compute_match(self, vehicle: str, ts: int, speed: int) -> tuple:
        m = self._match_temp(vehicle, ts)
        if m is None:
            return (None, None, None, "missing", None)
        temp_id, temp_ts, temp_value = m
        label = "exact" if temp_ts == ts else "near"
        stress = speed * max(0, temp_value - TEMP_THRESHOLD_F)
        return (temp_id, temp_ts, temp_value, label, stress)

    def _write_anchor(self, vehicle: str, sid, ts: int, speed: int,
                      match: tuple) -> None:
        temp_id, temp_ts, temp_value, label, stress = match
        self._conn.execute(
            "INSERT INTO anchors(vehicle, speed_id, ts, speed,"
            " temp_id, temp_ts, temp_value, label, stress)"
            " VALUES(?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(vehicle, speed_id) DO UPDATE SET"
            " speed=excluded.speed,"
            " temp_id=excluded.temp_id, temp_ts=excluded.temp_ts,"
            " temp_value=excluded.temp_value, label=excluded.label,"
            " stress=excluded.stress",
            (vehicle, sid, ts, speed,
             temp_id, temp_ts, temp_value, label, stress),
        )
        self.counters["anchors_written"] += 1

    def _recompute_rolling(self, vehicle: str, lo: int, hi: int) -> None:
        cur = self._conn.execute(
            "SELECT ts, speed_id, stress, rolling_num, rolling_den"
            " FROM anchors WHERE vehicle=? AND ts>? AND ts<? ORDER BY ts",
            (vehicle, lo - ROLLING_WINDOW_MS, hi),
        )
        window: deque[tuple[int, Optional[int]]] = deque()
        total = 0
        count = 0
        pending: list[tuple] = []
        for ts, sid, stress, old_num, old_den in cur:
            self.counters["anchors_scanned"] += 1
            limit = ts - ROLLING_WINDOW_MS
            while window and window[0][0] <= limit:
                _, w_stress = window.popleft()
                if w_stress is not None:
                    total -= w_stress
                    count -= 1
            window.append((ts, stress))
            if stress is not None:
                total += stress
                count += 1
            if ts < lo:
                continue
            num, den = (total, count) if count else (None, None)
            if num != old_num or den != old_den:
                pending.append((num, den, vehicle, sid))
                if len(pending) >= _CHUNK:
                    self._flush_rolling(pending)
                    pending.clear()
        if pending:
            self._flush_rolling(pending)

    def _flush_rolling(self, pending: list[tuple]) -> None:
        self._conn.executemany(
            "UPDATE anchors SET rolling_num=?, rolling_den=?"
            " WHERE vehicle=? AND speed_id=?",
            pending,
        )
        self.counters["anchors_written"] += len(pending)

    def query(self, vehicle: str, start: int, end: int) -> Iterator[dict]:
        cur = self._conn.execute(
            "SELECT speed_id, ts, speed, temp_id, label, stress,"
            " rolling_num, rolling_den FROM anchors"
            " WHERE vehicle=? AND ts>=? AND ts<? ORDER BY ts",
            (vehicle, start, end),
        )
        for sid, ts, speed, temp_id, label, stress, num, den in cur:
            yield {
                "speed_id": sid,
                "ts": ts,
                "speed": speed,
                "temp_id": temp_id,
                "label": label,
                "stress": stress,
                "rolling": None if num is None else {"num": num, "den": den},
            }

    def explain(self, vehicle: str, speed_id: Any) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT ts, speed, temp_id, temp_ts, temp_value, label, stress,"
            " rolling_num, rolling_den FROM anchors"
            " WHERE vehicle=? AND speed_id=?",
            (vehicle, speed_id),
        ).fetchone()
        if row is None:
            return None
        ts, speed, temp_id, temp_ts, temp_value, label, stress, num, den = row

        def contributions() -> Iterator[dict]:
            for sid, tid in self._conn.execute(
                "SELECT speed_id, temp_id FROM anchors"
                " WHERE vehicle=? AND ts>? AND ts<=? AND stress IS NOT NULL"
                " ORDER BY ts",
                (vehicle, ts - ROLLING_WINDOW_MS, ts),
            ):
                yield {"speed_id": sid, "temp_id": tid}

        return {
            "version": VERSION,
            "method": METHOD,
            "vehicle": vehicle,
            "speed_id": speed_id,
            "ts": ts,
            "speed": speed,
            "match": None if temp_id is None else {
                "temp_id": temp_id,
                "temp_ts": temp_ts,
                "temp_value": temp_value,
                "label": label,
            },
            "label": label,
            "stress": stress,
            "rolling": None if num is None else {"num": num, "den": den},
            "contributions": contributions(),
        }


def _points_to_intervals(points: list[int], span: int) -> list[list[int]]:
    """Sorted points -> merged half-open intervals [p-span, p+span+1)."""
    intervals: list[list[int]] = []
    for p in points:
        lo, hi = p - span, p + span + 1
        if intervals and lo <= intervals[-1][1]:
            if hi > intervals[-1][1]:
                intervals[-1][1] = hi
        else:
            intervals.append([lo, hi])
    return intervals


def _union_intervals(
    a: list[list[int]], b: list[list[int]]
) -> list[list[int]]:
    out: list[list[int]] = []
    for lo, hi in heapq.merge(a, b):
        if out and lo <= out[-1][1]:
            if hi > out[-1][1]:
                out[-1][1] = hi
        else:
            out.append([lo, hi])
    return out


def _range_stream(
    points_a: list[int], points_b: list[int], span: int
) -> Iterator[tuple[int, int]]:
    """Merged [p, p+span) ranges from two sorted point streams, O(1) memory."""
    cur: Optional[list[int]] = None
    for p in heapq.merge(points_a, points_b):
        lo, hi = p, p + span
        if cur is None:
            cur = [lo, hi]
        elif lo <= cur[1]:
            if hi > cur[1]:
                cur[1] = hi
        else:
            yield cur[0], cur[1]
            cur = [lo, hi]
    if cur is not None:
        yield cur[0], cur[1]
