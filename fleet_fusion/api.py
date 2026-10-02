"""Public API: ingest / query / explain.

Incremental maintenance strategy
--------------------------------
Raw records are upserted first; every derived change is then computed from
the post-upsert raw state, so the result equals a from-scratch recompute.

* new/revised speed  -> (re)match against nearest temp, stress delta event
* new temp           -> rematch anchors inside +/-30s, stress delta events
* revised temp       -> restress anchors currently matched to it
* stress deltas      -> sweep-line over anchors in [t, t+300s) per event,
  updating stored roll_sum/roll_count in O(affected anchors), never
  rescanning a full 300s window per anchor.
"""

from __future__ import annotations

import sqlite3
from bisect import bisect_right
from collections import deque
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from . import store

_CHUNK = 32768
_UPDATE_BATCH = 8192


def _normalize(rec) -> Tuple[str, str, str, int, int]:
    if isinstance(rec, dict):
        rid, vehicle, source, ts, value = (
            rec["id"], rec["vehicle"], rec["source"], rec["ts"], rec["value"])
    else:
        rid, vehicle, source, ts, value = (
            rec.id, rec.vehicle, rec.source, rec.ts, rec.value)
    return str(rid), str(vehicle), str(source), int(ts), int(value)


class Fusion:
    """Incremental fusion of speed/temp telemetry backed by SQLite."""

    VERSION = "v1"
    MATCH_METHOD = "nearest-temp-within-30000ms-tie-earlier"

    def __init__(self, path: str) -> None:
        self._conn = store.connect(path)
        # instrumentation: reproducible evidence of per-ingest work volume
        self.stats = {"rows_read": 0, "rows_written": 0}

    # ---------------------------------------------------------------- ingest
    def ingest(self, records: Iterable[dict]) -> None:
        buf: List[tuple] = []
        for rec in records:
            buf.append(_normalize(rec))
            if len(buf) >= _CHUNK:
                self._ingest_chunk(buf)
                buf = []
        if buf:
            self._ingest_chunk(buf)

    def _ingest_chunk(self, recs: List[tuple]) -> None:
        # tolerate intra-chunk redelivery/revision: last occurrence wins,
        # effects are computed from the post-upsert raw state either way
        dedup: Dict[str, tuple] = {}
        for rec in recs:
            dedup[rec[0]] = rec
        by_vehicle: Dict[str, List[tuple]] = {}
        for rec in dedup.values():
            by_vehicle.setdefault(rec[1], []).append(rec)
        with self._conn:  # one transaction per chunk
            for vehicle, rows in by_vehicle.items():
                self._ingest_vehicle(vehicle, rows)

    def _ingest_vehicle(self, vehicle: str, recs: List[tuple]) -> None:
        conn = self._conn
        existing: Dict[str, Tuple[str, int]] = {}
        ids = [r[0] for r in recs]
        for i in range(0, len(ids), 500):
            part = ids[i:i + 500]
            marks = ",".join("?" * len(part))
            for rid, source, value in conn.execute(
                    f"SELECT id, source, value FROM records WHERE id IN ({marks})",
                    part):
                existing[rid] = (source, value)
            self.stats["rows_read"] += len(part)
        conn.executemany(
            "INSERT INTO records(id, vehicle, source, ts, value) VALUES(?,?,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET value=excluded.value",
            recs)
        self.stats["rows_written"] += len(recs)

        events: Dict[int, List[int]] = {}  # anchor ts -> [dsum, dcount]
        # phase A: match new speeds and pull their pre-chunk window sums
        # (anchors table still holds only pre-chunk anchors at this point)
        new_speeds = []
        for rid, _v, source, ts, value in recs:
            if source == "speed" and existing.get(rid) is None:
                match = store.nearest_temp(self._conn, vehicle, ts)
                self.stats["rows_read"] += 2
                new_speeds.append([rid, ts, value, match, 0, 0])
        self._pull_rolls(vehicle, new_speeds)
        # phase B: mutations of pre-existing anchors
        new_temps = []
        for rid, _v, source, ts, value in recs:
            old = existing.get(rid)
            if old is None:
                if source == "temp":
                    new_temps.append((rid, ts, value))
            elif source == "speed":
                self._revise_speed(vehicle, rid, value, events)
            else:
                self._revise_temp(vehicle, rid, value, events)
        self._add_temps(vehicle, new_temps, events)
        # phase C: insert new anchors carrying their pulled window sums
        for rid, ts, value, match, pull_sum, pull_cnt in new_speeds:
            self._add_speed(vehicle, rid, ts, value, match,
                            pull_sum, pull_cnt, events)
        # phase D: propagate all stress deltas
        self._apply_events(vehicle, events)

    def _pull_rolls(self, vehicle: str, new_speeds: List[list]) -> None:
        """One streaming sweep computing, for each new anchor, the sum/count
        of non-null stress of pre-existing anchors in (ts-300s, ts]."""
        if not new_speeds:
            return
        new_speeds.sort(key=lambda item: item[1])
        lo = new_speeds[0][1] - store.ROLL_WINDOW_MS
        hi = new_speeds[-1][1]
        cur = self._conn.execute(
            "SELECT ts, stress FROM anchors"
            " WHERE vehicle=? AND ts>? AND ts<=? AND stress IS NOT NULL"
            " ORDER BY ts",
            (vehicle, lo, hi))
        window: deque = deque()
        ahead: deque = deque()
        run_sum = 0
        exhausted = False
        for item in new_speeds:
            ts = item[1]
            while True:
                if not ahead and not exhausted:
                    rows = cur.fetchmany(4096)
                    self.stats["rows_read"] += len(rows)
                    ahead.extend(rows)
                    exhausted = len(rows) < 4096
                if not ahead or ahead[0][0] > ts:
                    break
                row_ts, row_stress = ahead.popleft()
                window.append((row_ts, row_stress))
                run_sum += row_stress
            limit = ts - store.ROLL_WINDOW_MS
            while window and window[0][0] <= limit:
                run_sum -= window.popleft()[1]
            item[4] = run_sum
            item[5] = len(window)

    def _event(self, events: Dict[int, List[int]], ts: int,
               old_stress: Optional[int], new_stress: Optional[int]) -> None:
        dsum = (new_stress or 0) - (old_stress or 0)
        dcount = (new_stress is not None) - (old_stress is not None)
        if dsum == 0 and dcount == 0:
            return
        slot = events.setdefault(ts, [0, 0])
        slot[0] += dsum
        slot[1] += dcount

    def _add_speed(self, vehicle, rid, ts, value, match,
                   pull_sum, pull_cnt, events) -> None:
        temp_id = temp_ts = temp_val = stress = None
        label = "missing"
        if match is not None:
            temp_id, temp_ts, temp_val, label = match
            stress = store.stress_of(value, temp_val)
        self._conn.execute(
            "INSERT INTO anchors(speed_id, vehicle, ts, speed, temp_id, temp_ts,"
            " temp_val, label, stress, roll_sum, roll_count)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (rid, vehicle, ts, value, temp_id, temp_ts, temp_val, label,
             stress, pull_sum, pull_cnt))
        self.stats["rows_written"] += 1
        self._event(events, ts, None, stress)

    def _revise_speed(self, vehicle, rid, value, events) -> None:
        row = self._conn.execute(
            "SELECT ts, temp_val, stress FROM anchors WHERE speed_id=?",
            (rid,)).fetchone()
        self.stats["rows_read"] += 1
        ts, temp_val, old_stress = row
        stress = store.stress_of(value, temp_val)
        self._conn.execute(
            "UPDATE anchors SET speed=?, stress=? WHERE speed_id=?",
            (value, stress, rid))
        self.stats["rows_written"] += 1
        self._event(events, ts, old_stress, stress)

    def _add_temps(self, vehicle: str, new_temps: List[tuple],
                   events: Dict[int, List[int]]) -> None:
        """Rematch pre-existing anchors after new temps arrive.

        One scan of the union of the temps' +/-30s ranges; per anchor a
        binary search over the new temps. A new temp replaces the current
        match iff it is strictly closer, or equidistant and earlier --
        exactly the from-scratch nearest rule, since the current match is
        the nearest of all old temps."""
        if not new_temps:
            return
        new_temps.sort(key=lambda t: t[1])
        nts = [t[1] for t in new_temps]
        ranges: List[List[int]] = []
        for _, ts, _ in new_temps:
            lo, hi = ts - store.MATCH_WINDOW_MS, ts + store.MATCH_WINDOW_MS
            if ranges and lo <= ranges[-1][1]:
                ranges[-1][1] = max(ranges[-1][1], hi)
            else:
                ranges.append([lo, hi])
        conn = self._conn
        for lo, hi in ranges:
            rows = conn.execute(
                "SELECT speed_id, ts, speed, temp_id, temp_ts, stress"
                " FROM anchors WHERE vehicle=? AND ts>=? AND ts<=?",
                (vehicle, lo, hi)).fetchall()
            self.stats["rows_read"] += len(rows)
            for speed_id, ats, speed, old_tid, old_tts, old_stress in rows:
                i = bisect_right(nts, ats)
                best = None
                if i > 0:
                    best = new_temps[i - 1]
                if i < len(new_temps):
                    cand = new_temps[i]
                    if best is None or cand[1] - ats < ats - best[1]:
                        best = cand
                if best is None:
                    continue
                rid, tts, tval = best
                d_new = abs(ats - tts)
                if old_tid is not None:
                    d_cur = abs(ats - old_tts)
                    if d_new > d_cur or (d_new == d_cur and tts > old_tts):
                        continue
                stress = store.stress_of(speed, tval)
                label = "exact" if tts == ats else "near"
                conn.execute(
                    "UPDATE anchors SET temp_id=?, temp_ts=?, temp_val=?,"
                    " label=?, stress=? WHERE speed_id=?",
                    (rid, tts, tval, label, stress, speed_id))
                self.stats["rows_written"] += 1
                self._event(events, ats, old_stress, stress)

    def _revise_temp(self, vehicle, rid, value, events) -> None:
        rows = self._conn.execute(
            "SELECT speed_id, ts, speed, stress FROM anchors WHERE temp_id=?",
            (rid,)).fetchall()
        self.stats["rows_read"] += len(rows)
        for speed_id, ats, speed, old_stress in rows:
            stress = store.stress_of(speed, value)
            self._conn.execute(
                "UPDATE anchors SET temp_val=?, stress=? WHERE speed_id=?",
                (value, stress, speed_id))
            self.stats["rows_written"] += 1
            self._event(events, ats, old_stress, stress)

    def _apply_events(self, vehicle: str, events: Dict[int, List[int]]) -> None:
        if not events:
            return
        conn = self._conn
        ev_ts = sorted(events)
        ev_sum = [events[t][0] for t in ev_ts]
        ev_cnt = [events[t][1] for t in ev_ts]

        ranges: List[List[int]] = []
        for t in ev_ts:
            lo, hi = t, t + store.ROLL_WINDOW_MS
            if ranges and lo <= ranges[-1][1]:
                ranges[-1][1] = max(ranges[-1][1], hi)
            else:
                ranges.append([lo, hi])

        add_i = sub_i = 0
        cur_sum = cur_cnt = 0
        batch: List[tuple] = []
        for lo, hi in ranges:
            cur = conn.execute(
                "SELECT speed_id, ts, roll_sum, roll_count FROM anchors"
                " WHERE vehicle=? AND ts>=? AND ts<? ORDER BY ts",
                (vehicle, lo, hi))
            while True:
                rows = cur.fetchmany(_UPDATE_BATCH)
                if not rows:
                    break
                self.stats["rows_read"] += len(rows)
                for speed_id, ts, roll_sum, roll_count in rows:
                    while add_i < len(ev_ts) and ev_ts[add_i] <= ts:
                        cur_sum += ev_sum[add_i]
                        cur_cnt += ev_cnt[add_i]
                        add_i += 1
                    limit = ts - store.ROLL_WINDOW_MS
                    while sub_i < len(ev_ts) and ev_ts[sub_i] <= limit:
                        cur_sum -= ev_sum[sub_i]
                        cur_cnt -= ev_cnt[sub_i]
                        sub_i += 1
                    batch.append((roll_sum + cur_sum, roll_count + cur_cnt,
                                  speed_id))
                conn.executemany(
                    "UPDATE anchors SET roll_sum=?, roll_count=?"
                    " WHERE speed_id=?", batch)
                self.stats["rows_written"] += len(batch)
                batch.clear()

    # ----------------------------------------------------------------- query
    def query(self, vehicle: str, start: int, end: int) -> Iterator[dict]:
        cur = self._conn.execute(
            "SELECT speed_id, ts, speed, temp_id, label, stress,"
            " roll_sum, roll_count FROM anchors"
            " WHERE vehicle=? AND ts>=? AND ts<? ORDER BY ts",
            (vehicle, int(start), int(end)))
        for speed_id, ts, speed, temp_id, label, stress, rsum, rcnt in cur:
            yield {
                "speed_id": speed_id,
                "ts": ts,
                "speed": speed,
                "temp_id": temp_id,
                "label": label,
                "stress": stress,
                "rolling": (rsum, rcnt) if rcnt > 0 else None,
            }

    # --------------------------------------------------------------- explain
    def explain(self, vehicle: str, speed_id: str) -> dict:
        row = self._conn.execute(
            "SELECT ts, speed, temp_id, temp_ts, label, stress,"
            " roll_sum, roll_count FROM anchors WHERE speed_id=?",
            (speed_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown speed_id: {speed_id!r}")
        ts, speed, temp_id, temp_ts, label, stress, rsum, rcnt = row
        lo = ts - store.ROLL_WINDOW_MS

        def sources() -> Iterator[dict]:
            cur = self._conn.execute(
                "SELECT speed_id, temp_id FROM anchors"
                " WHERE vehicle=? AND ts>? AND ts<=? AND stress IS NOT NULL"
                " ORDER BY ts",
                (vehicle, lo, ts))
            for sid, tid in cur:
                yield {"speed_id": sid, "temp_id": tid}

        return {
            "version": self.VERSION,
            "method": self.MATCH_METHOD,
            "vehicle": vehicle,
            "speed_id": speed_id,
            "ts": ts,
            "speed": speed,
            "match": None if temp_id is None else {
                "temp_id": temp_id,
                "temp_ts": temp_ts,
                "label": label,
            },
            "stress": stress,
            "rolling": (rsum, rcnt) if rcnt > 0 else None,
            "rolling_sources": sources(),
        }

    # ------------------------------------------------------------------ misc
    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Fusion":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def open_fusion(path: str) -> Fusion:
    return Fusion(path)
