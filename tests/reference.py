"""Independent full-recompute reference implementation.

Given the full set of raw records, computes anchors/match/stress/rolling
from scratch with straightforward brute-force-friendly logic. Used by the
differential tests to validate the incremental engine.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import deque
from typing import Any, Iterable, Mapping

MATCH_WINDOW_MS = 30_000
ROLLING_WINDOW_MS = 300_000
TEMP_THRESHOLD_F = 180


def full_recompute(
    records: Iterable[Mapping[str, Any]],
) -> dict[str, list[dict]]:
    latest: dict[Any, Mapping[str, Any]] = {}
    for r in records:
        latest[r["id"]] = r  # redelivery/revision: last write wins on value

    per_vehicle: dict[str, dict[str, list[tuple]]] = {}
    for r in latest.values():
        per_vehicle.setdefault(r["vehicle"], {"speed": [], "temp": []})[
            r["source"]
        ].append((r["ts"], r["id"], r["value"]))

    out: dict[str, list[dict]] = {}
    for vehicle, src in per_vehicle.items():
        speeds = sorted(src["speed"])
        temps = sorted(src["temp"])
        temp_ts = [t[0] for t in temps]

        anchors = []
        for ts, sid, speed in speeds:
            match = _nearest(temp_ts, temps, ts)
            if match is None:
                temp_id, label, stress = None, "missing", None
            else:
                _, temp_id, temp_value = match
                label = "exact" if match[0] == ts else "near"
                stress = speed * max(0, temp_value - TEMP_THRESHOLD_F)
            anchors.append({
                "speed_id": sid,
                "ts": ts,
                "speed": speed,
                "temp_id": temp_id,
                "label": label,
                "stress": stress,
            })

        window: deque = deque()
        total = 0
        count = 0
        for a in anchors:
            limit = a["ts"] - ROLLING_WINDOW_MS
            while window and window[0][0] <= limit:
                _, w_stress = window.popleft()
                if w_stress is not None:
                    total -= w_stress
                    count -= 1
            window.append((a["ts"], a["stress"]))
            if a["stress"] is not None:
                total += a["stress"]
                count += 1
            a["rolling"] = None if count == 0 else {"num": total, "den": count}
        out[vehicle] = anchors
    return out


def _nearest(temp_ts, temps, ts):
    best = None
    i = bisect_right(temp_ts, ts) - 1
    if i >= 0 and ts - temp_ts[i] <= MATCH_WINDOW_MS:
        best = temps[i]
    j = bisect_left(temp_ts, ts + 1)
    if j < len(temps) and temps[j][0] - ts <= MATCH_WINDOW_MS:
        if best is None or temps[j][0] - ts < ts - best[0]:
            best = temps[j]
    return best


def reference_contributions(anchors: list[dict], speed_id) -> list[dict]:
    target = next(a for a in anchors if a["speed_id"] == speed_id)
    lo = target["ts"] - ROLLING_WINDOW_MS
    return [
        {"speed_id": a["speed_id"], "temp_id": a["temp_id"]}
        for a in anchors
        if lo < a["ts"] <= target["ts"] and a["stress"] is not None
    ]
