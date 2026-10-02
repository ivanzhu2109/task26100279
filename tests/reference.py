"""Naive from-scratch reference: apply all raw records, recompute everything.
Independent of fleet_fusion internals; used as ground truth for diff tests."""

from bisect import bisect_left, bisect_right

MATCH_WINDOW_MS = 30_000
ROLL_WINDOW_MS = 300_000


def apply_records(state: dict, records) -> None:
    """state: id -> [vehicle, source, ts, value]; revisions overwrite value."""
    for rec in records:
        get = rec.get if isinstance(rec, dict) else None
        rid, vehicle, source, ts, value = (
            rec["id"], rec["vehicle"], rec["source"], rec["ts"], rec["value"])
        if rid in state:
            state[rid][3] = value  # revision: only the value changes
        else:
            state[rid] = [vehicle, source, ts, value]


def compute_query(state: dict, vehicle: str, start: int, end: int):
    speeds = []  # (ts, id, value)
    temps = []   # (ts, id, value)
    for rid, (v, source, ts, value) in state.items():
        if v != vehicle:
            continue
        (speeds if source == "speed" else temps).append((ts, rid, value))
    speeds.sort()
    temps.sort()
    temp_ts = [t[0] for t in temps]

    # stress per anchor
    anchors = []
    for ts, sid, speed in speeds:
        match = _nearest(temps, temp_ts, ts)
        temp_id = stress = None
        label = "missing"
        if match is not None:
            temp_id, _tts, tval, label = match
            stress = speed * max(0, tval - 180)
        anchors.append([ts, sid, speed, temp_id, label, stress])

    # rolling via prefix sums over non-null stress
    times = [a[0] for a in anchors]
    pref_sum = [0] * (len(anchors) + 1)
    pref_cnt = [0] * (len(anchors) + 1)
    for i, a in enumerate(anchors):
        pref_sum[i + 1] = pref_sum[i] + (a[5] or 0)
        pref_cnt[i + 1] = pref_cnt[i] + (a[5] is not None)

    out = []
    for i, (ts, sid, speed, temp_id, label, stress) in enumerate(anchors):
        if not (start <= ts < end):
            continue
        lo = bisect_right(times, ts - ROLL_WINDOW_MS)   # left-open
        hi = bisect_right(times, ts)                    # right-closed
        rsum = pref_sum[hi] - pref_sum[lo]
        rcnt = pref_cnt[hi] - pref_cnt[lo]
        out.append({
            "speed_id": sid,
            "ts": ts,
            "speed": speed,
            "temp_id": temp_id,
            "label": label,
            "stress": stress,
            "rolling": (rsum, rcnt) if rcnt > 0 else None,
        })
    return out


def compute_explain(state: dict, vehicle: str, speed_id: str):
    row = state[speed_id]
    ts = row[2]
    full = compute_query(state, vehicle, ts, ts + 1)
    assert len(full) == 1 and full[0]["speed_id"] == speed_id
    me = full[0]
    # rolling sources: anchors in (ts-300s, ts] with non-null stress
    wider = compute_query(state, vehicle, ts - ROLL_WINDOW_MS, ts + 1)
    sources = [{"speed_id": a["speed_id"], "temp_id": a["temp_id"]}
               for a in wider if a["stress"] is not None]
    return {
        "version": "v1",
        "ts": ts,
        "speed": me["speed"],
        "match": None if me["temp_id"] is None else {
            "temp_id": me["temp_id"],
            "temp_ts": state[me["temp_id"]][2],
            "label": me["label"],
        },
        "stress": me["stress"],
        "rolling": me["rolling"],
        "rolling_sources": sources,
    }


def _nearest(temps, temp_ts, ts):
    if not temps:
        return None
    i = bisect_right(temp_ts, ts)
    best = None
    if i > 0 and ts - temp_ts[i - 1] <= MATCH_WINDOW_MS:
        best = temps[i - 1]
    if i < len(temps) and temp_ts[i] - ts <= MATCH_WINDOW_MS:
        if best is None or temp_ts[i] - ts < ts - best[0]:
            best = temps[i]
    if best is None:
        return None
    label = "exact" if best[0] == ts else "near"
    return (best[1], best[0], best[2], label)
