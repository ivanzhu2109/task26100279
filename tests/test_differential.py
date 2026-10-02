"""Differential test: random out-of-order batches, redeliveries, revisions,
verified against the independent full-recompute reference, including after
closing and reopening the SQLite file."""

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import reference

from fleet_fusion import Fusion

VEHICLES = ["car-a", "car-b", "car-c"]


def make_stream(rng, n_records):
    """Unique records; timestamps clustered so matches/windows interact."""
    recs = []
    for i in range(n_records):
        vehicle = rng.choice(VEHICLES)
        source = "speed" if rng.random() < 0.6 else "temp"
        ts = rng.randrange(0, 3_000_000)  # dense: many within 30s/300s
        value = rng.randrange(0, 300)
        recs.append({"id": f"r{i}", "vehicle": vehicle, "source": source,
                     "ts": ts, "value": value})
    return recs


def make_batches(rng, recs):
    """Shuffle, split into random batches, inject redeliveries + revisions."""
    recs = recs[:]
    rng.shuffle(recs)
    batches, cur = [], []
    for rec in recs:
        cur.append(rec)
        if rng.random() < 0.08:
            batches.append(cur)
            cur = []
    if cur:
        batches.append(cur)
    out = []
    for batch in batches:
        extra = []
        for rec in batch:
            if rng.random() < 0.05:  # redeliver same record in same ingest call
                extra.append(dict(rec))
        out.append(batch + extra)
        if rng.random() < 0.35:  # follow-up batch of revisions/redeliveries
            tweaks = []
            for rec in batch:
                roll = rng.random()
                if roll < 0.15:  # revision: same id+fields, new value
                    tweaks.append({**rec, "value": rng.randrange(0, 300)})
                elif roll < 0.3:  # exact redelivery
                    tweaks.append(dict(rec))
            if tweaks:
                out.append(tweaks)
    return out


def check_all(fx, state, rng):
    for vehicle in VEHICLES:
        for _ in range(3):
            start = rng.randrange(0, 3_000_000)
            end = start + rng.randrange(0, 1_500_000)
            got = list(fx.query(vehicle, start, end))
            want = reference.compute_query(state, vehicle, start, end)
            assert got == want, f"query mismatch {vehicle} [{start},{end})"
        speed_ids = [rid for rid, r in state.items()
                     if r[0] == vehicle and r[1] == "speed"]
        for sid in rng.sample(speed_ids, min(5, len(speed_ids))):
            got = fx.explain(vehicle, sid)
            got["rolling_sources"] = list(got["rolling_sources"])
            want = reference.compute_explain(state, vehicle, sid)
            for key, val in want.items():
                assert got[key] == val, f"explain.{key} mismatch for {sid}"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_differential(tmp_path, seed):
    rng = random.Random(seed)
    path = str(tmp_path / "diff.db")
    state = {}
    recs = make_stream(rng, 1500)
    batches = make_batches(rng, recs)

    fx = Fusion(path)
    for i, batch in enumerate(batches):
        fx.ingest(batch)
        reference.apply_records(state, batch)
        if i % 4 == 0 or i == len(batches) - 1:
            check_all(fx, state, rng)
    fx.close()

    # reopen: persisted derived state must still match the reference
    fx = Fusion(path)
    check_all(fx, state, rng)
    # keep mutating after reopen
    for batch in batches[:6]:
        fx.ingest(batch)
        reference.apply_records(state, batch)
    check_all(fx, state, rng)
    fx.close()
