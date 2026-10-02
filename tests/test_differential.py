"""Differential test: random out-of-order batches, redeliveries and
revisions are ingested incrementally; after every batch the persisted
derived state must equal an independent full recompute, including after
closing and reopening the SQLite file."""

import os
import random
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fleet_fusion import Fusion
from reference import full_recompute, reference_contributions


def _assert_state_matches(f: Fusion, history: list, vehicles) -> None:
    expected = full_recompute(history)
    for vehicle in vehicles:
        want = expected.get(vehicle, [])
        got = list(f.query(vehicle, 0, 10**15))
        assert got == want, f"vehicle {vehicle} query mismatch"
        for anchor in random.sample(want, min(4, len(want))):
            info = f.explain(vehicle, anchor["speed_id"])
            assert info is not None
            assert info["version"] == "v1"
            assert info["method"] == "nearest_temp_within_30s_tie_earlier"
            assert info["match"] is None or (
                info["match"]["temp_id"] == anchor["temp_id"]
            )
            assert (info["match"] is None) == (anchor["temp_id"] is None)
            got_contribs = list(info["contributions"])
            want_contribs = reference_contributions(want, anchor["speed_id"])
            assert got_contribs == want_contribs


def _scenario(seed: int, n_records: int = 400):
    rng = random.Random(seed)
    vehicles = ["veh-a", "veh-b"]
    # unique (vehicle, source, ts) points
    used = set()
    pool = []
    while len(pool) < n_records:
        vehicle = rng.choice(vehicles)
        source = rng.choice(["speed", "temp"])
        ts = rng.randrange(0, 2_000_000)
        if (vehicle, source, ts) in used:
            continue
        used.add((vehicle, source, ts))
        value = rng.randrange(0, 400) if source == "temp" \
            else rng.randrange(0, 120)
        pool.append({"id": f"r{len(pool)}", "vehicle": vehicle,
                     "source": source, "ts": ts, "value": value})

    batches = []
    delivered = []
    i = 0
    while i < len(pool):
        size = rng.randrange(1, 40)
        batch = pool[i:i + size]
        i += size
        batch = list(batch)
        # random redeliveries (identical) and revisions (new value)
        if delivered and rng.random() < 0.6:
            for _ in range(rng.randrange(1, 4)):
                old = rng.choice(delivered)
                if rng.random() < 0.5:
                    batch.append(dict(old))
                else:
                    rev = dict(old)
                    rev["value"] = rng.randrange(0, 400)
                    batch.append(rev)
        rng.shuffle(batch)
        batches.append(batch)
        delivered.extend(batch)
    return batches, vehicles


@pytest.mark.parametrize("seed", [1, 2, 3, 7, 42])
def test_differential_random_batches(seed, tmp_path):
    batches, vehicles = _scenario(seed)
    path = str(tmp_path / "fusion.db")
    f = Fusion(path)
    history: list = []
    for batch in batches:
        f.ingest(batch)
        history.extend(batch)
        _assert_state_matches(f, history, vehicles)
    f.close()

    # persistence: reopen the same file and verify again
    f2 = Fusion(path)
    _assert_state_matches(f2, history, vehicles)
    f2.close()
