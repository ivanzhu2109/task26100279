"""Hand-computed boundary cases: ties, 30s match window, 300s rolling
window open/close edges, late temp reselection, revisions, lineage."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fleet_fusion import Fusion
from reference import full_recompute, reference_contributions


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "fusion.db")


def rec(rid, source, ts, value, vehicle="v"):
    return {"id": rid, "vehicle": vehicle, "source": source,
            "ts": ts, "value": value}


def test_equidistant_tie_picks_earlier(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t-early", "temp", 5_000, 200),
        rec("t-late", "temp", 15_000, 200),
        rec("s1", "speed", 10_000, 10),
    ])
    rows = list(f.query("v", 0, 100_000))
    assert len(rows) == 1
    assert rows[0]["temp_id"] == "t-early"
    assert rows[0]["label"] == "near"
    assert rows[0]["stress"] == 10 * 20
    f.close()


def test_exact_match_label(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t1", "temp", 10_000, 200),
        rec("s1", "speed", 10_000, 10),
    ])
    row = list(f.query("v", 0, 100_000))[0]
    assert row["label"] == "exact"
    f.close()


def test_match_window_30s_inclusive(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t-before", "temp", 70_000, 200),   # delta exactly 30_000
        rec("s-in", "speed", 100_000, 10),
        rec("s-out", "speed", 200_000, 10),
        rec("t-far", "temp", 230_001, 200),     # delta 30_001 -> no match
    ])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 1_000_000)}
    assert rows["s-in"]["temp_id"] == "t-before"
    assert rows["s-in"]["label"] == "near"
    assert rows["s-out"]["temp_id"] is None
    assert rows["s-out"]["label"] == "missing"
    assert rows["s-out"]["stress"] is None
    f.close()


def test_rolling_window_open_closed_edges(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t0", "temp", 1_000_000, 190),
        rec("s0", "speed", 1_000_000, 10),       # stress 100
        rec("t1", "temp", 1_300_000, 200),
        rec("s1", "speed", 1_300_000, 10),       # stress 200, exactly +300s
        rec("t2", "temp", 1_300_001, 210),
        rec("s2", "speed", 1_300_001, 10),       # stress 300
    ])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 2_000_000)}
    assert rows["s0"]["rolling"] == {"num": 100, "den": 1}
    # left-open: s0 at exactly t-300_000 is excluded
    assert rows["s1"]["rolling"] == {"num": 200, "den": 1}
    # right-closed: s1 and s2 included, s0 still excluded
    assert rows["s2"]["rolling"] == {"num": 500, "den": 2}
    f.close()


def test_unmatched_anchor_still_has_rolling(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t1", "temp", 1_000, 200),
        rec("s1", "speed", 1_000, 10),           # stress 200
        rec("s2", "speed", 50_000, 10),          # no temp within 30s
    ])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 100_000)}
    assert rows["s2"]["stress"] is None
    assert rows["s2"]["rolling"] == {"num": 200, "den": 1}
    f.close()


def test_late_temp_reselects_and_revises_rolling(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t1", "temp", 0, 200),
        rec("s1", "speed", 10_000, 10),
        rec("s2", "speed", 40_000, 10),
    ])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 100_000)}
    assert rows["s1"]["stress"] == 200
    assert rows["s2"]["label"] == "missing"
    assert rows["s2"]["rolling"] == {"num": 200, "den": 1}

    # Late temp: s2 re-matches, its stress and rolling change.
    f.ingest([rec("t2", "temp", 45_000, 300)])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 100_000)}
    assert rows["s2"]["temp_id"] == "t2"
    assert rows["s2"]["stress"] == 10 * 120
    assert rows["s2"]["rolling"] == {"num": 200 + 1200, "den": 2}
    assert rows["s1"]["temp_id"] == "t1"  # t2 is 35s away from s1

    # Revision of the old temp value propagates into later rolling.
    f.ingest([rec("t1", "temp", 0, 210)])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 100_000)}
    assert rows["s1"]["stress"] == 300
    assert rows["s1"]["rolling"] == {"num": 300, "den": 1}
    assert rows["s2"]["rolling"] == {"num": 300 + 1200, "den": 2}

    # Revision of a speed value likewise.
    f.ingest([rec("s1", "speed", 10_000, 20)])
    rows = {r["speed_id"]: r for r in f.query("v", 0, 100_000)}
    assert rows["s1"]["stress"] == 600
    assert rows["s2"]["rolling"] == {"num": 600 + 1200, "den": 2}
    f.close()


def test_explain_lineage(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("t1", "temp", 0, 200),
        rec("s1", "speed", 10_000, 10),
        rec("s2", "speed", 40_000, 10),
        rec("t2", "temp", 45_000, 300),
        rec("s3", "speed", 500_000, 10),  # outside s2's 300s window? no: 460s
    ])
    info = f.explain("v", "s2")
    assert info["version"] == "v1"
    assert info["method"]
    assert info["match"]["temp_id"] == "t2"
    contribs = list(info["contributions"])
    assert contribs == [
        {"speed_id": "s1", "temp_id": "t1"},
        {"speed_id": "s2", "temp_id": "t2"},
    ]
    assert f.explain("v", "nope") is None
    f.close()


def test_redelivery_is_idempotent(db_path):
    f = Fusion(db_path)
    batch = [
        rec("t1", "temp", 0, 200),
        rec("s1", "speed", 10_000, 10),
        rec("s2", "speed", 40_000, 10),
    ]
    f.ingest(batch)
    first = list(f.query("v", 0, 100_000))
    f.ingest(batch)
    f.ingest(batch[:1])
    second = list(f.query("v", 0, 100_000))
    assert first == second
    f.close()


def test_query_range_left_closed_right_open(db_path):
    f = Fusion(db_path)
    f.ingest([
        rec("s1", "speed", 1_000, 10),
        rec("s2", "speed", 2_000, 10),
        rec("s3", "speed", 3_000, 10),
    ])
    rows = list(f.query("v", 2_000, 3_000))
    assert [r["speed_id"] for r in rows] == ["s2"]
    f.close()
