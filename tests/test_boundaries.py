"""Hand-computed boundary and mutation scenarios."""

import pytest

from fleet_fusion import Fusion


@pytest.fixture()
def fx(tmp_path):
    f = Fusion(str(tmp_path / "t.db"))
    yield f
    f.close()


def one(fx, vehicle, ts):
    rows = list(fx.query(vehicle, ts, ts + 1))
    assert len(rows) == 1
    return rows[0]


def test_tie_prefers_earlier_temp(fx):
    fx.ingest([
        {"id": "ta", "vehicle": "v", "source": "temp", "ts": 70_000, "value": 200},
        {"id": "tb", "vehicle": "v", "source": "temp", "ts": 130_000, "value": 190},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 100_000, "value": 60},
    ])
    row = one(fx, "v", 100_000)
    assert row["temp_id"] == "ta"          # equidistant -> earlier wins
    assert row["label"] == "near"
    assert row["stress"] == 60 * 20


def test_exact_label(fx):
    fx.ingest([
        {"id": "t1", "vehicle": "v", "source": "temp", "ts": 50_000, "value": 181},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 50_000, "value": 10},
    ])
    row = one(fx, "v", 50_000)
    assert row["label"] == "exact"
    assert row["stress"] == 10 * 1


def test_30s_match_window_boundary(fx):
    fx.ingest([
        {"id": "t_in", "vehicle": "v", "source": "temp", "ts": 70_000, "value": 200},
        {"id": "t_out", "vehicle": "v", "source": "temp", "ts": 169_999, "value": 300},
        {"id": "s_in", "vehicle": "v", "source": "speed", "ts": 100_000, "value": 10},
        {"id": "s_out", "vehicle": "v", "source": "speed", "ts": 200_000, "value": 10},
    ])
    assert one(fx, "v", 100_000)["temp_id"] == "t_in"     # exactly 30_000 ms
    row = one(fx, "v", 200_000)
    assert row["temp_id"] is None and row["label"] == "missing"  # 30_001 ms
    assert row["stress"] is None


def test_300s_rolling_window_open_closed(fx):
    fx.ingest([
        {"id": "t0", "vehicle": "v", "source": "temp", "ts": 0, "value": 200},
        {"id": "s0", "vehicle": "v", "source": "speed", "ts": 0, "value": 10},
        {"id": "t1", "vehicle": "v", "source": "temp", "ts": 1_000_000, "value": 200},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 1_000_000, "value": 1},
        {"id": "t2", "vehicle": "v", "source": "temp", "ts": 1_299_999, "value": 200},
        {"id": "s2", "vehicle": "v", "source": "speed", "ts": 1_299_999, "value": 2},
        {"id": "t3", "vehicle": "v", "source": "temp", "ts": 1_300_000, "value": 200},
        {"id": "s3", "vehicle": "v", "source": "speed", "ts": 1_300_000, "value": 4},
    ])
    # s2 window (999_999, 1_299_999] includes s1 -> 20 + 40
    assert one(fx, "v", 1_299_999)["rolling"] == (60, 2)
    # s3 window (1_000_000, 1_300_000] is left-open: s1@1_000_000 excluded
    assert one(fx, "v", 1_300_000)["rolling"] == (120, 2)
    # s0 far away from everything
    assert one(fx, "v", 0)["rolling"] == (200, 1)


def test_unmatched_anchor_still_has_rolling(fx):
    fx.ingest([
        {"id": "t1", "vehicle": "v", "source": "temp", "ts": 100_000, "value": 200},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 100_000, "value": 5},
        {"id": "s2", "vehicle": "v", "source": "speed", "ts": 200_000, "value": 7},
    ])
    row = one(fx, "v", 200_000)
    assert row["label"] == "missing" and row["stress"] is None
    assert row["rolling"] == (100, 1)  # s1's stress still contributes


def test_late_temp_rematches_and_shifts_rolling(fx):
    fx.ingest([
        {"id": "t1", "vehicle": "v", "source": "temp", "ts": 120_000, "value": 200},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 100_000, "value": 60},
        {"id": "s2", "vehicle": "v", "source": "speed", "ts": 150_000, "value": 30},
    ])
    assert one(fx, "v", 100_000)["stress"] == 60 * 20
    assert one(fx, "v", 150_000)["rolling"] == (1200 + 600, 2)

    fx.ingest([{"id": "t2", "vehicle": "v", "source": "temp",
                "ts": 105_000, "value": 250}])
    r1 = one(fx, "v", 100_000)
    assert r1["temp_id"] == "t2" and r1["stress"] == 60 * 70
    r2 = one(fx, "v", 150_000)
    assert r2["temp_id"] == "t1"                       # t2 is 45s away
    assert r2["rolling"] == (4200 + 600, 2)            # s1's new stress

    e = fx.explain("v", "s2")
    assert e["version"] == "v1"
    assert e["method"] == Fusion.MATCH_METHOD
    assert e["rolling"] == (4800, 2)
    assert list(e["rolling_sources"]) == [
        {"speed_id": "s1", "temp_id": "t2"},
        {"speed_id": "s2", "temp_id": "t1"},
    ]


def test_revision_updates_stress_and_rolling(fx):
    fx.ingest([
        {"id": "t1", "vehicle": "v", "source": "temp", "ts": 100_000, "value": 250},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 100_000, "value": 60},
        {"id": "t2", "vehicle": "v", "source": "temp", "ts": 150_000, "value": 250},
        {"id": "s2", "vehicle": "v", "source": "speed", "ts": 150_000, "value": 30},
    ])
    assert one(fx, "v", 150_000)["rolling"] == (4200 + 2100, 2)

    fx.ingest([{"id": "t1", "vehicle": "v", "source": "temp",
                "ts": 100_000, "value": 190}])  # revise temp value
    assert one(fx, "v", 100_000)["stress"] == 60 * 10
    assert one(fx, "v", 150_000)["rolling"] == (600 + 2100, 2)

    fx.ingest([{"id": "s1", "vehicle": "v", "source": "speed",
                "ts": 100_000, "value": 100}])  # revise speed value
    assert one(fx, "v", 100_000)["stress"] == 100 * 10
    assert one(fx, "v", 150_000)["rolling"] == (1000 + 2100, 2)


def test_redelivery_is_idempotent(fx):
    batch = [
        {"id": "t1", "vehicle": "v", "source": "temp", "ts": 100_000, "value": 200},
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 100_000, "value": 60},
        {"id": "s2", "vehicle": "v", "source": "speed", "ts": 150_000, "value": 30},
    ]
    fx.ingest(batch)
    before = list(fx.query("v", 0, 10**9))
    fx.ingest(batch)
    fx.ingest(batch[:1])
    assert list(fx.query("v", 0, 10**9)) == before


def test_query_range_is_left_closed_right_open(fx):
    fx.ingest([
        {"id": "s1", "vehicle": "v", "source": "speed", "ts": 10, "value": 1},
        {"id": "s2", "vehicle": "v", "source": "speed", "ts": 20, "value": 1},
        {"id": "s3", "vehicle": "v", "source": "speed", "ts": 30, "value": 1},
    ])
    assert [r["speed_id"] for r in fx.query("v", 10, 30)] == ["s1", "s2"]


def test_explain_unknown_anchor_raises(fx):
    with pytest.raises(KeyError):
        fx.explain("v", "nope")


def test_empty_ingest_and_attr_records(fx):
    fx.ingest([])
    fx.ingest(iter(()))

    class Rec:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    fx.ingest([Rec(id="t1", vehicle="v", source="temp", ts=5, value=200),
               Rec(id="s1", vehicle="v", source="speed", ts=5, value=3)])
    assert one(fx, "v", 5)["stress"] == 60


def test_vehicles_are_isolated(fx):
    fx.ingest([
        {"id": "t1", "vehicle": "a", "source": "temp", "ts": 0, "value": 200},
        {"id": "s1", "vehicle": "a", "source": "speed", "ts": 0, "value": 1},
        {"id": "s2", "vehicle": "b", "source": "speed", "ts": 0, "value": 1},
    ])
    assert one(fx, "a", 0)["label"] == "exact"
    assert one(fx, "b", 0)["label"] == "missing"
