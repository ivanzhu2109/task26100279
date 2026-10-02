"""Memory ceiling and locality-of-work evidence.

- 1M raw records with >=100k matched anchors inside one 300s rolling
  window: peak extra Python memory during ingest and streaming query
  must stay below 64 MiB.
- A fixed local change is replayed against growing amounts of unrelated
  history: the number of records/anchors read or written by ingest must
  not grow with history size.
"""

import os
import sys
import tracemalloc

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fleet_fusion import Fusion

MIB = 1024 * 1024
MEMORY_LIMIT = 64 * MIB
DENSE_T0 = 10**9


def _gen_1m():
    # 100k matched anchors inside one 300s window (3ms spacing)
    for k in range(100_000):
        yield {"id": f"d{k}", "vehicle": "fleet", "source": "speed",
               "ts": DENSE_T0 + 3 * k, "value": 60}
    for k in range(300):
        yield {"id": f"dt{k}", "vehicle": "fleet", "source": "temp",
               "ts": DENSE_T0 + 1000 * k, "value": 200}
    # sparse unrelated records filling the timeline up to the dense window
    n_sparse = 1_000_000 - 100_000 - 300
    step = DENSE_T0 // n_sparse
    for k in range(n_sparse):
        if k % 9 == 0:
            yield {"id": f"s{k}", "vehicle": "fleet", "source": "speed",
                   "ts": k * step, "value": 50}
        else:
            yield {"id": f"t{k}", "vehicle": "fleet", "source": "temp",
                   "ts": k * step, "value": 150 + (k % 80)}


@pytest.mark.slow
def test_memory_1m_records(tmp_path):
    f = Fusion(str(tmp_path / "big.db"))
    tracemalloc.start()
    f.ingest(_gen_1m())
    _, peak_ingest = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"\ningest peak: {peak_ingest / MIB:.1f} MiB")
    assert peak_ingest < MEMORY_LIMIT

    # dense window correctness (checked without tracing: the materialized
    # list would measure the test harness, not the library)
    count = 0
    last = None
    for row in f.query("fleet", DENSE_T0, DENSE_T0 + 300_000):
        count += 1
        last = row
    assert count == 100_000
    assert last is not None
    assert last["rolling"] == {"num": 1200 * 100_000, "den": 100_000}

    # streaming consumption stays flat even for full scans
    tracemalloc.start()
    n = sum(1 for _ in f.query("fleet", 0, 2 * DENSE_T0))
    _, peak_stream = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"query(streaming {n} rows) peak: {peak_stream / MIB:.1f} MiB")
    n_sparse_speeds = (1_000_000 - 100_000 - 300) // 9 + 1  # k % 9 == 0
    assert n == 100_000 + n_sparse_speeds
    assert peak_stream < MEMORY_LIMIT

    info = f.explain("fleet", "d99999")
    assert info is not None
    tracemalloc.start()
    m = sum(1 for _ in info["contributions"])
    _, peak_explain = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"explain(streaming {m} contribs) peak: {peak_explain / MIB:.1f} MiB")
    assert m == 100_000
    assert peak_explain < MEMORY_LIMIT
    f.close()


def _history(n: int, t0: int, t1: int):
    """n records spread over [t0, t1], far from the change point."""
    step = max(1, (t1 - t0) // n)
    for k in range(n):
        ts = t0 + k * step
        if k % 2 == 0:
            yield {"id": f"h{k}", "vehicle": "fleet", "source": "speed",
                   "ts": ts, "value": 40}
        else:
            yield {"id": f"h{k}", "vehicle": "fleet", "source": "temp",
                   "ts": ts, "value": 170}


@pytest.mark.parametrize("n_history", [2_000, 8_000, 32_000])
def test_local_change_work_is_history_independent(n_history, tmp_path):
    change_ts = 10**10
    f = Fusion(str(tmp_path / f"scale{n_history}.db"))
    # unrelated history: half long before, half long after the change point
    f.ingest(_history(n_history // 2, 0, change_ts - 500_000))
    f.ingest(_history(n_history // 2, change_ts + 500_000,
                      change_ts + 500_000 + n_history))
    f.ingest([
        {"id": "anchor-temp", "vehicle": "fleet", "source": "temp",
         "ts": change_ts, "value": 200},
        {"id": "anchor-speed", "vehicle": "fleet", "source": "speed",
         "ts": change_ts + 1000, "value": 10},
    ])
    before = dict(f.counters)
    f.ingest([{"id": "anchor-temp", "vehicle": "fleet", "source": "temp",
               "ts": change_ts, "value": 210}])  # local revision
    delta = {k: f.counters[k] - before[k] for k in f.counters}
    print(f"\nhistory={n_history}: ingest delta counters={delta}")
    f.close()
    assert delta == EXPECTED_LOCAL_DELTA


# The exact counts are deterministic; the point is that they must be
# identical for every history size above.
EXPECTED_LOCAL_DELTA = {
    "records_read": 1,
    "anchors_scanned": 2,
    "anchors_written": 2,
}
