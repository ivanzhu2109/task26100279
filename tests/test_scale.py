"""Scale, memory and locality evidence.

* 1M raw records, >=100k valid anchors inside one 300s window, extra
  working memory during ingest must stay below 64 MiB.
* A fixed local change must touch a number of rows that does not grow
  with unrelated history (instrumented via Fusion.stats).
"""

import resource
import tracemalloc

import pytest

from fleet_fusion import Fusion

MIB = 1024 * 1024


def dense_window_records(n=100_000, ts0=1_000_000, step_ms=3):
    """n temp+speed pairs at the same ms: all anchors exact-matched,
    all inside one 300s window (span = (n-1)*3 ms < 300_000)."""
    for i in range(n):
        ts = ts0 + i * step_ms
        yield {"id": f"d-t{i}", "vehicle": "v", "source": "temp",
               "ts": ts, "value": 200}
        yield {"id": f"d-s{i}", "vehicle": "v", "source": "speed",
               "ts": ts, "value": 10}


def sparse_records(n, ts0=2_000_000, step_ms=2000):
    for i in range(n):
        source = "speed" if i % 2 == 0 else "temp"
        yield {"id": f"s-{i}", "vehicle": "v", "source": source,
               "ts": ts0 + i * step_ms, "value": 10 + (i % 97)}


def feed(fx, records, batch=32_768):
    buf = []
    for rec in records:
        buf.append(rec)
        if len(buf) >= batch:
            fx.ingest(buf)
            buf = []
    if buf:
        fx.ingest(buf)


def _maxrss_mib():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // MIB


@pytest.mark.slow
def test_million_records_memory(tmp_path):
    # pass 1: RSS-based extra working memory (interpreter baseline excluded)
    fx = Fusion(str(tmp_path / "scale-rss.db"))
    rss_before = _maxrss_mib()
    feed(fx, dense_window_records())          # 200k records, 100k anchors
    feed(fx, sparse_records(800_000))         # 800k records, 400k anchors
    rss_after = _maxrss_mib()
    fx.close()
    print(f"\nprocess maxrss: {rss_before} -> {rss_after} MiB "
          f"(extra {rss_after - rss_before} MiB)")
    assert rss_after - rss_before < 64, "RSS working memory exceeds 64 MiB"

    # pass 2: python-level allocation profile on a fresh database
    fx = Fusion(str(tmp_path / "scale.db"))
    tracemalloc.start()
    feed(fx, dense_window_records())
    feed(fx, sparse_records(800_000))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"peak python alloc: {peak // MIB} MiB")
    assert peak < 64 * MIB, f"extra working memory {peak / MIB:.1f} MiB"

    # all 100k dense anchors share one 300s window; the last one's rolling
    # mean aggregates every dense stress (10 * (200-180) = 200 each)
    last_ts = 1_000_000 + 99_999 * 3
    row = list(fx.query("v", last_ts, last_ts + 1))[0]
    assert row["label"] == "exact"
    assert row["stress"] == 200
    assert row["rolling"] == (200 * 100_000, 100_000)

    # streaming consumption of the full result set
    assert sum(1 for _ in fx.query("v", 0, 10**12)) == 500_000
    fx.close()


def test_local_work_is_history_independent(tmp_path, capsys):
    fx = Fusion(str(tmp_path / "loc.db"))
    fx.ingest([
        {"id": "tt", "vehicle": "target", "source": "temp", "ts": 10**12,
         "value": 200},
        {"id": "sa", "vehicle": "target", "source": "speed",
         "ts": 10**12 - 1000, "value": 60},
        {"id": "sb", "vehicle": "target", "source": "speed",
         "ts": 10**12 + 1000, "value": 30},
    ])

    def local_change(value):
        fx.stats["rows_read"] = 0
        fx.stats["rows_written"] = 0
        fx.ingest([{"id": "tt", "vehicle": "target", "source": "temp",
                    "ts": 10**12, "value": value}])
        return fx.stats["rows_read"] + fx.stats["rows_written"]

    def add_unrelated(n, tag):
        def gen():
            for i in range(n):
                yield {"id": f"u{tag}-{i}", "vehicle": f"other-{tag}",
                       "source": "speed" if i % 2 else "temp",
                       "ts": i * 1000, "value": i % 100}
        fx.ingest(gen())
        # far-past anchors of the same vehicle, outside any affected window
        fx.ingest({"id": f"p{tag}-{i}", "vehicle": "target",
                   "source": "speed", "ts": i * 1000, "value": 1}
                  for i in range(n // 10))

    measurements = []
    for n in (0, 50_000, 200_000):
        if n:
            add_unrelated(n, n)
        measurements.append((local_change(250), local_change(200)))

    with capsys.disabled():
        print("\nrows touched per fixed local change vs unrelated history:")
        for n, m in zip((0, 50_000, 200_000), measurements):
            print(f"  history={n:>7}  fwd={m[0]}  revert={m[1]}")

    first = measurements[0]
    for m in measurements[1:]:
        assert m == first, f"work grew with history: {measurements}"
    fx.close()
