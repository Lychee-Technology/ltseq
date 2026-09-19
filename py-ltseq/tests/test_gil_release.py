"""GIL release around heavy execution paths (issue #142).

Every Rust entry point that performs real execution or I/O (collect,
execute_stream, file scans, rayon Parquet scans, Arrow IPC encoding) must
release the GIL for the duration of that work, so other Python threads
(web workers, Jupyter background threads, progress bars) keep running.

The detector is a heartbeat thread that ticks every ~1 ms. While the main
thread is inside a call that holds the GIL for its whole duration, the
heartbeat cannot acquire the interpreter and makes at most one or two ticks
(at the call boundaries). Once the GIL is released around execution, it makes
hundreds of ticks per second. The assertion uses that order-of-magnitude gap
rather than tight timing, so it is robust on slow or loaded CI machines.

The workloads are built on a lazy Parquet scan (or a sort over it), so each
measured call decodes or sorts ~1.5M rows: a MemTable-backed `collect` only
clones Arc'd batches and finishes in a couple of milliseconds, which is too
short to observe anything. Loaders (`read_csv`, `read_parquet`) only read
metadata / the first rows for schema inference and are equally unmeasurable;
they are wrapped the same way in Rust but have no test here.
"""

import os
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

N_ROWS = 1_500_000
N_USERS = 15_000

# A call shorter than this cannot be assessed (the heartbeat would not get a
# chance to tick even with the GIL released). Failing loudly here means the
# workload must be made heavier, not that the GIL was held.
MIN_ELAPSED_S = 0.03
# With the GIL released a 30 ms call yields ~25 ticks; held, at most ~2.
MIN_TICKS = 5


def _arrow_events() -> pa.Table:
    """Session-like events sorted by (userid, eventtime), ~1.5M rows."""
    import numpy as np

    rng = np.random.default_rng(142)
    userid = np.sort(rng.integers(0, N_USERS, size=N_ROWS, dtype=np.int64))
    eventtime = np.arange(N_ROWS, dtype=np.int64) * 7
    value = rng.random(N_ROWS)
    pages = np.array(["landing/x", "product/y", "checkout/z", "other/w"])
    url = pages[rng.integers(0, 4, size=N_ROWS)]
    return pa.table(
        {
            "userid": userid,
            "eventtime": eventtime,
            "value": value,
            "url": pa.array(url, type=pa.string()),
        }
    )


@pytest.fixture(scope="module")
def parquet_path(tmp_path_factory) -> str:
    path = str(tmp_path_factory.mktemp("gil") / "events.parquet")
    pq.write_table(_arrow_events(), path, row_group_size=200_000)
    return path


@pytest.fixture
def events(parquet_path) -> LTSeq:
    """Lazy Parquet-backed table: every terminal call re-decodes the file."""
    return LTSeq.read_parquet(parquet_path).assume_sorted("userid", "eventtime")


@pytest.fixture(scope="module")
def events_mem(parquet_path) -> LTSeq:
    """In-memory table: exercises the DataFusion (non-Parquet) fallbacks."""
    return LTSeq.from_arrow(pq.read_table(parquet_path)).assume_sorted(
        "userid", "eventtime"
    )


class Heartbeat:
    """Counts how many times a background Python thread got to run."""

    def __init__(self) -> None:
        self.ticks = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.ticks += 1
            time.sleep(0.001)

    def __enter__(self) -> "Heartbeat":
        self._thread.start()
        # Let the thread reach its loop before measuring.
        time.sleep(0.01)
        self.ticks = 0
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()


def assert_releases_gil(fn):
    """Run ``fn`` on the main thread and assert a heartbeat thread progressed."""
    with Heartbeat() as hb:
        t0 = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - t0
        ticks = hb.ticks
    assert elapsed >= MIN_ELAPSED_S, (
        f"call finished in {elapsed * 1000:.0f} ms, too fast to observe the GIL; "
        "make the workload heavier"
    )
    assert ticks >= MIN_TICKS, (
        f"heartbeat made only {ticks} ticks during a {elapsed * 1000:.0f} ms call: "
        "the GIL was held for the whole execution"
    )
    return result


# ---------------------------------------------------------------------------
# Terminal / export paths
# ---------------------------------------------------------------------------


def test_count_releases_gil(events):
    # Pre-existing control: count() already used py.detach before #142.
    n = assert_releases_gil(events.filter(lambda r: r.url.s.starts_with("landing/")).count)
    assert 0 < n < N_ROWS


def test_materialize_releases_gil(events):
    t = assert_releases_gil(events.sort("value").collect)
    assert t.count() == N_ROWS


def test_to_arrow_releases_gil(events):
    tbl = assert_releases_gil(events.sort("value").to_arrow)
    assert tbl.num_rows == N_ROWS


def test_show_releases_gil(events):
    assert_releases_gil(lambda: events.sort("value").show(5))


def test_write_parquet_releases_gil(events, tmp_path):
    out = str(tmp_path / "out.parquet")
    assert_releases_gil(lambda: events.write_parquet(out))
    assert pq.read_metadata(out).num_rows == N_ROWS


def test_write_csv_releases_gil(events, tmp_path):
    out = str(tmp_path / "out.csv")
    assert_releases_gil(lambda: events.write_csv(out))
    assert os.path.getsize(out) > 0


# ---------------------------------------------------------------------------
# Snapshot-based sequence ops (rvs / step / keyed distinct) and set predicates
# ---------------------------------------------------------------------------


def test_rvs_releases_gil(events):
    # The snapshot (collect + concat) happens inside rvs() itself; the count
    # afterwards is a statistics lookup on the snapshot and proves nothing.
    reversed_t = assert_releases_gil(events.rvs)
    assert reversed_t.count() == N_ROWS


def test_keyed_distinct_releases_gil(events):
    distinct_t = assert_releases_gil(lambda: events.distinct("userid"))
    assert distinct_t.count() == N_USERS


def test_is_subset_releases_gil(events):
    smaller = events.filter(lambda r: r.value > 0.9)
    assert assert_releases_gil(lambda: smaller.is_subset(events)) is True


# ---------------------------------------------------------------------------
# Specialized sequence paths (pattern match, group_ordered count, asof, pivot)
# ---------------------------------------------------------------------------


def _funnel(t: LTSeq) -> int:
    return t.search_pattern_count(
        lambda r: r.url.s.starts_with("landing/"),
        lambda r: r.url.s.starts_with("product/"),
        lambda r: r.url.s.starts_with("checkout/"),
        partition_by="userid",
    )


def test_search_pattern_count_parquet_parallel_releases_gil(events):
    assert assert_releases_gil(lambda: _funnel(events)) > 0


def test_search_pattern_count_datafusion_fallback_releases_gil(events_mem):
    assert assert_releases_gil(lambda: _funnel(events_mem)) > 0


def test_search_pattern_releases_gil(events_mem):
    matches = assert_releases_gil(
        lambda: events_mem.search_pattern(
            lambda r: r.url.s.starts_with("landing/"),
            lambda r: r.url.s.starts_with("product/"),
            partition_by="userid",
        )
    )
    assert matches.count() > 0


def test_group_ordered_count_releases_gil(events_mem):
    groups = events_mem.group_ordered(lambda r: r.userid != r.userid.shift(1))
    assert assert_releases_gil(groups.first().count) == N_USERS


def test_asof_join_releases_gil(events):
    left = events.select("eventtime", "value").sort("eventtime")
    right = events.select("eventtime", "url").sort("eventtime")
    result = assert_releases_gil(
        lambda: left.asof_join(right, on=lambda l, r: l.eventtime >= r.eventtime)
    )
    assert result.count() == N_ROWS


def test_pivot_releases_gil(events_mem):
    # events_mem: pivot() does not accept the Utf8View strings a Parquet scan
    # yields (pre-existing, unrelated to the GIL); the two collects inside
    # pivot() do real aggregation work on the in-memory table anyway.
    result = assert_releases_gil(
        lambda: events_mem.pivot(index="userid", columns="url", values="value", agg_fn="sum")
    )
    assert result.count() == N_USERS


# ---------------------------------------------------------------------------
# Mutation APIs (collect + splice)
# ---------------------------------------------------------------------------


@pytest.fixture
def numeric_events(events) -> LTSeq:
    # Still a lazy Parquet scan (collect decodes the file), minus the string
    # column: insert() cannot build a row for the Utf8View strings a Parquet
    # scan yields (pre-existing, unrelated to the GIL).
    return events.select("userid", "eventtime", "value")


def test_insert_row_releases_gil(numeric_events):
    row = {"userid": -1, "eventtime": -1, "value": 0.0}
    t = assert_releases_gil(lambda: numeric_events.insert(0, row))
    assert t.count() == N_ROWS + 1


def test_delete_row_releases_gil(numeric_events):
    t = assert_releases_gil(lambda: numeric_events.delete(0))
    assert t.count() == N_ROWS - 1


def test_modify_row_releases_gil(numeric_events):
    t = assert_releases_gil(lambda: numeric_events.modify(0, value=42.0))
    assert t.count() == N_ROWS


# ---------------------------------------------------------------------------
# Streaming cursor
# ---------------------------------------------------------------------------


def test_cursor_shared_between_threads_does_not_deadlock(parquet_path):
    """Two threads draining one cursor must serialize, not deadlock.

    Releasing the GIL while holding the cursor's stream mutex would let a
    second thread block on the mutex *with* the GIL, so the first thread
    could never re-acquire it. The lock is taken inside the detached
    section for exactly this reason. (A per-batch heartbeat measurement is
    not robust: batches are short and Python runs between them anyway.)
    """
    import pyarrow.ipc as ipc

    cursor = LTSeq.scan_parquet(parquet_path)
    totals = [0, 0]
    errors: list[BaseException] = []

    def drain(slot: int) -> None:
        try:
            while True:
                batch_bytes = cursor._inner.next_batch()
                if batch_bytes is None:
                    return
                totals[slot] += ipc.open_stream(batch_bytes).read_next_batch().num_rows
        except BaseException as e:  # pragma: no cover - surfaced below
            errors.append(e)

    threads = [threading.Thread(target=drain, args=(i,)) for i in range(2)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=120)
    assert not any(th.is_alive() for th in threads), "cursor threads deadlocked"
    assert not errors, errors
    assert sum(totals) == N_ROWS
    assert cursor.exhausted
