"""Zero-row results are ordinary, queryable tables (issue #161).

A query that found nothing used to return a schema-only stub with no plan:
count(), len() and every further transform on it raised "No data loaded",
and cum_sum() silently returned it without the new column. rvs(), step()
and keyed distinct() raised the same error on any zero-row input. Each of
those exits now returns a zero-row table, and "No data loaded" is left to a
table that was never loaded.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

EVENTS = {
    "userid": [1, 1, 2],
    "eventtime": [1, 2, 3],
    "ev": ["x", "y", "z"],
    "v": [1.0, 2.0, 3.0],
}
COLUMNS = list(EVENTS)


def starts(prefix):
    return lambda r: r.ev.s.starts_with(prefix)


def none_left(t):
    return t.filter(lambda r: r.v < 0)


@pytest.fixture
def events(tmp_path):
    """Pre-sorted Parquet table, the setting of the issue's funnel query."""
    path = tmp_path / "events.parquet"
    pq.write_table(pa.table(EVENTS), path)
    return LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")


# Every way a query can come back empty.
ZERO_ROW_RESULTS = {
    # search_pattern exits: no step-1 match (the issue's repro), fewer rows
    # than steps, and an input that is already empty.
    "search_pattern_no_match": lambda t: t.search_pattern(
        starts("a/"), starts("b/"), partition_by="userid"
    ),
    "search_pattern_short_input": lambda t: t.filter(
        lambda r: r.userid == 2
    ).search_pattern(starts("z"), starts("z")),
    "search_pattern_empty_input": lambda t: none_left(t).search_pattern(starts("x")),
    "search_first_no_match": lambda t: t.search_first(lambda r: r.v > 100.0),
    "filter_to_empty": none_left,
    "collect_of_empty": lambda t: none_left(t).collect(),
    "rvs_of_empty": lambda t: none_left(t).rvs(),
    "step_of_empty": lambda t: none_left(t).step(2),
    "keyed_distinct_of_empty": lambda t: none_left(t).distinct("userid"),
    "delete_last_row": lambda t: t.filter(lambda r: r.userid == 2).delete(0),
}

FOLLOW_UPS = {
    "count": lambda t: t.count(),
    "len": lambda t: len(t),
    "to_dicts": lambda t: len(t.to_dicts()),
    "filter_derive": lambda t: t.filter(lambda r: r.v > 0).derive(w=lambda r: r.v * 2).count(),
    "select": lambda t: t.select("v").count(),
    "sort": lambda t: t.sort("v").count(),
    "slice": lambda t: t.slice(0, 5).count(),
    "collect": lambda t: t.collect().count(),
    "distinct": lambda t: t.distinct().count(),
    "rvs": lambda t: t.rvs().count(),
    "union": lambda t: t.union(t).count(),
}


@pytest.mark.parametrize("produce", ZERO_ROW_RESULTS.values(), ids=ZERO_ROW_RESULTS.keys())
class TestZeroRowResults:
    def test_keeps_schema(self, events, produce):
        result = produce(events)
        assert result.columns == COLUMNS
        assert result.schema == events.schema

    def test_to_pandas_is_empty_with_columns(self, events, produce):
        df = produce(events).to_pandas()
        assert len(df) == 0
        assert list(df.columns) == COLUMNS

    @pytest.mark.parametrize("follow_up", FOLLOW_UPS.values(), ids=FOLLOW_UPS.keys())
    def test_follow_up_sees_zero_rows(self, events, produce, follow_up):
        assert follow_up(produce(events)) == 0


class TestSearchPatternNoMatch:
    def test_issue_repro_counts_zero(self, events):
        r = events.search_pattern(
            lambda r: r.ev.s.starts_with("a/"),
            lambda r: r.ev.s.starts_with("b/"),
            partition_by="userid",
        )
        assert r.count() == 0

    def test_in_memory_table_counts_zero(self):
        t = LTSeq.from_pandas(pd.DataFrame(EVENTS)).sort("userid", "eventtime")
        r = t.search_pattern(starts("x"), starts("q"))
        assert r.count() == 0
        assert r.columns == COLUMNS

    def test_keeps_declared_order_for_window_ops(self, events):
        # The stub used to make cum_sum() return the table without v_cumsum.
        out = events.search_pattern(starts("a/")).cum_sum("v")
        assert out.columns == COLUMNS + ["v_cumsum"]
        assert out.count() == 0

    def test_can_be_searched_again(self, events):
        none = events.search_pattern(starts("a/"))
        assert none.search_pattern(starts("x")).count() == 0
        assert none.search_pattern_count(starts("x")) == 0
        assert none.search_first(lambda r: r.v > 0).count() == 0


def test_group_ordered_count_on_empty_input_is_zero():
    t = none_left(LTSeq.from_pandas(pd.DataFrame(EVENTS)).sort("userid", "eventtime"))
    session_start = lambda r: r.userid != r.userid.shift(1)  # noqa: E731
    # Call the kernel's fast path directly: first().count() falls back to
    # materializing on any error, which hid the "No data loaded" it raised.
    assert t._inner.group_ordered_count(t._capture_expr(session_start)) == 0
    assert t.group_ordered(session_start).first().count() == 0


def test_never_loaded_table_still_raises_no_data():
    with pytest.raises(RuntimeError, match="never loaded from a data source"):
        LTSeq().count()
