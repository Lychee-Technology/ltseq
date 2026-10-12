"""search_pattern step predicates under NULLs, against filter (#220).

`search_pattern` and `search_pattern_count` evaluate step predicates with
their own evaluator (`src/ops/pattern_match.rs`); on sorted Parquet with
`partition_by`, the count runs it per row group in the parallel scan and
again on the rows around each row-group seam. A step matches a row where its
predicate is TRUE, so a step selects the rows `filter` selects, which follows
SQL's three-valued logic: `NULL | TRUE` is TRUE and `~(NULL & FALSE)` is
TRUE. The evaluator used to give NULL whenever either side of `&` or `|` was
NULL, which dropped those rows.
"""

from __future__ import annotations

import itertools

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq

# x > 100 and y == 1 each take every truth value: TRUE, FALSE and NULL.
X_VALUES = [200, 1, None]
Y_VALUES = [1, 0, None]
COMBOS = list(itertools.product(X_VALUES, Y_VALUES))
# Every ordered pair of combos, once each, as two adjacent rows, so that a
# two-step pattern meets every pair of step results.
PAIRS = [row for pair in itertools.product(COMBOS, COMBOS) for row in pair]
# Partitions of three rows cut every third pair. With two-row groups, some
# partition boundaries fall on a row-group seam and some inside a group.
G = [i // 3 for i in range(len(PAIRS))]

# Named for the expression over a = (x > 100) and b = (y == 1);
# "negated_e" is ~(e).
PREDICATES = {
    "a_or_b": lambda r: (r.x > 100) | (r.y == 1),
    "a_and_b": lambda r: (r.x > 100) & (r.y == 1),
    "negated_a_and_b": lambda r: ~((r.x > 100) & (r.y == 1)),
    "negated_a_or_b": lambda r: ~((r.x > 100) | (r.y == 1)),
    "not_a_or_b": lambda r: ~(r.x > 100) | (r.y == 1),
    "a_or_b_and_not_a": lambda r: ((r.x > 100) | (r.y == 1)) & ~(r.x > 100),
    "b_or_none": lambda r: (r.y == 1) | None,
    "negated_a_and_none": lambda r: ~((r.x > 100) & None),
    # Tests of a combination's value. NULL & FALSE used to be NULL, which
    # is_null() matched and != True did not.
    "a_and_b_is_null": lambda r: ((r.x > 100) & (r.y == 1)).is_null(),
    "a_and_b_ne_true": lambda r: ((r.x > 100) & (r.y == 1)) != True,  # noqa: E712
}


def _pairs() -> pa.Table:
    return pa.table(
        {
            "i": pa.array(range(len(PAIRS)), pa.int64()),
            "g": pa.array(G, pa.int64()),
            "x": pa.array([x for x, _ in PAIRS], pa.int64()),
            "y": pa.array([y for _, y in PAIRS], pa.int64()),
        }
    )


def _table(rows: pa.Table, layout: str, tmp_path) -> LTSeq:
    """``rows`` in memory, or as a sorted Parquet file of ``layout``'s row
    groups. The Parquet table keeps its source path, so a count with
    ``partition_by`` takes the parallel scan."""
    if layout == "memory":
        return LTSeq.from_arrow(rows).sort("i")
    rows_per_group = int(layout.removeprefix("parquet_rg"))
    path = str(tmp_path / "rows.parquet")
    pq.write_table(rows, path, row_group_size=rows_per_group)
    return LTSeq.read_parquet(path).assume_sorted("i")


def _ids(table: LTSeq) -> list[int]:
    return table.to_arrow().column("i").to_pylist()


def _reference(t: LTSeq, steps, partition_by: str | None) -> list[int]:
    """Start rows *i* such that `filter(steps[k])` selects row *i + k* for
    every *k*, all in one partition."""
    selected = [set(_ids(t.filter(step))) for step in steps]
    last = len(steps) - 1
    return [
        i
        for i in range(len(PAIRS) - last)
        if all(i + k in rows for k, rows in enumerate(selected))
        and (partition_by is None or G[i] == G[i + last])
    ]


# Two-row groups hold one pair of PAIRS each: a two-step match that starts
# on a pair's first row stays in its row group, and one that starts on the
# second row crosses a seam. One-row groups put every step on a seam.
LAYOUTS = ["memory", "parquet_rg1", "parquet_rg2"]


@pytest.mark.parametrize("partition_by", [None, "g"])
@pytest.mark.parametrize("name", PREDICATES)
@pytest.mark.parametrize("layout", LAYOUTS)
def test_one_step_selects_filter_rows(tmp_path, layout, name, partition_by):
    t = _table(_pairs(), layout, tmp_path)
    pred = PREDICATES[name]
    expected = _reference(t, [pred], partition_by)
    assert _ids(t.search_pattern(pred, partition_by=partition_by)) == expected
    assert t.search_pattern_count(pred, partition_by=partition_by) == len(expected)


@pytest.mark.parametrize("partition_by", [None, "g"])
@pytest.mark.parametrize("name", PREDICATES)
@pytest.mark.parametrize("layout", LAYOUTS)
def test_two_steps_select_filter_rows(tmp_path, layout, name, partition_by):
    t = _table(_pairs(), layout, tmp_path)
    steps = [PREDICATES[name]] * 2
    expected = _reference(t, steps, partition_by)
    assert _ids(t.search_pattern(*steps, partition_by=partition_by)) == expected
    assert t.search_pattern_count(*steps, partition_by=partition_by) == len(expected)


# The table of #220, with a single partition `g`. The expected rows are
# worked out by hand, so these tests do not rest on `filter` being right.
ISSUE_ROWS = pa.table(
    {
        "i": pa.array(range(6), pa.int64()),
        "g": pa.array([0] * 6, pa.int64()),
        "x": pa.array([1, None, 3, None, 12, 25], pa.int64()),
        "y": pa.array([1, 1, 0, 0, 1, 0], pa.int64()),
    }
)


@pytest.fixture(params=["memory", "parquet_rg2"])
def issue_table(request, tmp_path) -> LTSeq:
    return _table(ISSUE_ROWS, request.param, tmp_path)


@pytest.mark.parametrize(
    ("pred", "expected"),
    [
        # Row 1 is NULL | TRUE.
        (lambda r: (r.x > 100) | (r.y == 1), [0, 1, 4]),
        # Row 3 is ~(NULL & FALSE).
        (lambda r: ~((r.x > 100) & (r.y == 1)), [0, 2, 3, 4, 5]),
        # `%` propagates NULL, and `|` then reads it as above (#219).
        (lambda r: (r.x % 10 == 3) | (r.y == 1), [0, 1, 2, 4]),
    ],
    ids=["null_or_true", "not_null_and_false", "mod_or"],
)
def test_issue_predicates(issue_table, pred, expected):
    t = issue_table
    assert _ids(t.filter(pred)) == expected
    assert _ids(t.search_pattern(pred)) == expected
    assert _ids(t.search_pattern(pred, partition_by="g")) == expected
    assert t.search_pattern_count(pred) == len(expected)
    assert t.search_pattern_count(pred, partition_by="g") == len(expected)


@pytest.mark.parametrize(
    ("steps", "expected"),
    [
        # Start 0: row 1 completes it with NULL | TRUE.
        ((lambda r: r.y == 1, lambda r: (r.x > 100) | (r.y == 1)), [0]),
        # Start 2: row 3 completes it with ~(NULL & FALSE).
        ((lambda r: r.y == 0, lambda r: ~((r.x > 100) & (r.y == 1))), [2, 3]),
    ],
    ids=["null_or_true", "not_null_and_false"],
)
def test_issue_predicates_as_second_step(issue_table, steps, expected):
    t = issue_table
    assert _ids(t.search_pattern(*steps)) == expected
    assert _ids(t.search_pattern(*steps, partition_by="g")) == expected
    assert t.search_pattern_count(*steps) == len(expected)
    assert t.search_pattern_count(*steps, partition_by="g") == len(expected)
