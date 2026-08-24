from __future__ import annotations

import pytest

from ltseq import LTSeq


def test_search_pattern_count_counts_sorted_funnel_matches_from_parquet(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "funnel.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 1, 2, 2, 2, 3, 3],
            "eventtime": [1, 2, 3, 4, 1, 2, 3, 1, 2],
            "watchid": [10, 11, 12, 13, 20, 21, 22, 30, 31],
            "url": [
                "landing/a",
                "product/a",
                "checkout/a",
                "other",
                "landing/b",
                "product/b",
                "other",
                "landing/c",
                "product/c",
            ],
        }
    )
    pq.write_table(table, path)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime", "watchid")

    count = t.search_pattern_count(
        lambda r: r.url.s.starts_with("landing/"),
        lambda r: r.url.s.starts_with("product/"),
        lambda r: r.url.s.starts_with("checkout/"),
        partition_by="userid",
    )

    assert count == 1


def test_search_pattern_count_supports_same_non_url_string_column_fast_path(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "path_funnel.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 2, 2, 2],
            "eventtime": [1, 2, 3, 1, 2, 3],
            "watchid": [10, 11, 12, 20, 21, 22],
            "path": [
                "landing/a",
                "product/a",
                "checkout/a",
                "landing/b",
                "product/b",
                "other",
            ],
        }
    )
    pq.write_table(table, path)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime", "watchid")

    count = t.search_pattern_count(
        lambda r: r.path.s.starts_with("landing/"),
        lambda r: r.path.s.starts_with("product/"),
        lambda r: r.path.s.starts_with("checkout/"),
        partition_by="userid",
    )

    assert count == 1


def test_search_pattern_count_falls_back_for_mixed_columns(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "mixed_columns.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 2, 2, 2],
            "eventtime": [1, 2, 3, 1, 2, 3],
            "watchid": [10, 11, 12, 20, 21, 22],
            "path": [
                "landing/a",
                "product/a",
                "checkout/a",
                "landing/b",
                "product/b",
                "checkout/b",
            ],
            "referrer": [
                "entry/a",
                "detail/a",
                "exit/a",
                "entry/b",
                "detail/b",
                "other",
            ],
        }
    )
    pq.write_table(table, path)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime", "watchid")

    count = t.search_pattern_count(
        lambda r: r.path.s.starts_with("landing/"),
        lambda r: r.referrer.s.starts_with("detail/"),
        lambda r: r.path.s.starts_with("checkout/"),
        partition_by="userid",
    )

    assert count == 2


# ---------------------------------------------------------------------------
# Regression tests for issue #139: parallel fast path silently returning 0,
# hardcoded "url" column, and swallowed evaluation failures.
# ---------------------------------------------------------------------------


def test_search_pattern_supports_ne_le_ge_predicates(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "vals.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 1],
            "eventtime": [1, 2, 3, 4],
            "value": [0, 3, 4, 9],
        }
    )
    pq.write_table(table, path)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    matches = t.search_pattern(
        lambda r: r.value != 0,
        lambda r: r.value <= 5,
        lambda r: r.value >= 2,
        partition_by="userid",
    )

    # Only start row 1 matches: 3 != 0, 4 <= 5, 9 >= 2
    assert matches.count() == 1


def test_search_pattern_count_parallel_path_supports_ne_le_ge(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "vals_parallel.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 2, 2, 2],
            "eventtime": [1, 2, 3, 1, 2, 3],
            "value": [0, 3, 4, 1, 2, 9],
        }
    )
    # Small row groups so the match for user 2 spans row-group boundaries.
    pq.write_table(table, path, row_group_size=2)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    count = t.search_pattern_count(
        lambda r: r.value != 0,
        lambda r: r.value <= 5,
        lambda r: r.value >= 2,
        partition_by="userid",
    )

    # User 1 fails at step 1 (value 0); user 2 matches: 1 != 0, 2 <= 5, 9 >= 2.
    assert count == 1


def test_search_pattern_count_parallel_path_supports_arithmetic(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "vals_arith.parquet"
    table = pa.table(
        {
            "userid": [1, 1],
            "eventtime": [1, 2],
            "value": [3, 4],
        }
    )
    pq.write_table(table, path)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    count = t.search_pattern_count(
        lambda r: r.value * 2 == 6,
        lambda r: r.value + 1 == 5,
        partition_by="userid",
    )

    assert count == 1


def test_search_pattern_count_unsupported_predicate_raises_instead_of_zero(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "vals_mod.parquet"
    table = pa.table(
        {
            "userid": [1, 1],
            "eventtime": [1, 2],
            "value": [3, 4],
        }
    )
    pq.write_table(table, path)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    # Modulo is not supported by the pattern-match evaluator: the parallel
    # path must fall back, and the general path must raise — never return 0.
    with pytest.raises(Exception, match="Mod|[Uu]nsupported"):
        t.search_pattern_count(
            lambda r: r.value % 2 == 1,
            lambda r: r.value % 2 == 0,
            partition_by="userid",
        )


def test_search_pattern_count_general_path_ignores_decoy_url_column():
    pa = pytest.importorskip("pyarrow")

    # No Parquet source: exercises the general (non-parallel) count path.
    # Step 1 references "url" so the column is projected; steps 2..N apply
    # to "path" and must not be matched against "url".
    t = LTSeq.from_arrow(
        pa.table(
            {
                "userid": [1, 1, 1],
                "path": ["x", "product/x", "checkout/x"],
                "url": ["landing/x", "zzz", "zzz"],
            }
        )
    ).assume_sorted("userid")

    count = t.search_pattern_count(
        lambda r: r.url.s.starts_with("landing/"),
        lambda r: r.path.s.starts_with("product/"),
        lambda r: r.path.s.starts_with("checkout/"),
        partition_by="userid",
    )

    # The prefixes apply to "path"; the decoy "url" column must be ignored.
    assert count == 1


def test_search_pattern_count_general_path_mixed_columns_with_decoy_url():
    pa = pytest.importorskip("pyarrow")

    t = LTSeq.from_arrow(
        pa.table(
            {
                "userid": [1, 1, 1],
                "path": ["x", "other", "checkout/x"],
                "referrer": ["other", "detail/x", "other"],
                "url": ["landing/x", "zzz", "zzz"],
            }
        )
    ).assume_sorted("userid")

    count = t.search_pattern_count(
        lambda r: r.url.s.starts_with("landing/"),
        lambda r: r.referrer.s.starts_with("detail/"),
        lambda r: r.path.s.starts_with("checkout/"),
        partition_by="userid",
    )

    assert count == 1


def test_search_pattern_count_match_spanning_three_row_groups_fast_path(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "span3_fast.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 1, 1, 1],
            "eventtime": [1, 2, 3, 4, 5, 6],
            "ev": ["x", "s1/a", "s2/a", "s3/a", "s4/a", "x"],
        }
    )
    # Row groups of 2 rows: the match at rows 1..4 spans three row groups.
    pq.write_table(table, path, row_group_size=2)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    count = t.search_pattern_count(
        lambda r: r.ev.s.starts_with("s1/"),
        lambda r: r.ev.s.starts_with("s2/"),
        lambda r: r.ev.s.starts_with("s3/"),
        lambda r: r.ev.s.starts_with("s4/"),
        partition_by="userid",
    )

    assert count == 1


def test_search_pattern_count_match_spanning_three_row_groups_general_path(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "span3_general.parquet"
    table = pa.table(
        {
            "userid": [1, 1, 1, 1, 1, 1],
            "eventtime": [1, 2, 3, 4, 5, 6],
            "ev": ["x", "s1/a", "s2/a", "s3/a", "s4/a", "x"],
        }
    )
    pq.write_table(table, path, row_group_size=2)

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    # Equality predicates avoid the starts_with fast path and exercise the
    # general cross-row-group evaluation path.
    count = t.search_pattern_count(
        lambda r: r.ev == "s1/a",
        lambda r: r.ev == "s2/a",
        lambda r: r.ev == "s3/a",
        lambda r: r.ev == "s4/a",
        partition_by="userid",
    )

    assert count == 1


def test_search_pattern_count_match_spanning_empty_row_group(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "empty_rg.parquet"
    schema = pa.schema(
        [
            ("userid", pa.int64()),
            ("eventtime", pa.int64()),
            ("ev", pa.string()),
        ]
    )

    def chunk(userids, eventtimes, evs):
        return pa.table(
            {"userid": userids, "eventtime": eventtimes, "ev": evs},
            schema=schema,
        )

    writer = pq.ParquetWriter(path, schema)
    writer.write_table(chunk([1, 1], [1, 2], ["x", "s1/a"]))
    writer.write_table(chunk([], [], []))
    writer.write_table(chunk([1, 1], [3, 4], ["s2/a", "s3/a"]))
    writer.close()

    t = LTSeq.read_parquet(str(path)).assume_sorted("userid", "eventtime")

    count = t.search_pattern_count(
        lambda r: r.ev.s.starts_with("s1/"),
        lambda r: r.ev.s.starts_with("s2/"),
        lambda r: r.ev.s.starts_with("s3/"),
        partition_by="userid",
    )

    # The match at rows 1..3 crosses the empty middle row group.
    assert count == 1
