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
