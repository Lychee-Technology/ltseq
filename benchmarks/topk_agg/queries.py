"""The grouped Top-K queries of the #386 harness and their DuckDB check.

Every shape is ``group by key, count, order by count descending, first K``
through LTSeq's public API:

- ``count_star``: DataFusion's ``COUNT(*)`` form, ``count(1)``. The lambda
  DSL has no row count inside ``agg`` (``g.count()`` reads as a column named
  ``count``), so the harness builds the call the Rust side plans as
  ``count(1)`` directly: ``CallExpr("count")`` with no column.
- ``count_col``: ``agg(cnt=count(c))`` over a column other than the key
  (``flag`` in the synthetic fixtures, ``key`` itself on ClickBench).
- ``projected``: ``count_star`` with a post-aggregate projection
  (``derive``) between the aggregate and the sort.

``filter`` (``flag_lt_5`` or ``nonempty_key``) puts a predicate below the
aggregate, as ClickBench Q13 does.

``check`` is tie-aware: when several keys share the K-th count, any of them
is a correct answer, so it compares the returned counts with DuckDB's top-K
counts and each returned key's count with DuckDB's count of that key.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
from ltseq.expr.types import CallExpr

SHAPES = ("count_star", "count_col", "projected")


def _filtered(table, key: str, predicate: str | None):
    if predicate is None:
        return table
    if predicate == "flag_lt_5":
        return table.filter(lambda r: r.flag < 5)
    if predicate == "nonempty_key":
        return table.filter(lambda r: getattr(r, key) != "")
    raise ValueError(predicate)


def _count_column(table, key: str) -> str:
    return "flag" if "flag" in table.columns else key


def top_k(table, shape: str, key: str, k: int, predicate: str | None = None):
    """The lazy LTSeq table of the query; nothing runs until a terminal."""
    table = _filtered(table, key, predicate)
    by = lambda r: getattr(r, key)  # noqa: E731
    if shape in ("count_star", "projected"):
        grouped = table.agg(by=by, cnt=lambda g: CallExpr("count"))
    elif shape == "count_col":
        column = _count_column(table, key)
        grouped = table.agg(by=by, cnt=lambda g: getattr(g, column).count())
    else:
        raise ValueError(shape)
    if shape == "projected":
        grouped = grouped.derive(lambda r: {"share": r.cnt * 2})
    return grouped.sort("cnt", desc=True).slice(0, k)


def _sql_parts(source: str, shape: str, key: str, predicate: str | None) -> tuple[str, str, str]:
    names = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()
    columns = {row[0] for row in names}
    measure = "count(*)"
    if shape == "count_col":
        measure = f'count("{"flag" if "flag" in columns else key}")'
    where = ""
    if predicate == "flag_lt_5":
        where = "WHERE flag < 5"
    elif predicate == "nonempty_key":
        where = f"WHERE \"{key}\" <> ''"
    return measure, where, f"read_parquet('{source}')"


def check(result: pa.Table, source: str, shape: str, key: str, k: int, predicate: str | None) -> dict:
    """Whether ``result`` is an exact top-K of the query, ties allowed."""
    measure, where, src = _sql_parts(source, shape, key, predicate)
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    ref_counts = [
        row[0]
        for row in con.sql(
            f'SELECT {measure} AS c FROM {src} {where} GROUP BY "{key}" ORDER BY c DESC LIMIT {k}'
        ).fetchall()
    ]
    got = result.select([key, "cnt"]).to_pylist()
    got_keys = [row[key] for row in got]
    got_counts = [row["cnt"] for row in got]
    ret = pa.table({"k": pa.array(got_keys, type=result.schema.field(key).type)})
    con.register("ret", ret)
    truth = dict(
        con.sql(
            f'SELECT "{key}", {measure} FROM {src} {where} '
            f'GROUP BY "{key}" HAVING "{key}" IN (SELECT k FROM ret)'
        ).fetchall()
    )
    problems = []
    if len(set(got_keys)) != len(got_keys):
        problems.append("duplicate keys")
    if got_counts != sorted(got_counts, reverse=True):
        problems.append("counts not in descending order")
    if got_counts != ref_counts:
        problems.append(f"counts differ from DuckDB's top-{k}")
    wrong = [kk for kk, c in zip(got_keys, got_counts) if truth.get(kk) != c]
    if wrong:
        problems.append(f"{len(wrong)} keys with a count DuckDB does not give them")
    return {
        "ok": not problems,
        "problems": problems,
        "rows": len(got),
        "kth_count": ref_counts[-1] if ref_counts else None,
        "top_count": ref_counts[0] if ref_counts else None,
    }
