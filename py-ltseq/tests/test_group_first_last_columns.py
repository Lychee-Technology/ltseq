"""group_ordered(...).first()/last() keep user columns whose names look internal (issue #214).

Only the grouping columns `__group_id__`, `__group_count__` and `__rn__` are
removed from the result; they used to be removed by prefix, which also
dropped user columns such as `__rn_x`.
"""

import pyarrow as pa
import pytest

from ltseq import LTSeq

LOOKALIKES = ["__rn_x", "__rn", "__cnt", "__mask_a", "__row_num_b", "__group_id__x"]


@pytest.fixture
def table() -> LTSeq:
    columns = {"k": [1, 1, 2], "v": [5, 6, 7]}
    for i, name in enumerate(LOOKALIKES):
        columns[name] = [10 * i + 1, 10 * i + 2, 10 * i + 3]
    return LTSeq.from_arrow(pa.table(columns)).sort("k")


@pytest.mark.parametrize("pick, rows", [("first", [0, 2]), ("last", [1, 2])])
def test_first_last_keep_lookalike_user_columns(table, pick, rows):
    out = getattr(table.group_ordered(lambda r: r.k), pick)().to_pandas()
    assert list(out.columns) == ["k", "v", *LOOKALIKES]
    for i, name in enumerate(LOOKALIKES):
        assert out[name].tolist() == [10 * i + 1 + r for r in rows]


@pytest.mark.parametrize("pick", ["first", "last"])
def test_first_last_still_drop_grouping_columns(table, pick):
    out = getattr(table.group_ordered(lambda r: r.k), pick)().to_pandas()
    assert not {"__group_id__", "__group_count__", "__rn__"} & set(out.columns)


@pytest.mark.parametrize("pick, rows", [("first", [0, 2]), ("last", [1, 2])])
def test_first_last_keep_names_sql_would_rewrite(pick, rows):
    # The projection used to go through DataFusion's col(), which parses its
    # argument: "UserID" became userid and "a.b" a qualified reference, so
    # first()/last() raised "No field named userid".
    t = LTSeq.from_arrow(pa.table({"k": [1, 1, 2], "UserID": [1, 2, 3], "a.b": [4, 5, 6]})).sort("k")
    out = getattr(t.group_ordered(lambda r: r.k), pick)().to_pandas()
    assert list(out.columns) == ["k", "UserID", "a.b"]
    assert out["UserID"].tolist() == [1 + r for r in rows]
    assert out["a.b"].tolist() == [4 + r for r in rows]


def _picked(t: LTSeq, grouping: str, pick: str, filtered: bool) -> LTSeq:
    grouped = getattr(t, grouping)(lambda r: r.k)
    if filtered:
        grouped = grouped.filter(lambda g: g.count() > 1)
    return getattr(grouped, pick)()


@pytest.mark.parametrize("filtered", [False, True])
@pytest.mark.parametrize("grouping", ["group_ordered", "group_sorted"])
@pytest.mark.parametrize("pick", ["first", "last"])
def test_first_last_schema_lists_only_the_result_columns(pick, grouping, filtered):
    # last() used to copy the schema of the flattened plan it filters, so
    # .columns and .schema listed the grouping columns its plan projects
    # away; select() of them failed and no export held them (issue #315).
    t = LTSeq.from_rows([{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": 3}]).sort("k", "v")
    out = _picked(t, grouping, pick, filtered)
    assert out.columns == ["k", "v"]
    assert list(out.schema) == ["k", "v"]
    assert out.to_arrow().column_names == out.columns
    assert out.select(*out.columns).to_arrow().column_names == ["k", "v"]


@pytest.mark.parametrize("pick", ["first", "last"])
def test_first_last_schema_keeps_lookalike_user_columns(table, pick):
    out = getattr(table.group_ordered(lambda r: r.k), pick)()
    assert out.columns == ["k", "v", *LOOKALIKES]
    assert out.to_arrow().column_names == out.columns


class _TaggedLTSeq(LTSeq):
    def __init__(self):
        super().__init__()
        self.tag = "initialized"


def test_last_runs_subclass_init_and_reads_result_schema():
    # last() used to copy flattened's schema; building the result with
    # _from_inner fixed that but skipped a subclass's __init__ (#315).
    base = LTSeq.from_rows([{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": 3}]).sort("k", "v")
    t = _TaggedLTSeq._from_inner(base._inner)
    out = t.group_ordered(lambda r: r.k).last()
    assert type(out) is _TaggedLTSeq
    assert out.tag == "initialized"
    assert out.columns == ["k", "v"]
    assert list(out.schema) == ["k", "v"]
    assert out.to_arrow().column_names == ["k", "v"]
    assert out.to_pandas()["v"].tolist() == [2, 3]
