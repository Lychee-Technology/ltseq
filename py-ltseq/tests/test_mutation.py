"""Tests for MutationMixin: insert, delete, update, modify operations."""

import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pd = pytest.importorskip("pandas")

try:
    from ltseq import LTSeq
except ImportError:
    import sys
    sys.path.insert(0, "py-ltseq")
    from ltseq import LTSeq


@pytest.fixture
def sample():
    """Three-row table: id=[1,2,3], name=[alice,bob,carol], score=[10,20,30]."""
    return LTSeq.from_rows([
        {"id": 1, "name": "alice", "score": 10},
        {"id": 2, "name": "bob",   "score": 20},
        {"id": 3, "name": "carol", "score": 30},
    ])


# ─── insert ──────────────────────────────────────────────────────────────────

class TestInsert:
    def test_insert_at_start(self, sample):
        result = sample.insert(0, {"id": 0, "name": "zero", "score": 0})
        rows = result.to_pandas().to_dict("records")
        assert rows[0] == {"id": 0, "name": "zero", "score": 0}
        assert len(rows) == 4

    def test_insert_at_end(self, sample):
        result = sample.insert(3, {"id": 4, "name": "dave", "score": 40})
        rows = result.to_pandas().to_dict("records")
        assert rows[-1]["name"] == "dave"
        assert len(rows) == 4

    def test_insert_in_middle(self, sample):
        result = sample.insert(1, {"id": 99, "name": "mid", "score": 15})
        rows = result.to_pandas().to_dict("records")
        assert rows[1]["name"] == "mid"
        assert rows[2]["name"] == "bob"

    def test_insert_beyond_end_clamps(self, sample):
        result = sample.insert(100, {"id": 9, "name": "end", "score": 90})
        rows = result.to_pandas().to_dict("records")
        assert rows[-1]["name"] == "end"
        assert len(rows) == 4

    def test_insert_negative_pos_clamps_to_zero(self, sample):
        result = sample.insert(-5, {"id": 0, "name": "first", "score": 0})
        rows = result.to_pandas().to_dict("records")
        assert rows[0]["name"] == "first"

    def test_insert_does_not_mutate_original(self, sample):
        _ = sample.insert(0, {"id": 0, "name": "zero", "score": 0})
        assert len(sample.to_pandas()) == 3


# ─── delete ──────────────────────────────────────────────────────────────────

class TestDelete:
    def test_delete_by_predicate(self, sample):
        result = sample.delete(lambda r: r.name == "bob")
        names = result.to_pandas()["name"].tolist()
        assert "bob" not in names
        assert len(names) == 2

    def test_delete_by_index(self, sample):
        result = sample.delete(0)
        rows = result.to_pandas()
        assert rows.iloc[0]["name"] == "bob"
        assert len(rows) == 2

    def test_delete_last_row_by_index(self, sample):
        result = sample.delete(2)
        assert len(result.to_pandas()) == 2
        assert result.to_pandas().iloc[-1]["name"] == "bob"

    def test_delete_out_of_range_index_is_noop(self, sample):
        result = sample.delete(99)
        assert len(result.to_pandas()) == 3

    def test_delete_predicate_no_match_returns_all(self, sample):
        result = sample.delete(lambda r: r.id == 999)
        assert len(result.to_pandas()) == 3

    def test_delete_does_not_mutate_original(self, sample):
        _ = sample.delete(0)
        assert len(sample.to_pandas()) == 3


def _nullable():
    """Rows where every predicate below is TRUE on some, FALSE on some and
    NULL on others, including a row that is NULL in every column."""
    return LTSeq.from_arrow(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                "x": pa.array([1, None, 3, 4, None, 0], pa.int64()),
                "f": pa.array([0.5, float("nan"), None, 2.5, None, -1.0], pa.float64()),
                "s": pa.array(["active", None, "expired", "expired", None, "x"]),
                "b": pa.array([True, None, False, True, None, False]),
            }
        )
    )


# Predicates that are NULL on some rows: comparisons, Boolean columns,
# Kleene AND/OR, NOT, string methods and membership. A window predicate
# would belong here, but filter() does not take one yet.
NULL_PRODUCING = {
    "gt": lambda r: r.x > 1,
    "eq_str": lambda r: r.s == "expired",
    "ne_str": lambda r: r.s != "expired",
    "float_cmp": lambda r: r.f > 0.0,
    "bool_column": lambda r: r.b,
    "not": lambda r: ~(r.x > 1),
    "and": lambda r: (r.x > 1) & (r.s == "expired"),
    "or": lambda r: (r.x > 3) | r.b,
    "is_in": lambda r: r.x.is_in([0, 3]),
    "str_method": lambda r: r.s.s.starts_with("ex"),
    "between": lambda r: r.x.between(1, 3),
}


def _multiset(rows):
    from collections import Counter

    def key(row):
        # NaN != NaN, so compare floats by their repr.
        return tuple(sorted((k, repr(v)) for k, v in row.items()))

    return Counter(key(row) for row in rows)


class TestDeleteNullPredicate:
    """``delete(p)`` removes only the rows where ``p`` is TRUE (#265)."""

    def test_archived_reference_example_keeps_the_null_row(self):
        t = LTSeq.from_dict({"id": [1, 2, 3], "status": ["active", None, "expired"]})
        assert t.delete(lambda r: r.status == "expired").to_dicts() == [
            {"id": 1, "status": "active"},
            {"id": 2, "status": None},
        ]

    def test_null_predicate_keeps_the_row(self):
        t = LTSeq.from_dict({"x": [1, None, 3]})
        assert t.delete(lambda r: r.x > 1).to_dicts() == [{"x": 1}, {"x": None}]

    def test_delete_is_not_filter_of_the_negation(self):
        t = LTSeq.from_dict({"x": [1, None, 3]})
        assert t.filter(lambda r: ~(r.x > 1)).count() == 1
        assert t.delete(lambda r: r.x > 1).count() == 2

    def test_is_none_predicate_deletes_the_null_rows(self):
        t = LTSeq.from_dict({"x": [1, None, 3]})
        # LTSeq rewrites `r.x is None` to `r.x.is_null()`; pyright sees a bool.
        result = t.delete(lambda r: r.x is None)  # pyright: ignore[reportArgumentType]
        assert result.to_dicts() == [{"x": 1}, {"x": 3}]

    def test_or_is_null_deletes_the_null_rows_too(self):
        t = LTSeq.from_dict({"x": [1, None, 3]})
        result = t.delete(lambda r: (r.x > 1) | (r.x > 1).is_null())
        assert result.to_dicts() == [{"x": 1}]

    @pytest.mark.parametrize("name", NULL_PRODUCING)
    def test_delete_and_filter_partition_the_rows(self, engine_layout, name):
        """Property: ``t.delete(p)`` and ``t.filter(p)`` together hold the
        multiset of rows of ``t``, for deterministic predicates that are NULL
        on some rows. A volatile predicate draws anew in each query, so the
        property does not hold for it."""
        p = NULL_PRODUCING[name]
        t = _nullable().sort("id")
        kept = t.delete(p).to_dicts()
        removed = t.filter(p).to_dicts()
        assert _multiset(kept) + _multiset(removed) == _multiset(t.to_dicts())
        # The predicate is NULL somewhere, so delete keeps more than filter(~p).
        assert len(kept) > t.filter(lambda r: ~p(r)).count()

    @pytest.mark.parametrize("index_type", [pa.int8(), pa.int32()])
    @pytest.mark.parametrize(
        "values",
        [
            [True, False, None, True, None, False],
            [True, False, True, False],
            [None, None, None],
            [],
        ],
        ids=["nullable", "non_nullable", "all_null", "empty"],
    )
    def test_dictionary_encoded_boolean_predicate(
        self, engine_layout, index_type, values
    ):
        """A dictionary-encoded Boolean column as the predicate: delete keeps
        the FALSE and NULL rows, filter selects the TRUE ones."""
        t = LTSeq.from_arrow(
            pa.table(
                {
                    "id": pa.array(range(len(values)), pa.int64()),
                    "p": pa.array(values, pa.dictionary(index_type, pa.bool_())),
                }
            )
        ).sort("id")
        kept = t.delete(lambda r: r.p).to_dicts()
        removed = t.filter(lambda r: r.p).to_dicts()
        assert [row["p"] for row in kept] == [v for v in values if v is not True]
        assert [row["p"] for row in removed] == [v for v in values if v is True]
        assert _multiset(kept) + _multiset(removed) == _multiset(t.to_dicts())

    @pytest.mark.parametrize("column", ["x", "s"])
    def test_non_boolean_predicate_is_refused(self, column):
        """A predicate that is not Boolean raises, as ``filter(~p)`` does;
        the rewrite does not turn it into one."""
        t = _nullable()
        with pytest.raises(Exception, match="Boolean"):
            t.delete(lambda r: getattr(r, column)).to_dicts()

    def test_volatile_nullable_predicate_is_evaluated_once_per_row(self):
        """``delete(p)`` draws a volatile ``p`` once per row (R2-F1).

        ``p`` is all NULL, so ``(p & (rand() < 0.5)) | (rand() < 0.5)`` is
        TRUE about half the time and NULL or FALSE otherwise: delete keeps
        about half the rows. A rewrite naming the predicate twice, as
        ``NOT p OR p IS NULL`` did, draws twice and kept about 43.75%.
        With 200,000 rows one standard deviation of the kept share is
        about 0.11 points, so the bounds are some 25 deviations from
        either outcome."""
        from ltseq.expr import rand

        n = 200_000
        t = LTSeq.from_arrow(pa.table({"p": pa.array([None] * n, pa.bool_())}))

        def p(r):
            return (r.p & (rand() < 0.5)) | (rand() < 0.5)

        kept = t.delete(p).count()
        assert 0.47 * n < kept < 0.53 * n
        # filter selects about half the rows as well: delete is its complement.
        assert 0.47 * n < t.filter(p).count() < 0.53 * n

    def test_delete_keeps_table_order(self):
        t = _nullable().sort("id", desc=True)
        ids = [row["id"] for row in t.delete(lambda r: r.x > 1).to_dicts()]
        assert ids == [6, 5, 2, 1]


# ─── update ──────────────────────────────────────────────────────────────────
    @pytest.mark.parametrize("pos", [0, 4, 5, 9, 14])
    def test_delete_by_index_multi_batch_removes_exactly_one_row(self, pos):
        """Regression (#176): only the batch holding `pos` is spliced.

        The per-batch offset math used `saturating_sub`, which yielded 0 for
        every batch after the target and deleted their first rows too. Small
        single-batch tables never showed it.
        """
        import pyarrow as pa
        from ltseq import LTSeq

        batches = [
            pa.record_batch({"id": list(range(start, start + 5))})
            for start in (0, 5, 10)
        ]
        t = LTSeq.from_arrow(pa.Table.from_batches(batches))
        assert t.count() == 15

        result = t.delete(pos)
        ids = [row["id"] for row in result.to_dicts()]
        assert ids == [i for i in range(15) if i != pos]


class TestUpdate:
    def test_update_matching_rows(self, sample):
        result = sample.update(lambda r: r.name == "bob", score=99)
        df = result.to_pandas()
        assert df[df["name"] == "bob"]["score"].iloc[0] == 99
        assert df[df["name"] == "alice"]["score"].iloc[0] == 10

    def test_update_multiple_columns(self, sample):
        result = sample.update(lambda r: r.id == 1, score=100, name="ALICE")
        df = result.to_pandas()
        row = df[df["id"] == 1].iloc[0]
        assert row["score"] == 100
        assert row["name"] == "ALICE"

    def test_update_no_match_returns_unchanged(self, sample):
        result = sample.update(lambda r: r.id == 999, score=0)
        assert result.to_pandas()["score"].tolist() == [10, 20, 30]

    def test_update_no_kwargs_returns_self(self, sample):
        result = sample.update(lambda r: r.id == 1)
        assert len(result.to_pandas()) == 3

    def test_update_does_not_mutate_original(self, sample):
        _ = sample.update(lambda r: r.id == 1, score=999)
        assert sample.to_pandas().iloc[0]["score"] == 10

    def test_null_predicate_leaves_the_row_unchanged(self):
        """Pins the current behavior (#265): a row where the predicate is
        NULL keeps its values, as a row where it is FALSE does."""
        t = LTSeq.from_dict({"x": [1, None, 3], "y": [10, 20, 30]})
        assert t.update(lambda r: r.x > 1, y=0).to_dicts() == [
            {"x": 1, "y": 10},
            {"x": None, "y": 20},
            {"x": 3, "y": 0},
        ]

    @pytest.mark.parametrize("name", NULL_PRODUCING)
    def test_update_changes_only_rows_where_the_predicate_is_true(self, name):
        p = NULL_PRODUCING[name]
        t = _nullable().sort("id")
        hit = {row["id"] for row in t.filter(p).to_dicts()}
        before = t.to_dicts()
        after = t.update(p, s="UPDATED").to_dicts()
        for old, new in zip(before, after, strict=True):
            expected = dict(old, s="UPDATED") if old["id"] in hit else old
            assert _multiset([new]) == _multiset([expected])


# ─── modify ──────────────────────────────────────────────────────────────────

class TestModify:
    def test_modify_single_column(self, sample):
        result = sample.modify(1, score=99)
        df = result.to_pandas()
        assert df.iloc[1]["score"] == 99
        assert df.iloc[0]["score"] == 10

    def test_modify_multiple_columns(self, sample):
        result = sample.modify(0, score=0, name="zero")
        row = result.to_pandas().iloc[0]
        assert row["score"] == 0
        assert row["name"] == "zero"

    def test_modify_out_of_range_is_noop(self, sample):
        result = sample.modify(99, score=999)
        assert result.to_pandas()["score"].tolist() == [10, 20, 30]

    def test_modify_does_not_mutate_original(self, sample):
        _ = sample.modify(0, score=999)
        assert sample.to_pandas().iloc[0]["score"] == 10


# ─── string column types ─────────────────────────────────────────────────────

# Arrow has three physical string types, each of which can also be
# dictionary- or run-end-encoded. `read_parquet` yields string_view (DataFusion 55 scans
# Parquet strings as Utf8View) unless the file stores a dictionary-encoded
# column, e.g. a pandas categorical; `from_arrow` / `from_pandas` keep the
# source type; `from_rows` gives string.
STRING_TYPES = [
    pa.string(),
    pa.large_string(),
    pa.string_view(),
    pa.dictionary(pa.int8(), pa.string()),
    pa.dictionary(pa.int8(), pa.large_string()),  # a pandas 3 categorical
    pa.run_end_encoded(pa.int32(), pa.string()),
]


def _people(name_type: pa.DataType) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["alice", "bob", "carol"], name_type),
            "score": pa.array([10, 20, 30], pa.int64()),
        }
    )


def _names(t: LTSeq) -> list:
    # Read through Arrow: to_dicts() goes via pandas and turns nulls into NaN.
    return t.to_arrow().column("name").to_pylist()


class TestStringColumnTypes:
    """Mutations must write a Python str into every Arrow string type (#177)."""

    @pytest.fixture(params=STRING_TYPES, ids=str)
    def typed(self, request) -> LTSeq:
        return LTSeq.from_arrow(_people(request.param))

    def test_insert_str(self, typed):
        result = typed.insert(1, {"id": 9, "name": "zed", "score": 0})
        assert _names(result) == ["alice", "zed", "bob", "carol"]

    def test_insert_null_str(self, typed):
        result = typed.insert(0, {"id": 9, "name": None, "score": 0})
        assert _names(result) == [None, "alice", "bob", "carol"]

    def test_modify_str(self, typed):
        assert _names(typed.modify(1, name="zed")) == ["alice", "zed", "carol"]

    def test_update_str(self, typed):
        result = typed.update(lambda r: r.score >= 20, name="senior")
        assert _names(result) == ["alice", "senior", "senior"]

    @pytest.mark.parametrize(
        ("written", "scanned"),
        [
            (pa.string(), pa.string_view()),
            (pa.dictionary(pa.int8(), pa.string()), pa.dictionary(pa.int8(), pa.string())),
        ],
        ids=["string", "dictionary"],
    )
    def test_parquet_scan_string_column(self, tmp_path, written, scanned):
        """The reported path: a Parquet scan hands the mutations string_view data,
        or dictionary data when the file stores a dictionary-encoded column."""
        path = tmp_path / "people.parquet"
        pq.write_table(_people(written), path)
        t = LTSeq.read_parquet(str(path))
        # Guard the premise, which depends on DataFusion's scan defaults.
        assert t.to_arrow().schema.field("name").type == scanned
        assert _names(t.insert(0, {"id": 0, "name": "zero", "score": 0})) == ["zero", "alice", "bob", "carol"]
        assert _names(t.modify(0, name="zed")) == ["zed", "bob", "carol"]
        assert _names(t.update(lambda r: r.id == 3, name="carla")) == ["alice", "bob", "carla"]

    def test_dictionary_encoded_int_column(self):
        """A dictionary-encoded column takes values of its value type."""
        t = LTSeq.from_arrow(pa.table({"code": pa.array([7, 8], pa.int64()).dictionary_encode()}))

        def codes(result: LTSeq) -> list:
            return result.to_arrow().column("code").to_pylist()

        assert codes(t.insert(1, {"code": 9})) == [7, 9, 8]
        assert codes(t.modify(0, code=9)) == [9, 8]
        assert codes(t.update(lambda r: r.code == 8, code=9)) == [7, 9]


# ─── schema-level metadata ───────────────────────────────────────────────────


class TestSplicedBatchSchema:
    """insert / modify splice their new batch among the collected ones, so all
    of them must share one schema. That schema keeps the collected batches'
    schema-level metadata, which the table schema can lack, and allows nulls
    wherever the table schema does, which the batches may not."""

    @pytest.fixture
    def tagged(self) -> LTSeq:
        # Two chunks, so the new batch can land before an untouched one. The
        # metadata stands in for the `pandas` key that from_pandas attaches.
        chunk = _people(pa.string())
        table = pa.concat_tables([chunk, chunk]).replace_schema_metadata({"origin": "test"})
        return LTSeq.from_arrow(table)

    def test_from_pandas_insert_at_start(self):
        t = LTSeq.from_pandas(pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"], "score": [10, 20]}))
        assert _names(t.insert(0, {"id": 0, "name": "zero", "score": 0})) == ["zero", "alice", "bob"]

    def test_insert_at_start(self, tagged):
        result = tagged.insert(0, {"id": 0, "name": "zero", "score": 0})
        assert _names(result) == ["zero"] + ["alice", "bob", "carol"] * 2

    def test_insert_at_start_after_derive(self, tagged):
        # derive() rebuilds the table schema without metadata; the scan keeps it.
        derived = tagged.derive(double=lambda r: r.score * 2)
        result = derived.insert(0, {"id": 0, "name": "zero", "score": 0, "double": 0})
        assert _names(result) == ["zero"] + ["alice", "bob", "carol"] * 2

    def test_modify_first_batch_after_derive(self, tagged):
        derived = tagged.derive(double=lambda r: r.score * 2)
        assert _names(derived.modify(0, name="zed")) == ["zed", "bob", "carol", "alice", "bob", "carol"]

    @pytest.mark.parametrize(
        "make_empty",
        [
            lambda empty, full: empty,
            lambda empty, full: empty.derive(y=lambda r: r.x + 1),
            # A filter that matches nothing collects to no batches at all.
            lambda empty, full: full.filter(lambda r: r.x > 5),
            lambda empty, full: full.derive(y=lambda r: r.x + 1).filter(lambda r: r.x > 5),
        ],
        ids=["empty", "derived", "filtered", "derived-filtered"],
    )
    def test_insert_into_empty_table_keeps_pandas_index(self, make_empty):
        # The row is the result's only data, so its schema must still carry the
        # `pandas` metadata that tells to_pandas() which field is the index.
        index = pd.Index([], dtype="int64", name="idx")
        empty = LTSeq.from_pandas(pd.DataFrame({"x": pd.Series([], dtype="int64")}, index=index))
        full = LTSeq.from_pandas(pd.DataFrame({"x": [1, 2]}, index=pd.Index([10, 11], name="idx")))
        t = make_empty(empty, full)
        assert len(t) == 0
        out = t.insert(0, {"idx": 7, "x": 1}).to_pandas()
        assert out.index.name == "idx"
        assert out.index.tolist() == [7]
        assert out["x"].tolist() == [1]

    @pytest.fixture(params=["join", "link"])
    def joined(self, request) -> tuple[LTSeq, str]:
        """An inner join / link result and its right-side value column. The
        table schema marks every right-side field nullable, while the collected
        batches keep the right table's non-nullable fields non-nullable."""
        left = LTSeq.from_arrow(pa.table({"k": pa.array([1, 2, 3], pa.int64())}))
        right_schema = pa.schema(
            [pa.field("rk", pa.int64(), nullable=False), pa.field("n", pa.int64(), nullable=False)]
        )
        right = LTSeq.from_arrow(pa.table({"rk": [1, 2, 3], "n": [10, 20, 30]}, schema=right_schema))
        if request.param == "join":
            t, col = left.join(right, on=lambda l, r: l.k == r.rk), "n"
        else:
            t, col = left.link(right, on=lambda l, r: l.k == r.rk, as_="r").to_ltseq(), "r_n"
        t = t.sort("k")
        # Guard the premise: the batches declare the column non-nullable.
        assert not t.to_arrow().schema.field(col).nullable
        return t, col

    @pytest.mark.parametrize("value", [None, 0])
    @pytest.mark.parametrize("pos", [0, 1, 3])
    def test_insert_into_non_nullable_batch_column(self, joined, pos, value):
        t, col = joined
        # Omitted columns are written as null; for link that includes r_rk.
        result = t.insert(pos, {"k": 9, col: value})
        expected = [10, 20, 30]
        expected.insert(pos, value)
        assert result.to_arrow().column(col).to_pylist() == expected

    def test_modify_null_in_non_nullable_batch_column(self, joined):
        t, col = joined
        assert t.modify(0, **{col: None}).to_arrow().column(col).to_pylist() == [None, 20, 30]
        assert t.modify(2, **{col: None}).to_arrow().column(col).to_pylist() == [10, 20, None]


# ─── value conversion ────────────────────────────────────────────────────────

# Each mutation writes ``values`` into the first row of a table whose ``i``
# is 1 there.
WRITES = [
    pytest.param(lambda t, values: t.insert(0, values), id="insert"),
    pytest.param(lambda t, values: t.modify(0, **values), id="modify"),
    pytest.param(lambda t, values: t.update(lambda r: r.i == 1, **values), id="update"),
]

FLOAT32_MAX = (2 - 2.0**-23) * 2.0**127


class TestValueConversion:
    """Mutation values are typed literals, placed into the column's type as a
    ``shift`` default is (#194). The literal grid checks every literal kind
    against every column type; these are the cases reported on the issue."""

    @pytest.fixture
    def typed(self) -> LTSeq:
        from decimal import Decimal

        return LTSeq.from_arrow(
            pa.table(
                {
                    "i": pa.array([1, 2], pa.int64()),
                    "d": pa.array([Decimal("1.50"), Decimal("2.25")], pa.decimal128(5, 2)),
                    "ts": pa.array([0, 1], pa.timestamp("us")),
                    "f": pa.array([0.0, 1.0], pa.float32()),
                }
            )
        )

    @staticmethod
    def _col(t: LTSeq, name: str) -> list:
        return t.to_arrow().column(name).to_pylist()

    def test_decimal_and_datetime_values_are_written(self, typed):
        from datetime import datetime
        from decimal import Decimal

        # modify used to leave the decimal unchanged without an error, and
        # insert and update raised for a Decimal or a datetime.
        assert self._col(typed.modify(0, d=Decimal("9.99")), "d") == [Decimal("9.99"), Decimal("2.25")]
        inserted = typed.insert(1, {"i": 9, "d": Decimal("3.75"), "ts": datetime(2024, 1, 1)})
        assert self._col(inserted, "d") == [Decimal("1.50"), Decimal("3.75"), Decimal("2.25")]
        assert self._col(inserted, "ts")[1] == datetime(2024, 1, 1)
        updated = typed.update(lambda r: r.i == 1, d=Decimal("7.77"))
        assert self._col(updated, "d") == [Decimal("7.77"), Decimal("2.25")]

    @pytest.mark.parametrize(
        "write",
        [
            lambda t: t.insert(0, {"d": 9.99}),
            lambda t: t.modify(0, d=9.99),
            lambda t: t.update(lambda r: r.i == 1, d=9.99),
        ],
        ids=["insert", "modify", "update"],
    )
    def test_value_the_column_cannot_hold_exactly_raises(self, typed, write):
        # The float 9.99 is 9.9900000000000002131…, which decimal(5, 2) does
        # not hold (D-j); modify used to skip it silently.
        with pytest.raises(ValueError, match=r"column 'd' cannot hold 9\.99 exactly"):
            write(typed)

    @pytest.mark.parametrize("write", WRITES)
    @pytest.mark.parametrize(
        "values",
        [
            {"i": 3, "nosuch": 1},
            {"nosuch": [1]},  # a type no literal has
            {"nosuch": 2**64},  # an int outside Int64
            {"i": [1], "nosuch": 1},  # the keys are checked before any value
        ],
        ids=["valid", "unsupported", "out-of-range", "bad-value-first"],
    )
    def test_unknown_column_raises_whatever_the_values(self, typed, write, values):
        from ltseq import ColumnNotFoundError

        with pytest.raises(ColumnNotFoundError, match="Column 'nosuch' not found"):
            write(typed, values)

    @pytest.mark.parametrize("write", WRITES)
    @pytest.mark.parametrize(
        "value",
        # The last two lie just past the greatest float32, where IEEE rounding
        # would bring them back to it: the range ends there (contract §17.6).
        [1e39, -1e39, 1.7976931348623157e308, math.nextafter(FLOAT32_MAX, math.inf), math.nextafter(-FLOAT32_MAX, -math.inf)],
        ids=repr,
    )
    def test_finite_value_beyond_float32_range_raises(self, typed, write, value):
        # No float32 is near it; a cast would write an infinity.
        with pytest.raises(ValueError, match=r"column 'f' cannot hold \S+ exactly \(Float32\)"):
            write(typed, {"f": value})

    @pytest.mark.parametrize("write", WRITES)
    @pytest.mark.parametrize(
        "value, written",
        [
            (0.1, pa.scalar(0.1, pa.float32()).as_py()),
            (FLOAT32_MAX, FLOAT32_MAX),
            (-FLOAT32_MAX, -FLOAT32_MAX),
            (math.inf, math.inf),
            (-math.inf, -math.inf),
        ],
        ids=repr,
    )
    def test_float32_column_takes_the_nearest_float32_in_its_range(self, typed, write, value, written):
        assert self._col(write(typed, {"f": value}), "f")[0] == written

    def test_modify_checks_values_at_any_position(self, typed):
        with pytest.raises(ValueError, match="column 'd'"):
            typed.modify(99, d=9.99)

    def test_update_value_must_be_a_literal(self, typed):
        with pytest.raises(TypeError, match="column 'i': Unsupported literal type"):
            typed.update(lambda r: r.i == 1, i=lambda r: r.i + 1)

    def test_insert_row_must_be_a_mapping(self, typed):
        with pytest.raises(TypeError, match="insert\\(\\) takes a dict of column values, got list"):
            typed.insert(0, [1, 2])
