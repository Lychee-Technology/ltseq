"""Tests for #111: exception hierarchy, _repr_html_, __iter__."""

import pytest

from ltseq import (
    LTSeq,
    LTSeqError,
    SortRequiredError,
    SchemaMismatchError,
    ColumnNotFoundError,
)
from ltseq.expr import SchemaProxy


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


@pytest.fixture
def sample():
    return make_table(
        [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}],
        {"id": "int64", "v": "string"},
    )


class TestExceptionHierarchy:
    def test_isinstance_lattice(self):
        assert issubclass(SortRequiredError, LTSeqError)
        assert issubclass(SortRequiredError, ValueError)
        assert issubclass(SchemaMismatchError, LTSeqError)
        assert issubclass(SchemaMismatchError, ValueError)
        assert issubclass(ColumnNotFoundError, LTSeqError)
        assert issubclass(ColumnNotFoundError, ValueError)
        assert issubclass(ColumnNotFoundError, AttributeError)

    def test_column_not_found_from_proxy(self):
        proxy = SchemaProxy({"id": "int64"})
        with pytest.raises(ColumnNotFoundError):
            _ = proxy.nope
        # Also catchable as each base
        with pytest.raises(AttributeError):
            _ = proxy.nope
        with pytest.raises(LTSeqError):
            _ = proxy.nope

    def test_hasattr_protocol_intact(self):
        """ColumnNotFoundError subclasses AttributeError, so hasattr works."""
        proxy = SchemaProxy({"id": "int64"})
        assert hasattr(proxy, "id") is True
        assert hasattr(proxy, "nope") is False

    def test_filter_bad_column_raises_column_not_found(self, sample):
        with pytest.raises(ColumnNotFoundError):
            sample.filter(lambda r: r.nonexistent > 0)
        # Backward compat: still catchable as ValueError
        with pytest.raises(ValueError):
            sample.filter(lambda r: r.nonexistent > 0)

    def test_sort_required_from_merge_join(self):
        a = make_table([{"id": 3}, {"id": 1}], {"id": "int64"})
        b = make_table([{"id": 1}], {"id": "int64"})
        with pytest.raises(SortRequiredError):
            a.join(b, on=lambda x, y: x.id == y.id, strategy="merge")
        # Backward compat
        with pytest.raises(ValueError):
            a.join(b, on=lambda x, y: x.id == y.id, strategy="merge")

    def test_schema_mismatch_from_union_rust_bridge(self):
        """Rust-origin error routes through the ltseq.exceptions bridge."""
        c = make_table([{"x": 1}], {"x": "int64"})
        d = make_table([{"y": 1}], {"y": "int64"})
        with pytest.raises(SchemaMismatchError):
            c.union(d)
        with pytest.raises(LTSeqError):
            c.union(d)
        with pytest.raises(ValueError):
            c.union(d)

    def test_column_not_found_from_linked_proxy(self):
        orders = make_table([{"oid": 1, "pid": 10}], {"oid": "int64", "pid": "int64"})
        products = make_table([{"id": 10, "pname": "x"}], {"id": "int64", "pname": "string"})
        linked = orders.link(products, on=lambda o, p: o.pid == p.id, as_="prod")
        with pytest.raises(ColumnNotFoundError):
            linked.select(lambda r: [r.prod.nonexistent])


class TestReprHtml:
    def test_repr_html_wraps_show(self, sample):
        html = sample._repr_html_()
        assert "<pre>" in html
        assert "</pre>" in html
        assert "rows" in html  # dimensions caption

    def test_repr_html_empty(self):
        t = LTSeq()
        html = t._repr_html_()
        assert "<pre>" in html


class TestIter:
    def test_iter_yields_row_dicts(self, sample):
        rows = list(sample)
        assert rows == [
            {"id": 1, "v": "a"},
            {"id": 2, "v": "b"},
            {"id": 3, "v": "c"},
        ]

    def test_iter_in_for_loop(self, sample):
        ids = [row["id"] for row in sample]
        assert ids == [1, 2, 3]

    def test_iter_after_transform(self, sample):
        rows = list(sample.filter(lambda r: r.id > 1))
        assert [r["id"] for r in rows] == [2, 3]
