"""Tests for #109 naming-consistency aliases.

Every alias is additive: the old name keeps working, the new name behaves
identically. New-semantics methods (find) get behavior tests of their own.
"""

import pytest

from ltseq import LTSeq
from ltseq.advanced_ops import SetOpsMixin
from ltseq.expr import char, str_char


def make_table(rows, schema):
    return LTSeq._from_rows(rows, schema)


@pytest.fixture
def strings_table():
    return make_table(
        [
            {"id": 1, "email": "alice@example.com"},
            {"id": 2, "email": "bob@test.org"},
            {"id": 3, "email": "no-at-sign"},
        ],
        {"id": "int64", "email": "string"},
    )


class TestStrAccessorAlias:
    """.str is an alias for .s (Pandas/Polars muscle memory)."""

    def test_str_on_column(self, strings_table):
        via_s = strings_table.filter(lambda r: r.email.s.contains("@")).to_dicts()
        via_str = strings_table.filter(lambda r: r.email.str.contains("@")).to_dicts()
        assert via_s == via_str
        assert len(via_str) == 2

    def test_str_chains_on_call_result(self, strings_table):
        rows = strings_table.derive(
            up=lambda r: r.email.str.upper().str.contains("ALICE")
        ).to_dicts()
        assert rows[0]["up"] is True or rows[0]["up"] == 1


class TestFind:
    """find(): 0-based position, -1 when not found (Python str.find)."""

    def test_find_found_and_not_found(self, strings_table):
        rows = strings_table.derive(idx=lambda r: r.email.s.find("@")).to_dicts()
        assert rows[0]["idx"] == 5  # "alice@..." → 0-based 5
        assert rows[1]["idx"] == 3  # "bob@..." → 0-based 3
        assert rows[2]["idx"] == -1  # not found

    def test_find_matches_pos_minus_one(self, strings_table):
        rows = strings_table.derive(
            p=lambda r: r.email.s.pos("@"),
            f=lambda r: r.email.s.find("@"),
        ).to_dicts()
        for row in rows:
            assert row["f"] == row["p"] - 1


class TestSplitPart:
    """split_part(): same behavior as split(), SQL-explicit name."""

    def test_split_part_equals_split(self, strings_table):
        rows = strings_table.derive(
            a=lambda r: r.email.s.split("@", 2),
            b=lambda r: r.email.s.split_part("@", 2),
        ).to_dicts()
        for row in rows:
            assert row["a"] == row["b"]
        assert rows[0]["b"] == "example.com"


class TestOrdAndChar:
    """ord() aliases asc(); char() aliases str_char()."""

    def test_ord_equals_asc(self, strings_table):
        rows = strings_table.derive(
            a=lambda r: r.email.s.asc(),
            o=lambda r: r.email.s.ord(),
        ).to_dicts()
        for row in rows:
            assert row["o"] == row["a"]
        assert rows[0]["o"] == ord("a")

    def test_char_equals_str_char(self):
        t = make_table([{"code": 65}, {"code": 97}], {"code": "int64"})
        rows = t.derive(
            c1=lambda r: str_char(r.code),
            c2=lambda r: char(r.code),
        ).to_dicts()
        assert [r["c2"] for r in rows] == ["A", "a"]
        for row in rows:
            assert row["c1"] == row["c2"]


class TestSetOpAliases:
    """concat = union (UNION ALL), subtract = except_."""

    def test_alias_identity(self):
        assert SetOpsMixin.concat is SetOpsMixin.union
        assert SetOpsMixin.subtract is SetOpsMixin.except_

    def test_concat_keeps_duplicates(self):
        t1 = make_table([{"x": 1}, {"x": 2}], {"x": "int64"})
        t2 = make_table([{"x": 2}, {"x": 3}], {"x": "int64"})
        rows = t1.concat(t2).to_dicts()
        assert sorted(r["x"] for r in rows) == [1, 2, 2, 3]

    def test_subtract(self):
        t1 = make_table([{"x": 1}, {"x": 2}, {"x": 3}], {"x": "int64"})
        t2 = make_table([{"x": 2}], {"x": "int64"})
        rows = t1.subtract(t2).to_dicts()
        assert sorted(r["x"] for r in rows) == [1, 3]


class TestLinkAlias:
    """link(alias=) mirrors link(as_=)."""

    def _tables(self):
        orders = make_table(
            [{"oid": 1, "pid": 10}, {"oid": 2, "pid": 20}],
            {"oid": "int64", "pid": "int64"},
        )
        products = make_table(
            [{"id": 10, "pname": "apple"}, {"id": 20, "pname": "banana"}],
            {"id": "int64", "pname": "string"},
        )
        return orders, products

    def test_alias_keyword(self):
        orders, products = self._tables()
        linked = orders.link(products, on=lambda o, p: o.pid == p.id, alias="prod")
        rows = linked.select(lambda r: [r.oid, r.prod.pname]).to_dicts()
        assert rows[0]["prod_pname"] == "apple"

    def test_as_still_works(self):
        orders, products = self._tables()
        linked = orders.link(products, on=lambda o, p: o.pid == p.id, as_="prod")
        rows = linked.select(lambda r: [r.oid, r.prod.pname]).to_dicts()
        assert rows[1]["prod_pname"] == "banana"

    def test_both_raises(self):
        orders, products = self._tables()
        with pytest.raises(ValueError, match="not both"):
            orders.link(products, on=lambda o, p: o.pid == p.id, as_="a", alias="b")

    def test_neither_raises(self):
        orders, products = self._tables()
        with pytest.raises(ValueError, match="alias"):
            orders.link(products, on=lambda o, p: o.pid == p.id)


class TestOverDesc:
    """over(desc=) mirrors over(descending=)."""

    def test_desc_equals_descending(self):
        from ltseq import row_number

        t = make_table(
            [{"g": "a", "v": 1}, {"g": "a", "v": 3}, {"g": "b", "v": 2}],
            {"g": "string", "v": "int64"},
        )
        via_descending = t.derive(
            rn=lambda r: row_number().over(order_by=r.v, descending=True)
        ).to_dicts()
        via_desc = t.derive(
            rn=lambda r: row_number().over(order_by=r.v, desc=True)
        ).to_dicts()
        assert via_desc == via_descending
        by_v = {row["v"]: row["rn"] for row in via_desc}
        assert by_v[3] == 1  # largest value ranks first when descending
