"""Private pipeline diagnostics, ``LTSeq._explain_pipeline()`` (#238).

The diagnostics plan a table as its terminals would and report the result as
data: column environment, declared order, both plans, and the physical
operator tree with per-operator counts. They never execute the plan and read
the same planning ``explain_plan()`` does, rather than re-deriving it.
"""

from collections import Counter

import pyarrow as pa
import pytest

from ltseq import LTSeq

FIELDS = {
    "columns",
    "sort_keys",
    "logical_plan",
    "physical_plan",
    "operators",
    "operator_counts",
}


def _table():
    return LTSeq.from_arrow(pa.table({"g": [2, 1, 2], "v": [10.0, None, 30.0]}))


class TestShape:
    def test_reports_exactly_the_documented_fields(self):
        """Adding a field is a deliberate change of this private contract:
        update the docstring of ``LTSeq._explain_pipeline`` and this set."""
        assert set(_table()._explain_pipeline()) == FIELDS

    def test_column_environment(self):
        explained = _table().derive(w=lambda r: r.v * 2)._explain_pipeline()
        assert explained["columns"] == [
            {"name": "g", "dtype": "int64", "nullable": True},
            {"name": "v", "dtype": "float64", "nullable": True},
            {"name": "w", "dtype": "float64", "nullable": True},
        ]

    def test_sort_keys_are_the_declared_order(self):
        assert _table()._explain_pipeline()["sort_keys"] == []
        # A computed key ends the declared order: only "g" is declared.
        sorted_t = _table().sort("g", lambda r: r.v * 2, desc=[True, False])
        explained = sorted_t._explain_pipeline()
        assert explained["sort_keys"] == sorted_t._inner.get_sort_keys()
        assert explained["sort_keys"] == [("g", True)]

    def test_operators_are_a_pre_order_walk_and_counts_tally_them(self):
        explained = _table().filter(lambda r: r.g > 1).sort("v")._explain_pipeline()
        operators = explained["operators"]
        assert operators[0]["depth"] == 0
        for i, op in enumerate(operators[1:], start=1):
            # Each node is a child of some node before it.
            assert any(p["depth"] + 1 == op["depth"] for p in operators[:i]), operators
            assert op["partitions"] >= 1
        assert explained["operator_counts"] == dict(Counter(op["name"] for op in operators))


class TestSamePlanningAsExplainPlan:
    def test_plans_match_explain_plan(self):
        t = _table().filter(lambda r: r.g > 1).sort("g")
        logical, physical = t.explain_plan()
        explained = t._explain_pipeline()
        assert explained["logical_plan"] == logical
        assert explained["physical_plan"] == physical

    def test_every_operator_appears_in_the_physical_plan_text(self):
        explained = _table().sort("g").derive(c=lambda r: r.v.cum_sum())._explain_pipeline()
        for name in explained["operator_counts"]:
            assert f"{name}:" in explained["physical_plan"], name

    def test_never_executes(self):
        """A cast that fails on its data raises when the query runs; the
        diagnostics plan the same query and succeed."""
        t = LTSeq.from_arrow(pa.table({"s": ["1", "abc"]})).derive(n=lambda r: r.s.cast("int64"))
        explained = t._explain_pipeline()
        assert "ProjectionExec" in explained["operator_counts"]
        with pytest.raises(ValueError, match="Cast error"):
            t.to_pandas()

    def test_unloaded_table_raises(self):
        with pytest.raises(Exception):
            LTSeq()._explain_pipeline()


class TestOperatorCounts:
    """The structured counts replace searches of the physical-plan text."""

    def test_sort_on_undeclared_order_plans_one_sort(self, engine_config):
        engine_config(target_partitions=1)
        counts = _table().sort("g")._explain_pipeline()["operator_counts"]
        assert counts.get("SortExec") == 1, counts

    def test_sort_on_declared_order_plans_none(self, engine_config):
        engine_config(target_partitions=1)
        declared = _table().sort("g").assume_sorted("g")
        counts = declared.sort("g")._explain_pipeline()["operator_counts"]
        assert "SortExec" not in counts, counts

    def test_parquet_scan_reports_its_file_groups(self, tmp_path, engine_config):
        engine_config(target_partitions=4)
        import pyarrow.parquet as pq

        for i in range(3):
            pq.write_table(pa.table({"x": [2 - i]}), tmp_path / f"part-{i}.parquet")
        t = LTSeq.read_parquet(str(tmp_path))
        explained = t._explain_pipeline()
        scans = [op for op in explained["operators"] if op["name"] == "DataSourceExec"]
        assert len(scans) == 1, explained["physical_plan"]
        assert scans[0]["partitions"] == 3, explained["physical_plan"]

        # Sorting three partitions sorts each and merges them.
        sorted_plan = t.sort("x")._explain_pipeline()
        counts = sorted_plan["operator_counts"]
        assert counts.get("SortExec") == 1, sorted_plan["physical_plan"]
        assert counts.get("SortPreservingMergeExec") == 1, sorted_plan["physical_plan"]
        assert sorted_plan["operators"][0]["name"] == "SortPreservingMergeExec"
        assert sorted_plan["operators"][0]["partitions"] == 1
