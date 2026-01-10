"""
Phase 4.2 Tests: Row Numbering and Ranking Functions

Tests for row_number(), rank(), dense_rank(), and ntile() window functions.
These functions provide different ways to assign row numbers/ranks to data.
"""

import pytest
from ltseq import LTSeq, row_number, rank, dense_rank, ntile
import csv
import tempfile
import os


@pytest.fixture
def sample_csv():
    """Create a temporary CSV with data for ranking operations."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["id", "group", "score", "date"])
        writer.writeheader()
        data = [
            {"id": "1", "group": "A", "score": "100", "date": "2024-01-01"},
            {"id": "2", "group": "A", "score": "90", "date": "2024-01-02"},
            {
                "id": "3",
                "group": "A",
                "score": "100",
                "date": "2024-01-03",
            },  # Tie with id=1
            {"id": "4", "group": "B", "score": "85", "date": "2024-01-01"},
            {"id": "5", "group": "B", "score": "95", "date": "2024-01-02"},
            {
                "id": "6",
                "group": "B",
                "score": "95",
                "date": "2024-01-03",
            },  # Tie with id=5
        ]
        for row in data:
            writer.writerow(row)
        temp_path = f.name

    yield temp_path
    os.unlink(temp_path)


@pytest.fixture
def simple_csv():
    """Create a simple CSV for basic row_number tests."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["value", "date"])
        writer.writeheader()
        data = [
            {"value": "10", "date": "2024-01-01"},
            {"value": "20", "date": "2024-01-02"},
            {"value": "30", "date": "2024-01-03"},
            {"value": "40", "date": "2024-01-04"},
            {"value": "50", "date": "2024-01-05"},
        ]
        for row in data:
            writer.writerow(row)
        temp_path = f.name

    yield temp_path
    os.unlink(temp_path)


class TestRowNumber:
    """Test row_number() window function."""

    def test_row_number_basic(self, simple_csv):
        """row_number() should assign sequential numbers to rows."""
        t = LTSeq.read_csv(simple_csv)
        result = t.derive(lambda r: {"rn": row_number().over(order_by=r.date)})

        df = result.to_pandas()
        assert "rn" in df.columns
        # Row numbers should be 1, 2, 3, 4, 5
        assert list(df.sort_values("date")["rn"]) == [1, 2, 3, 4, 5]

    def test_row_number_with_partition(self, sample_csv):
        """row_number() with partition_by should restart numbering per group."""
        t = LTSeq.read_csv(sample_csv)
        result = t.derive(
            lambda r: {"rn": row_number().over(partition_by=r.group, order_by=r.date)}
        )

        df = result.to_pandas()
        # Group A should have 1, 2, 3
        group_a = df[df["group"] == "A"].sort_values("date")
        assert list(group_a["rn"]) == [1, 2, 3]
        # Group B should have 1, 2, 3
        group_b = df[df["group"] == "B"].sort_values("date")
        assert list(group_b["rn"]) == [1, 2, 3]

    def test_row_number_descending(self, simple_csv):
        """row_number() with descending=True should order in reverse."""
        t = LTSeq.read_csv(simple_csv)
        result = t.derive(
            lambda r: {"rn": row_number().over(order_by=r.date, descending=True)}
        )

        df = result.to_pandas()
        # Earliest date should get highest row number
        earliest = df[df["date"] == "2024-01-01"]["rn"].values[0]
        latest = df[df["date"] == "2024-01-05"]["rn"].values[0]
        assert earliest > latest

    def test_row_number_returns_expr(self):
        """row_number() should return a CallExpr that can be chained."""
        from ltseq.expr import CallExpr
        from ltseq.expr.types import WindowExpr

        expr = row_number()
        assert isinstance(expr, CallExpr)

        # .over() should return a WindowExpr
        from ltseq.expr import SchemaProxy

        schema = {"date": "date"}
        r = SchemaProxy(schema)
        window_expr = row_number().over(order_by=r.date)
        assert isinstance(window_expr, WindowExpr)


class TestRank:
    """Test rank() window function."""

    def test_rank_with_ties(self, sample_csv):
        """rank() should assign the same rank to ties, with gaps after."""
        t = LTSeq.read_csv(sample_csv)
        # Group A has scores: 100, 90, 100 - ties at 100
        result = t.filter(lambda r: r.group == "A").derive(
            lambda r: {"rank": rank().over(order_by=r.score, descending=True)}
        )

        df = result.to_pandas()
        # Two rows with score=100 should both get rank 1
        # Row with score=90 should get rank 3 (gap after tie)
        top_ranks = df[df["score"] == 100]["rank"].unique()
        assert len(top_ranks) == 1
        assert top_ranks[0] == 1

        bottom_rank = df[df["score"] == 90]["rank"].values[0]
        assert bottom_rank == 3  # Gap: 1, 1, 3 (not 1, 1, 2)

    def test_rank_with_partition(self, sample_csv):
        """rank() with partition_by should rank within each group."""
        t = LTSeq.read_csv(sample_csv)
        result = t.derive(
            lambda r: {
                "rank": rank().over(
                    partition_by=r.group, order_by=r.score, descending=True
                )
            }
        )

        df = result.to_pandas()
        # Each group should have its own ranking
        for group in ["A", "B"]:
            group_df = df[df["group"] == group]
            # Max rank in each group should be at most 3
            assert group_df["rank"].max() <= 3


class TestDenseRank:
    """Test dense_rank() window function."""

    def test_dense_rank_no_gaps(self, sample_csv):
        """dense_rank() should assign ranks without gaps after ties."""
        t = LTSeq.read_csv(sample_csv)
        # Group A has scores: 100, 90, 100 - ties at 100
        result = t.filter(lambda r: r.group == "A").derive(
            lambda r: {"drank": dense_rank().over(order_by=r.score, descending=True)}
        )

        df = result.to_pandas()
        # Two rows with score=100 should both get rank 1
        # Row with score=90 should get rank 2 (no gap)
        top_ranks = df[df["score"] == 100]["drank"].unique()
        assert len(top_ranks) == 1
        assert top_ranks[0] == 1

        bottom_rank = df[df["score"] == 90]["drank"].values[0]
        assert bottom_rank == 2  # No gap: 1, 1, 2 (not 1, 1, 3)

    def test_dense_rank_vs_rank(self, sample_csv):
        """Compare dense_rank() and rank() behavior with ties."""
        t = LTSeq.read_csv(sample_csv)
        result = t.filter(lambda r: r.group == "A").derive(
            lambda r: {
                "rank": rank().over(order_by=r.score, descending=True),
                "drank": dense_rank().over(order_by=r.score, descending=True),
            }
        )

        df = result.to_pandas()
        # For tied values, rank and dense_rank are the same
        ties = df[df["score"] == 100]
        assert all(ties["rank"] == ties["drank"])

        # For the non-tied value, dense_rank should be lower than rank
        non_tie = df[df["score"] == 90]
        assert non_tie["drank"].values[0] < non_tie["rank"].values[0]


class TestNtile:
    """Test ntile() window function."""

    def test_ntile_basic(self, simple_csv):
        """ntile(n) should divide rows into n roughly equal buckets."""
        t = LTSeq.read_csv(simple_csv)
        result = t.derive(lambda r: {"quartile": ntile(4).over(order_by=r.value)})

        df = result.to_pandas()
        assert "quartile" in df.columns
        # With 5 rows and 4 buckets, expect distribution like [1,1,2,3,4] or similar
        quartiles = sorted(df["quartile"].tolist())
        assert min(quartiles) >= 1
        assert max(quartiles) <= 4

    def test_ntile_two_buckets(self, simple_csv):
        """ntile(2) should divide into two halves."""
        t = LTSeq.read_csv(simple_csv)
        result = t.derive(lambda r: {"half": ntile(2).over(order_by=r.value)})

        df = result.to_pandas()
        # Should have roughly 2-3 in each half
        counts = df["half"].value_counts()
        assert len(counts) == 2
        assert all(c >= 2 for c in counts.values)

    def test_ntile_with_partition(self, sample_csv):
        """ntile() with partition_by should bucket within each group."""
        t = LTSeq.read_csv(sample_csv)
        result = t.derive(
            lambda r: {"tercile": ntile(3).over(partition_by=r.group, order_by=r.score)}
        )

        df = result.to_pandas()
        # Each group has 3 rows, ntile(3) should give each a different bucket
        for group in ["A", "B"]:
            group_df = df[df["group"] == group]
            # With 3 rows and 3 buckets, each row gets a different bucket
            assert sorted(group_df["tercile"].tolist()) == [1, 2, 3]


class TestMultipleRankingColumns:
    """Test using multiple ranking functions in a single derive."""

    def test_multiple_rankings_same_derive(self, sample_csv):
        """Multiple ranking functions can be used in a single derive()."""
        t = LTSeq.read_csv(sample_csv)
        result = t.derive(
            lambda r: {
                "rn": row_number().over(partition_by=r.group, order_by=r.date),
                "score_rank": rank().over(
                    partition_by=r.group, order_by=r.score, descending=True
                ),
                "score_drank": dense_rank().over(
                    partition_by=r.group, order_by=r.score, descending=True
                ),
            }
        )

        df = result.to_pandas()
        assert "rn" in df.columns
        assert "score_rank" in df.columns
        assert "score_drank" in df.columns

    def test_ranking_with_other_columns(self, sample_csv):
        """Ranking functions can be used alongside regular column operations."""
        t = LTSeq.read_csv(sample_csv)
        result = t.derive(
            lambda r: {
                "rn": row_number().over(order_by=r.date),
                "score_doubled": r.score * 2,
            }
        )

        df = result.to_pandas()
        assert "rn" in df.columns
        assert "score_doubled" in df.columns
        # Verify regular expression works correctly
        assert df["score_doubled"].tolist() == [s * 2 for s in df["score"].tolist()]


class TestRankingSerialization:
    """Test that ranking expressions serialize correctly."""

    def test_row_number_serialization(self):
        """row_number().over() should serialize to Window expression."""
        from ltseq.expr import SchemaProxy

        schema = {"date": "date", "group": "string"}
        r = SchemaProxy(schema)

        expr = row_number().over(order_by=r.date)
        serialized = expr.serialize()

        assert serialized["type"] == "Window"
        assert serialized["expr"]["type"] == "Call"
        assert serialized["expr"]["func"] == "row_number"
        assert serialized["order_by"]["type"] == "Column"
        assert serialized["order_by"]["name"] == "date"

    def test_ntile_serialization(self):
        """ntile(n).over() should serialize with the n argument."""
        from ltseq.expr import SchemaProxy

        schema = {"value": "int64"}
        r = SchemaProxy(schema)

        expr = ntile(4).over(order_by=r.value)
        serialized = expr.serialize()

        assert serialized["type"] == "Window"
        assert serialized["expr"]["type"] == "Call"
        assert serialized["expr"]["func"] == "ntile"
        assert serialized["expr"]["args"][0]["type"] == "Literal"
        assert serialized["expr"]["args"][0]["value"] == 4

    def test_partition_and_order_serialization(self):
        """Partition and order_by should both serialize correctly."""
        from ltseq.expr import SchemaProxy

        schema = {"date": "date", "group": "string", "score": "int64"}
        r = SchemaProxy(schema)

        expr = rank().over(partition_by=r.group, order_by=r.score, descending=True)
        serialized = expr.serialize()

        assert serialized["type"] == "Window"
        assert serialized["partition_by"]["type"] == "Column"
        assert serialized["partition_by"]["name"] == "group"
        assert serialized["order_by"]["type"] == "Column"
        assert serialized["order_by"]["name"] == "score"
        assert serialized["descending"] is True
