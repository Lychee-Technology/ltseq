"""Regression tests for issue #95 (Bug 2): metadata invalidation of the
Parquet fast path.

filter() used to keep source_parquet_path while returning a lazy filtered
DataFrame; downstream Parquet fast paths (group_ordered count) re-read the
raw file directly, silently ignoring the filter.
"""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq


@pytest.fixture
def presorted_parquet():
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        path = f.name
    t = pa.table({"ts": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]})
    pq.write_table(t, path)
    yield path
    os.unlink(path)


class TestFilterBeforeGroupOrderedCount:
    def test_filter_invalidates_parquet_fast_path(self, presorted_parquet):
        """filter → group_ordered → count must NOT use raw Parquet fast path."""
        t = LTSeq.read_parquet(presorted_parquet).assume_sorted("ts")
        filtered = t.filter(lambda r: r.val >= 30)
        # All values are distinct, so every row starts a new group: the group
        # count equals the row count. Filtered: 3. Raw (unfiltered) file: 5.
        grouped = filtered.group_ordered(lambda r: r.val != r.val.shift(1))
        count = grouped.first().count()
        assert count == 3
