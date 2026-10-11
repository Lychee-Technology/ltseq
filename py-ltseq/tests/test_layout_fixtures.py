"""The layout fixtures pin what they claim (#274).

These check the infrastructure itself: a pinned engine layout reaches the
tables built under it and only those, and every input layout reads back the
rows it was given. Row order across partitions is not asserted: multi-batch
and multi-file inputs lose it today even at ten rows (#148, fixed by M1),
which is what these fixtures exist to expose. At one partition every layout
keeps it.
"""

from __future__ import annotations

import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq._testing import engine_config, pinned_engine

from . import layouts
from .layouts import BATCH_SIZES, CPUS, PARTITION_COUNTS

ROWS = pa.table(
    {
        "i": pa.array(range(10), pa.int64()),
        "s": pa.array([None if i % 3 == 1 else f"s{i}" for i in range(10)], pa.string()),
        "f": pa.array([i / 2 for i in range(10)], pa.float64()),
        "b": pa.array([i % 2 == 0 for i in range(10)], pa.bool_()),
    }
)


def file_groups(table: LTSeq) -> int:
    """The number of file groups (partitions) of a file scan's plan."""
    match = re.search(r"file_groups=\{(\d+) groups?", table.explain_plan()[1])
    assert match, table.explain_plan()[1]
    return int(match.group(1))


def batch_rows(table: LTSeq) -> list[int]:
    return [batch.num_rows for batch in pa.RecordBatchReader.from_stream(table)]


def rows_by_key(table: pa.Table) -> list[dict]:
    return sorted(table.to_pylist(), key=lambda row: row["i"])


class TestPinnedEngine:
    def test_pins_inside_the_block_and_restores_after(self):
        before = engine_config()
        with pinned_engine(target_partitions=3, batch_size=5) as config:
            assert config == engine_config()
            assert (config["target_partitions"], config["batch_size"]) == (3, 5)
        assert engine_config() == before

    def test_none_is_the_engine_default(self):
        with pinned_engine(target_partitions=3, batch_size=5):
            with pinned_engine(batch_size=7) as config:
                assert config["target_partitions"] == config["default_target_partitions"]
                assert config["batch_size"] == 7
            assert engine_config()["target_partitions"] == 3

    def test_restores_when_the_block_raises(self):
        before = engine_config()
        with pytest.raises(RuntimeError, match="inside"):
            with pinned_engine(target_partitions=3):
                raise RuntimeError("inside")
        assert engine_config() == before

    @pytest.mark.parametrize("argument", ["target_partitions", "batch_size"])
    def test_refuses_zero_and_keeps_the_layout(self, argument):
        before = engine_config()
        with pytest.raises(ValueError, match=f"{argument} must be at least 1"):
            with pinned_engine(**{argument: 0}):
                pass
        assert engine_config() == before

    def test_a_table_keeps_the_layout_of_its_reader_call(self, tmp_path):
        directory = layouts.write_parquet(ROWS, tmp_path / "files", files=3)
        with pinned_engine(target_partitions=2):
            pinned = LTSeq.read_parquet(str(directory))
        with pinned_engine(target_partitions=1):
            single = LTSeq.read_parquet(str(directory))
            assert file_groups(pinned.filter(lambda r: r.i >= 0)) == 2
        assert (file_groups(pinned), file_groups(single)) == (2, 1)


class TestEngineLayoutFixture:
    def test_sweeps_partitions_and_batch_sizes(self):
        assert sorted(PARTITION_COUNTS.values()) == [1, 2, 2 * CPUS]
        assert list(BATCH_SIZES.values()) == [1, 7, None]

    def test_reaches_the_tables_the_test_builds(self, engine_layout, tmp_path):
        rows = pa.table({"i": pa.array(range(20), pa.int64())})
        table = LTSeq.read_parquet(str(layouts.write_parquet(rows, tmp_path, files=3)))
        assert engine_layout == engine_config()
        assert file_groups(table) == min(engine_layout["target_partitions"], 3)
        sizes = batch_rows(table)
        assert sum(sizes) == 20
        assert max(sizes) <= engine_layout["batch_size"]


def test_engine_config_fixture_pins_until_the_test_ends(engine_config):
    config = engine_config(target_partitions=3, batch_size=5)
    assert (config["target_partitions"], config["batch_size"]) == (3, 5)
    config = engine_config(batch_size=9)
    assert (config["target_partitions"], config["batch_size"]) == (
        config["default_target_partitions"],
        9,
    )


class TestDataLayouts:
    @pytest.mark.parametrize("n", [0, 1, 10])
    def test_reads_back_the_same_rows(self, data_layout, engine_layout, n):
        rows = ROWS.slice(0, n)
        result = layouts.plain_types(data_layout(rows).to_arrow())
        assert rows_by_key(result) == rows.to_pylist()
        if n:
            assert result.schema == rows.schema

    @pytest.mark.parametrize("batch_size", [1, None])
    def test_one_partition_keeps_input_order(self, data_layout, engine_config, batch_size):
        engine_config(target_partitions=1, batch_size=batch_size)
        result = layouts.plain_types(data_layout(ROWS).to_arrow())
        assert result.to_pylist() == ROWS.to_pylist()

    def test_many_means_several_batches_files_and_row_groups(self, tmp_path, engine_config):
        engine_config(target_partitions=1)
        assert batch_rows(layouts.build("one_batch", ROWS, tmp_path / "a")) == [10]
        assert batch_rows(layouts.build("many_batches", ROWS, tmp_path / "b")) == [4, 4, 2]

        one_file = layouts.write_parquet(ROWS, tmp_path / "c", files=1, row_groups=3)
        metadata = pq.read_metadata(one_file)
        groups = [metadata.row_group(k).num_rows for k in range(metadata.num_row_groups)]
        assert groups == [4, 4, 2]

        for suffix, directory in [
            ("parquet", layouts.write_parquet(ROWS, tmp_path / "d", files=3)),
            ("csv", layouts.write_csv(ROWS, tmp_path / "e", files=3)),
        ]:
            names = sorted(path.name for path in directory.iterdir())
            assert names == [f"part-0000{k}.{suffix}" for k in range(3)]

    def test_csv_floats_read_back_as_floats_in_every_file(self, tmp_path):
        # The first file holds only whole floats, which pyarrow alone would
        # write as integers.
        rows = pa.table({"f": pa.array([1.0, 2.0, 0.5, -0.0, 1e300], pa.float64())})
        table = LTSeq.read_csv(str(layouts.write_csv(rows, tmp_path, files=3)))
        result = table.to_arrow()
        assert result.schema.field("f").type == pa.float64()
        assert sorted(result.column("f").to_pylist()) == sorted(rows.column("f").to_pylist())

    def test_split_keeps_rows_contiguous_and_non_empty(self):
        assert [part.num_rows for part in layouts.split(ROWS, 3)] == [4, 4, 2]
        assert [part.num_rows for part in layouts.split(ROWS.slice(0, 2), 3)] == [1, 1]
        assert [part.num_rows for part in layouts.split(ROWS.slice(0, 0), 3)] == [0]
        assert pa.concat_tables(layouts.split(ROWS, 3)).equals(ROWS)
