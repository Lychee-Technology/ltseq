"""Regression test for prepare_data.py's sample column casing.

The raw ClickBench parquet uses PascalCase column names (URL, UserID,
EventTime, WatchID).  ``sort_data`` renames them to lowercase, and both
``bench_vs.py`` and LTSeq/DataFusion (which is case-sensitive) rely on the
lowercase names.  ``create_sample`` used a bare ``SELECT *`` that preserved
whatever case the source had, so when the sorted file was absent (e.g. the
14GB sort was skipped or OOM-killed) the sample fell back to the raw file and
kept PascalCase — making every LTSeq round fail with "Column 'url' not found".
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def load_prepare_data_module():
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "benchmarks" / "prepare_data.py"
    spec = importlib.util.spec_from_file_location("ltseq_prepare_data", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_create_sample_lowercases_pascalcase_columns(tmp_path, monkeypatch):
    """A sample taken from the raw PascalCase file must come out lowercase."""
    pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    prepare_data = load_prepare_data_module()

    raw = tmp_path / "hits.parquet"
    sorted_ = tmp_path / "hits_sorted.parquet"  # intentionally absent
    sample = tmp_path / "hits_sample.parquet"

    # Mirror the raw ClickBench schema's casing on the columns the benchmark uses.
    table = pa.table(
        {
            "URL": ["a", "b", "c"],
            "UserID": [1, 2, 3],
            "EventTime": [10, 20, 30],
            "WatchID": [100, 200, 300],
        }
    )
    pq.write_table(table, raw)

    monkeypatch.setattr(prepare_data, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(prepare_data, "HITS_RAW", str(raw))
    monkeypatch.setattr(prepare_data, "HITS_SORTED", str(sorted_))
    monkeypatch.setattr(prepare_data, "HITS_SAMPLE", str(sample))

    prepare_data.create_sample()

    cols = pq.read_schema(sample).names
    assert cols == ["url", "userid", "eventtime", "watchid"], cols
