from __future__ import annotations

import importlib
import sys
from pathlib import Path


def load_report_module():
    repo_root = Path(__file__).resolve().parents[2]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)
    return importlib.import_module("benchmarks.autoresearch.report")


def test_generate_report_labels_explicit_sample_data_as_sample():
    report = load_report_module()

    rendered = report.generate_report(
        run_dir=Path("results/test-run"),
        bench_vs_results={"data_file": "benchmarks/data/hits_sample.parquet", "rounds": []},
        bench_core_results=None,
        hotspots={},
        hypotheses=[],
        history=[],
        data_flag="--data=benchmarks/data/hits_sample.parquet",
    )

    assert "**Dataset**: 1M-row sample" in rendered


def test_generate_report_labels_custom_data_path():
    report = load_report_module()

    rendered = report.generate_report(
        run_dir=Path("results/test-run"),
        bench_vs_results={"data_file": "/tmp/custom-bench.parquet", "rounds": []},
        bench_core_results=None,
        hotspots={},
        hypotheses=[],
        history=[],
        data_flag="--data=/tmp/custom-bench.parquet",
    )

    assert "**Dataset**: custom dataset (/tmp/custom-bench.parquet)" in rendered
