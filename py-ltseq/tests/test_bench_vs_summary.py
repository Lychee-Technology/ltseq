from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path


def load_bench_vs_module():
    repo_root = Path(__file__).resolve().parents[2]
    bench_vs_path = repo_root / "benchmarks" / "bench_vs.py"

    fake_duckdb = types.SimpleNamespace(sql=lambda *_args, **_kwargs: None)
    fake_psutil = types.SimpleNamespace(
        Process=lambda *_args, **_kwargs: None,
        virtual_memory=lambda: types.SimpleNamespace(total=0),
    )
    fake_ltseq = types.SimpleNamespace(LTSeq=object)

    sys.modules.setdefault("duckdb", fake_duckdb)
    sys.modules.setdefault("psutil", fake_psutil)
    sys.modules.setdefault("ltseq", fake_ltseq)

    spec = importlib.util.spec_from_file_location("ltseq_bench_vs", bench_vs_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_mark_infra_failure_adds_machine_readable_validation():
    bench_vs = load_bench_vs_module()

    round_result = bench_vs.make_round_result("r1_top_urls", "R1: Top URLs")
    bench_vs.mark_infra_failure(round_result, RuntimeError("duckdb unavailable"))

    assert round_result["benchmark_status"] == "infra_failure"
    assert round_result["error"] == "duckdb unavailable"
    assert round_result["validation"]["status"] == "infra_failure"
    assert round_result["validation"]["detail"] == "duckdb unavailable"
    assert round_result["validation"]["error"] == "duckdb unavailable"


def test_save_results_writes_gating_summary_fields(tmp_path, monkeypatch):
    bench_vs = load_bench_vs_module()

    monkeypatch.setattr(bench_vs, "BENCHMARKS_DIR", str(tmp_path))
    monkeypatch.setattr(
        bench_vs,
        "get_system_info",
        lambda: {"platform": "test", "cpu_count": 1, "ram_gb": 1.0, "python_version": "3.x"},
    )

    passing_round = bench_vs.make_round_result("r1_top_urls", "R1: Top URLs")
    passing_round["duckdb"] = {"median_s": 1.0}
    passing_round["ltseq"] = {"median_s": 0.8}
    passing_round["validation"] = bench_vs.make_validation(
        "pass",
        "same top 10 URLs",
        ["a"],
        ["a"],
        compared_metric="top_10_urls",
    )

    failing_round = bench_vs.make_round_result("r2_sessionization", "R2: Sessionization")
    bench_vs.mark_infra_failure(failing_round, RuntimeError("ltseq crashed"))

    bench_vs.save_results(
        [passing_round, failing_round],
        data_file="benchmarks/data/hits_sample.parquet",
        warmup=1,
        iterations=3,
    )

    payload = json.loads((tmp_path / "clickbench_results.json").read_text())

    assert payload["passed"] is False
    assert payload["correctness_failures"] == 0
    assert payload["infra_failures"] == 1
    assert payload["completed_rounds"] == 1
    assert payload["total_rounds"] == 2
    assert payload["rounds"][0]["validation"]["status"] == "pass"
    assert payload["rounds"][1]["benchmark_status"] == "infra_failure"
    assert payload["rounds"][1]["validation"]["status"] == "infra_failure"


def test_print_results_table_handles_infra_failure_rounds(capsys):
    bench_vs = load_bench_vs_module()

    passing_round = bench_vs.make_round_result("r3_funnel", "R3: Funnel")
    passing_round["duckdb"] = {"median_s": 0.5}
    passing_round["ltseq"] = {"median_s": 0.25}
    passing_round["validation"] = bench_vs.make_validation(
        "pass",
        "both engines returned 123 matches",
        123,
        123,
        compared_metric="funnel_match_count",
    )

    failing_round = bench_vs.make_round_result("r1_top_urls", "R1: Top URLs")
    bench_vs.mark_infra_failure(failing_round, RuntimeError("pandas unavailable"))

    bench_vs.print_results_table([failing_round, passing_round])

    output = capsys.readouterr().out
    assert "R1: Top URLs" in output
    assert "R3: Funnel" in output
    assert "n/a" in output
