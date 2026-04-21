from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def load_runner_module():
    repo_root = Path(__file__).resolve().parents[2]
    runner_path = repo_root / "benchmarks" / "autoresearch" / "runner.py"
    spec = importlib.util.spec_from_file_location("ltseq_autoresearch_runner", runner_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_research_supports_output_dir_without_timestamp_guess(tmp_path, monkeypatch):
    runner = load_runner_module()

    report_text = "# fake report\n"
    history = [{"git_sha": "abc123"}]

    monkeypatch.setattr(runner, "load_history", lambda n=10: history)
    monkeypatch.setattr(runner, "generate_report", lambda **kwargs: report_text)

    run_dir = runner.run_research(
        rounds=[1],
        data_flag="--sample",
        skip_profile=True,
        profiler="pyspy",
        bench_core=False,
        iterations=1,
        warmup=0,
        report_only=True,
        output_dir=str(tmp_path / "custom-output"),
    )

    assert run_dir == tmp_path / "custom-output"
    assert run_dir.exists()
    assert (run_dir / "report.md").read_text() == report_text


def test_run_bench_vs_merges_sequential_round_results_without_reusing_stale_json(
    tmp_path, monkeypatch
):
    runner = load_runner_module()

    results_json = tmp_path / "clickbench_results.json"
    monkeypatch.setattr(runner, "BENCHMARKS_DIR", tmp_path)
    monkeypatch.setattr(runner, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(runner.sys, "executable", "/usr/bin/python")

    stale_payload = {
        "rounds": [
            {
                "round_id": "stale",
                "round_name": "stale",
                "benchmark_status": "completed",
                "validation": {"status": "pass"},
                "ltseq": {"median_s": 9.9, "times": [9.9]},
                "duckdb": {"median_s": 9.9, "times": [9.9]},
            }
        ]
    }
    results_json.write_text(json.dumps(stale_payload), encoding="utf-8")

    per_round_payloads = {
        1: {
            "rounds": [
                {
                    "round_id": "r1_top_urls",
                    "round_name": "R1: Top URLs",
                    "benchmark_status": "completed",
                    "validation": {"status": "pass"},
                    "ltseq": {"median_s": 0.9, "times": [0.9]},
                    "duckdb": {"median_s": 1.0, "times": [1.0]},
                }
            ]
        },
        3: {
            "rounds": [
                {
                    "round_id": "r3_funnel",
                    "round_name": "R3: Funnel",
                    "benchmark_status": "infra_failure",
                    "validation": {"status": "infra_failure", "detail": "duckdb crashed"},
                }
            ]
        },
    }

    calls: list[list[str]] = []

    class FakeCompletedProcess:
        def __init__(self, returncode: int):
            self.returncode = returncode

    def fake_run(cmd, cwd=None, text=None):
        calls.append(cmd)
        round_index = cmd.index("--round")
        round_num = int(cmd[round_index + 1])
        results_json.write_text(json.dumps(per_round_payloads[round_num]), encoding="utf-8")
        return FakeCompletedProcess(0)

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    merged = runner.run_bench_vs("--sample", [1, 3], iterations=2, warmup=1)

    assert merged is not None
    assert [round_payload["round_id"] for round_payload in merged["rounds"]] == [
        "r1_top_urls",
        "r3_funnel",
    ]
    assert merged["infra_failures"] == 1
    assert merged["completed_rounds"] == 1
    assert merged["passed"] is False
    assert all(round_payload["round_id"] != "stale" for round_payload in merged["rounds"])
    assert len(calls) == 2


def test_run_bench_vs_returns_none_when_sequential_round_missing_json(tmp_path, monkeypatch):
    runner = load_runner_module()

    results_json = tmp_path / "clickbench_results.json"
    monkeypatch.setattr(runner, "BENCHMARKS_DIR", tmp_path)
    monkeypatch.setattr(runner, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(runner.sys, "executable", "/usr/bin/python")

    class FakeCompletedProcess:
        def __init__(self, returncode: int):
            self.returncode = returncode

    def fake_run(cmd, cwd=None, text=None):
        if results_json.exists():
            results_json.unlink()
        return FakeCompletedProcess(0)

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    assert runner.run_bench_vs("--sample", [2], iterations=2, warmup=1) is None
