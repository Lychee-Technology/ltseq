from __future__ import annotations

import importlib
import json
import math
import sys
from pathlib import Path

import pytest


def load_pilot_modules():
    repo_root = Path(__file__).resolve().parents[2]
    autoresearch_root = repo_root / "benchmarks" / "autoresearch"
    autoresearch_root_str = str(autoresearch_root)
    if autoresearch_root_str not in sys.path:
        sys.path.insert(0, autoresearch_root_str)

    common = importlib.import_module("pilot.common")
    evaluate = importlib.import_module("pilot.scripts.evaluate_benchmark_candidate")
    gate = importlib.import_module("pilot.scripts.benchmark_gate")
    return common, evaluate, gate


def load_capture_module(module_name: str):
    repo_root = Path(__file__).resolve().parents[2]
    autoresearch_root = repo_root / "benchmarks" / "autoresearch"
    autoresearch_root_str = str(autoresearch_root)
    if autoresearch_root_str not in sys.path:
        sys.path.insert(0, autoresearch_root_str)
    return importlib.import_module(module_name)


def make_summary(*, target: str, r1: tuple[float, float], r2: tuple[float, float], r3: tuple[float, float]) -> dict:
    target_workload = "r3_funnel" if target == "clickbench_funnel" else "r2_sessionization"

    return {
        "target": target,
        "git_sha": "abc123",
        "timestamp": "2026-04-20T12:00:00",
        "data_file": "benchmarks/data/hits_sorted.parquet",
        "warmup": 1,
        "iterations": 3,
        "passed": True,
        "correctness_failures": 0,
        "infra_failures": 0,
        "workloads": [
            {
                "id": "r1_top_urls",
                "name": "R1: Top URLs",
                "role": "target" if target_workload == "r1_top_urls" else "protected",
                "benchmark_status": "completed",
                "validation_status": "pass",
                "ltseq": {"median_s": r1[0], "p95_s": r1[1], "times": [r1[0]], "mem_delta_mb": 1.0},
                "duckdb": {"median_s": 1.0, "p95_s": 1.0, "times": [1.0], "mem_delta_mb": 1.0},
            },
            {
                "id": "r2_sessionization",
                "name": "R2: Sessionization",
                "role": "target" if target_workload == "r2_sessionization" else "protected",
                "benchmark_status": "completed",
                "validation_status": "pass",
                "ltseq": {"median_s": r2[0], "p95_s": r2[1], "times": [r2[0]], "mem_delta_mb": 1.0},
                "duckdb": {"median_s": 1.0, "p95_s": 1.0, "times": [1.0], "mem_delta_mb": 1.0},
            },
            {
                "id": "r3_funnel",
                "name": "R3: Funnel",
                "role": "target" if target_workload == "r3_funnel" else "protected",
                "benchmark_status": "completed",
                "validation_status": "pass",
                "ltseq": {"median_s": r3[0], "p95_s": r3[1], "times": [r3[0]], "mem_delta_mb": 1.0},
                "duckdb": {"median_s": 1.0, "p95_s": 1.0, "times": [1.0], "mem_delta_mb": 1.0},
            },
        ],
    }


def workload_map(diff: dict) -> dict:
    return {workload["id"]: workload for workload in diff["workloads"]}


def test_build_benchmark_summary_records_dataset_label_for_custom_data():
    common, _evaluate, _gate = load_pilot_modules()

    runner_summary = {
        "git_sha": "abc123",
        "timestamp": "2026-04-20T12:00:00",
        "bench_vs": {
            "data_file": "/tmp/custom-bench.parquet",
            "warmup": 1,
            "iterations": 3,
            "passed": True,
            "correctness_failures": 0,
            "infra_failures": 0,
            "rounds": [],
        },
    }

    summary = common.build_benchmark_summary("clickbench_funnel", runner_summary)

    assert summary["dataset_label"] == "custom dataset (/tmp/custom-bench.parquet)"
    rendered = common.render_benchmark_result(summary)
    assert "- Dataset: custom dataset (/tmp/custom-bench.parquet)" in rendered


def test_build_benchmark_summary_marks_empty_round_capture_as_failed():
    common, _evaluate, _gate = load_pilot_modules()

    runner_summary = {
        "git_sha": "abc123",
        "timestamp": "2026-04-20T12:00:00",
        "bench_vs": {
            "data_file": "benchmarks/data/hits_sample.parquet",
            "warmup": 1,
            "iterations": 3,
            "passed": True,
            "correctness_failures": 0,
            "infra_failures": 0,
            "rounds": [],
        },
    }

    summary = common.build_benchmark_summary("clickbench_funnel", runner_summary)

    assert summary["passed"] is False
    assert summary["workloads"] == []


def test_ensure_complete_benchmark_capture_rejects_missing_workloads():
    common, _evaluate, _gate = load_pilot_modules()

    runner_summary = {
        "bench_vs": {
            "rounds": [
                {
                    "round_id": "r1_top_urls",
                    "round_name": "R1: Top URLs",
                    "benchmark_status": "completed",
                    "validation": {"status": "pass"},
                }
            ]
        }
    }

    spec = common.resolve_target("clickbench_funnel")
    with pytest.raises(SystemExit, match="missing bench_vs workloads: r2_sessionization, r3_funnel"):
        common.ensure_complete_benchmark_capture(spec, runner_summary)


def test_benchmark_candidate_fails_explicitly_when_runner_summary_is_incomplete(
    tmp_path, monkeypatch
):
    candidate = load_capture_module("pilot.scripts.benchmark_candidate")

    output_dir = tmp_path / "candidate"
    output_dir.mkdir()
    (output_dir / "summary.json").write_text(
        json.dumps(
            {
                "git_sha": "abc123",
                "timestamp": "2026-04-20T12:00:00",
                "bench_vs": None,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(candidate, "ensure_report_dirs", lambda: None)
    monkeypatch.setattr(candidate, "resolve_report_dir", lambda phase, target: output_dir)
    monkeypatch.setattr(candidate, "clean_output_dir", lambda path: None)
    monkeypatch.setattr(candidate, "run_build", lambda **kwargs: None)
    monkeypatch.setattr(candidate, "run_runner", lambda *args, **kwargs: None)
    monkeypatch.setattr(sys, "argv", ["benchmark_candidate.py", "clickbench_funnel", "--skip-build"])

    with pytest.raises(SystemExit, match="missing bench_vs benchmark results"):
        candidate.main()

    assert not (output_dir / "benchmark-summary.json").exists()


def test_clickbench_funnel_target_spec_is_configured():
    common, _evaluate, _gate = load_pilot_modules()

    spec = common.resolve_target("clickbench_funnel")

    assert spec.target_workloads == ("r3_funnel",)
    assert spec.protected_workloads == ("r1_top_urls", "r2_sessionization")
    assert spec.source_files == ("src/ops/pattern_match.rs",)
    assert spec.target_improvement_threshold_pct == -3.0
    assert spec.protected_regression_threshold_pct == 5.0


def test_clickbench_funnel_diff_persists_target_and_protected_deltas():
    common, _evaluate, _gate = load_pilot_modules()

    baseline = make_summary(
        target="clickbench_funnel",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_funnel",
        r1=(1.01, 1.12),
        r2=(0.49, 0.58),
        r3=(1.78, 1.95),
    )

    diff = common.build_diff("clickbench_funnel", baseline, candidate)
    workloads = workload_map(diff)

    assert list(workload["id"] for workload in diff["workloads"]) == [
        "r3_funnel",
        "r1_top_urls",
        "r2_sessionization",
    ]
    assert math.isclose(workloads["r3_funnel"]["ltseq_median_delta_pct"], -11.0, abs_tol=1e-9)
    assert math.isclose(workloads["r1_top_urls"]["ltseq_median_delta_pct"], 1.0, abs_tol=1e-9)
    assert math.isclose(workloads["r2_sessionization"]["ltseq_median_delta_pct"], -2.0, abs_tol=1e-9)


def test_clickbench_funnel_evaluator_keeps_target_improvement_without_protected_regression():
    _common, evaluate, _gate = load_pilot_modules()

    baseline = make_summary(
        target="clickbench_funnel",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_funnel",
        r1=(1.01, 1.12),
        r2=(0.49, 0.58),
        r3=(1.78, 1.95),
    )

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", baseline, candidate)

    assert evaluation["recommendation"] == "keep"
    assert evaluation["protected_status"] == "clean"
    assert len(evaluation["target_wins_detail"]) == 1
    assert evaluation["target_wins_detail"][0]["id"] == "r3_funnel"
    assert evaluation["target_wins_detail"][0]["name"] == "R3: Funnel"
    assert evaluation["target_wins_detail"][0]["role"] == "target"
    assert math.isclose(evaluation["target_wins_detail"][0]["median_delta_pct"], -11.0, abs_tol=1e-9)
    assert math.isclose(
        evaluation["target_wins_detail"][0]["p95_delta_pct"],
        (1.95 - 2.2) / 2.2 * 100.0,
        abs_tol=1e-9,
    )
    assert evaluation["protected_regressions_detail"] == []
    assert evaluation["thresholds"] == {
        "target_improvement_threshold_pct": -3.0,
        "protected_regression_threshold_pct": 5.0,
    }


def test_clickbench_funnel_evaluator_discards_protected_regression():
    _common, evaluate, _gate = load_pilot_modules()

    baseline = make_summary(
        target="clickbench_funnel",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_funnel",
        r1=(1.08, 1.2),
        r2=(0.49, 0.58),
        r3=(1.78, 1.95),
    )

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", baseline, candidate)

    assert evaluation["recommendation"] == "discard"
    assert evaluation["target_wins_detail"][0]["id"] == "r3_funnel"
    assert evaluation["protected_regressions_detail"][0]["id"] == "r1_top_urls"
    assert "R1: Top URLs" in evaluation["protected_status"]


def test_clickbench_funnel_gate_writes_target_specific_diff_and_evaluation(tmp_path, monkeypatch):
    common, _evaluate, gate = load_pilot_modules()

    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidates"
    diff_dir = tmp_path / "diff"
    baseline_dir.mkdir()
    candidate_dir.mkdir()
    diff_dir.mkdir()

    baseline = make_summary(
        target="clickbench_funnel",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_funnel",
        r1=(1.01, 1.12),
        r2=(0.49, 0.58),
        r3=(1.78, 1.95),
    )
    (baseline_dir / "benchmark-summary.json").write_text(json.dumps(baseline), encoding="utf-8")
    (candidate_dir / "benchmark-summary.json").write_text(json.dumps(candidate), encoding="utf-8")

    def fake_resolve_report_dir(phase: str, target: str) -> Path:
        assert target == "clickbench_funnel"
        mapping = {
            "baseline": baseline_dir,
            "candidates": candidate_dir,
            "diff": diff_dir,
        }
        return mapping[phase]

    monkeypatch.setattr(gate, "resolve_report_dir", fake_resolve_report_dir)
    monkeypatch.setattr(gate, "ensure_report_dirs", lambda: None)
    monkeypatch.setattr(sys, "argv", ["benchmark_gate.py", "clickbench_funnel"])

    assert gate.main() == 0

    diff_payload = json.loads((diff_dir / "benchmark-diff.json").read_text(encoding="utf-8"))
    evaluation_payload = json.loads((diff_dir / "evaluation.json").read_text(encoding="utf-8"))

    assert diff_payload["target"] == "clickbench_funnel"
    assert [workload["id"] for workload in diff_payload["workloads"]] == [
        "r3_funnel",
        "r1_top_urls",
        "r2_sessionization",
    ]
    assert evaluation_payload["recommendation"] == "keep"
    assert evaluation_payload["target_wins_detail"][0]["id"] == "r3_funnel"


def test_clickbench_funnel_gate_replaces_stale_diff_artifacts(tmp_path, monkeypatch):
    _common, _evaluate, gate = load_pilot_modules()

    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidates"
    diff_dir = tmp_path / "diff"
    baseline_dir.mkdir()
    candidate_dir.mkdir()
    diff_dir.mkdir()
    (diff_dir / "stale.txt").write_text("stale", encoding="utf-8")

    baseline = make_summary(
        target="clickbench_funnel",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_funnel",
        r1=(1.01, 1.12),
        r2=(0.49, 0.58),
        r3=(1.78, 1.95),
    )
    (baseline_dir / "benchmark-summary.json").write_text(json.dumps(baseline), encoding="utf-8")
    (candidate_dir / "benchmark-summary.json").write_text(json.dumps(candidate), encoding="utf-8")

    def fake_resolve_report_dir(phase: str, target: str) -> Path:
        assert target == "clickbench_funnel"
        mapping = {
            "baseline": baseline_dir,
            "candidates": candidate_dir,
            "diff": diff_dir,
        }
        return mapping[phase]

    monkeypatch.setattr(gate, "resolve_report_dir", fake_resolve_report_dir)
    monkeypatch.setattr(gate, "ensure_report_dirs", lambda: None)
    monkeypatch.setattr(sys, "argv", ["benchmark_gate.py", "clickbench_funnel"])

    assert gate.main() == 0

    assert not (diff_dir / "stale.txt").exists()
    assert (diff_dir / "benchmark-diff.json").exists()
    assert (diff_dir / "evaluation.json").exists()


def test_clickbench_sessionization_target_spec_is_configured():
    common, _evaluate, _gate = load_pilot_modules()

    spec = common.resolve_target("clickbench_sessionization")

    assert spec.target_workloads == ("r2_sessionization",)
    assert spec.protected_workloads == ("r1_top_urls", "r3_funnel")
    assert spec.source_files == ("src/ops/window.rs", "src/ops/derive.rs")
    assert spec.target_improvement_threshold_pct == -3.0
    assert spec.protected_regression_threshold_pct == 5.0


def test_clickbench_sessionization_diff_persists_target_and_protected_deltas():
    common, _evaluate, _gate = load_pilot_modules()

    baseline = make_summary(
        target="clickbench_sessionization",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_sessionization",
        r1=(1.02, 1.11),
        r2=(0.44, 0.53),
        r3=(2.01, 2.19),
    )

    diff = common.build_diff("clickbench_sessionization", baseline, candidate)
    workloads = workload_map(diff)

    assert list(workload["id"] for workload in diff["workloads"]) == [
        "r2_sessionization",
        "r1_top_urls",
        "r3_funnel",
    ]
    assert math.isclose(workloads["r2_sessionization"]["ltseq_median_delta_pct"], -12.0, abs_tol=1e-9)
    assert math.isclose(workloads["r1_top_urls"]["ltseq_median_delta_pct"], 2.0, abs_tol=1e-9)
    assert math.isclose(workloads["r3_funnel"]["ltseq_median_delta_pct"], 0.5, abs_tol=1e-9)


def test_clickbench_sessionization_evaluator_keeps_target_improvement_without_protected_regression():
    _common, evaluate, _gate = load_pilot_modules()

    baseline = make_summary(
        target="clickbench_sessionization",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_sessionization",
        r1=(1.02, 1.11),
        r2=(0.44, 0.53),
        r3=(2.01, 2.19),
    )

    evaluation = evaluate.evaluate_candidate("clickbench_sessionization", baseline, candidate)

    assert evaluation["recommendation"] == "keep"
    assert evaluation["protected_status"] == "clean"
    assert len(evaluation["target_wins_detail"]) == 1
    assert evaluation["target_wins_detail"][0]["id"] == "r2_sessionization"
    assert evaluation["target_wins_detail"][0]["name"] == "R2: Sessionization"
    assert evaluation["target_wins_detail"][0]["role"] == "target"
    assert math.isclose(evaluation["target_wins_detail"][0]["median_delta_pct"], -12.0, abs_tol=1e-9)
    assert math.isclose(
        evaluation["target_wins_detail"][0]["p95_delta_pct"],
        (0.53 - 0.6) / 0.6 * 100.0,
        abs_tol=1e-9,
    )
    assert evaluation["protected_regressions_detail"] == []
    assert evaluation["thresholds"] == {
        "target_improvement_threshold_pct": -3.0,
        "protected_regression_threshold_pct": 5.0,
    }


def test_clickbench_sessionization_evaluator_discards_protected_regression():
    _common, evaluate, _gate = load_pilot_modules()

    baseline = make_summary(
        target="clickbench_sessionization",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_sessionization",
        r1=(1.02, 1.11),
        r2=(0.44, 0.53),
        r3=(2.12, 2.32),
    )

    evaluation = evaluate.evaluate_candidate("clickbench_sessionization", baseline, candidate)

    assert evaluation["recommendation"] == "discard"
    assert evaluation["target_wins_detail"][0]["id"] == "r2_sessionization"
    assert evaluation["protected_regressions_detail"][0]["id"] == "r3_funnel"
    assert "R3: Funnel" in evaluation["protected_status"]


def test_clickbench_sessionization_gate_writes_target_specific_diff_and_evaluation(tmp_path, monkeypatch):
    _common, _evaluate, gate = load_pilot_modules()

    baseline_dir = tmp_path / "baseline"
    candidate_dir = tmp_path / "candidates"
    diff_dir = tmp_path / "diff"
    baseline_dir.mkdir()
    candidate_dir.mkdir()
    diff_dir.mkdir()

    baseline = make_summary(
        target="clickbench_sessionization",
        r1=(1.0, 1.1),
        r2=(0.5, 0.6),
        r3=(2.0, 2.2),
    )
    candidate = make_summary(
        target="clickbench_sessionization",
        r1=(1.02, 1.11),
        r2=(0.44, 0.53),
        r3=(2.01, 2.19),
    )
    (baseline_dir / "benchmark-summary.json").write_text(json.dumps(baseline), encoding="utf-8")
    (candidate_dir / "benchmark-summary.json").write_text(json.dumps(candidate), encoding="utf-8")

    def fake_resolve_report_dir(phase: str, target: str) -> Path:
        assert target == "clickbench_sessionization"
        mapping = {
            "baseline": baseline_dir,
            "candidates": candidate_dir,
            "diff": diff_dir,
        }
        return mapping[phase]

    monkeypatch.setattr(gate, "resolve_report_dir", fake_resolve_report_dir)
    monkeypatch.setattr(gate, "ensure_report_dirs", lambda: None)
    monkeypatch.setattr(sys, "argv", ["benchmark_gate.py", "clickbench_sessionization"])

    assert gate.main() == 0

    diff_payload = json.loads((diff_dir / "benchmark-diff.json").read_text(encoding="utf-8"))
    evaluation_payload = json.loads((diff_dir / "evaluation.json").read_text(encoding="utf-8"))

    assert diff_payload["target"] == "clickbench_sessionization"
    assert [workload["id"] for workload in diff_payload["workloads"]] == [
        "r2_sessionization",
        "r1_top_urls",
        "r3_funnel",
    ]
    assert evaluation_payload["recommendation"] == "keep"
    assert evaluation_payload["target_wins_detail"][0]["id"] == "r2_sessionization"
