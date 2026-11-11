"""Baseline/candidate benchmark capture, comparison and keep/discard evaluation.

Every test uses small in-memory summaries; none runs a benchmark or needs the
ClickBench dataset.
"""

from __future__ import annotations

import copy
import importlib
import json
import math
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
AUTORESEARCH_ROOT = REPO_ROOT / "benchmarks" / "autoresearch"
SCRIPTS_DIR = AUTORESEARCH_ROOT / "pilot" / "scripts"

TARGET_WORKLOAD = {
    "clickbench_funnel": "r3_funnel",
    "clickbench_sessionization": "r2_sessionization",
    "clickbench_top_urls": "r1_top_urls",
}
WORKLOAD_NAMES = {
    "r1_top_urls": "R1: Top URLs",
    "r2_sessionization": "R2: Sessionization",
    "r3_funnel": "R3: Funnel",
}
# (median, p95) per workload for a baseline, and a candidate whose target
# improves by more than 3% in both metrics while nothing else moves past 5%.
BASELINE_TIMES = {"r1_top_urls": (1.0, 1.1), "r2_sessionization": (0.5, 0.6), "r3_funnel": (2.0, 2.2)}
IMPROVED = {"r1_top_urls": (0.89, 0.97), "r2_sessionization": (0.44, 0.53), "r3_funnel": (1.78, 1.95)}
STEADY = {"r1_top_urls": (1.01, 1.12), "r2_sessionization": (0.49, 0.58), "r3_funnel": (2.01, 2.21)}
SYSTEM = {"platform": "Linux-x86_64", "processor": "x86_64", "cpu_count": 8, "python_version": "3.13.1"}


def load_pilot_modules():
    if str(AUTORESEARCH_ROOT) not in sys.path:
        sys.path.insert(0, str(AUTORESEARCH_ROOT))
    common = importlib.import_module("pilot.common")
    evaluate = importlib.import_module("pilot.scripts.evaluate_benchmark_candidate")
    gate = importlib.import_module("pilot.scripts.benchmark_gate")
    return common, evaluate, gate


def make_summary(target: str = "clickbench_funnel", times: dict | None = None, **overrides) -> dict:
    """A benchmark-summary.json payload; `times` maps workload id to (median, p95)."""
    times = {**BASELINE_TIMES, **(times or {})}
    target_workload = TARGET_WORKLOAD[target]
    summary = {
        "target": target,
        "title": target,
        "git_sha": "abc123",
        "timestamp": "2026-04-20T12:00:00",
        "data_file": "benchmarks/data/hits_sorted.parquet",
        "data_file_bytes": 14_000_000_000,
        "dataset_label": "full dataset",
        "warmup": 1,
        "iterations": 3,
        "system": dict(SYSTEM),
        "passed": True,
        "correctness_failures": 0,
        "infra_failures": 0,
        "workloads": [
            {
                "id": workload_id,
                "name": WORKLOAD_NAMES[workload_id],
                "role": "target" if workload_id == target_workload else "protected",
                "benchmark_status": "completed",
                "passed": True,
                "validation_status": "pass",
                "ltseq": {"median_s": median, "p95_s": p95, "times": [median], "mem_delta_mb": 1.0},
                "duckdb": {"median_s": 1.0, "p95_s": 1.0, "times": [1.0], "mem_delta_mb": 1.0},
            }
            for workload_id, (median, p95) in times.items()
        ],
    }
    summary.update(overrides)
    return summary


def improved_candidate(target: str = "clickbench_funnel", **overrides) -> dict:
    target_workload = TARGET_WORKLOAD[target]
    return make_summary(target, {**STEADY, target_workload: IMPROVED[target_workload]}, **overrides)


def workload(summary: dict, workload_id: str) -> dict:
    return next(item for item in summary["workloads"] if item["id"] == workload_id)


def workload_map(diff: dict) -> dict:
    return {item["id"]: item for item in diff["workloads"]}


def runner_summary(rounds: list[dict] | None = None, **bench_vs) -> dict:
    return {
        "git_sha": "abc123",
        "timestamp": "2026-04-20T12:00:00",
        "bench_vs": {
            "data_file": "benchmarks/data/hits_sample.parquet",
            "warmup": 1,
            "iterations": 3,
            "system": dict(SYSTEM),
            "passed": True,
            "correctness_failures": 0,
            "infra_failures": 0,
            "rounds": rounds if rounds is not None else [],
            **bench_vs,
        },
    }


def bench_vs_round(workload_id: str, times: list[float]) -> dict:
    return {
        "round_id": workload_id,
        "round_name": WORKLOAD_NAMES[workload_id],
        "benchmark_status": "completed",
        "validation": {"status": "pass", "detail": "matched"},
        "ltseq": {"median_s": sorted(times)[len(times) // 2], "times": times, "mem_delta_mb": 1.0},
        "duckdb": {"median_s": 1.0, "times": [1.0], "mem_delta_mb": 1.0},
    }


# --------------------------------------------------------------------------
# Targets and summaries
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "target, target_workloads, protected_workloads",
    [
        ("clickbench_funnel", ("r3_funnel",), ("r1_top_urls", "r2_sessionization")),
        ("clickbench_sessionization", ("r2_sessionization",), ("r1_top_urls", "r3_funnel")),
        ("clickbench_top_urls", ("r1_top_urls",), ("r2_sessionization", "r3_funnel")),
    ],
)
def test_target_spec_is_configured(target, target_workloads, protected_workloads):
    common, _evaluate, _gate = load_pilot_modules()

    spec = common.resolve_target(target)

    assert spec.target_workloads == target_workloads
    assert spec.protected_workloads == protected_workloads
    assert spec.target_improvement_threshold_pct == -3.0
    assert spec.protected_regression_threshold_pct == 5.0


def test_resolve_target_rejects_unknown_target():
    common, _evaluate, _gate = load_pilot_modules()

    with pytest.raises(common.BenchmarkInputError, match="unknown benchmark target: nope"):
        common.resolve_target("nope")


def test_build_benchmark_summary_records_dataset_label_for_custom_data():
    common, _evaluate, _gate = load_pilot_modules()

    summary = common.build_benchmark_summary(
        "clickbench_funnel", runner_summary(data_file="/tmp/custom-bench.parquet")
    )

    assert summary["dataset_label"] == "custom dataset (/tmp/custom-bench.parquet)"
    rendered = common.render_benchmark_result(summary)
    assert "- Dataset: custom dataset (/tmp/custom-bench.parquet)" in rendered


def test_build_benchmark_summary_records_measurement_metadata(tmp_path):
    common, _evaluate, _gate = load_pilot_modules()
    data_file = tmp_path / "hits_sample.parquet"
    data_file.write_bytes(b"x" * 123)

    summary = common.build_benchmark_summary(
        "clickbench_funnel",
        runner_summary(
            [bench_vs_round(workload_id, [1.0, 2.0, 3.0]) for workload_id in BASELINE_TIMES],
            data_file=str(data_file),
        ),
    )

    assert summary["data_file_bytes"] == 123
    assert summary["system"] == SYSTEM
    assert (summary["warmup"], summary["iterations"]) == (1, 3)
    assert summary["passed"] is True
    r3 = workload(summary, "r3_funnel")
    assert r3["role"] == "target"
    # p95 interpolates between the two slowest of three samples.
    assert math.isclose(r3["ltseq"]["p95_s"], 2.9, abs_tol=1e-9)


def test_build_benchmark_summary_marks_empty_round_capture_as_failed():
    common, _evaluate, _gate = load_pilot_modules()

    summary = common.build_benchmark_summary("clickbench_funnel", runner_summary([]))

    assert summary["passed"] is False
    assert summary["workloads"] == []


def test_ensure_complete_benchmark_capture_rejects_missing_workloads():
    common, _evaluate, _gate = load_pilot_modules()

    spec = common.resolve_target("clickbench_funnel")
    with pytest.raises(SystemExit, match="missing bench_vs workloads: r2_sessionization, r3_funnel"):
        common.ensure_complete_benchmark_capture(
            spec, runner_summary([bench_vs_round("r1_top_urls", [1.0])])
        )


# --------------------------------------------------------------------------
# Capture
# --------------------------------------------------------------------------


def patch_capture(monkeypatch, common, output_dir: Path, summary: dict | None):
    calls = {"cleaned": 0}

    def fake_runner(out_dir, rounds, **kwargs):
        if summary is not None:
            (out_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")

    def fake_clean(path):
        calls["cleaned"] += 1
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(common, "ensure_report_dirs", lambda: None)
    monkeypatch.setattr(common, "resolve_report_dir", lambda phase, target: output_dir / phase)
    monkeypatch.setattr(common, "clean_output_dir", fake_clean)
    monkeypatch.setattr(common, "run_build", lambda **kwargs: None)
    monkeypatch.setattr(common, "run_runner", fake_runner)
    return calls


@pytest.mark.parametrize("phase, report_dir", [("baseline", "baseline"), ("candidate", "candidates")])
def test_capture_writes_summary_artifacts(tmp_path, monkeypatch, phase, report_dir):
    common, _evaluate, _gate = load_pilot_modules()
    rounds = [bench_vs_round(workload_id, [1.0, 1.1, 1.2]) for workload_id in BASELINE_TIMES]
    patch_capture(monkeypatch, common, tmp_path, runner_summary(rounds))

    script = importlib.import_module(f"pilot.scripts.benchmark_{phase}")
    assert script.main(["clickbench_funnel", "--skip-build"]) == 0

    output_dir = tmp_path / report_dir
    summary = json.loads((output_dir / "benchmark-summary.json").read_text(encoding="utf-8"))
    assert summary["target"] == "clickbench_funnel"
    assert [item["id"] for item in summary["workloads"]] == list(BASELINE_TIMES)
    assert (output_dir / "benchmark-result.json").exists()
    assert "LTSeq median" in (output_dir / "benchmark-result.md").read_text(encoding="utf-8")


def test_benchmark_candidate_fails_explicitly_when_runner_summary_is_incomplete(
    tmp_path, monkeypatch
):
    common, _evaluate, _gate = load_pilot_modules()
    patch_capture(
        monkeypatch,
        common,
        tmp_path,
        {"git_sha": "abc123", "timestamp": "2026-04-20T12:00:00", "bench_vs": None},
    )
    candidate = importlib.import_module("pilot.scripts.benchmark_candidate")

    with pytest.raises(SystemExit, match="missing bench_vs benchmark results"):
        candidate.main(["clickbench_funnel", "--skip-build"])

    assert not (tmp_path / "candidates" / "benchmark-summary.json").exists()


def test_capture_dry_run_keeps_existing_artifacts(tmp_path, monkeypatch):
    common, _evaluate, _gate = load_pilot_modules()
    calls = patch_capture(monkeypatch, common, tmp_path, None)
    baseline = importlib.import_module("pilot.scripts.benchmark_baseline")

    assert baseline.main(["clickbench_funnel", "--dry-run"]) == 0
    assert calls["cleaned"] == 0


# --------------------------------------------------------------------------
# Diff and keep/discard policy
# --------------------------------------------------------------------------


def test_diff_persists_target_and_protected_deltas():
    common, _evaluate, _gate = load_pilot_modules()

    diff = common.build_diff("clickbench_funnel", make_summary(), improved_candidate())
    workloads = workload_map(diff)

    assert [item["id"] for item in diff["workloads"]] == ["r3_funnel", "r1_top_urls", "r2_sessionization"]
    assert math.isclose(workloads["r3_funnel"]["ltseq_median_delta_pct"], -11.0, abs_tol=1e-9)
    assert math.isclose(workloads["r1_top_urls"]["ltseq_median_delta_pct"], 1.0, abs_tol=1e-9)
    assert math.isclose(workloads["r2_sessionization"]["ltseq_median_delta_pct"], -2.0, abs_tol=1e-9)


@pytest.mark.parametrize("target", sorted(TARGET_WORKLOAD))
def test_evaluator_keeps_target_improvement_without_protected_regression(target):
    _common, evaluate, _gate = load_pilot_modules()
    target_workload = TARGET_WORKLOAD[target]

    evaluation = evaluate.evaluate_candidate(target, make_summary(target), improved_candidate(target))

    assert evaluation["recommendation"] == "keep"
    assert evaluation["reason_code"] == "target_improvement"
    assert evaluation["protected_status"] == "clean"
    assert [item["id"] for item in evaluation["target_wins_detail"]] == [target_workload]
    win = evaluation["target_wins_detail"][0]
    assert win["role"] == "target"
    base_median, base_p95 = BASELINE_TIMES[target_workload]
    cand_median, cand_p95 = IMPROVED[target_workload]
    assert math.isclose(win["median_delta_pct"], (cand_median - base_median) / base_median * 100, abs_tol=1e-9)
    assert math.isclose(win["p95_delta_pct"], (cand_p95 - base_p95) / base_p95 * 100, abs_tol=1e-9)
    assert evaluation["protected_regressions_detail"] == []
    assert evaluation["problems"] == []
    assert evaluation["thresholds"] == {
        "target_improvement_threshold_pct": -3.0,
        "protected_regression_threshold_pct": 5.0,
        "source": "target_spec",
    }


def test_evaluator_discards_protected_regression():
    _common, evaluate, _gate = load_pilot_modules()
    candidate = improved_candidate()
    workload(candidate, "r1_top_urls")["ltseq"].update(median_s=1.08, p95_s=1.2)

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", make_summary(), candidate)

    assert evaluation["recommendation"] == "discard"
    assert evaluation["reason_code"] == "protected_regression"
    assert evaluation["target_wins_detail"][0]["id"] == "r3_funnel"
    assert evaluation["protected_regressions_detail"][0]["id"] == "r1_top_urls"
    assert "R1: Top URLs" in evaluation["protected_status"]


@pytest.mark.parametrize(
    "r3, kept",
    [
        ((1.90, 2.15), False),  # median -5%, p95 -2.3%
        ((1.97, 2.05), False),  # median -1.5%, p95 -6.8%
        ((1.90, 2.10), True),  # both past -3%
    ],
    ids=["only-median-improves", "only-p95-improves", "both-improve"],
)
def test_target_win_requires_median_and_p95(r3, kept):
    _common, evaluate, _gate = load_pilot_modules()

    evaluation = evaluate.evaluate_candidate(
        "clickbench_funnel", make_summary(), make_summary(times={**STEADY, "r3_funnel": r3})
    )

    assert (evaluation["recommendation"] == "keep") is kept
    if not kept:
        assert evaluation["reason_code"] == "no_target_improvement"
        assert evaluation["target_wins_detail"] == []
        assert "no target workload met the improvement threshold" in evaluation["reason"]


def test_protected_regression_in_p95_alone_rejects():
    _common, evaluate, _gate = load_pilot_modules()
    candidate = make_summary(times={**STEADY, "r1_top_urls": (1.01, 1.16), "r3_funnel": (1.90, 2.10)})

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", make_summary(), candidate)

    assert evaluation["recommendation"] == "discard"
    assert evaluation["reason_code"] == "protected_regression"
    assert evaluation["protected_regressions_detail"][0]["id"] == "r1_top_urls"
    assert math.isclose(
        evaluation["protected_regressions_detail"][0]["p95_delta_pct"],
        (1.16 - 1.1) / 1.1 * 100.0,
        abs_tol=1e-9,
    )


def test_threshold_override_is_recorded():
    _common, evaluate, _gate = load_pilot_modules()
    # Target improves by 2.5% in both metrics: under the default 3% bar.
    candidate = make_summary(times={**STEADY, "r3_funnel": (1.95, 2.145)})

    default = evaluate.evaluate_candidate("clickbench_funnel", make_summary(), candidate)
    relaxed = evaluate.evaluate_candidate(
        "clickbench_funnel", make_summary(), candidate, min_improvement_pct=2.0, max_regression_pct=8.0
    )

    assert default["recommendation"] == "discard"
    assert relaxed["recommendation"] == "keep"
    assert relaxed["thresholds"] == {
        "target_improvement_threshold_pct": -2.0,
        "protected_regression_threshold_pct": 8.0,
        "source": "override",
    }


# --------------------------------------------------------------------------
# Integrity: inputs that must never pass
# --------------------------------------------------------------------------


def _with_workload(summary: dict, workload_id: str, **fields) -> dict:
    summary = copy.deepcopy(summary)
    workload(summary, workload_id).update(fields)
    return summary


def _with_metric(summary: dict, workload_id: str, metric: str, value) -> dict:
    summary = copy.deepcopy(summary)
    workload(summary, workload_id)["ltseq"][metric] = value
    return summary


def _without_workload(summary: dict, workload_id: str) -> dict:
    summary = copy.deepcopy(summary)
    summary["workloads"] = [item for item in summary["workloads"] if item["id"] != workload_id]
    return summary


@pytest.mark.parametrize(
    "make_candidate, reason_code",
    [
        (lambda: improved_candidate(correctness_failures=1, passed=False), "correctness_failure"),
        (
            lambda: _with_workload(improved_candidate(), "r1_top_urls", validation_status="fail"),
            "correctness_failure",
        ),
        (lambda: improved_candidate(infra_failures=1, passed=False), "infra_failure"),
        (
            lambda: _with_workload(
                improved_candidate(), "r3_funnel", benchmark_status="infra_failure", validation_status="infra_failure"
            ),
            "infra_failure",
        ),
        (lambda: _without_workload(improved_candidate(), "r2_sessionization"), "missing_workloads"),
        (
            lambda: _with_workload(improved_candidate(), "r1_top_urls", validation_status="unknown"),
            "unvalidated_workload",
        ),
        (lambda: _with_metric(improved_candidate(), "r3_funnel", "p95_s", None), "missing_metrics"),
        (lambda: _with_metric(improved_candidate(), "r2_sessionization", "median_s", None), "missing_metrics"),
        (lambda: _with_metric(improved_candidate(), "r1_top_urls", "median_s", float("nan")), "missing_metrics"),
        (lambda: _with_metric(improved_candidate(), "r3_funnel", "median_s", "bad"), "missing_metrics"),
        (lambda: _with_workload(improved_candidate(), "r1_top_urls", ltseq="bad"), "missing_metrics"),
    ],
    ids=[
        "correctness-count",
        "workload-validation-failed",
        "infra-count",
        "workload-infra-failure",
        "missing-protected-workload",
        "workload-not-validated",
        "target-p95-missing",
        "protected-median-missing",
        "protected-median-nan",
        "target-median-string",
        "ltseq-not-an-object",
    ],
)
def test_candidate_integrity_failures_discard(make_candidate, reason_code):
    _common, evaluate, _gate = load_pilot_modules()

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", make_summary(), make_candidate())

    assert evaluation["recommendation"] == "discard"
    assert evaluation["reason_code"] == reason_code
    assert evaluation["problems"]
    assert evaluate.exit_status(evaluation) == evaluate.EXIT_DISCARD


def test_missing_workload_is_named():
    _common, evaluate, _gate = load_pilot_modules()

    evaluation = evaluate.evaluate_candidate(
        "clickbench_funnel", make_summary(), _without_workload(improved_candidate(), "r2_sessionization")
    )

    assert evaluation["missing_workloads"] == ["r2_sessionization"]
    assert "missing workloads: r2_sessionization" in evaluation["reason"]


@pytest.mark.parametrize(
    "make_baseline",
    [
        lambda: make_summary(correctness_failures=1, passed=False),
        lambda: make_summary(infra_failures=2, passed=False),
        lambda: _without_workload(make_summary(), "r3_funnel"),
        lambda: _with_metric(make_summary(), "r3_funnel", "median_s", 0.0),
    ],
    ids=["correctness", "infra", "missing-workload", "zero-median"],
)
def test_invalid_baseline_discards(make_baseline):
    _common, evaluate, _gate = load_pilot_modules()

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", make_baseline(), improved_candidate())

    assert evaluation["recommendation"] == "discard"
    assert evaluation["reason_code"] == "invalid_baseline"


@pytest.mark.parametrize(
    "candidate_overrides, problem",
    [
        ({"data_file": "benchmarks/data/hits_sample.parquet", "data_file_bytes": 70_000_000}, "datasets differ"),
        ({"data_file_bytes": 13_999_999_999}, "datasets differ"),
        ({"data_file": None}, "dataset unknown"),
        ({"iterations": 5}, "iterations differs"),
        ({"warmup": 0}, "warmup differs"),
        ({"target": "clickbench_sessionization"}, "summary is for target 'clickbench_sessionization'"),
    ],
    ids=["other-file", "same-name-other-size", "unknown-dataset", "iterations", "warmup", "target"],
)
def test_mismatched_inputs_discard(candidate_overrides, problem):
    _common, evaluate, _gate = load_pilot_modules()

    evaluation = evaluate.evaluate_candidate(
        "clickbench_funnel", make_summary(), improved_candidate(**candidate_overrides)
    )

    assert evaluation["recommendation"] == "discard"
    assert evaluation["reason_code"] == "incompatible_inputs"
    assert any(problem in item for item in evaluation["problems"])


def _without_fields(summary: dict, *fields: str) -> dict:
    return {key: value for key, value in summary.items() if key not in fields}


@pytest.mark.parametrize(
    "unset, problem",
    [
        (lambda summary: _without_fields(summary, "target"), "baseline summary does not name its target"),
        (lambda summary: _without_fields(summary, "iterations"), "iterations unknown"),
        (lambda summary: _without_fields(summary, "warmup"), "warmup unknown"),
        (lambda summary: {**summary, "iterations": 0}, "iterations unknown"),
        (lambda summary: {**summary, "iterations": "3"}, "iterations unknown"),
        (lambda summary: {**summary, "warmup": -1}, "warmup unknown"),
        (lambda summary: {**summary, "warmup": True}, "warmup unknown"),
        (lambda summary: {**summary, "data_file": 7}, "dataset unknown"),
    ],
    ids=[
        "no-target",
        "no-iterations",
        "no-warmup",
        "zero-iterations",
        "string-iterations",
        "negative-warmup",
        "bool-warmup",
        "data-file-not-a-path",
    ],
)
def test_settings_missing_from_both_summaries_discard(unset, problem):
    """Two summaries that lack the same setting agree only vacuously."""
    _common, evaluate, _gate = load_pilot_modules()

    evaluation = evaluate.evaluate_candidate(
        "clickbench_funnel", unset(make_summary()), unset(improved_candidate())
    )

    assert evaluation["recommendation"] == "discard"
    assert evaluation["reason_code"] == "incompatible_inputs"
    assert any(problem in item for item in evaluation["problems"])


def test_same_dataset_in_another_checkout_is_comparable():
    _common, evaluate, _gate = load_pilot_modules()
    candidate = improved_candidate(data_file="/elsewhere/benchmarks/data/hits_sorted.parquet")

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", make_summary(), candidate)

    assert evaluation["recommendation"] == "keep"


def test_system_difference_is_a_warning_not_a_rejection():
    _common, evaluate, _gate = load_pilot_modules()
    candidate = improved_candidate(system={**SYSTEM, "cpu_count": 16})

    evaluation = evaluate.evaluate_candidate("clickbench_funnel", make_summary(), candidate)

    assert evaluation["recommendation"] == "keep"
    assert evaluation["warnings"] == ["system cpu_count differs: baseline 8, candidate 16"]


@pytest.mark.parametrize("bad", [[], {"workloads": "r1"}, {"workloads": [{"name": "no id"}]}])
def test_malformed_summary_is_an_input_error(bad):
    common, evaluate, _gate = load_pilot_modules()

    with pytest.raises(common.BenchmarkInputError):
        evaluate.evaluate_candidate("clickbench_funnel", make_summary(), bad)


# --------------------------------------------------------------------------
# Gate CLI: artifacts and exit status
# --------------------------------------------------------------------------


@pytest.fixture
def report_dirs(tmp_path, monkeypatch):
    _common, _evaluate, gate = load_pilot_modules()
    dirs = {phase: tmp_path / phase for phase in ("baseline", "candidates", "diff")}
    for path in dirs.values():
        path.mkdir()
    monkeypatch.setattr(gate, "resolve_report_dir", lambda phase, target: dirs[phase])
    monkeypatch.setattr(gate, "ensure_report_dirs", lambda: None)
    return dirs


def write_summaries(dirs: dict, baseline, candidate) -> None:
    for phase, payload in (("baseline", baseline), ("candidates", candidate)):
        if payload is None:
            continue
        text = payload if isinstance(payload, str) else json.dumps(payload)
        (dirs[phase] / "benchmark-summary.json").write_text(text, encoding="utf-8")


def test_gate_keep_exits_zero_and_writes_artifacts(report_dirs):
    _common, _evaluate, gate = load_pilot_modules()
    (report_dirs["diff"] / "stale.txt").write_text("stale", encoding="utf-8")
    write_summaries(report_dirs, make_summary(), improved_candidate())

    assert gate.main(["clickbench_funnel"]) == 0

    diff_payload = json.loads((report_dirs["diff"] / "benchmark-diff.json").read_text(encoding="utf-8"))
    evaluation = json.loads((report_dirs["diff"] / "evaluation.json").read_text(encoding="utf-8"))
    assert diff_payload["target"] == "clickbench_funnel"
    assert [item["id"] for item in diff_payload["workloads"]] == ["r3_funnel", "r1_top_urls", "r2_sessionization"]
    assert evaluation["recommendation"] == "keep"
    assert evaluation["target_wins_detail"][0]["id"] == "r3_funnel"
    assert "reason_code=target_improvement" in (report_dirs["diff"] / "evaluation.txt").read_text(encoding="utf-8")
    assert not (report_dirs["diff"] / "stale.txt").exists()


@pytest.mark.parametrize(
    "candidate",
    [improved_candidate(correctness_failures=1, passed=False), make_summary(times=STEADY)],
    ids=["correctness-failure", "no-improvement"],
)
def test_gate_discard_exits_one_and_still_writes_evaluation(report_dirs, candidate):
    _common, _evaluate, gate = load_pilot_modules()
    write_summaries(report_dirs, make_summary(), candidate)

    assert gate.main(["clickbench_funnel"]) == 1

    evaluation = json.loads((report_dirs["diff"] / "evaluation.json").read_text(encoding="utf-8"))
    assert evaluation["recommendation"] == "discard"


@pytest.mark.parametrize(
    "candidate",
    [
        _with_metric(improved_candidate(), "r3_funnel", "median_s", "bad"),
        _with_workload(improved_candidate(), "r1_top_urls", ltseq=[1.0]),
    ],
    ids=["string-metric", "ltseq-not-an-object"],
)
def test_gate_writes_the_discard_for_a_malformed_metric(report_dirs, candidate):
    _common, _evaluate, gate = load_pilot_modules()
    write_summaries(report_dirs, make_summary(), candidate)

    assert gate.main(["clickbench_funnel"]) == 1

    evaluation = json.loads((report_dirs["diff"] / "evaluation.json").read_text(encoding="utf-8"))
    assert evaluation["reason_code"] == "missing_metrics"
    diff_payload = json.loads((report_dirs["diff"] / "benchmark-diff.json").read_text(encoding="utf-8"))
    malformed = next(item for item in diff_payload["workloads"] if item["ltseq_median_delta_pct"] is None)
    assert malformed["id"] in ("r3_funnel", "r1_top_urls")
    assert "n/a" in (report_dirs["diff"] / "benchmark-diff.txt").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "baseline, candidate, message",
    [
        (None, improved_candidate(), "baseline summary not found"),
        (make_summary(), None, "candidate summary not found"),
        (make_summary(), "{not json", "candidate summary is unreadable"),
        (make_summary(), json.dumps({"workloads": None}), "candidate summary has no valid 'workloads' list"),
    ],
    ids=["missing-baseline", "missing-candidate", "unparseable", "not-a-summary"],
)
def test_gate_input_errors_exit_two_and_leave_no_evaluation(report_dirs, capsys, baseline, candidate, message):
    _common, _evaluate, gate = load_pilot_modules()
    (report_dirs["diff"] / "evaluation.json").write_text('{"recommendation": "keep"}', encoding="utf-8")
    write_summaries(report_dirs, baseline, candidate)

    assert gate.main(["clickbench_funnel"]) == 2

    assert message in capsys.readouterr().err
    assert list(report_dirs["diff"].iterdir()) == []


def test_gate_rejects_unknown_target_with_usage_error(report_dirs):
    _common, _evaluate, gate = load_pilot_modules()

    with pytest.raises(SystemExit) as excinfo:
        gate.main(["not_a_target"])
    assert excinfo.value.code == 2


def test_gate_threshold_options(report_dirs):
    _common, _evaluate, gate = load_pilot_modules()
    write_summaries(report_dirs, make_summary(), make_summary(times={**STEADY, "r3_funnel": (1.95, 2.145)}))

    assert gate.main(["clickbench_funnel"]) == 1
    assert gate.main(["clickbench_funnel", "--min-improvement-pct", "2"]) == 0
    with pytest.raises(SystemExit) as excinfo:
        gate.main(["clickbench_funnel", "--min-improvement-pct", "-2"])
    assert excinfo.value.code == 2


@pytest.mark.parametrize(
    "args, message",
    [
        (["--min-improvement-pct", "-2"], "argument --min-improvement-pct: expected a positive percentage"),
        (["--max-regression-pct", "five"], "argument --max-regression-pct: expected a percentage"),
        (["--min-improvment-pct", "2"], "unrecognized arguments: --min-improvment-pct 2"),
    ],
    ids=["negative-threshold", "non-numeric-threshold", "misspelled-option"],
)
def test_gate_usage_errors_clear_the_previous_decision(report_dirs, capsys, args, message):
    _common, _evaluate, gate = load_pilot_modules()
    write_summaries(report_dirs, make_summary(), improved_candidate())
    assert gate.main(["clickbench_funnel"]) == 0
    assert (report_dirs["diff"] / "evaluation.json").exists()

    with pytest.raises(SystemExit) as excinfo:
        gate.main(["clickbench_funnel", *args])

    assert excinfo.value.code == 2
    assert message in capsys.readouterr().err
    assert list(report_dirs["diff"].iterdir()) == []


def test_gate_help_leaves_the_previous_decision(report_dirs):
    _common, _evaluate, gate = load_pilot_modules()
    write_summaries(report_dirs, make_summary(), improved_candidate())
    assert gate.main(["clickbench_funnel"]) == 0

    with pytest.raises(SystemExit) as excinfo:
        gate.main(["--help"])

    assert excinfo.value.code == 0
    assert (report_dirs["diff"] / "evaluation.json").exists()


# --------------------------------------------------------------------------
# The scripts run on their own, outside the repository root and without the
# retired controller.
# --------------------------------------------------------------------------


def run_script(name: str, *args: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / name), *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def test_evaluator_cli_exit_statuses(tmp_path):
    baseline = tmp_path / "baseline.json"
    keep = tmp_path / "keep.json"
    discard = tmp_path / "discard.json"
    baseline.write_text(json.dumps(make_summary()), encoding="utf-8")
    keep.write_text(json.dumps(improved_candidate()), encoding="utf-8")
    discard.write_text(json.dumps(improved_candidate(infra_failures=1)), encoding="utf-8")

    kept = run_script("evaluate_benchmark_candidate.py", str(baseline), str(keep), "clickbench_funnel", cwd=tmp_path)
    assert kept.returncode == 0, kept.stderr
    assert json.loads(kept.stdout)["recommendation"] == "keep"

    discarded = run_script(
        "evaluate_benchmark_candidate.py", str(baseline), str(discard), "clickbench_funnel", cwd=tmp_path
    )
    assert discarded.returncode == 1, discarded.stderr
    assert json.loads(discarded.stdout)["reason_code"] == "infra_failure"

    missing = run_script(
        "evaluate_benchmark_candidate.py", str(baseline), str(tmp_path / "nope.json"), "clickbench_funnel", cwd=tmp_path
    )
    assert missing.returncode == 2
    assert "candidate summary not found" in missing.stderr

    bad_option = run_script(
        "evaluate_benchmark_candidate.py",
        str(baseline),
        str(keep),
        "clickbench_funnel",
        "--min-improvement-pct",
        "0",
        cwd=tmp_path,
    )
    assert bad_option.returncode == 2
    assert "argument --min-improvement-pct: expected a positive percentage" in bad_option.stderr


def test_capture_cli_dry_run_prints_commands(tmp_path):
    result = run_script("benchmark_baseline.py", "clickbench_funnel", "--sample", "--dry-run", cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    assert '"maturin" "develop" "--release"' in result.stdout
    assert "runner.py" in result.stdout and '"--sample"' in result.stdout


@pytest.mark.parametrize(
    "args, data_flags",
    [
        ((), ["--full"]),
        (("--sample",), ["--sample"]),
        (("--data", "/data/custom.parquet"), ["--data", "/data/custom.parquet"]),
    ],
    ids=["full-by-default", "sample", "custom"],
)
def test_capture_asks_runner_for_the_requested_dataset(tmp_path, args, data_flags):
    # runner.py falls back to the 1M-row sample when no data flag is given, so
    # a full-dataset capture has to pass --full explicitly.
    result = run_script("benchmark_candidate.py", "clickbench_funnel", "--dry-run", "--skip-build", *args, cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    runner_line = next(line for line in result.stdout.splitlines() if "runner.py" in line)
    for flag in data_flags:
        assert json.dumps(flag) in runner_line
    other = {"--full", "--sample", "--data"} - set(data_flags)
    assert not [flag for flag in other if json.dumps(flag) in runner_line]


def test_capture_passes_zero_warmup_through(tmp_path):
    result = run_script("benchmark_baseline.py", "--dry-run", "--skip-build", "--warmup", "0", cwd=tmp_path)

    assert result.returncode == 0, result.stderr
    assert '"--warmup" "0"' in result.stdout


def test_retained_toolkit_has_no_shell_or_agent_dependency():
    """The supervised controller was retired (#174); nothing retained may call it."""
    retired = ("autoloop", "opencode", "common.sh", "results.tsv", "issues.tsv")
    assert not list(AUTORESEARCH_ROOT.rglob("*.sh"))
    for path in AUTORESEARCH_ROOT.rglob("*"):
        if path.is_file() and path.suffix in {".py", ".md"}:
            text = path.read_text(encoding="utf-8").lower()
            assert not [name for name in retired if name in text], path
