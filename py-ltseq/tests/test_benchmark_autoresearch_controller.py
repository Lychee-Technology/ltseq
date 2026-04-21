from __future__ import annotations

import json
import subprocess
import textwrap
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def autoloop_path() -> Path:
    return repo_root() / "benchmarks" / "autoresearch" / "pilot" / "scripts" / "autoloop.sh"


def run_autoloop_shell(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-lc", script],
        cwd=repo_root(),
        text=True,
        capture_output=True,
        check=False,
    )


def write_summary(path: Path, data_file: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"data_file": data_file}), encoding="utf-8")


def test_baseline_matches_requested_data_rejects_dataset_mismatch(tmp_path):
    baseline_summary = tmp_path / "benchmark-summary.json"
    write_summary(baseline_summary, "benchmarks/data/hits_sample.parquet")

    script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        ROOT_DIR={repo_root()!s}
        if baseline_matches_requested_data {baseline_summary!s} {repo_root() / 'benchmarks/data/hits_sorted.parquet'!s}; then
          exit 0
        fi
        exit 7
        """
    )

    result = run_autoloop_shell(script)

    assert result.returncode == 7


def test_validate_candidate_scope_allows_overlay_and_synced_assets_only(tmp_path):
    worktree = tmp_path / "worktree"
    (worktree / ".git").mkdir(parents=True)
    (worktree / ".opencode").mkdir()
    (worktree / "benchmarks" / "autoresearch" / "pilot").mkdir(parents=True)
    (worktree / ".opencode" / "session.json").write_text("{}", encoding="utf-8")
    (worktree / "benchmarks" / "autoresearch" / "pilot" / "README.md").write_text("pilot", encoding="utf-8")

    script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        TARGET=clickbench_funnel
        WORKTREE_DIR={worktree!s}
        base_overlay_manifest={tmp_path / 'overlay.txt'!s}
        : > "$base_overlay_manifest"
        git() {{
          if [[ "$*" == *"status --porcelain --untracked-files=all"* ]]; then
            printf '?? .opencode/session.json\n'
            printf '?? benchmarks/autoresearch/pilot/README.md\n'
            return 0
          fi
          command git "$@"
        }}
        scope_file={tmp_path / 'scope.txt'!s}
        if validate_candidate_scope "$scope_file"; then
          exit 0
        fi
        printf 'scope=%s\n' "$(cat "$scope_file")"
        exit 9
        """
    )

    result = run_autoloop_shell(script)

    assert result.returncode == 9
    assert "scope=no in-scope production changes detected" in result.stdout


def test_validate_candidate_scope_distinguishes_empty_from_out_of_scope(tmp_path):
    worktree = tmp_path / "worktree"
    (worktree / ".git").mkdir(parents=True)

    empty_script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        TARGET=clickbench_funnel
        WORKTREE_DIR={worktree!s}
        base_overlay_manifest={tmp_path / 'overlay-empty.txt'!s}
        : > "$base_overlay_manifest"
        git() {{
          if [[ "$*" == *"status --porcelain --untracked-files=all"* ]]; then
            return 0
          fi
          command git "$@"
        }}
        scope_file={tmp_path / 'scope-empty.txt'!s}
        validate_candidate_scope "$scope_file" || true
        printf '%s' "$(scope_failure_reason "$scope_file")"
        """
    )
    empty_result = run_autoloop_shell(empty_script)

    out_of_scope_script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        TARGET=clickbench_funnel
        WORKTREE_DIR={worktree!s}
        base_overlay_manifest={tmp_path / 'overlay-oos.txt'!s}
        : > "$base_overlay_manifest"
        git() {{
          if [[ "$*" == *"status --porcelain --untracked-files=all"* ]]; then
            printf ' M src/lib.rs\n'
            return 0
          fi
          command git "$@"
        }}
        scope_file={tmp_path / 'scope-oos.txt'!s}
        validate_candidate_scope "$scope_file" || true
        printf '%s\n%s' "$(scope_failure_reason "$scope_file")" "$(cat "$scope_file")"
        """
    )
    out_of_scope_result = run_autoloop_shell(out_of_scope_script)

    assert empty_result.returncode == 0
    assert empty_result.stdout == "empty-or-noop-changes"
    assert out_of_scope_result.returncode == 0
    assert out_of_scope_result.stdout.startswith("out-of-scope-changes\n")
    assert out_of_scope_result.stdout.endswith("src/lib.rs")


def test_run_iteration_clears_root_candidate_and_diff_on_gate_failure(tmp_path):
    worktree = tmp_path / "worktree"
    report_dir = tmp_path / "reports"
    logs_dir = report_dir / "logs"
    fake_bin_dir = tmp_path / "bin"
    root_candidate_dir = report_dir / "candidates" / "clickbench_funnel"
    root_diff_dir = report_dir / "diff" / "clickbench_funnel"
    baseline_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "reports" / "baseline" / "clickbench_funnel"
    candidate_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "reports" / "candidates" / "clickbench_funnel"
    diff_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "reports" / "diff" / "clickbench_funnel"
    runs_dir = report_dir / "runs" / "clickbench_funnel"

    logs_dir.mkdir(parents=True)
    root_candidate_dir.mkdir(parents=True)
    root_diff_dir.mkdir(parents=True)
    baseline_dir.mkdir(parents=True)
    candidate_dir.mkdir(parents=True)
    diff_dir.mkdir(parents=True)
    runs_dir.mkdir(parents=True)
    fake_bin_dir.mkdir(parents=True)
    (worktree / ".git").mkdir(parents=True)
    (root_candidate_dir / "stale.txt").write_text("candidate", encoding="utf-8")
    (root_diff_dir / "stale.txt").write_text("diff", encoding="utf-8")
    (candidate_dir / "benchmark-summary.json").write_text("{}", encoding="utf-8")
    (baseline_dir / "benchmark-summary.json").write_text("{}", encoding="utf-8")
    (fake_bin_dir / "python").write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1\" == \"benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py\" ]]; then\n"
        "  exit 0\n"
        "fi\n"
        "if [[ \"$1\" == \"benchmarks/autoresearch/pilot/scripts/benchmark_gate.py\" ]]; then\n"
        "  exit 1\n"
        "fi\n"
        "exec /usr/bin/python \"$@\"\n",
        encoding="utf-8",
    )
    (fake_bin_dir / "python").chmod(0o755)

    script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        TARGET=clickbench_funnel
        DRY_RUN=0
        ROOT_DIR={repo_root()!s}
        REPORT_DIR={report_dir!s}
        WORKTREE_DIR={worktree!s}
        export PATH={fake_bin_dir!s}:$PATH
        LOG_PREFIX=test-log
        LOOP_LOG={logs_dir / 'loop.log'!s}
        results_file={tmp_path / 'results.tsv'!s}
        issues_file={tmp_path / 'issues.tsv'!s}
        base_overlay_manifest={tmp_path / 'overlay.txt'!s}
        : > "$base_overlay_manifest"
        benchmark_log_path() {{
          printf '%s/%s.log\n' {logs_dir!s} "$1"
        }}
        run_single_candidate() {{
          local _run_index="$1"
          local decision_file="$2"
          local stdout_log="$3"
          printf 'status=keep\nscenario=controller-test\nreason=proceed\nevidence=ok\n' > "$decision_file"
          printf 'stdout\n' > "$stdout_log"
        }}
        validate_candidate_scope() {{ return 0; }}
        build_benchmark_args() {{ local -n out_ref=$1; out_ref=(); }}
        archive_run_artifacts() {{
          local run_dir={runs_dir / 'run-001'!s}
          mkdir -p "$run_dir"
          : > "$run_dir/patch.diff"
          printf '%s\n%s\n' "$run_dir" "$run_dir/patch.diff"
        }}
        discard_candidate_state() {{ :; }}
        append_issue() {{ :; }}
        record_issue_from_decision() {{ :; }}
        append_loop_log() {{ :; }}
        git() {{
          if [[ "$*" == *"rev-parse --short HEAD"* ]]; then
            printf 'abc123\n'
            return 0
          fi
          command git "$@"
        }}
        run_iteration 1
        """
    )

    result = run_autoloop_shell(script)

    assert result.returncode == 0
    assert list(root_candidate_dir.iterdir()) == []
    assert list(root_diff_dir.iterdir()) == []
    results_lines = (tmp_path / "results.tsv").read_text(encoding="utf-8").splitlines()
    assert results_lines[0].startswith("base_ref\ttarget\tmodel_status")
    assert "benchmark-gate-command-failed" in results_lines[1]


def test_run_baseline_if_needed_logs_reuse_for_matching_dataset(tmp_path):
    report_dir = tmp_path / "reports"
    baseline_summary = report_dir / "baseline" / "clickbench_funnel" / "benchmark-summary.json"
    baseline_summary.parent.mkdir(parents=True)
    write_summary(baseline_summary, "benchmarks/data/hits_sample.parquet")

    script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        TARGET=clickbench_funnel
        USE_SAMPLE=1
        RUN_BASELINE=0
        DRY_RUN=0
        ROOT_DIR={repo_root()!s}
        REPORT_DIR={report_dir!s}
        WORKTREE_DIR={tmp_path / 'worktree'!s}
        logs_file={tmp_path / 'loop.log'!s}
        append_loop_log() {{ printf '%s\n' "$1" >> "$logs_file"; }}
        build_benchmark_args() {{ local -n out_ref=$1; out_ref=(); }}
        archive_baseline_reports_from_worktree() {{ :; }}
        python() {{ exit 99; }}
        run_baseline_if_needed
        """
    )

    result = run_autoloop_shell(script)

    assert result.returncode == 0
    assert (tmp_path / "loop.log").read_text(encoding="utf-8").strip() == (
        "reusing baseline for clickbench_funnel on 1M-row sample"
    )


def test_run_iteration_archives_artifacts_and_records_result(tmp_path):
    worktree = tmp_path / "worktree"
    report_dir = tmp_path / "reports"
    logs_dir = report_dir / "logs"
    fake_bin_dir = tmp_path / "bin"
    runs_dir = report_dir / "runs" / "clickbench_funnel"
    baseline_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "reports" / "baseline" / "clickbench_funnel"
    candidate_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "reports" / "candidates" / "clickbench_funnel"
    diff_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "reports" / "diff" / "clickbench_funnel"
    scripts_dir = worktree / "benchmarks" / "autoresearch" / "pilot" / "scripts"

    logs_dir.mkdir(parents=True)
    fake_bin_dir.mkdir(parents=True)
    runs_dir.mkdir(parents=True)
    baseline_dir.mkdir(parents=True)
    candidate_dir.mkdir(parents=True)
    diff_dir.mkdir(parents=True)
    scripts_dir.mkdir(parents=True)
    (worktree / ".git").mkdir(parents=True)

    write_summary(baseline_dir / "benchmark-summary.json", "benchmarks/data/hits_sample.parquet")
    (scripts_dir / "evaluate_benchmark_candidate.py").write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "print(json.dumps({\n"
        "    'recommendation': 'keep',\n"
        "    'target_win': 'R3: Funnel(median=-6.00%,p95=-2.00%)',\n"
        "    'protected_status': 'clean',\n"
        "    'reason': 'target improvement without protected regression',\n"
        "}))\n",
        encoding="utf-8",
    )
    (fake_bin_dir / "python").write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "if [[ \"$1\" == \"benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py\" ]]; then\n"
        "  mkdir -p benchmarks/autoresearch/pilot/reports/candidates/clickbench_funnel\n"
        "  cat > benchmarks/autoresearch/pilot/reports/candidates/clickbench_funnel/benchmark-summary.json <<'JSON'\n"
        "{\"target\":\"clickbench_funnel\",\"correctness_failures\":0,\"infra_failures\":0,\"workloads\":[]}\n"
        "JSON\n"
        "  printf '# candidate\n' > benchmarks/autoresearch/pilot/reports/candidates/clickbench_funnel/benchmark-result.md\n"
        "  exit 0\n"
        "fi\n"
        "if [[ \"$1\" == \"benchmarks/autoresearch/pilot/scripts/benchmark_gate.py\" ]]; then\n"
        "  mkdir -p benchmarks/autoresearch/pilot/reports/diff/clickbench_funnel\n"
        "  cat > benchmarks/autoresearch/pilot/reports/diff/clickbench_funnel/benchmark-diff.json <<'JSON'\n"
        "{\"target\":\"clickbench_funnel\",\"workloads\":[]}\n"
        "JSON\n"
        "  cat > benchmarks/autoresearch/pilot/reports/diff/clickbench_funnel/evaluation.json <<'JSON'\n"
        "{\"recommendation\":\"keep\"}\n"
        "JSON\n"
        "  exit 0\n"
        "fi\n"
        "exec /usr/bin/python3 \"$@\"\n",
        encoding="utf-8",
    )
    (fake_bin_dir / "python").chmod(0o755)

    script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        TARGET=clickbench_funnel
        DRY_RUN=0
        ROOT_DIR={repo_root()!s}
        REPORT_DIR={report_dir!s}
        WORKTREE_DIR={worktree!s}
        export PATH={fake_bin_dir!s}:$PATH
        LOG_PREFIX=test-log
        LOOP_LOG={logs_dir / 'loop.log'!s}
        results_file={tmp_path / 'results.tsv'!s}
        issues_file={tmp_path / 'issues.tsv'!s}
        base_overlay_manifest={tmp_path / 'overlay.txt'!s}
        : > "$base_overlay_manifest"
        benchmark_log_path() {{
          printf '%s/%s.log\n' {logs_dir!s} "$1"
        }}
        run_single_candidate() {{
          local _run_index="$1"
          local decision_file="$2"
          local stdout_log="$3"
          printf 'status=keep\nscenario=archive-smoke\nreason=proceed\nevidence=ok\n' > "$decision_file"
          printf 'stdout\n' > "$stdout_log"
        }}
        validate_candidate_scope() {{ return 0; }}
        build_benchmark_args() {{ local -n out_ref=$1; out_ref=(); }}
        append_loop_log() {{ :; }}
        git() {{
          if [[ "$*" == *"rev-parse --short HEAD"* ]]; then
            printf 'abc123\n'
            return 0
          fi
          if [[ "$*" == *"diff -- ."* ]]; then
            printf 'diff --git a/src/ops/pattern_match.rs b/src/ops/pattern_match.rs\n'
            return 0
          fi
          if [[ "$*" == *"restore --worktree ."* ]] || [[ "$*" == *"clean -fd"* ]]; then
            return 0
          fi
          command git "$@"
        }}
        run_iteration 1
        """
    )

    result = run_autoloop_shell(script)

    assert result.returncode == 0, result.stderr

    run_dir = runs_dir / "run-001"
    assert (run_dir / "decision.txt").read_text(encoding="utf-8").startswith("status=keep\n")
    assert (run_dir / "evaluation.txt").exists()
    assert (run_dir / "stdout.log").read_text(encoding="utf-8") == "stdout\n"
    assert (run_dir / "patch.diff").read_text(encoding="utf-8").startswith("diff --git")
    assert (run_dir / "candidate" / "benchmark-summary.json").exists()
    assert (run_dir / "candidate" / "benchmark-result.md").exists()
    assert (run_dir / "diff" / "benchmark-diff.json").exists()
    assert (run_dir / "diff" / "evaluation.json").exists()

    assert (report_dir / "candidates" / "clickbench_funnel" / "benchmark-summary.json").exists()
    assert (report_dir / "diff" / "clickbench_funnel" / "benchmark-diff.json").exists()

    results_lines = (tmp_path / "results.tsv").read_text(encoding="utf-8").splitlines()
    assert len(results_lines) == 2
    assert results_lines[1].split("\t") == [
        "abc123",
        "clickbench_funnel",
        "keep",
        "keep",
        "archive-smoke",
        "R3: Funnel(median=-6.00%,p95=-2.00%)",
        "clean",
        "target improvement without protected regression",
        str(run_dir),
        str(run_dir / "patch.diff"),
    ]
    assert not (tmp_path / "issues.tsv").exists()


def test_sync_autoresearch_assets_preserves_existing_worktree_ledgers(tmp_path):
    worktree = tmp_path / "worktree"
    pilot_root = repo_root() / "benchmarks" / "autoresearch" / "pilot"
    worktree_pilot = worktree / "benchmarks" / "autoresearch" / "pilot"
    worktree_pilot.mkdir(parents=True)
    (worktree / ".git").mkdir(parents=True)

    root_results = pilot_root / "results.tsv"
    root_issues = pilot_root / "issues.tsv"
    original_results = root_results.read_text(encoding="utf-8")
    original_issues = root_issues.read_text(encoding="utf-8")

    worktree_results = worktree_pilot / "results.tsv"
    worktree_issues = worktree_pilot / "issues.tsv"
    worktree_results.write_text(
        "base_ref\ttarget\tmodel_status\trecommendation\thypothesis\ttarget_win\tprotected_status\tevidence\trun_dir\tpatch_path\n"
        "abc123\tclickbench_funnel\tkeep\tkeep\tscenario\twin\tclean\treason\trun\tpatch\n",
        encoding="utf-8",
    )
    worktree_issues.write_text(
        "id\tcategory\ttarget\tfile\ttitle\tevidence\tsuggested_fix\tstatus\trun_date\n"
        "AR-001\tharness\tclickbench_funnel\tfile\ttitle\tevidence\tfix\topen\t2026-04-20\n",
        encoding="utf-8",
    )

    script = textwrap.dedent(
        f"""
        source {autoloop_path()!s}
        ROOT_DIR={repo_root()!s}
        WORKTREE_DIR={worktree!s}
        AR_DIR={pilot_root!s}
        sync_autoresearch_assets
        """
    )

    result = run_autoloop_shell(script)

    assert result.returncode == 0, result.stderr
    assert worktree_results.read_text(encoding="utf-8").splitlines()[1].startswith(
        "abc123\tclickbench_funnel"
    )
    assert worktree_issues.read_text(encoding="utf-8").splitlines()[1].startswith(
        "AR-001\tharness\tclickbench_funnel"
    )
    assert root_results.read_text(encoding="utf-8") == original_results
    assert root_issues.read_text(encoding="utf-8") == original_issues
