from __future__ import annotations

import importlib.util
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
