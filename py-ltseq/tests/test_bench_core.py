"""bench_core measures what each benchmark names (issue #151).

Pinned down here:

- The timed region must execute the result. LTSeq transforms are lazy, so an
  unexecuted result times plan construction only (search_first used to), and
  ``len()`` lets DataFusion drop derived columns and sorts.
- Operator benchmarks must not read their source file inside the timed
  region; only the read and end-to-end benchmarks may.
- Order-dependent operators get inputs whose order DataFusion can see, so
  their timed plans do not sort the input again.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pytest

from ltseq import LTSeq


def load_bench_core():
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "benchmarks" / "bench_core.py"
    spec = importlib.util.spec_from_file_location("ltseq_bench_core", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bench_core = load_bench_core()


def small_table() -> LTSeq:
    return LTSeq.from_arrow(pa.table({"id": list(range(100))}))


# A column or sort key that fails only when it is evaluated: len() would
# return 100 for both without raising.
UNEVALUATED_BY_LEN = {
    "derived column": lambda t: t.derive(boom=lambda r: r.id / 0),
    "sort key": lambda t: t.sort(lambda r: r.id / 0),
}


@pytest.mark.parametrize("build", UNEVALUATED_BY_LEN.values(), ids=UNEVALUATED_BY_LEN)
def test_timed_region_executes_the_whole_result(tmp_path, build):
    bench = bench_core.Benchmarks(str(tmp_path))
    t = small_table()

    with pytest.raises(RuntimeError, match="Divide by zero"):
        bench.run_benchmark(
            "boom", 100, lambda: build(t), group=bench_core.OPERATOR, warmup=0
        )


def test_lazy_wrappers_are_rejected_instead_of_timed_as_no_ops(tmp_path):
    bench = bench_core.Benchmarks(str(tmp_path))
    t = small_table().sort("id")

    with pytest.raises(TypeError, match="NestedTable"):
        bench.run_benchmark(
            "nested",
            100,
            lambda: t.group_ordered(lambda r: r.id),
            group=bench_core.OPERATOR,
            warmup=0,
        )


@pytest.mark.parametrize("value", [None, 7])
def test_terminal_results_need_no_execution(tmp_path, value):
    bench = bench_core.Benchmarks(str(tmp_path))

    result = bench.run_benchmark(
        "terminal", 1, lambda: value, group=bench_core.IO, warmup=0, iterations=1
    )

    assert result.group == bench_core.IO


def reads_its_source(name: str, group: str) -> bool:
    return group == bench_core.END_TO_END or name.startswith("read_")


@dataclass
class SuiteRun:
    results: list
    # Benchmark name -> types its op returned, and the physical plan of the
    # last LTSeq it returned.
    returned: dict[str, set[type]] = field(default_factory=dict)
    plans: dict[str, str] = field(default_factory=dict)


@pytest.fixture
def suite_run(tmp_path, monkeypatch) -> SuiteRun:
    """Run the whole suite at tiny sizes, corrupting every CSV in the temp
    dir while a benchmark that should not read it is being timed, and record
    what each benchmark's op returned."""
    run = SuiteRun(results=[])
    current: list[str] = []

    original_consume = bench_core.consume

    def recording_consume(result):
        name = current[-1]
        run.returned.setdefault(name, set()).add(type(result))
        if isinstance(result, LTSeq):
            run.plans[name] = result.explain_plan()[1]
        original_consume(result)

    original_run = bench_core.Benchmarks.run_benchmark

    def guarded_run(self, name, rows, op, *, group, **kwargs):
        current.append(name)
        csvs = {} if reads_its_source(name, group) else {
            p: p.read_bytes() for p in Path(self.temp_dir).glob("*.csv")
        }
        for p in csvs:
            p.write_text("not,the\nbenchmark,input,at,all\n")
        try:
            return original_run(self, name, rows, op, group=group, **kwargs)
        except RuntimeError as e:
            if csvs and "Csv error" in str(e):
                pytest.fail(f"{name} parsed its source CSV inside the timed region: {e}")
            raise
        finally:
            for p, content in csvs.items():
                p.write_bytes(content)

    monkeypatch.setattr(bench_core, "consume", recording_consume)
    monkeypatch.setattr(bench_core.Benchmarks, "run_benchmark", guarded_run)

    bench = bench_core.Benchmarks(str(tmp_path))
    bench_core.run_suite(bench, small=20, medium=40, large=60)
    run.results = bench.results
    return run


def test_only_reads_and_end_to_end_benchmarks_touch_the_csv(suite_run):
    # Reaching this point means no other benchmark parsed a corrupted CSV.
    timed_without_source = [
        r for r in suite_run.results if not reads_its_source(r.name, r.group)
    ]

    assert {r.name for r in timed_without_source} >= {
        "filter_60",
        "chain_60",
        "sort_60",
        "search_first_40",
        "write_parquet_40",
    }


def test_every_benchmark_hands_its_result_to_the_harness(suite_run):
    for r in suite_run.results:
        expected = type(None) if r.name.startswith("write_parquet") else LTSeq
        assert suite_run.returned[r.name] == {expected}, r.name


def test_order_dependent_operators_do_not_sort_their_input(suite_run):
    # Their input is sorted before timing; a sort on the declared key inside
    # the timed plan would time that sort again.
    declared_key = {"window_lag": "id", "window_cumsum": "id", "group_ordered": "category"}
    checked = []
    for name, plan in suite_run.plans.items():
        for prefix, key in declared_key.items():
            if name.startswith(prefix):
                assert f"SortExec: expr=[{key}@" not in plan, f"{name}:\n{plan}"
                checked.append(name)

    assert len(checked) == 6


def test_groups_and_names_agree(suite_run, tmp_path):
    results = suite_run.results

    for r in results:
        assert r.group in bench_core.GROUP_DESCRIPTIONS, r.name
        assert r.name.startswith("e2e_") == (r.group == bench_core.END_TO_END), r.name
    assert {r.group for r in results} == set(bench_core.GROUP_DESCRIPTIONS)

    out = tmp_path / "results.json"
    bench_core.save_results(results, str(out))
    saved = json.loads(out.read_text())["results"]
    assert [row["group"] for row in saved] == [r.group for r in results]
