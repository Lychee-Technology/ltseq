# Exact Top-K aggregate pruning research (#386)

Measurements, a simulation and a spike that decide whether Pivot-style
slot-bound pruning ("Know Your LIMIT") is worth building for LTSeq's
`group by key, count, sort desc, first K` queries. The findings and the
Go/No-Go recommendation are in [REPORT.md](REPORT.md); the per-cell tables
are under [results/](results/).

| File | What it does |
|---|---|
| `fixtures.py` | Writes the synthetic Parquet fixtures (20M rows; uniform, Zipf, hot-key and flat counts over 1K, 100K and 1M keys; int and URL-like string keys) to `benchmarks/data/topk_agg/` |
| `queries.py` | The grouped Top-K query shapes through LTSeq's API, and a tie-aware DuckDB check of every result |
| `child.py` | One cell in a process of its own: warm-up, timed runs, peak memory, operator metrics, check |
| `run.py` | Phase 1: the baseline matrix → `results/cells.json` |
| `ceiling.py` | Simulates the pruner on a query's exact per-group counts (from DuckDB): how much final-merge input it could skip |
| `report.py` | Phase 1 tables: timings, stage CPU shares, the pruning simulation and the ceiling → `results/cells.md`, `results/summary.json`, `results/plans/` |
| `compare.py` | Phase 3: baseline vs the spike vs its control, interleaved in one process per cell → `results/compare.json`, `results/compare.md` |
| `selfcheck.py` | Randomized checks that the simulated pruner, and the spike's pre-merge thresholds, never skip a possible winner |

`results/v1/` holds the comparison of the first spike (fixed slots, merges
in doubling waves; commit `96a3b17`), and `results/late_bail/` a partial
rerun of the second spike before it had its probe (28 synthetic cells);
REPORT.md compares both with the current one.

## The research build

The metrics hook and the spike compile only with the `topk_research` cargo
feature, so no shipped wheel carries them and the default build is
unchanged (`src/topk_research/`):

- `ltseq_core._topk_analyze(table._inner)` executes the plan a terminal
  runs and returns its per-operator DataFusion metrics. Stage costs come
  from these counters, not from differences of wall-clock times.
- `ltseq_core._topk_set_prune(slots, prune=True, rows_per_slot=0)` plans
  the spike, `SlotPrunedCountExec`, in place of the final aggregate of an
  eligible query from the next terminal on: `slots` fixed slots per final
  partition, or one slot per `rows_per_slot` buffered partial rows when that
  is non-zero; both 0 turn it off. `prune=False` is the control: the same
  operator and first merge, then every row merged as it is.

`py-ltseq/tests/test_topk_research.py` holds the spike to the unpruned
answer on a measurement build and checks that a default build has neither
hook.

## Running

```bash
maturin develop --release --features topk_research       # ~11 min, fat LTO
uv run --group bench python benchmarks/topk_agg/fixtures.py   # ~1.5 min, ~4 GiB
uv run --group bench python benchmarks/prepare_data.py        # ClickBench hits.parquet, optional
python benchmarks/topk_agg/selfcheck.py
python benchmarks/topk_agg/run.py                             # ~45 min; --groups, --clickbench PATH
python benchmarks/topk_agg/report.py                          # ~3 min (DuckDB group counts)
python benchmarks/topk_agg/compare.py                         # ~75 min; --only, --clickbench PATH
```

`run.py` and `compare.py` reuse `benchmarks/m0/harness.py`: before each
cell they wait for the machine to go quiet, record the cores other
processes kept busy while the cell ran (`other cores`), and rerun a cell
above half a core. The ClickBench cells run when `hits.parquet` is found
(`benchmarks/data/hits.parquet` by default).
