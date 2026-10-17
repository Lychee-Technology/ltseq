# M0 feasibility measurements (#251)

Prototypes and a harness that price six commitments of the v0.5 contract
before they are built. The findings are in [REPORT.md](REPORT.md); the
per-cell tables are under [results/](results/).

| | Measurement | Prototype (`src/m0/`) | Gates |
|---|---|---|---|
| m1 | Checked integer `+ - * // % **`, `abs`, `sum` | `checked.rs` | M5, #221 |
| m2 | Exact Σx, Σx², Σab: sum, mean, var, std, cov, corr, rolling | `exact.rs`, `exact_acc.rs` | M13, M4, OD-W |
| m3 | Order-preserving merge, including TTFR with a slow partition | `ordered.rs` | M1, #148 |
| m4 | Masked `&`/`\|` with fallible operands (D10) | `masked.rs` | M16, #252 |
| m4b | Filter pushdown lost past sort and aggregate (D10a) | `pushdown.rs` | M16 |
| m5 | `assume_sorted` validated on every pair, no pruning (D7) | `validate.rs` | M3 |
| m6 | `string_view` normalized to `string` | `views.rs` | M18 |

## How the prototypes are built and switched

The prototypes compile only with the `m0` cargo feature, so no shipped wheel
contains them and the default build is unchanged. A measurement build runs
the real LTSeq paths, with the prototype rules added to the DataFusion
session (`src/m0/mod.rs`). Flags are process-wide and take effect when a
table is built, like `ltseq._testing.pinned_engine`'s layout override:

```python
from ltseq._testing import pinned_engine, prototypes

with pinned_engine(target_partitions=4), prototypes(checked="fast"):
    t = LTSeq.read_csv("data.csv")   # this table's session has the prototype
```

| Flag | Values |
|---|---|
| `checked` | `off`, `arrow` (arrow's checked kernels), `fast` (wrapping plus an overflow mask) |
| `exact` | `off`, `exact` (superaccumulator), `compensated` (double-double; not the contract) |
| `order` | `off`, `concat` (contiguous ranges, bounded buffers), `interleaved` (one unit per row group) |
| `order_window`, `order_buffer` | units in flight (default `target_partitions`), batches buffered per unit (2) |
| `slow_ms` | delay per batch of the first `1/target_partitions` of the scan |
| `masked` | `off`, `select`, `errors`, `optimistic` (with `checked` on, so operands can fail) |
| `restrict_pushdown` | `true` keeps a filter above the sort or aggregate below it |
| `validate` | `off`, `prune_off` (D7's read without the check), `validate` |
| `views` | `off`, `plain` (scan decodes `string`), `cast` (cast at the plan's root) |

Each prototype module's header says what it does and where it stops short
of the item it gates.

## Running

```bash
maturin develop --release --features m0           # ~11 min
uv run --group bench python benchmarks/prepare_data.py   # ClickBench files, for m3-m6
python benchmarks/m0/selfcheck.py                  # each prototype does what its measurement assumes
python benchmarks/m0/run.py --only m1,m2           # results/<m>.json, one subprocess per cell
python benchmarks/m0/report.py                     # results/<m>.md
```

`run.py` refuses a debug build unless given `--allow-debug`, and takes
`--partitions`, `--pipeline` and `--repeat` to run a subset. Rust unit
tests: `cargo test --features m0 --lib m0::`. The exact accumulator's
reference tests run on their own in [exact_acc/](exact_acc/).

## Method

Each cell (one pipeline, terminal, input, partition count and flag set)
runs in a fresh interpreter (`child.py`): it pins the layout, turns on the
flags, builds the input, runs the pipeline once over a 10K-row warm-up
input, then times the measured runs. A run's time covers building the plan
and draining the terminal. Peak memory follows the R3 harness
(`py-ltseq/tests/memory`): resident growth over the first measured run,
with the kernel's peak counter reset first. TTFR is the time to the first
non-empty batch of the Arrow C stream, which is what `to_batches` will
return. Inputs are `bench_core.py`'s synthetic table (held in memory for
operator measurements, read from CSV for the scan), the ClickBench files
and the R3 harness's input; `inputs.py` documents each.
