# Stream error spike (#256)

Before M1 builds `to_batches()` and the PyCapsule stream, #256 asks whether
the two error paths of the v0.5 contract hold: Python reads of
`to_batches()` raise LTSeq's own class (§15.1), and the C stream ends with
`EIO` or `EINVAL` and a message that starts with the class name (§16.4). It
also gates OD-X6: does closing the reader stop the execution? The findings
are in [REPORT.md](REPORT.md); the raw results are under
[results/](results/).

| | What | Script | Results |
|---|---|---|---|
| Steps 1 and 2 | What each consumer raises for each error source, and what `close()` and dropping a reader release | `probe.py` | `results/probe.md`, `.json` |
| Step 3 | What reading `to_batches()` through the Rust iterator costs against today's C stream, idle and with a busy Python thread | `bench.py` | `results/bench.md`, `.json` |

## The spike

Everything compiles only with the `stream_spike` cargo feature, so no shipped
build carries it, and M1 replaces it.

- `src/stream_spike/reader.rs`: `_SpikeBatches`, a PyO3 iterator over one
  execution. `ltseq._stream_spike.to_batches(t)` wraps it in
  `pyarrow.RecordBatchReader.from_batches`. Its `gil` argument picks how
  `next()` waits: `detach` (ADR 0016's rule), `attached` (a lower bound,
  not a candidate) or `poll`.
- `src/stream_spike/capsule.rs`: a C stream with its own callbacks, which
  ends with `EIO` for `LTSeqIOError` and `SourceNotFoundError` and `EINVAL`
  otherwise, with `get_last_error` returning `"<class>: <message>"`.
  `ltseq._stream_spike.Stream(t)` exposes it as `__arrow_c_stream__`.
- `src/stream_spike/classify.rs`: the class of an execution error, from the
  error at the bottom of DataFusion's chain.
- `py-ltseq/ltseq/_stream_spike.py`: stand-ins for the contract's exception
  classes (§20.1), which do not exist yet, and the helpers above. A
  `fault=(after, class, message)` raises a class after `after` batches;
  `ArithmeticOverflowError` and `OrderViolationError` have no execution error
  today, so only a fault can raise them.
- `py-ltseq/tests/test_stream_spike.py` holds the spike to the report's
  outcomes; it skips on a default build.

## Running

```bash
maturin develop --release --features stream_spike
uv pip install duckdb polars          # for their columns; neither is a project dependency
python benchmarks/stream_errors/probe.py
python benchmarks/stream_errors/bench.py           # about 15 minutes; --quick for a trial
pytest py-ltseq/tests/test_stream_spike.py
```

`bench.py` writes its 640 MiB input (the R3 harness's 8 Parquet files)
under `target/stream_errors/` on its first run, and runs each cell in a
fresh process. It takes `--partitions`, `--batch-sizes`,
`--busy-batch-sizes`, `--repeat` and `--busy-repeat`.
