# Stream error spike (#256)

Before M1 builds `to_batches()` and the PyCapsule stream, #256 asked whether
the two error paths of the v0.5 contract hold: Python reads of
`to_batches()` raise LTSeq's own class (§15.1), and the C stream ends with
`EIO` or `EINVAL` and a message that starts with the class name (§16.4). It
also gated OD-X6: does closing the reader stop the execution? The findings
are in [REPORT.md](REPORT.md); the raw results are under
[results/](results/).

| | What | Results |
|---|---|---|
| Steps 1 and 2 | What each consumer raises for each error source, and what `close()` and dropping a reader release, for pyarrow's readers and the native subclass | `results/probe.md`, `.json` |
| Step 3 | What reading `to_batches()` through the Rust iterator costs against today's C stream, idle and with a busy Python thread | `results/bench.md`, `.json` |

## The code is gone

The spike was retired, not ported, when LTSeq moved to DataFusion 56 and
arrow 60 (#296): its C stream wrote fields arrow 60 made private, and M1
replaces it anyway. The `stream_spike` cargo feature, `src/stream_spike/`,
`py-ltseq/ltseq/_stream_spike.py`, `py-ltseq/tests/test_stream_spike.py`
and this directory's scripts (`probe.py`, `bench.py`, `closing_reader.py`,
`_closing_reader.pyx`) were removed. Read them at commit `291aa14`, the
last commit that has them:

```bash
git show 291aa14:benchmarks/stream_errors/README.md   # how the spike was built and run
git ls-tree -r --name-only 291aa14 src/stream_spike benchmarks/stream_errors
```

Nothing checks the spike's outcomes any more (the class kept on reads, the
errno mapping, what `close()` does), so a pyarrow, DuckDB or Polars upgrade
that changes one goes unnoticed until M1's own tests cover those paths.
