# Memory harness on stable 56 (#296, PR #423)

`pytest py-ltseq/tests/memory -m memory -v` (contract row R3, #274), run
from each tree's root with that tree's fat-LTO release `.so` in
`py-ltseq/ltseq/` (CI's `memory-harness` job runs the same command on a
debug build). The full pytest output of each run is committed as plain-text
evidence, `.txt` like `bench_core/in-lists-*.txt`, since the repository's
`*.log` rule keeps local logs out of git: [`memory/harness-55.txt`](memory/harness-55.txt),
[`memory/harness-56.txt`](memory/harness-56.txt). The 56 run on the final Rust
sources (`5a6fdfa`) is [`final-head/harness-56.txt`](final-head/harness-56.txt).

| | 55.0.0 (`291aa14`) | 56.0.0 stable |
|---|---|---|
| Outcome | 16 passed, 5 deselected, 2 xfailed | 16 passed, 5 deselected, 2 xfailed |

Every test has the same outcome on both engines (the per-test lists match).
The 2 xfails are `write_csv` and `iterate`, marked expected failures in both trees. The peaks the
harness reports, above the level before each reader's call:

| Pipeline -> reader | 55.0.0 (`291aa14`) | 56.0.0 stable | Bound |
|---|---|---|---|
| `filter_select_derive -> to_batches` | peak 13.0 MiB, within | peak 10.3 MiB, within | 64.0 MiB budget |
| `filter_select_derive -> count` | peak 9.0 MiB, within | peak 6.2 MiB, within | 64.0 MiB budget |
| `filter_select_derive -> write_parquet` | peak 44.5 MiB, within | peak 41.8 MiB, within | 64.0 MiB budget |
| `filter_select_derive -> write_csv` | peak > 259.5 MiB (stopped), OVER | peak > 257.1 MiB (stopped), OVER | 64.0 MiB budget |
| `filter_select_derive -> iterate` | peak > 259.2 MiB (stopped), OVER | peak > 265.0 MiB (stopped), OVER | 64.0 MiB budget |
| `collect -> to_batches` | peak > 259.5 MiB (stopped), OVER | peak > 265.2 MiB (stopped), OVER | 64.0 MiB budget |
| `assume_sorted -> to_batches` | peak 12.6 MiB, within | peak 11.2 MiB, within | 64.0 MiB budget |
| `collect_head -> to_batches` | peak 42.9 MiB, OVER | peak 42.8 MiB, OVER | 28.6 MiB state bound |
| `shift -> to_batches` | peak 16.2 MiB, within | peak 13.9 MiB, within | 28.6 MiB state bound |
| `shift_by_key -> to_batches` | peak > 263.9 MiB (stopped), OVER | peak > 265.9 MiB (stopped), OVER | 28.6 MiB state bound |
| `rolling -> to_batches` | peak 15.7 MiB, within | peak 14.6 MiB, within | 28.6 MiB state bound |
| `cum_sum -> to_batches` | peak 14.9 MiB, within | peak 13.8 MiB, within | 28.6 MiB state bound |
| `row_number -> to_batches` | peak 30.1 MiB, OVER | peak 23.8 MiB, within | 28.6 MiB state bound |
| `group_ordered -> to_batches` | peak > 256.5 MiB (stopped), OVER | peak > 256.3 MiB (stopped), OVER | 28.6 MiB state bound |
| `search_pattern -> to_batches` | peak > 261.6 MiB (stopped), OVER | peak > 262.7 MiB (stopped), OVER | 28.6 MiB state bound |
| `asof_join -> to_batches` | peak > 263.9 MiB (stopped), OVER | peak > 264.5 MiB (stopped), OVER | 28.6 MiB state bound |
| `distinct -> to_batches` | peak > 260.7 MiB (stopped), OVER | peak > 257.3 MiB (stopped), OVER | 28.6 MiB state bound |
| `tail -> to_batches` | peak 9.1 MiB, within | peak 10.3 MiB, within | 28.6 MiB state bound |

The *Bound* column is 55's; 56's state bound is 27.2 MiB (an 11.2 MiB
baseline against 12.6 MiB on 55, with the same 16 MiB allowance). "OVER"
rows are the harness's reported should-bound pipelines (the same on both
engines) and its known materializing ones; the only change of verdict is
`row_number`, which comes within its bound on 56. No pipeline that streams
on 55 stops streaming on 56.
