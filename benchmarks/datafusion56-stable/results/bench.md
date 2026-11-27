# `bench_core.py` and `bench_in_lists.py` on stable 56 (#296, PR #423)

The fat-LTO release wheels of `../README.md` (55.0.0 from `291aa14`, 56.0.0
stable from PR #423's tree), each `.so` placed in its own tree's
`py-ltseq/ltseq/`, which `bench_core.py` puts first on `sys.path`. Each
tree's own `benchmarks/bench_core.py` ran (the file is the same in both),
pinned to 8 partitions and 16,384-row batches, in the order 55, 56, 55, 56,
on an otherwise idle machine:

```bash
python <tree>/benchmarks/bench_core.py --target-partitions 8 --batch-size 16384
python compare_core.py --a "55.0.0 (291aa14)" bench_core/core-55-1.json bench_core/core-55-2.json \
    --b "56.0.0 stable" bench_core/core-56-1.json bench_core/core-56-2.json
```

Times are the better of each engine's two runs (all four runs are in
`bench_core/`); *spread* is the larger of the two engines' ratios between
their own runs, a floor on the noise.

| Benchmark | 55.0.0 (291aa14) (ms) | 56.0.0 stable (ms) | 56.0.0 stable / 55.0.0 (291aa14) | Spread |
|---|---|---|---|---|
| `filter_10000` | 0.72 | 0.63 | 0.88 | 1.04 |
| `filter_100000` | 0.79 | 0.75 | 0.95 | 1.09 |
| `filter_1000000` | 4.81 | 4.83 | 1.00 | 1.16 |
| `filter_complex_100000` | 0.80 | 0.75 | 0.94 | 1.09 |
| `derive_10000` | 0.58 | 0.49 | 0.84 | 1.07 |
| `derive_100000` | 0.66 | 0.53 | 0.80 | 1.03 |
| `derive_1000000` | 1.31 | 1.18 | 0.90 | 1.22 |
| `derive_multi_100000` | 0.89 | 0.81 | 0.91 | 1.02 |
| `join_10000x10000` | 1.52 | 1.55 | 1.02 | 1.12 |
| `join_100000x10000` | 5.06 | 4.87 | 0.96 | 1.08 |
| `join_100000x100000` | 6.80 | 7.35 | 1.08 | 1.08 |
| `window_lag_10000` | 1.10 | 0.95 | 0.86 | 1.09 |
| `window_lag_100000` | 5.14 | 5.00 | 0.97 | 1.10 |
| `window_cumsum_10000` | 2.36 | 2.15 | 0.91 | 1.18 |
| `window_cumsum_100000` | 18.19 | 17.24 | 0.95 | 1.01 |
| `group_agg_10000` | 1.15 | 1.19 | 1.03 | 1.08 |
| `group_agg_100000` | 1.31 | 1.41 | 1.08 | 1.17 |
| `chain_10000` | 1.06 | 0.96 | 0.91 | 1.17 |
| `chain_100000` | 1.22 | 1.15 | 0.94 | 1.02 |
| `chain_1000000` | 3.05 | 2.82 | 0.92 | 1.06 |
| `sort_10000` | 1.95 | 1.88 | 0.96 | 1.03 |
| `sort_100000` | 10.44 | 10.65 | 1.02 | 1.08 |
| `sort_1000000` | 114.74 | 115.21 | 1.00 | 1.04 |
| `sort_multi_100000` | 13.79 | 13.68 | 0.99 | 1.14 |
| `group_ordered_10000` | 5.90 | 5.41 | 0.92 | 1.01 |
| `group_ordered_100000` | 50.85 | 49.07 | 0.96 | 1.07 |
| `search_first_10000` | 0.57 | 0.46 | 0.81 | 1.07 |
| `search_first_100000` | 0.59 | 0.48 | 0.81 | 1.24 |
| `mutation_insert_10000` | 0.28 | 0.18 | 0.64 | 1.00 |
| `mutation_insert_100000` | 0.36 | 0.26 | 0.72 | 1.11 |
| `mutation_delete_10000` | 0.60 | 0.51 | 0.85 | 1.30 |
| `mutation_delete_100000` | 0.82 | 0.86 | 1.05 | 1.50 |
| `mutation_update_10000` | 0.85 | 0.79 | 0.93 | 1.09 |
| `mutation_update_100000` | 1.30 | 1.16 | 0.89 | 1.17 |
| `read_csv_10000` | 2.53 | 3.18 | 1.26 | 1.01 |
| `read_csv_100000` | 5.48 | 5.94 | 1.08 | 1.15 |
| `read_csv_1000000` | 35.16 | 36.35 | 1.03 | 1.11 |
| `write_parquet_10000` | 1.37 | 1.12 | 0.82 | 1.02 |
| `write_parquet_100000` | 15.71 | 13.46 | 0.86 | 1.06 |
| `read_parquet_10000` | 1.12 | 1.12 | 1.00 | 1.23 |
| `read_parquet_100000` | 4.42 | 4.22 | 0.95 | 1.33 |
| `e2e_csv_filter_10000` | 3.06 | 3.05 | 1.00 | 1.06 |
| `e2e_csv_filter_100000` | 6.69 | 6.90 | 1.03 | 1.04 |
| `e2e_csv_filter_1000000` | 36.05 | 37.30 | 1.03 | 1.09 |
| `e2e_csv_chain_10000` | 3.31 | 3.08 | 0.93 | 1.10 |
| `e2e_csv_chain_100000` | 5.69 | 5.79 | 1.02 | 1.10 |
| `e2e_csv_chain_1000000` | 28.35 | 27.68 | 0.98 | 1.02 |

Over the 47 benchmarks the geometric mean of 56.0.0 stable / 55.0.0 (291aa14) is 0.940; the ratios lie between 0.64 and 1.26.

Nothing is slower on 56 beyond its noise except `read_csv_10000` (2.53 ms
against 3.18 ms, 1.26 times, spread 1.01), a fixed cost of about 0.65 ms per
CSV read that the 100,000- and 1,000,000-row reads do not show. The faster
rows are mostly sub-millisecond operations.

These absolute times are 5.6 to 23 times lower (on 55) than those in the RC1 archive's
`benchmarks/datafusion56/results/bench.md`, which records the same CPU and
the same `bench_core.py`, while `bench_in_lists.py` below agrees with it.
That gap was not investigated (the archive's wheels were thin-LTO, these are
fat-LTO, and the host's load then is unknown); only the ratios within one
run are compared here.

## `bench_in_lists.py`

`benchmarks/datafusion56/bench_in_lists.py`, unchanged: compound membership
filters on 10 million rows, 8 partitions (the CPU count), the median of 7
runs, two rounds with 55 and 56 alternating (`bench_core/in-lists-*.txt`):

| Filter | 55.0.0 (`291aa14`) (ms) | 56.0.0 stable (ms) |
|---|---|---|
| `x.is_in([1, 2, 5, 9]) & x.is_in([1, 3, 4, 6])`, Int64 | 4.2, 4.2 | 4.1, 4.7 |
| `x.is_in([1, 2, 5]) & ~x.is_in([None, 3, 4, 6])`, Int64 | 7.3, 8.2 | 4.4, 4.6 |
| `x.is_in([1, 2, 5, 9]) \| x.is_in([11, 13, 14, 16])`, Int64 | 7.4, 7.4 | 7.5, 7.4 |
| `f.is_in([1.0, 2.0, 5.0, 9.0]) & f.is_in([1.0, 3.0, 4.0, 6.0])`, float64 | 5.2, 5.4 | 9.0, 8.6 |

As on RC1: the float intersection costs about 1.65 times as much on 56,
because apache/datafusion#24952 no longer intersects float lists, so both
are evaluated; the Int64 difference is cheaper on 56, where it is also right
(55 plans it as `x IN (1, 2, 5)`, which keeps rows the reference drops).
