# bench_core.py on release builds (#411)

Release wheels of each engine built the same way (`maturin build --release` with `CARGO_PROFILE_RELEASE_LTO=thin` and `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`, not the shipped fat LTO), each installed in its own virtualenv; `python -I benchmarks/bench_core.py` at the defaults (8 partitions, 16,384-row batches), run 55, 56, 55, 56. Times are the better of the two runs; *spread* is the larger of the two engines' ratios between their own runs, a floor on the noise. The 56 build carries the patch's guard extension and switches (off). The machine was otherwise idle.

| Benchmark | 55.0.0 (ms) | 56.0.0-rc1 (ms) | 56 / 55 | Spread |
|---|---|---|---|---|
| `filter_10000` | 4.71 | 4.66 | 0.99 | 1.01 |
| `filter_100000` | 6.38 | 6.47 | 1.01 | 1.02 |
| `filter_1000000` | 34.43 | 34.01 | 0.99 | 1.02 |
| `filter_complex_100000` | 7.81 | 7.85 | 1.01 | 1.01 |
| `derive_10000` | 3.70 | 3.63 | 0.98 | 1.21 |
| `derive_100000` | 3.86 | 3.85 | 1.00 | 1.03 |
| `derive_1000000` | 7.38 | 7.01 | 0.95 | 1.04 |
| `derive_multi_100000` | 6.72 | 6.72 | 1.00 | 1.05 |
| `join_10000x10000` | 13.68 | 13.52 | 0.99 | 1.02 |
| `join_100000x10000` | 55.55 | 54.61 | 0.98 | 1.01 |
| `join_100000x100000` | 73.85 | 73.11 | 0.99 | 1.04 |
| `window_lag_10000` | 8.35 | 8.50 | 1.02 | 1.03 |
| `window_lag_100000` | 32.41 | 31.90 | 0.98 | 1.03 |
| `window_cumsum_10000` | 16.13 | 15.67 | 0.97 | 1.03 |
| `window_cumsum_100000` | 110.43 | 109.04 | 0.99 | 1.05 |
| `group_agg_10000` | 10.58 | 10.62 | 1.00 | 1.03 |
| `group_agg_100000` | 16.59 | 16.64 | 1.00 | 1.01 |
| `chain_10000` | 8.26 | 8.21 | 0.99 | 1.02 |
| `chain_100000` | 10.11 | 10.17 | 1.01 | 1.03 |
| `chain_1000000` | 43.06 | 41.86 | 0.97 | 1.03 |
| `sort_10000` | 30.97 | 31.10 | 1.00 | 1.01 |
| `sort_100000` | 159.16 | 158.75 | 1.00 | 1.02 |
| `sort_1000000` | 1596.10 | 1573.81 | 0.99 | 1.01 |
| `sort_multi_100000` | 253.94 | 255.61 | 1.01 | 1.01 |
| `group_ordered_10000` | 54.59 | 53.49 | 0.98 | 1.04 |
| `group_ordered_100000` | 531.05 | 535.07 | 1.01 | 1.04 |
| `search_first_10000` | 4.08 | 4.29 | 1.05 | 1.02 |
| `search_first_100000` | 4.48 | 4.33 | 0.97 | 1.00 |
| `mutation_insert_10000` | 1.72 | 1.71 | 0.99 | 1.02 |
| `mutation_insert_100000` | 2.06 | 2.04 | 0.99 | 1.06 |
| `mutation_delete_10000` | 4.48 | 4.38 | 0.98 | 1.05 |
| `mutation_delete_100000` | 7.01 | 6.95 | 0.99 | 1.07 |
| `mutation_update_10000` | 8.50 | 8.61 | 1.01 | 1.03 |
| `mutation_update_100000` | 13.97 | 13.92 | 1.00 | 1.04 |
| `read_csv_10000` | 45.06 | 44.78 | 0.99 | 1.01 |
| `read_csv_100000` | 92.58 | 91.45 | 0.99 | 1.09 |
| `read_csv_1000000` | 759.43 | 743.65 | 0.98 | 1.02 |
| `write_parquet_10000` | 28.47 | 28.30 | 0.99 | 1.05 |
| `write_parquet_100000` | 297.24 | 299.74 | 1.01 | 1.00 |
| `read_parquet_10000` | 10.12 | 10.44 | 1.03 | 1.02 |
| `read_parquet_100000` | 51.48 | 52.26 | 1.02 | 1.08 |
| `e2e_csv_filter_10000` | 49.37 | 49.30 | 1.00 | 1.01 |
| `e2e_csv_filter_100000` | 98.72 | 103.03 | 1.04 | 1.07 |
| `e2e_csv_filter_1000000` | 766.01 | 779.23 | 1.02 | 1.01 |
| `e2e_csv_chain_10000` | 46.32 | 46.96 | 1.01 | 1.05 |
| `e2e_csv_chain_100000` | 87.28 | 81.82 | 0.94 | 1.06 |
| `e2e_csv_chain_1000000` | 654.09 | 646.54 | 0.99 | 1.01 |

Over the 47 benchmarks the geometric mean of 56 / 55 is 0.995; every ratio
lies between 0.94 and 1.05, and none is outside its spread by more than 0.05.
`bench_core.py` has no compound membership test, which is where #296's fix
changes plans, so `bench_in_lists.py` times four on 10 million rows (8
partitions; median of 7 runs; two rounds, 55 and 56 alternating):

| Filter | 55.0.0 (ms) | 56.0.0-rc1 (ms) |
|---|---|---|
| `x.is_in([1, 2, 5, 9]) & x.is_in([1, 3, 4, 6])`, Int64 | 4.6, 5.3 | 4.3, 4.5 |
| `x.is_in([1, 2, 5]) & ~x.is_in([None, 3, 4, 6])`, Int64 | 8.3, 9.0 | 5.5, 5.1 |
| `x.is_in([1, 2, 5, 9]) \| x.is_in([11, 13, 14, 16])`, Int64 | 8.0, 7.9 | 7.6, 8.6 |
| `f.is_in([1.0, 2.0, 5.0, 9.0]) & f.is_in([1.0, 3.0, 4.0, 6.0])`, float64 | 5.4, 5.2 | 8.5, 8.6 |

The float intersection costs about 1.6 times as much on 56: #24952 no
longer intersects float lists, so both are evaluated. The Int64 difference
is cheaper on 56, where it is also right (the 55 plan is `x IN (1, 2, 5)`,
which keeps rows the reference drops).
