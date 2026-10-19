# Checks on m4b's sort cells

Two extra runs of `m4b.sort_then_filter`, at the same commit and build as
`../m4b.json`. Today's time for this pipeline at one partition is 0.09 s in
some runs and 0.13 s in others, so these runs test how far the
one-partition ratios in `../m4b.json` can be trusted. The report's m4b
section reads them.

- `sort_15_runs/`: every mode and partition count with 15 measured runs
  instead of 5 (`run.py --only m4b --pipeline sort_then_filter --repeat 15`).
- `sort_fixed_mmap_threshold/`: one partition, 15 runs, with glibc's mmap
  threshold fixed at 32 MiB, which stops glibc from raising it after large
  frees. The command in its header leaves out the environment:
  `MALLOC_MMAP_THRESHOLD_=33554432 python benchmarks/m0/run.py --only m4b
  --pipeline sort_then_filter --partitions 1 --repeat 15`.

`report.py --dir results/checks/<name>` regenerates each table.
