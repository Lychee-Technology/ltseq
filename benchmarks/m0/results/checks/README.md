# Checks

Extra runs that test how far a figure in `../` can be trusted. The report
reads each one in the section named. `report.py --dir results/checks/<name>`
regenerates each table.

## m4b's sort cells

Two extra runs of `m4b.sort_then_filter`, at the same commit and build as
`../m4b.json`. Today's time for this pipeline at one partition is 0.09 s in
some runs and 0.13 s in others, so these runs test how far the
one-partition ratios in `../m4b.json` can be trusted.

- `sort_15_runs/`: every mode and partition count with 15 measured runs
  instead of 5 (`run.py --only m4b --pipeline sort_then_filter --repeat 15`).
- `sort_fixed_mmap_threshold/`: one partition, 15 runs, with glibc's mmap
  threshold fixed at 32 MiB, which stops glibc from raising it after large
  frees. The command in its header leaves out the environment:
  `MALLOC_MMAP_THRESHOLD_=33554432 python benchmarks/m0/run.py --only m4b
  --pipeline sort_then_filter --partitions 1 --repeat 15`.

## m3's buffer when it binds

`m3_batch_4096/`: m3's R3 cells at 4, 12 and 24 partitions with 4,096-row
batches (`run.py --only m3 --pipeline r3_filter_select_derive --partitions
4,12,24 --batch-size 4096`). At the default batch size an R3 row group
filtered to 75% leaves 6 batches, so a buffer of 8 holds a whole unit and
never stops one. At 4,096 rows a unit is 24 batches, so every buffer
measured binds. It ran at `e168139`; since `bfa963a`, where `../m3.json`
ran, m3's code has changed only in a comment.
