# Stable D6 evidence: what is recorded (#296, PR #423)

| Item (R1-F5 of PR #423's review) | State |
|---|---|
| R1-F4: release-build deep `distinct` timings, 55 against stable 56 | recorded: `deep-distinct-55.json`, `deep-distinct-56.json`, `deep-distinct.md` |
| R1-F4: plain-DataFusion reproduction and draft upstream report | recorded: `../deep_sort_repro/`, `deep-sort-repro-55.tsv`, `deep-sort-repro-56.tsv`, `../UPSTREAM_DRAFT.md`; filing upstream is the operator's |
| The fat-LTO release build | both wheels built (see `../README.md`); packaging record to follow |
| 55-red / 56-green matrix on unsorted and sorted tables | to run |
| Optimized logical and physical plans of representative cases | to run |
| Direct SQL probe (runtime items, volatile values) | to run |
| B1 witnesses and v0.5 §8.5's membership contract | to run |
| Guard ablations on stable | to run |
| `bench_core.py` and `bench_in_lists.py` | to run |
| Memory harness | to run |
| macOS | from CI (the owner's decision) |
