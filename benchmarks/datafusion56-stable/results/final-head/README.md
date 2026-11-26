# The stable 56 evidence rerun on the final Rust sources (#296, PR #423)

The rest of `results/` ran on a release wheel built at `f3c9404`. One commit
after it changed Rust: `5a6fdfa` ("Keep ExprSchemable type lookups inside
Resolver") changes `src/transpiler/literal_policy.rs` and
`src/transpiler/resolve.rs`. The negative-scale string-cast check
(`negative_scale_string_cast`) now gets each operand's type through a closure
that `Resolver::resolve` passes in, `ask(|| operand.get_type(schema)).ok()`,
where it used to call `get_type` itself.

What changed in behaviour: a type lookup that returns an error still counts
as "not a string" and the check continues, as before. A lookup that
**panics** used to unwind out of the transpiler; it is now caught by `ask`
and also counts as "not a string". No other path changed, and no
DataFusion, arrow or `Cargo.lock` change came with it. That cannot change
a result the `f3c9404` runs recorded, because none of them panicked. The
items below that run LTSeq's transpiler were rerun on a release wheel of
`5a6fdfa` anyway, to check that.

Build: `5a6fdfaff773f95790fefff51b2f41317644abdf`, same machine, toolchain
and command as `../packaging.md` (fat LTO, `maturin build --release
--locked`, offline, no warnings). Wheel 33,473,654 bytes, SHA-256
`82361a32debb57d17ab5f54b6c2cd5c48bfe72ce06741528424f3007f022a6cc`;
`ltseq_core` `.so` 91,333,800 bytes (128 bytes more than `f3c9404`'s),
SHA-256 `8ae711e2e1bef1f4685d5796e377cf8852facfc927cd7a0146b662cf01c8f64a`.
The 55 side was not rebuilt. Its baseline is still `291aa14`, and the 55
files in `results/` are compared against these.

| Item | Files here | Against `f3c9404`'s 56 run |
|---|---|---|
| 55-red / 56-green matrix | `repro-56.json.gz`, `matrix.md` (`--compare` with `../repro-55.json.gz`) | the same: 308 wrong on 55 and right on 56, 0 right on 55 and wrong on 56, 0 wrong on both. Only four lines of the *Volatile values* table differ, by the random draws' fractions; every one is still `ok` |
| Plans | `plans-56.json` | `--compare-plans` with `../plans-55.json` gives `../plans.md` byte for byte |
| Witnesses | `witnesses-56.json` | `--compare` with `../witnesses-55.json` gives `../witnesses.md` byte for byte |
| Deep `distinct` timings | `deep-distinct-56.json`, `deep-distinct.md` | every median within 2 % of `f3c9404`'s (sorted, 200 levels: 28.3 s against 28.6 s; 25 levels: 0.022 s against 0.023 s) |
| Memory harness | `harness-56.log` | 16 passed, 5 deselected, 2 xfailed; per-test outcomes and every reader's within/OVER verdict the same. The peaks of pipelines that finish move by at most 1.7 MiB (`row_number`, 23.8 to 22.1 MiB), against a state bound of 26.9 MiB here (10.9 MiB baseline) |

Not rerun on `5a6fdfa`, and why:

- **Direct SQL probe** (`../probe-*.jsonl`, `../../df_probe56/`): plain
  DataFusion with no LTSeq code, so the commit cannot reach it.
- **Guard ablations** (`../suite.md`, debug switch builds): the commit
  touches neither the guards nor the rules the switches turn off. It
  changes only how the negative-scale check reads a type, which no ablated
  test depends on.
- **Traced plans** (`../plans-traced-*`): same switch builds as the
  ablations. The untraced plans above come out unchanged.
- **`bench_core.py` / `bench_in_lists.py`** (`../bench.md`): the commit adds
  one `catch_unwind` around each type lookup in that check, and it costs
  nothing unless something panics. The deep `distinct` runs plan the most
  expressions of anything measured here and show no change.
- **Packaging** (`../packaging.md`): the same profile builds without
  warnings; size and hashes are above.

```bash
REPO=$(git rev-parse --show-toplevel); cd "$REPO"
maturin build --release --locked -o /var/tmp/final/wheels      # at 5a6fdfa
mkdir /var/tmp/final/site && (cd /var/tmp/final/site && python -m zipfile -e /var/tmp/final/wheels/*.whl .)
S=benchmarks/datafusion56 R=benchmarks/datafusion56-stable/results O=$R/final-head
PYTHONPATH=/var/tmp/final/site python $S/repro_296.py --label "56.0.0 stable" --out $O/repro-56.json.gz
PYTHONPATH=/var/tmp/final/site python $S/repro_296.py --plans --label "56.0.0 stable" --out $O/plans-56.json
PYTHONPATH=/var/tmp/final/site python $S/witnesses.py --label "56.0.0 stable" --out $O/witnesses-56.json
(cd benchmarks/datafusion56-stable && PYTHONPATH=/var/tmp/final/site python bench_deep_distinct.py --label 56 --out results/final-head/deep-distinct-56.json)
python $S/repro_296.py --compare $R/repro-55.json.gz $O/repro-56.json.gz > $O/matrix.md
# Memory harness: an export of 5a6fdfa with the wheel's .so in py-ltseq/ltseq/
pytest py-ltseq/tests/memory -m memory -v
```
