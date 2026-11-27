# Draft upstream report: planning a sort or grouping over a deep expression is about cubic in its depth in 56.0.0

**Status: the operator reports filing this at https://github.com/apache/datafusion/issues/27763 (#296, 2026-11-26).** The
owner's decision on PR #423's review round 1 (R1-F4) asks the operator to
file this with apache/datafusion before the upgrade merges; the filed issue
is the tracked mitigation for the reduced `distinct` depth in
`test_operations_on_a_deep_plan_on_a_small_thread`. Its URL is also recorded
in PR #423's description. When it is fixed upstream, raise `DISTINCT_DEEP`
in `py-ltseq/tests/test_expression_depth.py` back to the test's `DEEP` (500).

The text below the line is the proposed issue body. The reproduction is
[`deep_sort_repro/repro.rs`](deep_sort_repro/repro.rs); it uses only
DataFusion's public API.

---

### Title

Physical planning of a Sort / Aggregate over a deeply nested projected expression is ~cubic in expression depth (regression from 55.0.0)

### Describe the bug

Since 56.0.0, creating the physical plan for a query that sorts or groups by
a column computed by a deeply nested expression takes time that grows about
as the cube of the expression's depth. 55.0.0 grows about quadratically and
is two orders of magnitude faster at depth 200. Logical optimization is
unaffected; the time is in `create_physical_plan`, and for a sort also in
execution.

The expression is an ordinary left-deep chain, `a + 1 + 1 + ... + 1`
(depth = the column plus `depth - 1` additions), projected as `s`, then
`ORDER BY s` or `GROUP BY a, s`.

### To Reproduce

```rust
// Cargo.toml: datafusion = "=56.0.0", tokio = { version = "1", features = ["rt-multi-thread"] }
fn deep(depth: usize) -> Expr {
    let mut e = col("a");
    for _ in 1..depth { e = e + lit(1i64); }
    e
}
// t: MemTable with one Int64 column `a` = [1, 2, 3]
let df = ctx.table("t").await?
    .select(vec![col("a"), deep(depth).alias("s")])?
    .sort(vec![col("s").sort(true, false)])?;        // or .aggregate(vec![col("a"), col("s")], vec![])?
let optimized = df.clone().into_optimized_plan()?;
let start = Instant::now();
let physical = ctx.state().create_physical_plan(&optimized).await?;
println!("{:?}", start.elapsed());
```

The complete program (with the timing of each phase, run on a 1 GiB stack
because planning recurses through the expression) builds unchanged against
55.0.0 and 56.0.0: `cargo run --release --locked -q -- 25 50 100 200`.

### Measurements

`create_physical_plan` time in seconds, release build (`cargo --release`),
Intel Core i7-8700K (8 CPUs visible, Linux 7.0.14), rustc 1.97.1, three
runs each (all runs are in `results/deep-sort-repro-55.tsv` and
`results/deep-sort-repro-56.tsv`):

| Shape | Depth | 55.0.0 | 56.0.0 | 56 / 55 |
|---|---:|---:|---:|---:|
| `ORDER BY s` | 25 | 0.0015 | 0.0057 | 3.8× |
| `ORDER BY s` | 50 | 0.0028 | 0.0455 | 16× |
| `ORDER BY s` | 100 | 0.0093 | 0.52 | 56× |
| `ORDER BY s` | 200 | 0.035 | 6.5–7.1 | ~195× |
| `GROUP BY a, s` | 25 | 0.0010 | 0.0040 | 4× |
| `GROUP BY a, s` | 50 | 0.0028 | 0.032 | 11× |
| `GROUP BY a, s` | 100 | 0.0092 | 0.34 | 37× |
| `GROUP BY a, s` | 200 | 0.034 | 4.3–5.2 | ~130× |

Each doubling of the depth multiplies 56's time by 8 to 14 (between
depth^3 and depth^3.8, steeper as the depth grows), against 2 to 4 on 55.
For `ORDER BY s`, collecting the three rows also takes 1.1–1.3 s on 56 at
depth 200, against 0.0002 s on 55.

### Expected behavior

Planning cost close to 55.0.0's, roughly linear or quadratic in the
expression's size.

### Additional context

A debug-build investigation in the downstream project (LTSeq) pointed at the
sort's equivalence properties (`extract_common_sort_prefix`, `reorder` in
`datafusion-physical-expr`'s equivalence code), which appear to compare or
normalize the whole sort expression repeatedly; that attribution has not been
re-profiled on the 56.0.0 release build. Downstream, a `distinct()` over a
200-level computed column on a sorted table takes 28.6 s on 56.0.0 against
0.11 s on 55.0.0 (release builds).
