# LTSeq vs DuckDB: ClickBench Benchmark — Detailed Execution Plan

> Generated from analysis of `click-bench-plan.md` and full codebase investigation.
> Date: 2026-02-25

---

## Critical Corrections to Original Plan

The original `click-bench-plan.md` contains several API errors that must be fixed:

| # | Original Plan | Actual API | Fix |
|---|---------------|-----------|-----|
| 1 | `TSeq.read_parquet(...)` | Class is `LTSeq`; **`read_parquet()` does not exist yet** | Implement `read_parquet()` in Rust + Python (Phase 0) |
| 2 | `.str.starts_with(...)` | String accessor is `.s`, not `.str` | Use `r.URL.s.starts_with(...)` |
| 3 | `.sort(lambda r: -r.count)` | `Expr` has no `__neg__`; no lambda-based descending sort | Use `.sort("cnt", desc=True)` |
| 4 | `.to_list()` | Method is `.collect()` (returns list of dicts) | Use `.collect()` |
| 5 | `group_ordered(cond).count()` | `.count()` on NestedTable returns a `CallExpr`, not int | Use `.flatten().select("__group_id__").distinct().count()` |
| 6 | URL patterns `/`, `/product%`, `/cart` | ClickBench data is Yandex Metrica (Russian sites) | Must investigate real URL patterns |
| 7 | `duckdb`, `psutil` not in deps | Only `pyarrow` is a runtime dep | Install manually or add to optional deps |

---

## Phase 0: Implement `read_parquet()` [PREREQUISITE]

LTSeq only has `scan_parquet()` → returns streaming `Cursor` (no `.filter()`, `.agg()`, etc.).
We need `read_parquet()` → returns `LTSeq` table (full API available).

### 0.1 — Rust: Add `read_parquet` to `LTSeqTable` (`src/lib.rs`)

Add inside the single `#[pymethods] impl LTSeqTable` block, modeled after `read_csv` (lines 53-80):

```rust
/// Read Parquet file into DataFusion DataFrame
///
/// Args:
///     path: Path to Parquet file
#[pyo3(signature = (path))]
fn read_parquet(&mut self, path: String) -> PyResult<()> {
    RUNTIME.block_on(async {
        let options = datafusion::datasource::file_format::parquet::ParquetReadOptions::default();
        let df = self.session.read_parquet(&path, options).await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to read Parquet: {}", e)
            )
        })?;
        let df_schema = df.schema();
        let arrow_fields: Vec<arrow::datatypes::Field> =
            df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        self.schema = Some(Arc::new(arrow_schema));
        self.dataframe = Some(Arc::new(df));
        Ok(())
    })
}
```

**Import note**: `ParquetReadOptions` is already used in `src/cursor.rs`; may need adding to `lib.rs` imports.

### 0.2 — Python: Add `read_parquet` classmethod (`py-ltseq/ltseq/io_ops.py`)

```python
@classmethod
def read_parquet(cls, path: str) -> "LTSeq":
    """Read a Parquet file into an LTSeq table.
    
    Args:
        path: Path to the Parquet file.
    
    Returns:
        LTSeq table with all data loaded.
    """
    from .core import LTSeq
    import os

    t = LTSeq()
    t._name = os.path.splitext(os.path.basename(path))[0]
    t._inner.read_parquet(path)
    # Parquet is self-describing; read schema from metadata
    import pyarrow.parquet as pq
    pq_schema = pq.read_schema(path)
    t._schema = {field.name: str(field.type) for field in pq_schema}
    return t
```

### 0.3 — Build & Test

```bash
maturin develop --release
pytest py-ltseq/tests/test_io.py -v -k parquet  # or write a new test
```

Quick smoke test:

```python
from ltseq import LTSeq
t = LTSeq.read_parquet("some_test_file.parquet")
print(t.count())           # should return row count
print(t.columns)           # should list column names
t.filter(lambda r: r.col > 0).count()  # should work
```

---

## Phase 1: Data Preparation

### 1.1 — Download ClickBench dataset

```bash
# ~14GB, 99,997,497 rows, 105 columns
wget https://datasets.clickhouse.com/hits_compatible/hits.parquet -O benchmarks/data/hits.parquet
```

Key column types (from Parquet, all "dirty" — no logical types):
- `UserID`: INT64 (BIGINT)
- `EventTime`: **INT64 (Unix epoch seconds, NOT a timestamp type)**
- `URL`: BYTE_ARRAY (string)

### 1.2 — Pre-sort for LTSeq

```python
import duckdb
duckdb.sql("""
    COPY (
        SELECT * FROM 'benchmarks/data/hits.parquet'
        ORDER BY UserID, EventTime
    ) TO 'benchmarks/data/hits_sorted.parquet' (FORMAT 'parquet')
""")
```

### 1.3 — Investigate actual URL patterns (CRITICAL)

The ClickBench data is from Yandex Metrica — real Russian web analytics data. The plan's
`/`, `/product%`, `/cart` patterns are fictional and will match **0 rows**.

```python
import duckdb

# Top URLs by frequency
duckdb.sql("""
    SELECT URL, count(*) as cnt
    FROM 'benchmarks/data/hits.parquet'
    GROUP BY URL ORDER BY cnt DESC LIMIT 50
""").show()

# URL diversity per user (find users with many distinct pages)
duckdb.sql("""
    SELECT UserID, count(DISTINCT URL) as n
    FROM 'benchmarks/data/hits.parquet'
    GROUP BY UserID ORDER BY n DESC LIMIT 20
""").show()

# Find common 2-step sequences
duckdb.sql("""
    SELECT URL, LEAD(URL) OVER (PARTITION BY UserID ORDER BY EventTime) as Next
    FROM 'benchmarks/data/hits.parquet'
    LIMIT 1000000
""")
```

Based on findings, define `PATTERN_1`, `PATTERN_2`, `PATTERN_3` for the funnel query.

### 1.4 — Create validation subset

```python
duckdb.sql("""
    COPY (
        SELECT * FROM 'benchmarks/data/hits_sorted.parquet' LIMIT 1000000
    ) TO 'benchmarks/data/hits_sample.parquet' (FORMAT 'parquet')
""")
```

---

## Phase 2: Benchmark Harness (`benchmarks/bench_vs.py`)

### 2.1 — Measurement infrastructure

```python
import time, gc, os, json
import duckdb
import psutil
from ltseq import LTSeq

DATA_FILE = "benchmarks/data/hits_sorted.parquet"
WARMUP = 1
ITERATIONS = 3

def benchmark(name, func, warmup=WARMUP, iterations=ITERATIONS):
    """Run function with warmup, return median time."""
    gc.collect()
    for _ in range(warmup):
        func()

    gc.collect()
    process = psutil.Process(os.getpid())
    times = []
    peak_mem = 0
    
    for _ in range(iterations):
        gc.collect()
        mem_before = process.memory_info().rss / (1024 * 1024)
        t0 = time.perf_counter()
        result = func()
        t1 = time.perf_counter()
        mem_after = process.memory_info().rss / (1024 * 1024)
        times.append(t1 - t0)
        peak_mem = max(peak_mem, mem_after - mem_before)

    median_time = sorted(times)[len(times) // 2]
    print(f"  [{name}] median={median_time:.3f}s  times={[f'{t:.3f}' for t in times]}  mem_delta={peak_mem:.0f}MB")
    return {
        "name": name,
        "median_s": round(median_time, 4),
        "times": [round(t, 4) for t in times],
        "mem_delta_mb": round(peak_mem, 1),
        "result": str(result)[:200],
    }
```

### 2.2 — Data loading (separate from query timing)

```python
# Pre-load for LTSeq (timed separately)
print("Loading data for LTSeq...")
t0 = time.perf_counter()
t_ltseq = LTSeq.read_parquet(DATA_FILE)
t_ltseq_sorted = t_ltseq.sort("UserID", "EventTime")
load_time = time.perf_counter() - t0
print(f"  LTSeq load time: {load_time:.3f}s")

# DuckDB uses lazy file scan — no pre-loading needed
```

---

## Phase 3: Round 1 — Basic Aggregation (Top 10 URLs)

**Goal**: Count visits per URL, return top 10. This is DuckDB's strength (Hash Agg).

### DuckDB

```python
def duckdb_top_url():
    return duckdb.sql(f"""
        SELECT URL, count(*) as cnt
        FROM '{DATA_FILE}'
        GROUP BY URL
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
```

### LTSeq (corrected)

```python
def ltseq_top_url():
    return (
        t_ltseq
        .agg(by=lambda r: r.URL, cnt=lambda g: g.count())
        .sort("cnt", desc=True)
        .slice(0, 10)
        .collect()
    )
```

**Expected**: DuckDB wins or ties. This proves LTSeq's baseline is competitive.

---

## Phase 4: Round 2 — User Sessionization (30-min gap)

**Goal**: 30 minutes of inactivity = new session. Count total sessions across all users.

### DuckDB

```python
def duckdb_session():
    return duckdb.sql(f"""
        WITH Diff AS (
            SELECT UserID,
                CASE WHEN EventTime - LAG(EventTime)
                    OVER (PARTITION BY UserID ORDER BY EventTime) > 1800
                THEN 1 ELSE 0 END AS IsNew
            FROM '{DATA_FILE}'
        )
        SELECT sum(IsNew) + count(DISTINCT UserID) as total_sessions
        FROM Diff
    """).fetchone()[0]
```

Note: Each user's first event always starts a session, so `total = sum(IsNew) + count(distinct UserID)`.

### LTSeq (corrected)

```python
def ltseq_session():
    cond = lambda r: (r.UserID != r.UserID.shift(1)) | \
                     (r.EventTime - r.EventTime.shift(1) > 1800)
    grouped = t_ltseq_sorted.group_ordered(cond)
    # .count() on NestedTable returns CallExpr, NOT int
    # To get number of groups: flatten → distinct group IDs → count
    return grouped.flatten().select("__group_id__").distinct().count()
```

**Alternative approach** (if the above doesn't work cleanly):
```python
def ltseq_session_v2():
    # Derive a session boundary flag, then sum
    t = t_ltseq_sorted.derive(lambda r: {
        "is_new": ((r.UserID != r.UserID.shift(1)) |
                   (r.EventTime - r.EventTime.shift(1) > 1800)).cast("int")
    })
    return t.agg(total=lambda g: g.is_new.sum()).collect()[0]["total"]
```

**Expected**: LTSeq wins. DuckDB must materialize WINDOW intermediate columns; LTSeq's `shift` is zero-copy pointer offset.

---

## Phase 5: Round 3 — Sequential Pattern Matching (Funnel)

**Goal**: Find strict 3-step URL sequence within the same user's session. Patterns TBD after Phase 1.3.

### DuckDB

```python
def duckdb_funnel():
    return duckdb.sql(f"""
        SELECT count(*)
        FROM (
            SELECT URL,
                LEAD(URL) OVER (PARTITION BY UserID ORDER BY EventTime) as Next1,
                LEAD(URL, 2) OVER (PARTITION BY UserID ORDER BY EventTime) as Next2
            FROM '{DATA_FILE}'
        )
        WHERE URL LIKE '{PATTERN_1}%'
          AND Next1 LIKE '{PATTERN_2}%'
          AND Next2 LIKE '{PATTERN_3}%'
    """).fetchone()[0]
```

### LTSeq (corrected)

```python
def ltseq_funnel():
    return t_ltseq_sorted.filter(lambda r:
        (r.URL.s.starts_with(PATTERN_1)) &          # .s not .str
        (r.URL.shift(-1).s.starts_with(PATTERN_2)) &  # shift(-1) → LEAD
        (r.URL.shift(-2).s.starts_with(PATTERN_3)) &
        (r.UserID == r.UserID.shift(-1)) &
        (r.UserID == r.UserID.shift(-2))
    ).count()
```

**Confirmed**: `shift(-1)` IS supported — Rust transpiler converts negative offsets to `LEAD(col, abs(N))`
(`src/transpiler/sql_gen.rs` lines 38-45).

**Expected**: LTSeq dominates. SQL requires materializing 2 LEAD columns for 100M rows; LTSeq filters inline.

---

## Phase 6: Reporting

### 6.1 — Output format

```
╔═══════════════════════╦════════════╦═══════════╦═════════╦══════════╦════════╗
║ Round                 ║ DuckDB (s) ║ LTSeq (s) ║ Speedup ║ LOC(SQL) ║ LOC(Py)║
╠═══════════════════════╬════════════╬═══════════╬═════════╬══════════╬════════╣
║ R1: Top URLs          ║   X.XXX    ║   X.XXX   ║  X.Xx   ║    5     ║    5   ║
║ R2: Sessionization    ║   X.XXX    ║   X.XXX   ║  X.Xx   ║   10     ║    4   ║
║ R3: Funnel            ║   X.XXX    ║   X.XXX   ║  X.Xx   ║   10     ║    6   ║
╚═══════════════════════╩════════════╩═══════════╩═════════╩══════════╩════════╝
```

### 6.2 — JSON output (`benchmarks/clickbench_results.json`)

```json
{
  "timestamp": "2026-02-25T...",
  "hardware": {"cpu": "...", "ram_gb": 64, "os": "..."},
  "data": {"file": "hits_sorted.parquet", "rows": 99997497},
  "ltseq_load_time_s": 0.0,
  "rounds": [
    {"round": 1, "duckdb": {...}, "ltseq": {...}},
    {"round": 2, "duckdb": {...}, "ltseq": {...}},
    {"round": 3, "duckdb": {...}, "ltseq": {...}}
  ]
}
```

### 6.3 — Narrative dimensions

1. **Performance**: wall-clock time, memory delta
2. **Code complexity**: lines of code, nesting depth (SQL CTE layers vs flat Python)
3. **Theory**: Set Operations (DuckDB) vs Sequence Operations (LTSeq)

---

## Risk Register

| Risk | Severity | Mitigation |
|------|----------|------------|
| `read_parquet()` not yet implemented | **Blocker** | Phase 0 — implement first |
| URL patterns in plan are fictional | **High** | Phase 1.3 — inspect real data, define real patterns |
| `group_ordered().count()` returns expr, not int | **Medium** | Use `flatten().select("__group_id__").distinct().count()` |
| 14GB parquet doesn't fit in memory | **Medium** | Need 32GB+ RAM; or use subset |
| LTSeq loads eagerly, DuckDB scans lazily | **Medium** | Separate load time from query time |
| `shift` in `filter` context may not work (needs window) | **Medium** | Test early; fallback to `derive` + `filter` two-step |

---

## Execution Order

| Step | Description | Depends On | Est. Time |
|------|-------------|-----------|-----------|
| 0.1 | Rust `read_parquet` | — | 30 min |
| 0.2 | Python `read_parquet` | 0.1 | 20 min |
| 0.3 | Build + test | 0.1, 0.2 | 15 min |
| 1.1 | Download hits.parquet | — | 30 min |
| 1.2 | Pre-sort data | 1.1 | 10 min |
| 1.3 | Investigate URL patterns | 1.1 | 15 min |
| 1.4 | Create sample subset | 1.2 | 5 min |
| 2 | Write benchmark harness | 0.3 | 30 min |
| 3 | Round 1 implementation | 2, 1.2 | 20 min |
| 4 | Round 2 implementation | 2, 1.2 | 30 min |
| 5 | Round 3 implementation | 2, 1.3 | 30 min |
| 6 | Validation + reporting | 3, 4, 5 | 30 min |

**Total**: ~4-5 hours (excluding download time)
