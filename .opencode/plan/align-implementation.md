# Implementation Plan: `align(ref_sequence, key)`

## Overview

The `align()` method reorders rows to match a reference sequence and inserts NULL rows for missing keys.

## Decisions Made

- **Duplicate keys**: Allowed - if ref_sequence has duplicates, output has duplicate rows
- **Unmatched rows**: Excluded - rows with keys not in ref_sequence are filtered out

## Files to Modify

### 1. `py-ltseq/ltseq/core.py`

Add the following method after `search_first()` (around line 1590):

```python
def align(self, ref_sequence: list, key: Callable) -> "LTSeq":
    """
    Align table rows to a reference sequence.

    Reorders rows to match the order of ref_sequence and inserts NULL rows
    for keys in ref_sequence that don't exist in the table. Rows with keys
    not in ref_sequence are excluded from the result.

    This is useful for:
    - Time series gap filling (insert NULL rows for missing dates)
    - Sequence synchronization (align multiple tables to same key order)
    - Master data alignment (align transactions to master list)

    Args:
        ref_sequence: List of key values defining the output order.
                     Keys can be strings, numbers, or dates.
                     Duplicate keys are allowed and produce duplicate rows.
        key: Lambda extracting the key column from each row.
             E.g., lambda r: r.date

    Returns:
        New LTSeq with:
        - Rows reordered to match ref_sequence
        - NULL rows inserted for missing keys
        - Original schema preserved

    Raises:
        ValueError: If ref_sequence is empty or schema not initialized
        TypeError: If key is not callable or doesn't reference a column

    SPL Equivalent:
        align

    Example:
        >>> t = LTSeq.read_csv("data.csv")  # Has dates: 01-02, 01-04, 01-05
        >>> # Align to continuous date sequence
        >>> ref_dates = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        >>> aligned = t.align(ref_dates, key=lambda r: r.date)
        >>> aligned.show()
        # Output:
        # 2024-01-01: NULL row
        # 2024-01-02: actual data
        # 2024-01-03: NULL row
        # 2024-01-04: actual data
        # (2024-01-05 excluded - not in ref_sequence)
    """
    if not self._schema:
        raise ValueError(
            "Schema not initialized. Call read_csv() first to populate the schema."
        )

    if not ref_sequence:
        raise ValueError("ref_sequence cannot be empty")

    if not callable(key):
        raise TypeError(f"key must be callable, got {type(key).__name__}")

    # Extract key column name from lambda
    key_expr = self._capture_expr(key)

    # Validate it's a simple column reference
    if key_expr.get("type") != "Column" or "name" not in key_expr:
        raise TypeError(
            "key must be a simple column reference (e.g., lambda r: r.date). "
            "Complex expressions are not supported."
        )

    key_col = key_expr["name"]

    # Validate column exists
    if key_col not in self._schema:
        raise AttributeError(
            f"Column '{key_col}' not found in schema. "
            f"Available columns: {list(self._schema.keys())}"
        )

    # Call Rust align() method
    try:
        result_inner = self._inner.align(ref_sequence, key_col)
    except RuntimeError as e:
        raise RuntimeError(f"align() failed: {e}")

    # Create new LTSeq with result
    result = LTSeq()
    result._inner = result_inner
    result._schema = self._schema.copy()
    # Sort order is unknown after align (order comes from ref_sequence, not data)
    result._sort_keys = None

    return result
```

### 2. `src/lib.rs`

Add PyO3 binding in the `#[pymethods] impl LTSeqTable` block (after `asof_join`):

```rust
/// Align table rows to a reference sequence
///
/// # Arguments
///
/// * `ref_sequence` - List of key values defining output order
/// * `key_col` - Column name to match against ref_sequence
///
/// # Returns
///
/// New table with rows reordered and NULL rows for missing keys
fn align(&self, ref_sequence: Vec<PyObject>, key_col: &str) -> PyResult<LTSeqTable> {
    crate::ops::advanced::align_impl(self, ref_sequence, key_col)
}
```

### 3. `src/ops/advanced.rs`

Add the implementation function at the end of the file:

```rust
/// Align table rows to a reference sequence
///
/// Creates a result table where:
/// - Rows are reordered to match ref_sequence order
/// - NULL rows inserted for keys not in the original table
/// - Rows with keys not in ref_sequence are excluded
///
/// # Algorithm
///
/// 1. Create temp table from ref_sequence with position column
/// 2. LEFT JOIN original table ON key
/// 3. ORDER BY position
///
/// This ensures:
/// - Exact ref_sequence order preserved
/// - Duplicate ref keys produce duplicate rows
/// - Missing keys get NULL rows
pub fn align_impl(
    table: &LTSeqTable,
    ref_sequence: Vec<PyObject>,
    key_col: &str,
) -> PyResult<LTSeqTable> {
    use pyo3::types::PyAnyMethods;
    
    // Validate table has data
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Table has no data. Call read_csv() first.",
        )
    })?;

    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Table schema not available.")
    })?;

    // Validate key column exists
    if !schema.fields().iter().any(|f| f.name() == key_col) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Key column '{}' not found in schema. Available: {:?}",
            key_col,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        )));
    }

    // Get key column data type
    let key_field = schema.fields().iter().find(|f| f.name() == key_col).unwrap();
    let key_dtype = key_field.data_type();

    // Collect dataframe to batches
    let batches = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .collect()
                .await
                .map_err(|e| format!("Failed to collect data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Get actual schema from batches
    let batch_schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::clone(schema));

    // Create temp table name
    let temp_table_name = format!("__ltseq_align_temp_{}", std::process::id());
    let ref_table_name = format!("__ltseq_align_ref_{}", std::process::id());

    // Register original table
    let temp_table = MemTable::try_new(Arc::clone(&batch_schema), vec![batches]).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create memory table: {}",
            e
        ))
    })?;

    let _ = table.session.deregister_table(&temp_table_name);
    table
        .session
        .register_table(&temp_table_name, Arc::new(temp_table))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to register temp table: {}",
                e
            ))
        })?;

    // Build ref_sequence table as VALUES clause
    // Convert Python objects to SQL literals
    Python::with_gil(|py| -> PyResult<()> {
        let mut values: Vec<String> = Vec::with_capacity(ref_sequence.len());
        
        for (pos, obj) in ref_sequence.iter().enumerate() {
            let bound = obj.bind(py);
            let val_str = if let Ok(s) = bound.extract::<String>() {
                format!("('{}'::VARCHAR, {})", s.replace("'", "''"), pos)
            } else if let Ok(i) = bound.extract::<i64>() {
                format!("({}, {})", i, pos)
            } else if let Ok(f) = bound.extract::<f64>() {
                format!("({}, {})", f, pos)
            } else {
                // Fallback: try to get string representation
                let s = bound.str()?.to_string();
                format!("('{}'::VARCHAR, {})", s.replace("'", "''"), pos)
            };
            values.push(val_str);
        }

        // Create ref sequence table using SQL
        let values_sql = values.join(", ");
        let create_ref_sql = format!(
            "CREATE TABLE \"{}\" AS SELECT * FROM (VALUES {}) AS t(__ref_key__, __pos__)",
            ref_table_name, values_sql
        );

        RUNTIME
            .block_on(async {
                table
                    .session
                    .sql(&create_ref_sql)
                    .await
                    .map_err(|e| format!("Failed to create ref table: {}", e))?
                    .collect()
                    .await
                    .map_err(|e| format!("Failed to execute ref table creation: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        Ok(())
    })?;

    // Build column list (excluding key column for explicit selection)
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .map(|f| format!("t.\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute LEFT JOIN to align rows
    // LEFT JOIN ensures we get NULL rows for missing keys
    let align_sql = format!(
        r#"SELECT {cols}
           FROM "{ref_table}" r
           LEFT JOIN "{data_table}" t ON CAST(r.__ref_key__ AS VARCHAR) = CAST(t."{key_col}" AS VARCHAR)
           ORDER BY r.__pos__"#,
        cols = columns_str,
        ref_table = ref_table_name,
        data_table = temp_table_name,
        key_col = key_col
    );

    let result_batches = RUNTIME
        .block_on(async {
            let result_df = table
                .session
                .sql(&align_sql)
                .await
                .map_err(|e| format!("Failed to execute align query: {}", e))?;

            result_df
                .collect()
                .await
                .map_err(|e| format!("Failed to collect align results: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Cleanup temp tables
    let _ = table.session.deregister_table(&temp_table_name);
    let _ = table.session.deregister_table(&ref_table_name);

    // Create result LTSeqTable
    if result_batches.is_empty() {
        return Ok(LTSeqTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: Some(Arc::clone(&batch_schema)),
            sort_exprs: Vec::new(),
        });
    }

    let result_schema = result_batches[0].schema();
    let result_mem = MemTable::try_new(Arc::clone(&result_schema), vec![result_batches])
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result table: {}",
                e
            ))
        })?;

    let result_df = table
        .session
        .read_table(Arc::new(result_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read result table: {}",
                e
            ))
        })?;

    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}
```

### 4. `py-ltseq/tests/test_align.py`

Create new test file:

```python
"""Tests for LTSeq.align() method."""

import os
import tempfile
import pytest
from ltseq import LTSeq


@pytest.fixture
def sample_csv():
    """Create a sample CSV with gaps in dates."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("date,value,name\n")
        f.write("2024-01-02,100,Alice\n")
        f.write("2024-01-04,200,Bob\n")
        f.write("2024-01-05,300,Charlie\n")
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def int_key_csv():
    """Create a sample CSV with integer keys."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,value\n")
        f.write("2,20\n")
        f.write("4,40\n")
        f.write("5,50\n")
        path = f.name
    yield path
    os.unlink(path)


class TestAlignBasic:
    """Basic functionality tests for align()."""

    def test_align_reorders_rows(self, sample_csv):
        """Test that align reorders rows to match ref_sequence."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-04", "2024-01-02"]  # Reversed order
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 2
        assert rows[0]["date"] == "2024-01-04"
        assert rows[1]["date"] == "2024-01-02"

    def test_align_inserts_null_rows(self, sample_csv):
        """Test that align inserts NULL rows for missing keys."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-01", "2024-01-02", "2024-01-03"]
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 3
        # First row: 2024-01-01 not in original -> NULL
        assert rows[0]["value"] is None
        # Second row: 2024-01-02 exists
        assert rows[1]["value"] == 100
        # Third row: 2024-01-03 not in original -> NULL
        assert rows[2]["value"] is None

    def test_align_excludes_unmatched(self, sample_csv):
        """Test that rows not in ref_sequence are excluded."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04"]  # Excludes 2024-01-05
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 2
        dates = [r["date"] for r in rows]
        assert "2024-01-05" not in dates

    def test_align_preserves_schema(self, sample_csv):
        """Test that align preserves the original schema."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)
        
        assert set(result._schema.keys()) == {"date", "value", "name"}

    def test_align_returns_ltseq(self, sample_csv):
        """Test that align returns an LTSeq instance."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)
        
        assert isinstance(result, LTSeq)


class TestAlignDuplicates:
    """Tests for duplicate key handling."""

    def test_align_duplicate_ref_keys(self, sample_csv):
        """Test that duplicate ref_sequence keys produce duplicate rows."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-02", "2024-01-04"]
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 3
        assert rows[0]["date"] == "2024-01-02"
        assert rows[1]["date"] == "2024-01-02"
        assert rows[2]["date"] == "2024-01-04"


class TestAlignIntKeys:
    """Tests for integer key handling."""

    def test_align_with_int_keys(self, int_key_csv):
        """Test align with integer keys."""
        t = LTSeq.read_csv(int_key_csv)
        ref = [1, 2, 3, 4]
        result = t.align(ref, key=lambda r: r.id)
        
        rows = list(result.to_cursor())
        assert len(rows) == 4
        # id=1 and id=3 should be NULL
        assert rows[0]["value"] is None
        assert rows[1]["value"] == 20
        assert rows[2]["value"] is None
        assert rows[3]["value"] == 40


class TestAlignErrors:
    """Error handling tests for align()."""

    def test_align_empty_ref_sequence(self, sample_csv):
        """Test that empty ref_sequence raises ValueError."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(ValueError, match="ref_sequence cannot be empty"):
            t.align([], key=lambda r: r.date)

    def test_align_no_schema(self):
        """Test that align raises if schema not initialized."""
        t = LTSeq()
        with pytest.raises(ValueError, match="Schema not initialized"):
            t.align(["a"], key=lambda r: r.col)

    def test_align_invalid_key(self, sample_csv):
        """Test that align raises for non-callable key."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(TypeError, match="key must be callable"):
            t.align(["a"], key="date")

    def test_align_nonexistent_column(self, sample_csv):
        """Test that align raises for nonexistent key column."""
        t = LTSeq.read_csv(sample_csv)
        with pytest.raises(AttributeError, match="not found in schema"):
            t.align(["a"], key=lambda r: r.nonexistent)


class TestAlignEdgeCases:
    """Edge case tests for align()."""

    def test_align_all_keys_missing(self, sample_csv):
        """Test align when all ref keys are missing from table."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-10", "2024-01-11"]
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 2
        assert all(r["value"] is None for r in rows)

    def test_align_all_keys_present(self, sample_csv):
        """Test align when all ref keys exist in table."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02", "2024-01-04", "2024-01-05"]
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 3
        assert all(r["value"] is not None for r in rows)

    def test_align_single_row(self, sample_csv):
        """Test align with single ref key."""
        t = LTSeq.read_csv(sample_csv)
        ref = ["2024-01-02"]
        result = t.align(ref, key=lambda r: r.date)
        
        rows = list(result.to_cursor())
        assert len(rows) == 1
        assert rows[0]["value"] == 100
```

### 5. Update `tasks.md`

Mark Phase 2.1 as complete:

```markdown
#### 2.1 `align(ref_sequence, key)` - Sequence Alignment
- [x] Add `align()` method to `LTSeq`
- [x] Create a reference sequence from the provided list
- [x] Insert NULL rows for missing keys
- [x] Reorder to match reference sequence
- [x] Add tests in `tests/test_align.py`

**Files**: `py-ltseq/ltseq/core.py`, `src/lib.rs`, `src/ops/advanced.rs`, `py-ltseq/tests/test_align.py`
**Status**: COMPLETED
```

And update progress tracking:
```markdown
- [x] Phase 2.1: `align` - **COMPLETED**
```

## Implementation Order

1. Python method in `core.py`
2. Rust binding in `lib.rs`
3. Rust implementation in `src/ops/advanced.rs`
4. Tests in `test_align.py`
5. Update `tasks.md`

## Testing Command

```bash
python -m pytest py-ltseq/tests/test_align.py -v
```
