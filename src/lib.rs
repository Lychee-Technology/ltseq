use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::common::DFSchema;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// Global Tokio runtime for async operations

// Module declarations - Organized for better maintainability
pub mod cursor;
pub mod engine; // DataFusion session and LTSeqTable struct
mod error;
pub mod format; // Formatting and display functions
pub mod ops; // Table operations grouped by category
pub mod transpiler; // PyExpr to DataFusion transpilation
mod types; // Streaming cursor for lazy iteration

// Re-exports for internal use
pub(crate) use format::format_table;
pub(crate) use types::{dict_to_py_expr, PyExpr};

use crate::engine::{create_session_context, RUNTIME};

/// Convert PyExpr to DataFusion Expr
fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    crate::transpiler::pyexpr_to_datafusion(py_expr, schema)
}

/// LTSeqTable: Holds DataFusion SessionContext and loaded data
/// This is the core Rust kernel backing LTSeq
#[pyclass]
pub struct LTSeqTable {
    session: Arc<SessionContext>,
    dataframe: Option<Arc<DataFrame>>,
    schema: Option<Arc<ArrowSchema>>,
    sort_exprs: Vec<String>, // Column names used for sorting, for Phase 6 window functions
    source_parquet_path: Option<String>, // Parquet file path for assume_sorted optimization
}

/// Helper constructors for LTSeqTable (non-PyO3).
///
/// These reduce boilerplate across 36+ construction sites in src/ops/*.
impl LTSeqTable {
    /// Extract Arrow schema from a DataFusion DFSchema.
    pub(crate) fn schema_from_df(df_schema: &DFSchema) -> Arc<ArrowSchema> {
        let fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        Arc::new(ArrowSchema::new(fields))
    }

    /// Wrap a DataFrame, recomputing the schema from its DFSchema.
    /// Use when the operation changes columns (select, derive, window, etc.).
    pub(crate) fn from_df(
        session: Arc<SessionContext>,
        df: DataFrame,
        sort_exprs: Vec<String>,
        source_parquet_path: Option<String>,
    ) -> Self {
        let schema = Self::schema_from_df(df.schema());
        LTSeqTable {
            session,
            dataframe: Some(Arc::new(df)),
            schema: Some(schema),
            sort_exprs,
            source_parquet_path,
        }
    }

    /// Wrap a DataFrame, keeping the provided schema unchanged.
    /// Use when the operation preserves columns (filter, slice, sort, distinct).
    pub(crate) fn from_df_with_schema(
        session: Arc<SessionContext>,
        df: DataFrame,
        schema: Arc<ArrowSchema>,
        sort_exprs: Vec<String>,
        source_parquet_path: Option<String>,
    ) -> Self {
        LTSeqTable {
            session,
            dataframe: Some(Arc::new(df)),
            schema: Some(schema),
            sort_exprs,
            source_parquet_path,
        }
    }

    /// Create a table from record batches via MemTable.
    /// Schema is inferred from the first batch; returns an empty table if batches are empty.
    pub(crate) fn from_batches(
        session: Arc<SessionContext>,
        batches: Vec<RecordBatch>,
        sort_exprs: Vec<String>,
        source_parquet_path: Option<String>,
    ) -> PyResult<Self> {
        if batches.is_empty() {
            return Ok(LTSeqTable {
                session,
                dataframe: None,
                schema: None,
                sort_exprs,
                source_parquet_path,
            });
        }

        let result_schema = batches[0].schema();
        let mem_table =
            MemTable::try_new(Arc::clone(&result_schema), vec![batches]).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create table: {}",
                    e
                ))
            })?;

        let result_df = session.read_table(Arc::new(mem_table)).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read table: {}",
                e
            ))
        })?;

        Ok(LTSeqTable {
            session,
            dataframe: Some(Arc::new(result_df)),
            schema: Some(result_schema),
            sort_exprs,
            source_parquet_path,
        })
    }

    /// Like `from_batches`, but uses a fallback schema when batches are empty.
    pub(crate) fn from_batches_with_schema(
        session: Arc<SessionContext>,
        batches: Vec<RecordBatch>,
        empty_schema: Arc<ArrowSchema>,
        sort_exprs: Vec<String>,
        source_parquet_path: Option<String>,
    ) -> PyResult<Self> {
        if batches.is_empty() {
            return Ok(LTSeqTable {
                session,
                dataframe: None,
                schema: Some(empty_schema),
                sort_exprs,
                source_parquet_path,
            });
        }

        let result_schema = batches[0].schema();
        let mem_table =
            MemTable::try_new(Arc::clone(&result_schema), vec![batches]).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create table: {}",
                    e
                ))
            })?;

        let result_df = session.read_table(Arc::new(mem_table)).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read table: {}",
                e
            ))
        })?;

        Ok(LTSeqTable {
            session,
            dataframe: Some(Arc::new(result_df)),
            schema: Some(result_schema),
            sort_exprs,
            source_parquet_path,
        })
    }

    /// Create an empty table with a known schema (dataframe: None).
    /// Used for early returns when no data is loaded.
    pub(crate) fn empty(
        session: Arc<SessionContext>,
        schema: Option<Arc<ArrowSchema>>,
        sort_exprs: Vec<String>,
        source_parquet_path: Option<String>,
    ) -> Self {
        LTSeqTable {
            session,
            dataframe: None,
            schema,
            sort_exprs,
            source_parquet_path,
        }
    }

    /// Get a reference to the DataFrame, or return a PyErr if no data is loaded.
    pub(crate) fn require_df(&self) -> PyResult<&Arc<DataFrame>> {
        self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })
    }

    /// Get a reference to the schema, or return a PyErr if unavailable.
    pub(crate) fn require_schema(&self) -> PyResult<&Arc<ArrowSchema>> {
        self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })
    }

    /// Get references to both the DataFrame and schema.
    pub(crate) fn require_df_and_schema(
        &self,
    ) -> PyResult<(&Arc<DataFrame>, &Arc<ArrowSchema>)> {
        Ok((self.require_df()?, self.require_schema()?))
    }
}

#[pymethods]
impl LTSeqTable {
    #[new]
    fn new() -> Self {
        let session = create_session_context();
        LTSeqTable {
            session,
            dataframe: None,
            schema: None,
            sort_exprs: Vec::new(),
            source_parquet_path: None,
        }
    }

    /// Read CSV file into DataFusion DataFrame
    ///
    /// Args:
    ///     path: Path to CSV file
    ///     has_header: Whether the CSV file has a header row (default: true)
    #[pyo3(signature = (path, has_header=true))]
    fn read_csv(&mut self, path: String, has_header: bool) -> PyResult<()> {
        RUNTIME.block_on(async {
            // Use DataFusion's built-in CSV reader with has_header option
            let options = CsvReadOptions::new().has_header(has_header);
            let df = self.session.read_csv(&path, options).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to read CSV: {}",
                    e
                ))
            })?;

            self.schema = Some(LTSeqTable::schema_from_df(df.schema()));
            self.dataframe = Some(Arc::new(df));
            self.source_parquet_path = None;
            Ok(())
        })
    }

    /// Read Parquet file into DataFusion DataFrame
    ///
    /// Args:
    ///     path: Path to Parquet file
    #[pyo3(signature = (path))]
    fn read_parquet(&mut self, path: String) -> PyResult<()> {
        RUNTIME.block_on(async {
            let options = ParquetReadOptions::default();
            let df = self.session.read_parquet(&path, options).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to read Parquet: {}",
                    e
                ))
            })?;

            self.schema = Some(LTSeqTable::schema_from_df(df.schema()));
            self.dataframe = Some(Arc::new(df));
            self.source_parquet_path = Some(path);
            Ok(())
        })
    }

    /// Scan CSV file and return a streaming cursor
    ///
    /// Unlike read_csv(), this returns a cursor for batch-by-batch iteration
    /// without loading the entire file into memory.
    ///
    /// Args:
    ///     path: Path to CSV file
    ///     has_header: Whether the CSV file has a header row (default: true)
    ///
    /// Returns:
    ///     LTSeqCursor for lazy iteration over batches
    #[staticmethod]
    #[pyo3(signature = (path, has_header=true))]
    fn scan_csv(path: String, has_header: bool) -> PyResult<crate::cursor::LTSeqCursor> {
        let session = create_session_context();
        crate::cursor::create_cursor_from_csv(session, &path, has_header)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
    }

    /// Scan Parquet file and return a streaming cursor
    ///
    /// Args:
    ///     path: Path to Parquet file
    ///
    /// Returns:
    ///     LTSeqCursor for lazy iteration over batches
    #[staticmethod]
    fn scan_parquet(path: String) -> PyResult<crate::cursor::LTSeqCursor> {
        let session = create_session_context();
        crate::cursor::create_cursor_from_parquet(session, &path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
    }

    /// Display the data as a pretty-printed ASCII table
    ///
    /// Args:
    ///     n: Maximum number of rows to display
    ///
    /// Returns:
    ///     Formatted table as string
    fn show(&self, n: usize) -> PyResult<String> {
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
        })?;

        RUNTIME.block_on(async {
            // Clone the DataFrame to collect data
            let df_clone = (**df).clone();
            let batches = df_clone.collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect data: {}",
                    e
                ))
            })?;

            let output = format_table(&batches, schema, n)?;
            Ok(output)
        })
    }

    /// Get column names from the table schema
    ///
    /// Returns:
    ///     List of column names as strings
    fn get_column_names(&self) -> PyResult<Vec<String>> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
        })?;
        Ok(schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect())
    }

    /// Get the number of rows in the table
    ///
    /// Returns:
    ///     The number of rows
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        let df_clone = (**df).clone();
        py.detach(|| {
            RUNTIME.block_on(async {
                df_clone.count().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to count rows: {}",
                        e
                    ))
                })
            })
        })
    }

    /// Collect the DataFrame and return all record batches as Arrow IPC bytes.
    ///
    /// Returns:
    ///     List of bytes objects, each containing one Arrow IPC-serialized RecordBatch
    fn to_arrow_ipc(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        use datafusion::arrow::ipc::writer::StreamWriter;

        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        let df_clone = (**df).clone();
        let batches = py.detach(|| {
            RUNTIME.block_on(async {
                df_clone.collect().await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to collect data: {}",
                        e
                    ))
                })
            })
        })?;

        let mut result = Vec::with_capacity(batches.len());
        for batch in &batches {
            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;
                writer.write(batch).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to write IPC batch: {}",
                        e
                    ))
                })?;
                writer.finish().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to finish IPC stream: {}",
                        e
                    ))
                })?;
            }
            result.push(pyo3::types::PyBytes::new(py, &buffer).into());
        }

        Ok(result)
    }

    /// Filter rows based on predicate expression
    ///
    /// Args:
    ///     expr_dict: Serialized expression dict (from Python)
    ///
    /// Returns:
    ///     New LTSeqTable with filtered data
    fn filter(&self, expr_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
        // 1. Deserialize expression
        let py_expr = dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(LTSeqTable::empty(
                Arc::clone(&self.session),
                self.schema.as_ref().map(Arc::clone),
                self.sort_exprs.clone(),
                self.source_parquet_path.clone(),
            ));
        }

        // 2. Get schema (required for transpilation)
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // 3. Transpile to DataFusion expr
        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // 4. Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // 5. Apply filter (async operation)
        let filtered_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .filter(df_expr)
                    .map_err(|e| format!("Filter execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 6. Return new LTSeqTable with filtered data (schema unchanged)
        Ok(LTSeqTable::from_df_with_schema(
            Arc::clone(&self.session),
            filtered_df,
            Arc::clone(schema),
            self.sort_exprs.clone(),
            self.source_parquet_path.clone(),
        ))
    }

    /// Find the first row matching a predicate
    ///
    /// This is an optimized version of filter() that returns only the first matching row.
    /// Much faster than filtering and then taking the first row, especially for large tables.
    ///
    /// Args:
    ///     expr_dict: Serialized filter expression
    ///
    /// Returns:
    ///     New LTSeqTable with at most one row
    fn search_first(&self, expr_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
        crate::ops::basic::search_first_impl(self, expr_dict)
    }

    /// Sequential pattern matching — find rows where consecutive rows match a sequence of predicates
    ///
    /// Args:
    ///     step_predicates: List of serialized predicate expression dicts
    ///     partition_by: Optional column name for partitioning (pattern cannot cross partition boundaries)
    ///
    /// Returns:
    ///     New LTSeqTable with matching rows (the row where step 1 matched)
    #[pyo3(signature = (step_predicates, partition_by=None))]
    fn search_pattern(
        &self,
        step_predicates: Vec<Bound<'_, PyDict>>,
        partition_by: Option<String>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::pattern_match::search_pattern_impl(self, step_predicates, partition_by)
    }

    /// Sequential pattern matching — count only (no full table collection)
    ///
    /// Args:
    ///     step_predicates: List of serialized predicate expression dicts
    ///     partition_by: Optional column name for partitioning
    ///
    /// Returns:
    ///     Number of matching pattern instances
    #[pyo3(signature = (step_predicates, partition_by=None))]
    fn search_pattern_count(
        &self,
        step_predicates: Vec<Bound<'_, PyDict>>,
        partition_by: Option<String>,
    ) -> PyResult<usize> {
        crate::ops::pattern_match::search_pattern_count_impl(self, step_predicates, partition_by)
    }

    /// Select columns or derived expressions
    ///
    /// Args:
    ///     exprs: List of serialized expression dicts (from Python)
    ///
    /// Returns:
    ///     New LTSeqTable with selected columns
    fn select(&self, exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(LTSeqTable::empty(
                Arc::clone(&self.session),
                self.schema.as_ref().map(Arc::clone),
                self.sort_exprs.clone(),
                self.source_parquet_path.clone(),
            ));
        }

        // 1. Get schema
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // 2. Deserialize and transpile all expressions
        let mut df_exprs = Vec::new();

        for expr_dict in exprs {
            // Deserialize
            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Transpile
            let df_expr = pyexpr_to_datafusion(py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

            df_exprs.push(df_expr);
        }

        // 3. Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // 4. Apply select (async operation)
        let selected_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .select(df_exprs)
                    .map_err(|e| format!("Select execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 5. Return new LTSeqTable with recomputed schema
        Ok(LTSeqTable::from_df(
            Arc::clone(&self.session),
            selected_df,
            self.sort_exprs.clone(),
            self.source_parquet_path.clone(),
        ))
    }

    /// Create derived columns based on expressions
    ///
    /// Args:
    ///     derived_cols: Dict mapping column names to serialized expression dicts
    ///
    /// Returns:
    ///     New LTSeqTable with added derived columns
    fn derive(&self, derived_cols: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
        crate::ops::derive::derive_impl(self, derived_cols)
    }

    /// Helper method to derive columns with window functions using SQL
    fn derive_with_window_functions(
        &self,
        derived_cols: &Bound<'_, PyDict>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::derive::derive_with_window_functions_impl(self, derived_cols)
    }

    /// Sort rows by one or more key expressions
    ///
    /// Args:
    ///     sort_exprs: List of serialized expression dicts (from Python)
    ///     desc_flags: List of boolean flags indicating descending order per sort key
    ///
    /// Returns:
    ///     New LTSeqTable with sorted data
    fn sort(
        &self,
        sort_exprs: Vec<Bound<'_, PyDict>>,
        desc_flags: Vec<bool>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::sort::sort_impl(self, sort_exprs, desc_flags)
    }

    /// Declare sort order without physically sorting the data.
    ///
    /// Use when reading pre-sorted data (e.g., pre-sorted Parquet files).
    /// Sets sort metadata so downstream operations (Arrow shift fast path,
    /// window functions) recognize the sort order without re-sorting.
    ///
    /// Args:
    ///     sort_exprs: List of serialized column expression dicts
    ///
    /// Returns:
    ///     New LTSeqTable with sort metadata set (same underlying data)
    fn assume_sorted(
        &self,
        sort_exprs: Vec<Bound<'_, PyDict>>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::sort::assume_sorted_impl(self, sort_exprs)
    }

    /// Add __group_id__ column for consecutive identical grouping values
    ///
    /// Phase B2: Foundation for group_ordered() lazy evaluation.
    /// Placeholder: will compute __group_id__ via window functions.
    fn group_id(&self, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
        crate::ops::grouping::group_id_impl(self, grouping_expr)
    }

    /// Fast path: count the number of groups without building metadata arrays.
    ///
    /// This is used for the `group_ordered(cond).first().count()` pattern,
    /// where we only need the total number of groups (sessions).
    /// Avoids allocating 3 x N arrays for 100M+ rows.
    fn group_ordered_count(&self, grouping_expr: Bound<'_, PyDict>) -> PyResult<usize> {
        crate::ops::grouping::group_ordered_count_impl(self, grouping_expr)
    }

    /// Get only the first row of each group (Phase B4)
    ///
    /// Requires __group_id__ column to exist. Returns a new table with only the
    /// first row per group.
    fn first_row(&self) -> PyResult<LTSeqTable> {
        crate::ops::grouping::first_row_impl(self)
    }

    /// Get only the last row of each group (Phase B4)
    ///
    /// Requires __group_id__ column to exist. Returns a new table with only the
    /// last row per group.
    fn last_row(&self) -> PyResult<LTSeqTable> {
        crate::ops::grouping::last_row_impl(self)
    }

    /// Remove duplicate rows based on key columns
    ///
    /// Args:
    ///     key_exprs: List of serialized expression dicts (from Python)
    ///                If empty, considers all columns for uniqueness
    ///
    /// Returns:
    ///     New LTSeqTable with unique rows
    fn distinct(&self, key_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
        crate::ops::set_ops::distinct_impl(self, key_exprs)
    }

    /// Select a contiguous range of rows
    ///
    /// Args:
    ///     offset: Starting row index (0-based)
    ///     length: Number of rows to include (None = all rows from offset to end)
    ///
    /// Returns:
    ///     New LTSeqTable with selected row range
    fn slice(&self, offset: i64, length: Option<i64>) -> PyResult<LTSeqTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(LTSeqTable::empty(
                Arc::clone(&self.session),
                self.schema.as_ref().map(Arc::clone),
                self.sort_exprs.clone(),
                self.source_parquet_path.clone(),
            ));
        }

        // Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // Apply slice: use limit() with offset and fetch parameters
        let sliced_df = RUNTIME
            .block_on(async {
                // DataFusion's limit(skip: usize, fetch: Option<usize>)
                let skip = offset as usize;
                let fetch = length.map(|len| len as usize);

                (**df)
                    .clone()
                    .limit(skip, fetch)
                    .map_err(|e| format!("Slice execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Return new LTSeqTable with sliced data (schema unchanged)
        Ok(LTSeqTable::from_df_with_schema(
            Arc::clone(&self.session),
            sliced_df,
            Arc::clone(self.schema.as_ref().unwrap()),
            self.sort_exprs.clone(),
            self.source_parquet_path.clone(),
        ))
    }

    /// Add cumulative sum columns for specified columns
    ///
    /// Args:
    ///     cum_exprs: List of serialized expression dicts (from Python)
    ///                Each expression identifies the column(s) to cumulate
    ///
    /// Returns:
    ///     New LTSeqTable with cumulative sum columns added
    fn cum_sum(&self, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
        crate::ops::derive::cum_sum_impl(self, cum_exprs)
    }

    /// Aggregate rows into a summary table with one row per group
    ///
    /// Performs SQL GROUP BY aggregation, optionally grouping by specified columns
    /// and computing aggregate functions (sum, count, min, max, avg).
    ///
    /// Args:
    ///     group_expr: Optional serialized expression dict for grouping keys.
    ///                If None, aggregates entire table into single row.
    ///     agg_dict: Dictionary mapping {agg_name: agg_expression_dict}
    ///              Each expression identifies an aggregate function like g.sales.sum()
    ///
    /// Returns:
    ///     New LTSeqTable with one row per group (or one row for full-table agg)
    fn agg(
        &self,
        group_expr: Option<Bound<'_, PyDict>>,
        agg_dict: &Bound<'_, PyDict>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::aggregation::agg_impl(self, group_expr, agg_dict)
    }

    /// Filter rows using a raw SQL WHERE clause
    ///
    /// This is used internally by group_ordered().filter() to execute SQL filtering
    /// on the flattened grouped table.
    ///
    /// Args:
    ///     where_clause: Raw SQL WHERE clause string (e.g., "__group_count__ > 2")
    ///
    /// Returns:
    ///     New LTSeqTable with rows matching the WHERE clause
    fn filter_where(&self, where_clause: &str) -> PyResult<LTSeqTable> {
        crate::ops::aggregation::filter_where_impl(self, where_clause)
    }

    /// Derive columns using raw SQL window expressions
    ///
    /// This method executes SQL window function expressions on a table that already
    /// has __group_id__ and __rn__ columns. Used by NestedTable.derive() to execute
    /// group aggregations via pure SQL.
    ///
    /// Args:
    ///     derive_exprs: HashMap mapping column names to SQL expressions
    ///                   e.g., {"span": "COUNT(*) OVER (PARTITION BY __group_id__)"}
    ///
    /// Returns:
    ///     New LTSeqTable with derived columns added (and __group_id__, __rn__ removed)
    fn derive_window_sql(
        &self,
        derive_exprs: std::collections::HashMap<String, String>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::derive_sql::derive_window_sql_impl(self, derive_exprs)
    }

    /// Phase 8B: Join two tables using pointer-based foreign keys
    ///
    /// This method implements an inner join between two tables based on specified join keys.
    /// It supports lazy evaluation - the join is only executed when the result is accessed.
    ///
    /// # DataFusion Schema Conflict Workaround
    ///
    /// DataFusion's join() API rejects tables with duplicate column names in the join input,
    /// even if the columns are from different tables. This implementation works around this
    /// limitation by:
    ///
    /// 1. Temporarily renaming the right table's join key to `__join_key_right__`
    /// 2. Executing the join on `left.join_key == right.__join_key_right__`
    /// 3. After joining, selecting all columns and renaming back to the desired schema with alias
    /// 4. The final schema has: `{all_left_cols, all_right_cols_prefixed_with_alias}`
    ///
    /// # Example
    ///
    /// For join(orders, products, left_key="product_id", right_key="product_id", alias="prod"):
    ///
    /// - orders schema: {id, product_id, quantity}
    /// - products schema: {product_id, name, price}
    /// - result schema: {id, product_id, quantity, prod_product_id, prod_name, prod_price}
    ///
    /// # Arguments
    ///
    /// * `other` - The table to join with (right table)
    /// * `left_key_expr_dict` - Serialized Column expression dict for left join key
    /// * `right_key_expr_dict` - Serialized Column expression dict for right join key
    /// * `join_type` - Type of join (currently only "inner" is supported in Phase 8B MVP)
    /// * `alias` - Prefix for right table columns (used to avoid name conflicts)
    ///
    /// # Returns
    ///
    /// A new LTSeqTable containing the joined result with combined schema
    ///
    /// # Panics
    ///
    /// Returns PyValueError if:
    /// - Join key expressions are not simple Column references
    /// - Join operation fails due to schema/data issues
    /// Phase 8B: Join two tables using pointer-based foreign keys
    ///
    /// This method implements an inner join between two tables based on specified join keys.
    /// It supports lazy evaluation - the join is only executed when the result is accessed.
    fn join(
        &self,
        other: &LTSeqTable,
        left_key_expr_dict: &Bound<'_, PyDict>,
        right_key_expr_dict: &Bound<'_, PyDict>,
        join_type: &str,
        alias: &str,
    ) -> PyResult<LTSeqTable> {
        crate::ops::join::join_impl(
            self,
            other,
            left_key_expr_dict,
            right_key_expr_dict,
            join_type,
            alias,
        )
    }

    /// As-of Join: Time-series join matching each left row with nearest right row
    ///
    /// This operation finds, for each row in the left table, the "nearest" matching
    /// row in the right table based on a time/key column. Commonly used in financial
    /// applications (e.g., matching trades with quotes).
    ///
    /// # Arguments
    ///
    /// * `other` - The right table to join with
    /// * `left_time_col` - Column name for time/key in left table
    /// * `right_time_col` - Column name for time/key in right table  
    /// * `direction` - Match direction: "backward", "forward", or "nearest"
    /// * `alias` - Prefix for right table columns (e.g., "_other")
    ///
    /// # Direction Semantics
    ///
    /// * `backward`: Find largest right.time where right.time <= left.time
    /// * `forward`: Find smallest right.time where right.time >= left.time
    /// * `nearest`: Find closest right.time (backward bias on ties)
    ///
    /// # Returns
    ///
    /// A new LTSeqTable with combined columns (right columns prefixed with alias).
    /// Unmatched left rows have NULL for right columns.
    ///
    /// # Algorithm
    ///
    /// Uses binary search for O(N log M) complexity where N = left rows, M = right rows.
    /// Both tables must be sorted by their respective time columns.
    fn asof_join(
        &self,
        other: &LTSeqTable,
        left_time_col: &str,
        right_time_col: &str,
        direction: &str,
        alias: &str,
    ) -> PyResult<LTSeqTable> {
        crate::ops::asof_join::asof_join_impl(
            self,
            other,
            left_time_col,
            right_time_col,
            direction,
            alias,
        )
    }

    /// Semi-join: Return rows from left table where keys exist in right table
    ///
    /// Unlike regular joins, semi-join:
    /// - Returns only columns from the left table (not merged schema)
    /// - Uses DISTINCT to avoid duplicate rows from multiple matches
    /// - Preserves sort order from the left table
    ///
    /// # Arguments
    ///
    /// * `other` - The right table to match against
    /// * `left_key_expr_dict` - Serialized expression for left join key
    /// * `right_key_expr_dict` - Serialized expression for right join key
    ///
    /// # Returns
    ///
    /// A new LTSeqTable containing only rows from the left table that have
    /// matching keys in the right table.
    ///
    /// # Example Use Cases
    ///
    /// * Find users who have placed at least one order
    /// * Filter products that have been reviewed
    /// * Select customers who have active subscriptions
    fn semi_join(
        &self,
        other: &LTSeqTable,
        left_key_expr_dict: &Bound<'_, PyDict>,
        right_key_expr_dict: &Bound<'_, PyDict>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::join::semi_join_impl(self, other, left_key_expr_dict, right_key_expr_dict)
    }

    /// Anti-join: Return rows from left table where keys do NOT exist in right table
    ///
    /// Unlike regular joins, anti-join:
    /// - Returns only columns from the left table (not merged schema)
    /// - Returns rows that have NO match in the right table
    /// - Preserves sort order from the left table
    ///
    /// # Arguments
    ///
    /// * `other` - The right table to match against
    /// * `left_key_expr_dict` - Serialized expression for left join key
    /// * `right_key_expr_dict` - Serialized expression for right join key
    ///
    /// # Returns
    ///
    /// A new LTSeqTable containing only rows from the left table that have
    /// NO matching keys in the right table.
    ///
    /// # Example Use Cases
    ///
    /// * Find users who have never placed an order
    /// * Filter products that have not been reviewed
    /// * Select customers who have no active subscriptions
    fn anti_join(
        &self,
        other: &LTSeqTable,
        left_key_expr_dict: &Bound<'_, PyDict>,
        right_key_expr_dict: &Bound<'_, PyDict>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::join::anti_join_impl(self, other, left_key_expr_dict, right_key_expr_dict)
    }

    /// Align table rows to a reference sequence
    ///
    /// Reorders rows to match the order of ref_sequence and inserts NULL rows
    /// for keys in ref_sequence that don't exist in the table. Rows with keys
    /// not in ref_sequence are excluded from the result.
    ///
    /// # Arguments
    ///
    /// * `ref_sequence` - List of key values defining the output order
    /// * `key_col` - Column name to match against ref_sequence
    ///
    /// # Returns
    ///
    /// A new LTSeqTable with rows reordered and NULL rows for missing keys
    ///
    /// # Algorithm
    ///
    /// Uses SQL LEFT JOIN approach:
    /// 1. Create temp table from ref_sequence with position column
    /// 2. LEFT JOIN original table ON key
    /// 3. ORDER BY position
    fn align(&self, ref_sequence: Vec<Py<PyAny>>, key_col: &str) -> PyResult<LTSeqTable> {
        crate::ops::align::align_impl(self, ref_sequence, key_col)
    }

    /// Pivot: Transform table from long to wide format
    ///
    /// Reshapes data where unique values in a column become new columns,
    /// and row groups are aggregated based on specified index columns.
    ///
    /// # Arguments
    ///
    /// * `index_cols` - Column names to keep as rows in the pivoted table
    /// * `pivot_col` - Column whose unique values become the new column names
    /// * `value_col` - Column containing the values to aggregate into cells
    /// * `agg_fn` - Aggregation function: "sum", "mean", "count", "min", "max"
    ///
    /// # Returns
    ///
    /// A new LTSeqTable with pivoted data (one row per unique index combination)
    ///
    /// # Example
    ///
    /// Input table: year, region, amount
    /// pivot(vec!["year"], "region", "amount", "sum")
    /// Output: year, West, East, Central (with aggregated amounts)
    fn pivot(
        &self,
        index_cols: Vec<String>,
        pivot_col: String,
        value_col: String,
        agg_fn: String,
    ) -> PyResult<LTSeqTable> {
        crate::ops::pivot::pivot_impl(self, index_cols, pivot_col, value_col, agg_fn)
    }

    /// Union: Vertically concatenate two tables with compatible schemas
    ///
    /// This is equivalent to UNION ALL in SQL - combines all rows from both
    /// tables without removing duplicates. Schemas must match exactly.
    ///
    /// # Arguments
    ///
    /// * `other` - The table to union with
    ///
    /// # Returns
    ///
    /// A new LTSeqTable containing all rows from both input tables
    ///
    /// # Errors
    ///
    /// Returns error if schemas don't match or data operations fail
    fn union(&self, other: &LTSeqTable) -> PyResult<LTSeqTable> {
        crate::ops::set_ops::union_impl(self, other)
    }

    /// Intersect: Return rows present in both tables
    ///
    /// This is equivalent to INTERSECT in SQL - returns rows that appear in
    /// both tables based on specified key columns.
    ///
    /// # Arguments
    ///
    /// * `other` - The table to intersect with
    /// * `key_expr_dict` - Optional serialized expression dict for join keys.
    ///                     If None, uses all columns for matching.
    ///
    /// # Returns
    ///
    /// A new LTSeqTable containing rows present in both tables
    fn intersect(
        &self,
        other: &LTSeqTable,
        key_expr_dict: Option<Bound<'_, PyDict>>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::set_ops::intersect_impl(self, other, key_expr_dict)
    }

    /// Diff: Return rows in this table but not in the other table
    ///
    /// This is equivalent to EXCEPT in SQL - returns rows from the left table
    /// that don't appear in the right table.
    ///
    /// # Arguments
    ///
    /// * `other` - The table to compare against
    /// * `key_expr_dict` - Optional serialized expression dict for join keys.
    ///                     If None, uses all columns for matching.
    ///
    /// # Returns
    ///
    /// A new LTSeqTable with rows in left table but not in right table
    fn diff(
        &self,
        other: &LTSeqTable,
        key_expr_dict: Option<Bound<'_, PyDict>>,
    ) -> PyResult<LTSeqTable> {
        crate::ops::set_ops::diff_impl(self, other, key_expr_dict)
    }

    /// Write table data to a CSV file
    ///
    /// Args:
    ///     path: Path to the output CSV file
    fn write_csv(&self, path: String) -> PyResult<()> {
        crate::ops::io::write_csv_impl(self, path)
    }

    /// Is Subset: Check if this table is a subset of another table
    ///
    /// Returns true if all rows in this table also appear in the other table.
    ///
    /// # Arguments
    ///
    /// * `other` - The table to check against
    /// * `key_expr_dict` - Optional serialized expression dict for join keys.
    ///                     If None, uses all columns for matching.
    ///
    /// # Returns
    ///
    /// Boolean indicating if this table is a subset of the other
    fn is_subset(
        &self,
        other: &LTSeqTable,
        key_expr_dict: Option<Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        crate::ops::set_ops::is_subset_impl(self, other, key_expr_dict)
    }
}

#[pymodule]
fn ltseq_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LTSeqTable>()?;
    m.add_class::<cursor::LTSeqCursor>()?;
    Ok(())
}
