use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// Global Tokio runtime for async operations

// Module declarations - Organized for better maintainability
mod error;
mod types;
pub mod format;      // Formatting and display functions
pub mod transpiler;  // PyExpr to DataFusion transpilation
pub mod engine;      // DataFusion session and LTSeqTable struct
pub mod ops;         // Table operations grouped by category
pub mod cursor;      // Streaming cursor for lazy iteration

// Re-exports for convenience
pub use types::{PyExpr, dict_to_py_expr};
pub use error::PyExprError;
pub use format::{format_table, format_cell};

use crate::engine::{RUNTIME, create_session_context};

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
}

/// Helper function: Apply OVER clause to all nested LAG/LEAD functions in an expression
/// This recursively finds all LAG(...) and LEAD(...) calls and adds the proper OVER clause
fn apply_over_to_window_functions(expr: &str, order_by: &str) -> String {
    let mut result = String::new();
    let mut i = 0;
    let bytes = expr.as_bytes();

    while i < bytes.len() {
        // Look for LAG(
        if i + 4 <= bytes.len() && &expr[i..i + 4] == "LAG(" {
            result.push_str("LAG(");
            i += 4;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        }
        // Look for LEAD(
        else if i + 5 <= bytes.len() && &expr[i..i + 5] == "LEAD(" {
            result.push_str("LEAD(");
            i += 5;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
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
        }
    }

    /// Read CSV file into DataFusion DataFrame
    ///
    /// Args:
    ///     path: Path to CSV file
    fn read_csv(&mut self, path: String) -> PyResult<()> {
        RUNTIME.block_on(async {
            // Use DataFusion's built-in CSV reader
            let df = self
                .session
                .read_csv(&path, CsvReadOptions::new())
                .await
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to read CSV: {}",
                        e
                    ))
                })?;

            // Get the DFSchema and convert to Arrow schema
            let df_schema = df.schema();
            let arrow_fields: Vec<arrow::datatypes::Field> =
                df_schema.fields().iter().map(|f| (**f).clone()).collect();
            let arrow_schema = ArrowSchema::new(arrow_fields);

            self.schema = Some(Arc::new(arrow_schema));
            self.dataframe = Some(Arc::new(df));
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
    ///
    /// Returns:
    ///     LTSeqCursor for lazy iteration over batches
    #[staticmethod]
    fn scan_csv(path: String) -> PyResult<crate::cursor::LTSeqCursor> {
        let session = create_session_context();
        crate::cursor::create_cursor_from_csv(session, &path)
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

    /// Get basic info about the table
    fn hello(&self) -> String {
        match (&self.dataframe, &self.schema) {
            (Some(_), Some(schema)) => {
                format!(
                    "Hello from LTSeqTable! Schema has {} columns",
                    schema.fields().len()
                )
            }
            _ => "Hello from LTSeqTable! No data loaded yet.".to_string(),
        }
    }

    /// Get column names from the table schema
    ///
    /// Returns:
    ///     List of column names as strings
    fn get_column_names(&self) -> PyResult<Vec<String>> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available.",
            )
        })?;
        Ok(schema.fields().iter().map(|f| f.name().to_string()).collect())
    }

    /// Get the number of rows in the table
    ///
    /// Returns:
    ///     The number of rows
    fn count(&self) -> PyResult<usize> {
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        RUNTIME.block_on(async {
            let df_clone = (**df).clone();
            let batches = df_clone.collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect data: {}",
                    e
                ))
            })?;

            let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
            Ok(total_rows)
        })
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
            return Ok(LTSeqTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
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

        // 6. Return new LTSeqTable with filtered data
        Ok(LTSeqTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(filtered_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
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
            return Ok(LTSeqTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
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

        // 5. Update schema to reflect selected DataFrame
        // Get new schema from the resulting DataFrame
        let df_schema = selected_df.schema();
        let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_schema = ArrowSchema::new(arrow_fields);

        // 6. Return new LTSeqTable with updated schema
        Ok(LTSeqTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(selected_df)),
            schema: Some(Arc::new(new_schema)),
            sort_exprs: self.sort_exprs.clone(),
        })
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
    fn sort(&self, sort_exprs: Vec<Bound<'_, PyDict>>, desc_flags: Vec<bool>) -> PyResult<LTSeqTable> {
        crate::ops::advanced::sort_impl(self, sort_exprs, desc_flags)
    }

    /// Add __group_id__ column for consecutive identical grouping values
    ///
    /// Phase B2: Foundation for group_ordered() lazy evaluation.
    /// Placeholder: will compute __group_id__ via window functions.
    fn group_id(&self, grouping_expr: Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
        crate::ops::advanced::group_id_impl(self, grouping_expr)
    }

    /// Get only the first row of each group (Phase B4)
    ///
    /// Requires __group_id__ column to exist. Returns a new table with only the
    /// first row per group.
    fn first_row(&self) -> PyResult<LTSeqTable> {
        crate::ops::advanced::first_row_impl(self)
    }

    /// Get only the last row of each group (Phase B4)
    ///
    /// Requires __group_id__ column to exist. Returns a new table with only the
    /// last row per group.
    fn last_row(&self) -> PyResult<LTSeqTable> {
        crate::ops::advanced::last_row_impl(self)
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
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(LTSeqTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // Get schema
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // If no key columns specified, use simple distinct on all columns
        if key_exprs.is_empty() {
            let distinct_df = RUNTIME
                .block_on(async {
                    (**df)
                        .clone()
                        .distinct()
                        .map_err(|e| format!("Distinct execution failed: {}", e))
                })
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

            return Ok(LTSeqTable {
                session: Arc::clone(&self.session),
                dataframe: Some(Arc::new(distinct_df)),
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // Convert key expressions to column names
        let mut key_cols = Vec::new();
        for expr_dict in key_exprs.iter() {
            let py_expr = crate::types::dict_to_py_expr(expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Get column name from expression
            let col_name = match &py_expr {
                crate::types::PyExpr::Column(name) => name.clone(),
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "distinct() key expressions must be simple column references",
                    ))
                }
            };

            // Verify column exists
            if !schema.fields().iter().any(|f| f.name() == &col_name) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Column '{}' not found in schema",
                    col_name
                )));
            }

            key_cols.push(col_name);
        }

        // Build SQL query using ROW_NUMBER() window function
        // SELECT * FROM (
        //   SELECT *, ROW_NUMBER() OVER (PARTITION BY key_cols ORDER BY key_cols) as __rn
        //   FROM __distinct_source
        // ) WHERE __rn = 1
        let all_cols: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();
        let all_cols_str = all_cols.join(", ");

        let partition_by = key_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        // Use the key columns for ordering (first occurrence based on natural order)
        // If there's no sort expression, use ROWID-like ordering
        let order_by = if !key_cols.is_empty() {
            partition_by.clone()
        } else {
            "1".to_string()
        };

        let sql = format!(
            "SELECT {} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) as __rn FROM __distinct_source) WHERE __rn = 1",
            all_cols_str, partition_by, order_by
        );

        // Register DataFrame as a temporary table and execute SQL
        let session = &self.session;
        let schema_clone = Arc::clone(schema);
        let distinct_df = RUNTIME
            .block_on(async {
                // Collect DataFrame to batches
                let batches = (**df).clone().collect().await.map_err(|e| {
                    format!("Failed to collect data for distinct: {}", e)
                })?;

                // Create a MemTable from the batches
                let temp_table = datafusion::datasource::MemTable::try_new(
                    schema_clone,
                    vec![batches],
                )
                .map_err(|e| format!("Failed to create temp table: {}", e))?;

                // Register the source DataFrame
                session
                    .register_table("__distinct_source", Arc::new(temp_table))
                    .map_err(|e| format!("Failed to register source table: {}", e))?;

                // Execute SQL
                let result = session
                    .sql(&sql)
                    .await
                    .map_err(|e| format!("Distinct SQL failed: {}", e))?;

                // Deregister temp table
                let _ = session.deregister_table("__distinct_source");

                Ok(result)
            })
            .map_err(|e: String| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Return new LTSeqTable with distinct data
        Ok(LTSeqTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(distinct_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
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
            return Ok(LTSeqTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
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

        // Return new LTSeqTable with sliced data
        Ok(LTSeqTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(sliced_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
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
    fn agg(&self, group_expr: Option<Bound<'_, PyDict>>, agg_dict: &Bound<'_, PyDict>) -> PyResult<LTSeqTable> {
        crate::ops::advanced::agg_impl(self, group_expr, agg_dict)
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
        crate::ops::advanced::filter_where_impl(self, where_clause)
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
    fn derive_window_sql(&self, derive_exprs: std::collections::HashMap<String, String>) -> PyResult<LTSeqTable> {
        crate::ops::advanced::derive_window_sql_impl(self, derive_exprs)
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
        crate::ops::advanced::join_impl(self, other, left_key_expr_dict, right_key_expr_dict, join_type, alias)
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
        crate::ops::advanced::pivot_impl(self, index_cols, pivot_col, value_col, agg_fn)
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
        crate::ops::basic::union_impl(self, other)
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
    fn intersect(&self, other: &LTSeqTable, key_expr_dict: Option<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
        crate::ops::basic::intersect_impl(self, other, key_expr_dict)
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
    fn diff(&self, other: &LTSeqTable, key_expr_dict: Option<Bound<'_, PyDict>>) -> PyResult<LTSeqTable> {
        crate::ops::basic::diff_impl(self, other, key_expr_dict)
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
    fn write_csv(&self, path: String) -> PyResult<()> {
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
        })?;

        RUNTIME.block_on(async {
            use std::fs::File;
            use std::io::Write;

            let df_clone = (**df).clone();
            let batches = df_clone.collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect data: {}",
                    e
                ))
            })?;

            let mut file = File::create(&path).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create file '{}': {}",
                    path, e
                ))
            })?;

            // Write header
            let fields: Vec<String> = schema.fields().iter()
                .map(|f| f.name().clone())
                .collect();
            writeln!(file, "{}", fields.join(",")).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to write CSV header: {}",
                    e
                ))
            })?;

            // Write data rows
            for batch in batches {
                for row_idx in 0..batch.num_rows() {
                    let mut row_values = Vec::new();
                    for col_idx in 0..batch.num_columns() {
                        let column = batch.column(col_idx);
                        let value_str = if column.is_null(row_idx) {
                            "".to_string()
                        } else {
                            // Use format_cell which has comprehensive type handling
                            let formatted = crate::format::format_cell(&**column, row_idx);
                            // Replace "[unsupported type]" with empty string for CSV  
                            // and remove "None" for null values
                            if formatted == "[unsupported type]" || formatted == "None" {
                                "".to_string()
                            } else {
                                formatted
                            }
                        };
                        row_values.push(value_str);
                    }
                    writeln!(file, "{}", row_values.join(",")).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write CSV row: {}",
                            e
                        ))
                    })?;
                }
            }

            Ok(())
        })
    }


    fn debug_schema(&self) -> PyResult<String> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
        })?;
        
        let mut output = String::new();
        for field in schema.fields() {
            output.push_str(&format!("{}: {}\n", field.name(), field.data_type()));
        }
        Ok(output)
    }

    fn is_subset(&self, other: &LTSeqTable, key_expr_dict: Option<Bound<'_, PyDict>>) -> PyResult<bool> {
        crate::ops::basic::is_subset_impl(self, other, key_expr_dict)
    }
}



#[pyfunction]
fn hello() -> PyResult<String> {
    Ok("hello from ltseq_core".to_string())
}

#[pymodule]
fn ltseq_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_class::<LTSeqTable>()?;
    m.add_class::<cursor::LTSeqCursor>()?;
    Ok(())
}
