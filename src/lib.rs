use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// Global Tokio runtime for async operations

// Module declarations - Organized for better maintainability
mod error;
mod types;
mod helpers;
pub mod format;      // Formatting and display functions
pub mod transpiler;  // PyExpr to DataFusion transpilation
pub mod engine;      // DataFusion session and RustTable struct
pub mod ops;         // Table operations grouped by category

// Re-exports for convenience
pub use types::{PyExpr, dict_to_py_expr};
pub use error::PyExprError;
pub use format::{format_table, format_cell};

use crate::engine::RUNTIME;

/// Convert PyExpr to DataFusion Expr
fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    crate::transpiler::pyexpr_to_datafusion(py_expr, schema)
}

/// RustTable: Holds DataFusion SessionContext and loaded data
/// This is the core Rust kernel backing LTSeq
#[pyclass]
pub struct RustTable {
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
impl RustTable {
    #[new]
    fn new() -> Self {
        let session = SessionContext::new();
        RustTable {
            session: Arc::new(session),
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
                    "Hello from RustTable! Schema has {} columns",
                    schema.fields().len()
                )
            }
            _ => "Hello from RustTable! No data loaded yet.".to_string(),
        }
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
    ///     New RustTable with filtered data
    fn filter(&self, expr_dict: &Bound<'_, PyDict>) -> PyResult<RustTable> {
        // 1. Deserialize expression
        let py_expr = dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
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

        // 6. Return new RustTable with filtered data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(filtered_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
    }

    /// Select columns or derived expressions
    ///
    /// Args:
    ///     exprs: List of serialized expression dicts (from Python)
    ///
    /// Returns:
    ///     New RustTable with selected columns
    fn select(&self, exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
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

        // 6. Return new RustTable with updated schema
        Ok(RustTable {
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
    ///     New RustTable with added derived columns
    fn derive(&self, derived_cols: &Bound<'_, PyDict>) -> PyResult<RustTable> {
        crate::ops::derive::derive_impl(self, derived_cols)
    }

    /// Helper method to derive columns with window functions using SQL
    fn derive_with_window_functions(
        &self,
        derived_cols: &Bound<'_, PyDict>,
    ) -> PyResult<RustTable> {
        crate::ops::derive::derive_with_window_functions_impl(self, derived_cols)
    }

    /// Sort rows by one or more key expressions
    ///
    /// Args:
    ///     sort_exprs: List of serialized expression dicts (from Python)
    ///     desc_flags: List of boolean flags indicating descending order per sort key
    ///
    /// Returns:
    ///     New RustTable with sorted data
    fn sort(&self, sort_exprs: Vec<Bound<'_, PyDict>>, desc_flags: Vec<bool>) -> PyResult<RustTable> {
        crate::ops::advanced::sort_impl(self, sort_exprs, desc_flags)
    }

    /// Add __group_id__ column for consecutive identical grouping values
    ///
    /// Phase B2: Foundation for group_ordered() lazy evaluation.
    /// Placeholder: will compute __group_id__ via window functions.
    fn group_id(&self, grouping_expr: Bound<'_, PyDict>) -> PyResult<RustTable> {
        crate::ops::advanced::group_id_impl(self, grouping_expr)
    }

    /// Get only the first row of each group (Phase B4)
    ///
    /// Requires __group_id__ column to exist. Returns a new table with only the
    /// first row per group.
    fn first_row(&self) -> PyResult<RustTable> {
        crate::ops::advanced::first_row_impl(self)
    }

    /// Get only the last row of each group (Phase B4)
    ///
    /// Requires __group_id__ column to exist. Returns a new table with only the
    /// last row per group.
    fn last_row(&self) -> PyResult<RustTable> {
        crate::ops::advanced::last_row_impl(self)
    }

    /// Remove duplicate rows based on key columns
    ///
    /// Args:
    ///     key_exprs: List of serialized expression dicts (from Python)
    ///                If empty, considers all columns for uniqueness
    ///
    /// Returns:
    ///     New RustTable with unique rows
    fn distinct(&self, _key_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // Get schema (for now, we don't use it for distinct, but keep it for future)
        let _schema = self.schema.as_ref().ok_or_else(|| {
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

        // For Phase 5, we implement simple distinct on all columns
        // TODO: In Phase 6+, support distinct with specific key columns
        // Note: key_exprs parameter is reserved for future use
        let distinct_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .distinct()
                    .map_err(|e| format!("Distinct execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Return new RustTable with distinct data
        Ok(RustTable {
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
    ///     New RustTable with selected row range
    fn slice(&self, offset: i64, length: Option<i64>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
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

        // Return new RustTable with sliced data
        Ok(RustTable {
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
    ///     New RustTable with cumulative sum columns added
    fn cum_sum(&self, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        crate::ops::derive::cum_sum_impl(self, cum_exprs)
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
    /// A new RustTable containing the joined result with combined schema
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
        other: &RustTable,
        left_key_expr_dict: &Bound<'_, PyDict>,
        right_key_expr_dict: &Bound<'_, PyDict>,
        join_type: &str,
        alias: &str,
    ) -> PyResult<RustTable> {
        crate::ops::advanced::join_impl(self, other, left_key_expr_dict, right_key_expr_dict, join_type, alias)
    }
}


#[pyfunction]
fn hello() -> PyResult<String> {
    Ok("hello from ltseq_core".to_string())
}

#[pymodule]
fn ltseq_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_class::<RustTable>()?;
    Ok(())
}
