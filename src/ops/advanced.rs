//! Advanced table operations
//!
//! This module contains helper functions for advanced operations like sorting and joining tables.
//!
//! # Architecture
//!
//! Due to PyO3 constraints (only one #[pymethods] impl block per struct), all actual
//! operation implementations are extracted as helper functions here. The #[pymethods]
//! impl block in lib.rs delegates to these helpers via simple stubs.
//!
//! # Operations Provided
//!
//! ## Sort (sort_impl)
//! 
//! Sorts table rows by one or more key expressions. The implementation:
//! - Deserializes Python expression dicts → PyExpr
//! - Transpiles PyExpr → DataFusion SortExpr (ascending order)
//! - Captures sort keys for later window function use (Phase 6)
//! - Executes multi-level sort via DataFusion
//!
//! **Key Feature**: Stores sort_exprs in RustTable for window function context
//!
//! ## Join (join_impl)
//!
//! Performs cross-table joins with automatic schema conflict handling.
//!
//! **Design Challenge**: DataFusion rejects joins where column names overlap between tables.
//! This is resolved via:
//! 1. Rename right table join keys temporarily (e.g., `id` → `__tmp_id`)
//! 2. Execute join on renamed keys
//! 3. Final SELECT renames right table columns with user-specified alias (e.g., `right_id`)
//!
//! **Supported Join Types**: Inner, Left, Right, Full
//!
//! # Implementation Pattern
//!
//! All operations follow:
//! 1. Validate schema and dataframe exist
//! 2. Deserialize Python expression dicts → PyExpr
//! 3. Transpile PyExpr → DataFusion Expr
//! 4. Execute via DataFusion query engine
//! 5. Collect results into new RustTable
//!
//! # Schema Handling
//!
//! - Sort: Preserves original schema
//! - Join: Merges schemas with:
//!   - Left table columns (unchanged)
//!   - Right table columns prefixed with alias (e.g., `right_id`, `right_value`)

use crate::RustTable;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::transpiler::pyexpr_to_datafusion;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::logical_expr::{JoinType, SortExpr};
use datafusion::prelude::*;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::engine::RUNTIME;

/// Extract join key column names from either a Column or And-expression
/// 
/// Phase 8H: Support both simple column references and composite And-expressions
/// 
/// For composite keys like: (o.id == p.id) & (o.year == p.year)
/// This function extracts the column pairs:
/// - Returns (left_cols, right_cols) where left_cols[i] should equal right_cols[i]
/// 
/// Examples:
/// - Column("id") passed as left_key_expr → returns (["id"], [])
/// - Column("id") passed as right_key_expr → returns ([], ["id"])
/// - And( Eq(Column("id"), Column("id")), Eq(Column("year"), Column("year")) )
///   → (["id", "year"], ["id", "year"])  // Composite case (same tree)
/// 
/// Returns: (left_col_names, right_col_names) tuple of column names
fn extract_join_key_columns(left_expr: &PyExpr, right_expr: &PyExpr) -> PyResult<(Vec<String>, Vec<String>)> {
    // Handle simple case: single column on each side
    if let (PyExpr::Column(left_name), PyExpr::Column(right_name)) = (left_expr, right_expr) {
        return Ok((vec![left_name.clone()], vec![right_name.clone()]));
    }
    
    // Handle composite case: And-expressions (should be same tree)
    if let (PyExpr::BinOp { op: op_left, .. }, PyExpr::BinOp { op: op_right, .. }) = (left_expr, right_expr) {
        if op_left == "And" && op_right == "And" {
            let mut left_cols = Vec::new();
            let mut right_cols = Vec::new();
            
            extract_cols_from_and(left_expr, &mut left_cols, &mut right_cols)?;
            return Ok((left_cols, right_cols));
        }
    }
    
    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
        "Join keys must be either simple columns or matching And-expressions",
    ))
}

/// Recursively extract column names from And-expressions containing Eq operations
fn extract_cols_from_and(
    expr: &PyExpr,
    left_cols: &mut Vec<String>,
    right_cols: &mut Vec<String>,
) -> PyResult<()> {
    match expr {
        PyExpr::BinOp { op, left, right } => {
            match op.as_str() {
                "And" => {
                    // Recursively process both sides of And
                    extract_cols_from_and(left, left_cols, right_cols)?;
                    extract_cols_from_and(right, left_cols, right_cols)?;
                    Ok(())
                }
                "Eq" => {
                    // Extract left and right column names from equality
                    if let PyExpr::Column(left_name) = left.as_ref() {
                        left_cols.push(left_name.clone());
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Left side of join equality must be a column reference",
                        ));
                    }
                    
                    if let PyExpr::Column(right_name) = right.as_ref() {
                        right_cols.push(right_name.clone());
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Right side of join equality must be a column reference",
                        ));
                    }
                    Ok(())
                }
                other => {
                    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!(
                            "Unsupported operator in join condition: {}. Expected 'And' or 'Eq'",
                            other
                        ),
                    ))
                }
            }
        }
        _ => {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Expected And or Eq expression in composite join",
            ))
        }
    }
}

/// Helper function to sort rows by one or more key expressions
///
/// Sorts the table in ascending order by default. Captures the sort keys
/// for use in subsequent window function operations.
///
/// Args:
///     table: Reference to RustTable
///     sort_exprs: List of serialized expression dicts (from Python)
pub fn sort_impl(table: &RustTable, sort_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
    // If no dataframe, return empty result (for unit tests)
    if table.dataframe.is_none() {
        return Ok(RustTable {
            session: Arc::clone(&table.session),
            dataframe: None,
            schema: table.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: Vec::new(),
        });
    }

    // Get schema (required for transpilation)
    let schema = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Schema not available. Call read_csv() first.",
        )
    })?;

    // Capture sort column names for Phase 6 window functions
    let mut captured_sort_keys = Vec::new();

    // Deserialize and transpile each sort expression
    let mut df_sort_exprs = Vec::new();
    for expr_dict in sort_exprs {
        let py_expr = dict_to_py_expr(&expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // Capture column name if this is a simple column reference
        if let PyExpr::Column(ref col_name) = py_expr {
            captured_sort_keys.push(col_name.clone());
        }

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
        // Create SortExpr with ascending order (default)
        df_sort_exprs.push(SortExpr {
            expr: df_expr,
            asc: true,
            nulls_first: false,
        });
    }

    // Get DataFrame
    let df = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "No data loaded. Call read_csv() first.",
        )
    })?;

    // Apply sort (async operation)
    let sorted_df = RUNTIME
        .block_on(async {
            (**df)
                .clone()
                .sort(df_sort_exprs)
                .map_err(|e| format!("Sort execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Return new RustTable with sorted data and captured sort keys
    Ok(RustTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(sorted_df)),
        schema: table.schema.as_ref().map(|s| Arc::clone(s)),
        sort_exprs: captured_sort_keys,
    })
}

/// Helper function to join two tables using pointer-based foreign keys
///
/// This method implements joins between two tables based on specified join keys.
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
/// * `table` - The left table
/// * `other` - The table to join with (right table)
/// * `left_key_expr_dict` - Serialized Column expression dict for left join key
/// * `right_key_expr_dict` - Serialized Column expression dict for right join key
/// * `join_type` - Type of join (currently only "inner" is supported)
/// * `alias` - Prefix for right table columns (used to avoid name conflicts)
pub fn join_impl(
    table: &RustTable,
    other: &RustTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: &str,
    alias: &str,
) -> PyResult<RustTable> {
    // 1. Validate both tables have data and schemas
    let df_left = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data. Call read_csv() first.",
        )
    })?;

    let df_right = other.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data. Call read_csv() first.",
        )
    })?;

    let schema_left = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available.",
        )
    })?;

    let schema_right = other.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available.",
        )
    })?;

    // 2. Deserialize join key expressions
    let left_key_expr = dict_to_py_expr(left_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let right_key_expr = dict_to_py_expr(right_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // 3. Extract column names from expressions
    // Phase 8H: Support both simple columns and composite And-expressions
    let (left_col_names, right_col_names) = extract_join_key_columns(&left_key_expr, &right_key_expr)?;

    // For MVP, we only support joining on the first key pair
    // Future phases can support true composite keys
    if left_col_names.is_empty() || right_col_names.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "No join keys found in expressions",
        ));
    }

    let left_col_names_copy = left_col_names.clone();
    let right_col_names_copy = right_col_names.clone();
    let left_col_name = left_col_names[0].clone();
    let right_col_name = right_col_names[0].clone();

    // 4. Validate columns exist in schemas
    for left_col in &left_col_names {
        if !schema_left.fields().iter().any(|f| f.name() == left_col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Column '{}' not found in left table", left_col),
            ));
        }
    }

    for right_col in &right_col_names {
        if !schema_right.fields().iter().any(|f| f.name() == right_col) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Column '{}' not found in right table", right_col),
            ));
        }
    }

    // 5. Map join type string to DataFusion JoinType
    let df_join_type = match join_type {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" => JoinType::Full,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown join type: {}", join_type),
            ))
        }
    };

    // Phase 9: Simple and robust join key renaming for chained joins
    // Key insight: Just rename the right table's join keys with unique names.
    // Don't try to rename the left table - that's more complex with qualified names.
    
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    
    // Don't rename left table - use columns as-is
    let df_left_renamed = (**df_left).clone();
    let actual_left_join_keys = left_col_names.clone();
    
    // 6. Rename ALL join keys from the right table
    
    let mut right_select_exprs = Vec::new();
    let mut temp_right_col_names = Vec::new();  // Track the renamed column names
    
    for field in schema_right.fields().iter() {
        let col_name = field.name();
        // Check if this column is a join key
        if right_col_names_copy.contains(&col_name.to_string()) {
            // Find its index in the join keys
            if let Some(idx) = right_col_names_copy.iter().position(|c| c == col_name) {
                // Use a completely unique temporary name
                let temp_join_key_name = format!("__ltseq_rkey_{}_{}__", idx, unique_suffix);
                right_select_exprs.push(col(col_name).alias(&temp_join_key_name));
                temp_right_col_names.push(temp_join_key_name);
            }
        } else {
            // Keep other columns as-is
            right_select_exprs.push(col(col_name));
        }
    }

    // Apply the select to rename the join keys
    let df_right_renamed = RUNTIME
        .block_on(async {
            (**df_right)
                .clone()
                .select(right_select_exprs)
                .map_err(|e| format!("Right table join key rename failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // 7. Execute the join with ALL join keys
    // For composite keys, join on multiple column pairs
    let left_join_keys: Vec<&str> = actual_left_join_keys.iter().map(|s| s.as_str()).collect();
    let right_join_keys: Vec<&str> = temp_right_col_names.iter().map(|s| s.as_str()).collect();
    
    let joined_df = RUNTIME
        .block_on(async {
            df_left_renamed
                .clone()
                .join(
                    df_right_renamed.clone(),
                    df_join_type,
                    left_join_keys.as_slice(),
                    right_join_keys.as_slice(),
                    None,
                )
                .map_err(|e| format!("Join execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // 8. Build the final schema with correct column names AND rename DataFrame columns
      // DataFusion joins produce qualified names like "?table?.col", we need to rename to user-facing names
       let mut final_fields = Vec::new();
       let mut select_exprs: Vec<Expr> = Vec::new();
       
       // Add left table columns - select them by their original names
       for field in schema_left.fields().iter() {
           final_fields.push((**field).clone());
           select_exprs.push(col(field.name()));
       }
       
       // Add right table columns with alias prefix - rename via SELECT
       for field in schema_right.fields().iter() {
           let col_name = field.name();
           let new_col_name = format!("{}_{}", alias, col_name);
           let new_field = Field::new(
               new_col_name.clone(),
               field.data_type().clone(),
               field.is_nullable(),
           );
           final_fields.push(new_field);
           // Select the column with its original name and alias it to the final name
           select_exprs.push(col(col_name).alias(&new_col_name));
       }
       
       let final_arrow_schema = ArrowSchema::new(final_fields);

       // 9. Apply final SELECT to rename all columns to match the schema
       let final_df = RUNTIME
           .block_on(async {
               joined_df
                   .clone()
                   .select(select_exprs)
                   .map_err(|e| format!("Final column rename SELECT failed: {}", e))
           })
           .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

       // 10. Return new RustTable with joined and renamed data
       Ok(RustTable {
           session: Arc::clone(&table.session),
           dataframe: Some(Arc::new(final_df)),
           schema: Some(Arc::new(final_arrow_schema)),
           sort_exprs: Vec::new(),
       })
}
