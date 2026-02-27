//! Align operation for LTSeqTable
//!
//! Aligns table rows to a reference sequence using SQL LEFT JOIN.

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use std::sync::Arc;

/// Align table rows to a reference sequence
///
/// Creates a result table where:
/// - Rows are reordered to match ref_sequence order
/// - NULL rows inserted for keys not in the original table
/// - Rows with keys not in ref_sequence are excluded
///
/// Uses SQL LEFT JOIN approach:
/// 1. Create temp table from ref_sequence with position column
/// 2. LEFT JOIN original table ON key
/// 3. ORDER BY position
pub fn align_impl(
    table: &LTSeqTable,
    ref_sequence: Vec<Py<PyAny>>,
    key_col: &str,
) -> PyResult<LTSeqTable> {
    // Validate table has data
    let (df, schema) = table.require_df_and_schema()?;

    // Validate key column exists
    if !schema.fields().iter().any(|f| f.name() == key_col) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Key column '{}' not found in schema. Available: {:?}",
            key_col,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        )));
    }

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

    // Create temp table names
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
    Python::attach(|py| -> PyResult<()> {
        let mut values: Vec<String> = Vec::with_capacity(ref_sequence.len());

        for (pos, obj) in ref_sequence.iter().enumerate() {
            let bound = obj.bind(py);
            let val_str = if let Ok(s) = bound.extract::<String>() {
                format!("('{}', {})", s.replace('\'', "''"), pos)
            } else if let Ok(i) = bound.extract::<i64>() {
                format!("({}, {})", i, pos)
            } else if let Ok(f) = bound.extract::<f64>() {
                format!("({}, {})", f, pos)
            } else {
                let s = bound.str()?.to_string();
                format!("('{}', {})", s.replace('\'', "''"), pos)
            };
            values.push(val_str);
        }

        let values_sql = values.join(", ");
        let create_ref_sql = format!(
            "CREATE TABLE \"{}\" AS SELECT column1 as __ref_key__, column2 as __pos__ FROM (VALUES {})",
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

    // Build column list for SELECT
    let column_list: Vec<String> = batch_schema
        .fields()
        .iter()
        .map(|f| format!("t.\"{}\"", f.name()))
        .collect();
    let columns_str = column_list.join(", ");

    // Execute LEFT JOIN to align rows
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
    LTSeqTable::from_batches_with_schema(
        Arc::clone(&table.session),
        result_batches,
        Arc::clone(&batch_schema),
        Vec::new(),
    )
}
