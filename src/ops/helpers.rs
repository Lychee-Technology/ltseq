//! Shared helpers for table operations
//!
//! Eliminates duplicated patterns across ops/ modules:
//! - **TempTableGuard**: RAII guard that deregisters a temp table on drop
//! - **register_temp_table**: Unified single-table registration
//! - **register_temp_table_pair**: Register two temp tables at once (for joins/set ops)

use crate::error::LtseqError;
use crate::LTSeqTable;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use pyo3::prelude::*;
use std::sync::Arc;

// ============================================================================
// TempTableGuard — RAII deregistration
// ============================================================================

/// RAII guard that deregisters a temporary table when dropped.
///
/// Ensures cleanup even on early returns or `?` error propagation.
///
/// # Example
/// ```ignore
/// let guard = register_temp_table(&session, &schema, batches, "prefix")?;
/// // ... use guard.name() in SQL ...
/// // table is deregistered when `guard` goes out of scope
/// ```
pub(crate) struct TempTableGuard<'a> {
    session: &'a SessionContext,
    name: String,
}

impl<'a> TempTableGuard<'a> {
    /// The registered table name, for use in SQL queries.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for TempTableGuard<'_> {
    fn drop(&mut self) {
        let _ = self.session.deregister_table(&self.name);
    }
}

// ============================================================================
// Single-table registration
// ============================================================================

/// Register a single temp table and return an RAII guard.
///
/// The guard deregisters the table automatically when dropped.
/// The table name is generated as `__ltseq_{prefix}_{pid}`.
///
/// Replaces the duplicated register/deregister pattern found in:
/// - `window.rs` (derive_with_window_functions_sql_fallback, cum_sum_impl)
/// - `aggregation.rs` (aggregate_impl, filter_where_impl)
/// - `derive_sql.rs` (derive_window_sql_impl)
/// - `pivot.rs` (pivot_impl)
/// - `grouping.rs` (register_temp_table)
/// - `set_ops.rs` (distinct_with_keys, rvs_impl, step_impl)
pub(crate) fn register_temp_table<'a>(
    session: &'a SessionContext,
    schema: &Arc<ArrowSchema>,
    batches: Vec<RecordBatch>,
    prefix: &str,
) -> PyResult<TempTableGuard<'a>> {
    let name = format!("__ltseq_{prefix}_{}", std::process::id());

    // Pre-clear any stale registration
    let _ = session.deregister_table(&name);

    let mem_table = MemTable::try_new(Arc::clone(schema), vec![batches])
        .map_err(|e| LtseqError::Runtime(format!("Failed to create memory table: {}", e)))?;

    session
        .register_table(&name, Arc::new(mem_table))
        .map_err(|e| LtseqError::Runtime(format!("Failed to register temp table: {}", e)))?;

    Ok(TempTableGuard { session, name })
}

/// Variant that returns `Result<_, String>` for use inside async blocks
/// that propagate String errors (e.g., set_ops.rs async closures).
pub(crate) fn register_temp_table_str<'a>(
    session: &'a SessionContext,
    schema: &Arc<ArrowSchema>,
    batches: Vec<RecordBatch>,
    prefix: &str,
) -> Result<TempTableGuard<'a>, String> {
    let name = format!("__ltseq_{prefix}_{}", std::process::id());
    let _ = session.deregister_table(&name);

    let mem_table = MemTable::try_new(Arc::clone(schema), vec![batches])
        .map_err(|e| format!("Failed to create memory table: {}", e))?;

    session
        .register_table(&name, Arc::new(mem_table))
        .map_err(|e| format!("Failed to register temp table: {}", e))?;

    Ok(TempTableGuard { session, name })
}

// ============================================================================
// Two-table registration (joins / set ops)
// ============================================================================

/// Register a pair of temp tables and return RAII guards for both.
///
/// Replaces:
/// - `join.rs::register_join_tables()`
/// - `set_ops.rs::register_temp_tables()`
pub(crate) fn register_temp_table_pair<'a>(
    session: &'a SessionContext,
    left_schema: &Arc<ArrowSchema>,
    left_batches: Vec<RecordBatch>,
    right_schema: &Arc<ArrowSchema>,
    right_batches: Vec<RecordBatch>,
    prefix: &str,
) -> PyResult<(TempTableGuard<'a>, TempTableGuard<'a>)> {
    let left_guard = register_temp_table(
        session,
        left_schema,
        left_batches,
        &format!("{prefix}_left"),
    )?;
    let right_guard = register_temp_table(
        session,
        right_schema,
        right_batches,
        &format!("{prefix}_right"),
    )?;
    Ok((left_guard, right_guard))
}

/// String-error variant for use inside async blocks.
pub(crate) fn register_temp_table_pair_str<'a>(
    session: &'a SessionContext,
    left_schema: &Arc<ArrowSchema>,
    left_batches: Vec<RecordBatch>,
    right_schema: &Arc<ArrowSchema>,
    right_batches: Vec<RecordBatch>,
    prefix: &str,
) -> Result<(TempTableGuard<'a>, TempTableGuard<'a>), String> {
    let left_guard = register_temp_table_str(
        session,
        left_schema,
        left_batches,
        &format!("{prefix}_left"),
    )?;
    let right_guard = register_temp_table_str(
        session,
        right_schema,
        right_batches,
        &format!("{prefix}_right"),
    )?;
    Ok((left_guard, right_guard))
}

// ============================================================================
// Empty-table early-return helper
// ============================================================================

/// Check if batches are empty and return an empty LTSeqTable if so.
///
/// Returns `Some(Ok(empty_table))` when batches are empty, `None` otherwise.
/// Callers use this as:
/// ```ignore
/// if let Some(result) = check_empty_batches(table, &batches, sort_exprs) {
///     return result;
/// }
/// ```
///
/// Replaces the duplicated pattern in ~9 locations across:
/// - `sort.rs`, `grouping.rs`, `aggregation.rs`, `pattern_match.rs`, `linear_scan.rs`
pub(crate) fn check_empty_batches(
    table: &LTSeqTable,
    batches: &[RecordBatch],
    sort_exprs: Vec<String>,
) -> Option<PyResult<LTSeqTable>> {
    if batches.is_empty() {
        Some(Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            sort_exprs,
            table.source_parquet_path.clone(),
        )))
    } else {
        None
    }
}

/// Variant that uses a provided schema instead of the table's schema.
///
/// Used when the caller has already extracted the schema (e.g., pattern_match).
pub(crate) fn check_empty_batches_with_schema(
    table: &LTSeqTable,
    batches: &[RecordBatch],
    schema: &Arc<ArrowSchema>,
    sort_exprs: Vec<String>,
) -> Option<PyResult<LTSeqTable>> {
    if batches.is_empty() {
        Some(Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            Some(Arc::clone(schema)),
            sort_exprs,
            table.source_parquet_path.clone(),
        )))
    } else {
        None
    }
}
