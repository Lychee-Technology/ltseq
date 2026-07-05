//! Table-level sort metadata: the `SortSpec` type and conversion helpers.
//!
//! `LTSeqTable.sort_specs` records the declared row order (column, direction,
//! nulls placement). Every downstream consumer that rebuilds an ORDER BY —
//! window functions, group boundary detection, linear scans, Parquet
//! file_sort_order declarations, SQL fallbacks — must derive it from
//! `SortSpec` via the helpers below instead of assuming ascending order.
//!
//! Propagation rules (issue #95): ops that preserve row order (filter,
//! select, slice, derive, head/tail) clone `sort_specs`; ops that destroy
//! or redefine order (sort output = new specs; distinct/join/agg/set ops/
//! insert/rvs = empty). `source_parquet_path` survives only `assume_sorted`:
//! any op that changes the row set must clear it so Parquet fast paths
//! cannot scan stale raw data.

use datafusion::common::Column;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, SortExpr};

/// One sort key: column name + direction + nulls placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortSpec {
    pub column: String,
    pub descending: bool,
    pub nulls_first: bool,
}

impl SortSpec {
    /// `nulls_first = descending` matches the historical `sort_impl` behavior
    /// (DESC sorts put nulls first, ASC sorts put nulls last).
    pub fn new(column: String, descending: bool) -> Self {
        SortSpec {
            column,
            descending,
            nulls_first: descending,
        }
    }

    pub fn to_df_sort_expr(&self) -> SortExpr {
        SortExpr {
            expr: Expr::Column(Column::new_unqualified(&self.column)),
            asc: !self.descending,
            nulls_first: self.nulls_first,
        }
    }

    pub fn to_window_sort(&self) -> Sort {
        Sort {
            expr: Expr::Column(Column::new_unqualified(&self.column)),
            asc: !self.descending,
            nulls_first: self.nulls_first,
        }
    }
}

pub(crate) fn sort_specs_to_df_sort_exprs(specs: &[SortSpec]) -> Vec<SortExpr> {
    specs.iter().map(|s| s.to_df_sort_expr()).collect()
}

pub(crate) fn sort_specs_to_window_sorts(specs: &[SortSpec]) -> Vec<Sort> {
    specs.iter().map(|s| s.to_window_sort()).collect()
}

/// The `Vec<Vec<SortExpr>>` shape expected by `file_sort_order` /
/// `MemTable::with_sort_order`.
pub(crate) fn sort_specs_to_file_sort_order(specs: &[SortSpec]) -> Vec<Vec<SortExpr>> {
    vec![sort_specs_to_df_sort_exprs(specs)]
}

/// Render an ORDER BY column list (without the `ORDER BY` keyword) for the
/// SQL fallback paths. Empty string when there are no sort specs.
pub(crate) fn sort_specs_to_sql_order_by(specs: &[SortSpec]) -> String {
    if specs.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = specs
        .iter()
        .map(|s| {
            let dir = if s.descending { "DESC" } else { "ASC" };
            let nulls = if s.nulls_first { "NULLS FIRST" } else { "NULLS LAST" };
            format!("\"{}\" {} {}", s.column, dir, nulls)
        })
        .collect();
    parts.join(", ")
}
