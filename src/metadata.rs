//! Table-level sort metadata: the `SortSpec` type and conversion helpers.
//!
//! `LTSeqTable.sort_specs` records the declared row order (column, direction,
//! nulls placement). Every downstream consumer that rebuilds an ORDER BY —
//! window functions, group boundary detection, linear scans, Parquet
//! file_sort_order declarations — must derive it from
//! `SortSpec` via the helpers below instead of assuming ascending order.
//!
//! Propagation rules (issue #95): ops that preserve row order (filter,
//! select, slice, derive, head/tail) clone `sort_specs`; ops that destroy
//! or redefine order (sort output = new specs; distinct/join/agg/set ops/
//! insert/delete/update/modify = empty). `source_parquet_path` survives
//! only `assume_sorted`:
//! any op that changes the row set must clear it so Parquet fast paths
//! cannot scan stale raw data.
//!
//! Declared order is also delivered order (#311): every path that reads
//! rows in table order executes `LTSeqTable::ordered_df`, which requires
//! `sort_specs` at the plan's root. A spec that no longer describes the
//! rows therefore reorders them, so an op keeps a spec only while the
//! column's values are unchanged. A projection that drops or replaces a
//! key keeps the input's full order in its own rows (`select_in_order`).

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::Column;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, SortExpr};

use crate::order_key::{float_order_key, needs_order_key};

/// One sort key: column name, direction, NULL placement, and whether the
/// column is a float, which sorts by its §9.4 key (`order_key`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortSpec {
    pub column: String,
    pub descending: bool,
    pub nulls_first: bool,
    /// The column is a float (plain or encoded): it sorts by
    /// `order_key(column)`, under which every NaN is above `+inf` and
    /// `-0.0 == 0.0` (§9.4). A spec is kept only while its column's values
    /// are unchanged, so their type is too.
    pub float_key: bool,
}

impl SortSpec {
    /// A key on `column`, of type `data_type`. NULL goes last when
    /// `nulls_last`, first otherwise, in either direction (§9.4).
    pub fn new(column: String, descending: bool, nulls_last: bool, data_type: &DataType) -> Self {
        SortSpec {
            column,
            descending,
            nulls_first: !nulls_last,
            float_key: needs_order_key(data_type),
        }
    }

    /// A key on `column` of `schema`, whose type decides `float_key`; a
    /// column the schema lacks is not a float key.
    pub fn of_schema(column: String, descending: bool, nulls_last: bool, schema: &Schema) -> Self {
        let data_type = schema
            .field_with_name(&column)
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Null);
        SortSpec::new(column, descending, nulls_last, &data_type)
    }

    /// `expr` (this key's column, or another name for its values) as the
    /// expression it sorts by.
    pub fn key_expr(&self, expr: Expr) -> Expr {
        if self.float_key {
            float_order_key(expr)
        } else {
            expr
        }
    }

    /// This key on the column `name`, which holds the same values.
    pub fn sort_expr_on(&self, name: &str) -> SortExpr {
        SortExpr {
            expr: self.key_expr(Expr::Column(Column::new_unqualified(name))),
            asc: !self.descending,
            nulls_first: self.nulls_first,
        }
    }

    pub fn to_df_sort_expr(&self) -> SortExpr {
        self.sort_expr_on(&self.column)
    }

    pub fn to_window_sort(&self) -> Sort {
        self.to_df_sort_expr()
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
