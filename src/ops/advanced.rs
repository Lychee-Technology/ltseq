//! Advanced table operations - Re-export module
//!
//! This module re-exports functions from split submodules for backward compatibility.
//! The original advanced.rs (2773 lines) has been split into:
//!
//! - `sort.rs` - Sort operations
//! - `grouping.rs` - Group ID, first/last row operations
//! - `join.rs` - Cross-table join operations  
//! - `asof_join.rs` - Time-series as-of join operations
//! - `aggregation.rs` - SQL GROUP BY and filtering
//! - `pivot.rs` - Long-to-wide table transformation
//! - `align.rs` - Row alignment to reference sequence
//! - `derive_sql.rs` - SQL-based derive operations
//!
//! All public function names remain unchanged for compatibility with lib.rs.

// Re-export sort operations
pub use crate::ops::sort::sort_impl;

// Re-export grouping operations
pub use crate::ops::grouping::{first_row_impl, group_id_impl, last_row_impl};

// Re-export join operations
pub use crate::ops::join::join_impl;

// Re-export as-of join operations
pub use crate::ops::asof_join::asof_join_impl;

// Re-export aggregation operations
pub use crate::ops::aggregation::{agg_impl, filter_where_impl};

// Re-export pivot operations
pub use crate::ops::pivot::pivot_impl;

// Re-export align operations
pub use crate::ops::align::align_impl;

// Re-export SQL derive operations
pub use crate::ops::derive_sql::{derive_impl, derive_window_sql_impl};
