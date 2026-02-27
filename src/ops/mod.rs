//! Table operations module
//!
//! This module organizes all table operations into logical submodules:
//! - **basic**: search_first operation
//! - **derive**: Column derivation coordination
//! - **window**: Window functions for derived columns
//! - **io**: File I/O operations (CSV writing, etc.)
//! - **sort**: Sort operations
//! - **grouping**: Consecutive group identification, first/last row selection
//! - **join**: Cross-table join operations
//! - **asof_join**: Time-series as-of join operations
//! - **aggregation**: SQL GROUP BY and filtering
//! - **pivot**: Long-to-wide table transformation
//! - **align**: Row alignment to reference sequence
//! - **derive_sql**: SQL-based derive operations
//! - **advanced**: Re-exports for backward compatibility
//!
//! # Architecture: Helper Function Delegation Pattern
//!
//! This module exists to work around a PyO3 constraint: only ONE `#[pymethods]` impl
//! block is allowed per struct. Since LTSeqTable needs 14 methods exposed to Python,
//! we use a **Logical Modularization** approach:
//!
//! 1. **LTSeqTable definition & #[pymethods]**: In lib.rs
//!    - Struct definition for Python compatibility
//!    - Single `#[pymethods]` impl block with method stubs
//!    - Each method delegates to an ops/ helper function (1-3 lines)
//!
//! 2. **Operation implementations**: Extracted to ops/ submodules
//!    - All actual logic in pub helper functions (e.g., `derive_impl()`)
//!    - Organized by operation category
//!    - Pure Rust functions that don't depend on PyO3
//!
//! # Design Benefits
//!
//! - **Modularity**: Each ops/ module is a cohesive unit of functionality
//! - **Maintainability**: Clear separation between delegation and implementation
//! - **Testability**: Helper functions can be unit tested without PyO3 overhead
//! - **Extensibility**: Easy to add new operations or refactor existing ones
//! - **Single Source of Truth**: Each operation logic lives in exactly one place

// Core modules
pub mod basic;
pub mod derive;
pub mod io;
pub mod set_ops;
pub mod window;

// Split from advanced.rs (Phase 3 refactoring)
pub mod aggregation;
pub mod align;
pub mod asof_join;
pub mod derive_sql;
pub mod grouping;
pub mod join;
pub mod pivot;
pub mod sort;
