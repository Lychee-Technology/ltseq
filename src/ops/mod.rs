//! Table operations module
//!
//! This module organizes all table operations into logical submodules:
//! - **basic**: Core read/write/filter/select operations
//! - **derive**: Column derivation and window functions
//! - **advanced**: Complex operations like sort, group_ordered, join
//! - **display**: Formatting and display operations
//!
//! # Architecture: Helper Function Delegation Pattern
//!
//! This module exists to work around a PyO3 constraint: only ONE `#[pymethods]` impl
//! block is allowed per struct. Since RustTable needs 14 methods exposed to Python,
//! we use a **Logical Modularization** approach (Option A from design analysis):
//!
//! 1. **RustTable definition & #[pymethods]**: In lib.rs
//!    - Struct definition for Python compatibility
//!    - Single `#[pymethods]` impl block with 14 method stubs
//!    - Each method delegates to an ops/ helper function (1-3 lines)
//!
//! 2. **Operation implementations**: Extracted to ops/ submodules
//!    - All actual logic in pub helper functions (e.g., `derive_impl()`)
//!    - Organized by operation category (basic, derive, advanced, display)
//!    - Pure Rust functions that don't depend on PyO3
//!
//! # Design Benefits
//!
//! - **Modularity**: Each ops/ module is a cohesive unit of functionality
//! - **Maintainability**: Clear separation between delegation and implementation
//! - **Testability**: Helper functions can be unit tested without PyO3 overhead
//! - **Extensibility**: Easy to add new operations or refactor existing ones
//! - **Single Source of Truth**: Each operation logic lives in exactly one place
//!
//! # Pattern: Delegation Stub
//!
//! Every RustTable method in lib.rs follows this pattern:
//!
//! ```rust,ignore
//! fn derive(&self, derived_cols: &Bound<'_, PyDict>) -> PyResult<RustTable> {
//!     crate::ops::derive::derive_impl(self, derived_cols)
//! }
//! ```
//!
//! This stub:
//! - Takes PyO3-compatible parameters
//! - Calls the ops/ helper function
//! - Returns the result (or error) to Python
//! - Keeps lib.rs clean and readable (~100 lines of stubs)
//!
//! # Module Sizes (Post-Refactoring)
//!
//! | Module | Lines | Purpose |
//! |--------|-------|---------|
//! | basic.rs | 249 | I/O and core filtering/selection |
//! | derive.rs | 597 | Column derivation and window functions |
//! | advanced.rs | 299 | Sorting and joining |
//! | display.rs | 174 | Table formatting and display |
//! | **Total** | **1,319** | All operation implementations |

pub mod basic;
pub mod derive;
pub mod advanced;
pub mod display;
