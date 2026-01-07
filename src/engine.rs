//! DataFusion engine and RustTable definition
//!
//! This module re-exports the RustTable struct which is defined in lib.rs
//! and provides access to the global RUNTIME.

use lazy_static::lazy_static;
use tokio::runtime::Runtime;

// Global Tokio runtime for async operations
lazy_static! {
    pub static ref RUNTIME: Runtime = Runtime::new().expect("Failed to create Tokio runtime");
}

// RustTable is defined in lib.rs (where #[pyclass] macro needs to be)
// This module just provides access to the runtime
