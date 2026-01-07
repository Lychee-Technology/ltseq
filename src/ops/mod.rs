//! Table operations module
//!
//! This module organizes all table operations into logical submodules:
//! - basic: Core read/write/filter/select operations
//! - derive: Column derivation and window functions (stub)
//! - advanced: Complex operations like sort, group_ordered, join (stub)
//! - display: Formatting and display operations

pub mod basic;
pub mod derive;
pub mod advanced;
pub mod display;
