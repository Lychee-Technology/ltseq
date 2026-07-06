//! Common utilities for table operations
//!
//! Shared, SQL-free helpers used across operation modules:
//! - Type-safe domain types (JoinType)
//! - Join schema construction (build_aliased_join_schema)
//!
//! The SQL temp-table guards and column-list builders that used to live here
//! died with the SQL round-trip paths (issue #91).

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use std::fmt;
use std::sync::Arc;

// ============================================================================
// Type-Safe Domain Types
// ============================================================================

/// Join type enum — replaces error-prone string literals
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "inner" => Some(JoinType::Inner),
            "left" => Some(JoinType::Left),
            "right" => Some(JoinType::Right),
            "full" => Some(JoinType::Full),
            _ => None,
        }
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "inner"),
            JoinType::Left => write!(f, "left"),
            JoinType::Right => write!(f, "right"),
            JoinType::Full => write!(f, "full"),
        }
    }
}

// ============================================================================
// SQL Column Name Builders
// ============================================================================




// ============================================================================
// Temporary Table Management (RAII)
// ============================================================================



// ============================================================================
// Schema Helpers
// ============================================================================

/// Build a combined schema by prefixing right schema columns with an alias.
///
/// Used for join result schemas where right table columns need disambiguation.
///
/// # Arguments
/// * `left_schema` - Left table schema (columns preserved as-is)
/// * `right_schema` - Right table schema (columns prefixed)
/// * `alias` - Prefix for right table columns
///
/// # Returns
/// Combined schema with aliased right columns
pub fn build_aliased_join_schema(
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    alias: &str,
) -> Arc<ArrowSchema> {
    let mut fields = Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

    // Add left columns as-is
    for field in left_schema.fields() {
        fields.push((**field).clone());
    }

    // Add right columns with alias prefix
    for field in right_schema.fields() {
        fields.push(datafusion::arrow::datatypes::Field::new(
            format!("{}_{}", alias, field.name()),
            field.data_type().clone(),
            true,
        ));
    }

    Arc::new(ArrowSchema::new(fields))
}


// ============================================================================
// Test Utilities
// ============================================================================
