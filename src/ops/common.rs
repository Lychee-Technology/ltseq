//! Common utilities for table operations
//!
//! This module provides shared functionality used across multiple operation modules:
//! - Temporary table lifecycle management (RAII guard)
//! - SQL column name building helpers
//! - Common expression extraction utilities
//! - Type-safe domain types (JoinType, etc.)
//!
//! # Design Goals
//!
//! - **Eliminate code duplication**: Extract repeated patterns into reusable functions
//! - **Resource safety**: RAII guards for automatic cleanup of temporary resources
//! - **Type safety**: Strongly-typed helpers to prevent common errors

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::fmt;
use std::sync::Arc;

// ============================================================================
// Type-Safe Domain Types
// ============================================================================

/// Join type enum — replaces error-prone string literals
///
/// # Example
///
/// ```rust
/// let join_type = JoinType::Inner;
/// let sql_keyword = join_type.to_sql(); // "INNER JOIN"
/// ```
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

    /// Convert to SQL keyword
    pub fn to_sql(self) -> &'static str {
        match self {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL OUTER JOIN",
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

/// Aggregation function enum
///
/// # Example
///
/// ```rust
/// let agg_fn = AggFn::Sum;
/// let sql_name = agg_fn.to_sql(); // "SUM"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Mean,
    Count,
    Min,
    Max,
    Std,
}

impl AggFn {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sum" => Some(AggFn::Sum),
            "mean" | "avg" => Some(AggFn::Mean),
            "count" => Some(AggFn::Count),
            "min" => Some(AggFn::Min),
            "max" => Some(AggFn::Max),
            "std" | "stddev" => Some(AggFn::Std),
            _ => None,
        }
    }

    /// Convert to SQL function name
    pub fn to_sql(self) -> &'static str {
        match self {
            AggFn::Sum => "SUM",
            AggFn::Mean => "AVG",
            AggFn::Count => "COUNT",
            AggFn::Min => "MIN",
            AggFn::Max => "MAX",
            AggFn::Std => "STDDEV",
        }
    }
}

impl fmt::Display for AggFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_sql())
    }
}

/// Parquet compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

impl ParquetCompression {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" => Some(ParquetCompression::None),
            "snappy" => Some(ParquetCompression::Snappy),
            "gzip" => Some(ParquetCompression::Gzip),
            "lz4" => Some(ParquetCompression::Lz4),
            "zstd" => Some(ParquetCompression::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParquetCompression::None => write!(f, "none"),
            ParquetCompression::Snappy => write!(f, "snappy"),
            ParquetCompression::Gzip => write!(f, "gzip"),
            ParquetCompression::Lz4 => write!(f, "lz4"),
            ParquetCompression::Zstd => write!(f, "zstd"),
        }
    }
}

// ============================================================================
// SQL Column Name Builders
// =============================================================================

// ============================================================================
// SQL Column Name Builders
// ============================================================================

/// Build a comma-separated list of quoted column names from a schema.
///
/// Example: `"col1", "col2", "col3"`
///
/// # Arguments
/// * `schema` - Arrow schema containing column definitions
///
/// # Returns
/// Comma-separated string of double-quoted column names
pub fn build_quoted_column_list(schema: &ArrowSchema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build a comma-separated list of qualified column names.
///
/// Example: `t1."col1", t1."col2"`
///
/// # Arguments
/// * `schema` - Arrow schema containing column definitions
/// * `table_alias` - Table alias to qualify columns with
///
/// # Returns
/// Comma-separated string of qualified column names
pub fn build_qualified_column_list(schema: &ArrowSchema, table_alias: &str) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}.\"{}\"", table_alias, f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build an AND-connected equality condition for multiple columns.
///
/// Example: `t1."id" = t2."id" AND t1."year" = t2."year"`
///
/// # Arguments
/// * `left_alias` - Alias for left table
/// * `right_alias` - Alias for right table
/// * `columns` - Column names to compare
///
/// # Returns
/// AND-connected equality conditions
pub fn build_equality_conditions(
    left_alias: &str,
    right_alias: &str,
    columns: &[String],
) -> String {
    columns
        .iter()
        .map(|c| format!("{}.\"{}\" = {}.\"{}\"", left_alias, c, right_alias, c))
        .collect::<Vec<_>>()
        .join(" AND ")
}

// ============================================================================
// Temporary Table Management (RAII)
// ============================================================================

/// RAII guard for temporary table registration.
///
/// Automatically deregisters the table when dropped, ensuring cleanup even on errors.
///
/// # Example
///
/// ```rust
/// let guard = TempTableGuard::register(
///     &session,
///     "my_temp_table",
///     schema,
///     batches,
/// )?;
///
/// // Use the table...
/// let result = session.sql("SELECT * FROM my_temp_table").await?;
///
/// // Table is automatically deregistered when guard is dropped
/// ```
pub struct TempTableGuard {
    session: Arc<SessionContext>,
    table_name: String,
}

impl TempTableGuard {
    /// Register a new temporary table and return a guard for automatic cleanup.
    ///
    /// # Arguments
    /// * `session` - DataFusion session context
    /// * `table_name` - Name for the temporary table
    /// * `schema` - Arrow schema for the table
    /// * `batches` - Record batches containing the data
    ///
    /// # Returns
    /// A guard that will deregister the table when dropped
    ///
    /// # Errors
    /// Returns error if MemTable creation or registration fails
    pub fn register(
        session: &Arc<SessionContext>,
        table_name: &str,
        schema: Arc<ArrowSchema>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, String> {
        // Deregister any existing table with the same name
        let _ = session.deregister_table(table_name);

        // Create MemTable from batches
        let mem_table = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            format!("Failed to create MemTable for '{}': {}", table_name, e)
        })?;

        // Register the table
        session
            .register_table(table_name, Arc::new(mem_table))
            .map_err(|e| format!("Failed to register temp table '{}': {}", table_name, e))?;

        Ok(TempTableGuard {
            session: Arc::clone(session),
            table_name: table_name.to_string(),
        })
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.table_name
    }
}

impl Drop for TempTableGuard {
    fn drop(&mut self) {
        // Silently deregister - errors during cleanup are not critical
        let _ = self.session.deregister_table(&self.table_name);
    }
}

/// RAII guard for multiple temporary tables.
///
/// Automatically deregisters all tables when dropped.
///
/// # Example
///
/// ```rust
/// let guards = MultiTempTableGuard::register_two(
///     &session,
///     "left_table",
///     left_schema,
///     left_batches,
///     "right_table",
///     right_schema,
///     right_batches,
/// )?;
///
/// // Use both tables...
/// let result = session.sql("SELECT * FROM left_table JOIN right_table...").await?;
///
/// // Both tables automatically deregistered when guard is dropped
/// ```
pub struct MultiTempTableGuard {
    tables: Vec<TempTableGuard>,
}

impl MultiTempTableGuard {
    /// Register two temporary tables (common pattern for joins)
    pub fn register_two(
        session: &Arc<SessionContext>,
        name1: &str,
        schema1: Arc<ArrowSchema>,
        batches1: Vec<RecordBatch>,
        name2: &str,
        schema2: Arc<ArrowSchema>,
        batches2: Vec<RecordBatch>,
    ) -> Result<Self, String> {
        let guard1 = TempTableGuard::register(session, name1, schema1, batches1)?;
        let guard2 = TempTableGuard::register(session, name2, schema2, batches2)?;

        Ok(MultiTempTableGuard {
            tables: vec![guard1, guard2],
        })
    }

    /// Get the names of registered tables
    pub fn names(&self) -> Vec<&str> {
        self.tables.iter().map(|g| g.name()).collect()
    }
}

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

/// Get schema from record batches, with fallback to stored schema
///
/// # Arguments
/// * `batches` - Record batches (schema taken from first non-empty batch)
/// * `fallback` - Schema to use if batches are empty
///
/// # Returns
/// Schema from batches or fallback
pub fn schema_from_batches_or_fallback(
    batches: &[RecordBatch],
    fallback: &Arc<ArrowSchema>,
) -> Arc<ArrowSchema> {
    batches
        .first()
        .and_then(|b| {
            if b.num_rows() > 0 {
                Some(b.schema())
            } else {
                None
            }
        })
        .unwrap_or_else(|| Arc::new((**fallback).clone()))
}

// ============================================================================
// Test Utilities
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_quoted_column_list() {
        let schema = ArrowSchema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", datafusion::arrow::datatypes::DataType::Int32, true),
            datafusion::arrow::datatypes::Field::new("name", datafusion::arrow::datatypes::DataType::Utf8, true),
        ]);

        let result = build_quoted_column_list(&schema);
        assert_eq!(result, "\"id\", \"name\"");
    }

    #[test]
    fn test_build_qualified_column_list() {
        let schema = ArrowSchema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", datafusion::arrow::datatypes::DataType::Int32, true),
        ]);

        let result = build_qualified_column_list(&schema, "t1");
        assert_eq!(result, "t1.\"id\"");
    }

    #[test]
    fn test_build_equality_conditions() {
        let columns = vec!["id".to_string(), "year".to_string()];
        let result = build_equality_conditions("L", "R", &columns);
        assert_eq!(result, "L.\"id\" = R.\"id\" AND L.\"year\" = R.\"year\"");
    }
}
