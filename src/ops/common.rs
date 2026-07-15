//! Common utilities for table operations
//!
//! Shared, SQL-free helpers used across operation modules:
//! - Type-safe domain types (JoinType)
//! - Join schema construction (build_aliased_join_schema)
//!
//! The SQL temp-table guards and column-list builders that used to live here
//! died with the SQL round-trip paths (issue #91).

use crate::error::LtseqError;
use crate::metadata::SortSpec;
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::HashSet;
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

/// Build a combined schema by prefixing every right column with `{alias}_`.
///
/// Used by the pointer-navigation `link()` path (LinkedTable), where the alias
/// deliberately namespaces the whole target table so linked columns are reached
/// as `alias_col` (e.g. `prod_name`). This is distinct from `join()`'s
/// Polars-style conflict-only suffix — do not conflate the two.
pub fn build_prefixed_join_schema(
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    alias: &str,
) -> Arc<ArrowSchema> {
    let mut fields =
        Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());
    for field in left_schema.fields() {
        fields.push((**field).clone());
    }
    for field in right_schema.fields() {
        fields.push(Field::new(
            format!("{}_{}", alias, field.name()),
            field.data_type().clone(),
            true,
        ));
    }
    Arc::new(ArrowSchema::new(fields))
}

/// Compute the output name of every right column under Polars-style suffix rules.
///
/// A right column keeps its original name unless it collides with a left column
/// name; on collision it becomes `{name}{suffix}`. Returns `(old_name, final_name)`
/// pairs in right-schema order. Errors if a suffixed name still collides with a
/// left column or an already-assigned right column (e.g. right has both `x` and
/// `x_right` while left has `x`) — the caller should surface a "pick another
/// suffix" message, matching Polars' behavior.
pub fn right_rename_map(
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    suffix: &str,
) -> Result<Vec<(String, String)>, LtseqError> {
    let left_names: HashSet<&str> =
        left_schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut taken: HashSet<String> =
        left_schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut map = Vec::with_capacity(right_schema.fields().len());
    for field in right_schema.fields() {
        let old = field.name().clone();
        let new = if left_names.contains(old.as_str()) {
            format!("{}{}", old, suffix)
        } else {
            old.clone()
        };
        if taken.contains(&new) {
            return Err(LtseqError::Validation(format!(
                "Join suffix collision: right column '{}' would become '{}', which already \
                 exists in the result. Choose a different suffix=.",
                old, new
            )));
        }
        taken.insert(new.clone());
        map.push((old, new));
    }
    Ok(map)
}

/// Build the joined result schema: all left fields as-is, then right fields
/// renamed per `right_rename_map`, skipping any right column named in
/// `drop_right_cols` (by original name — used to drop coalesced equi-join keys).
pub fn build_suffixed_join_schema(
    left_schema: &ArrowSchema,
    right_schema: &ArrowSchema,
    suffix: &str,
    drop_right_cols: &[String],
) -> Result<Arc<ArrowSchema>, LtseqError> {
    let map = right_rename_map(left_schema, right_schema, suffix)?;
    let mut fields =
        Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

    for field in left_schema.fields() {
        fields.push((**field).clone());
    }
    for (field, (old, new)) in right_schema.fields().iter().zip(map.iter()) {
        if drop_right_cols.contains(old) {
            continue;
        }
        fields.push(Field::new(new, field.data_type().clone(), true));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

// ============================================================================
// Derive replace-or-add (issue #125 finding 4)
// ============================================================================

/// Build a derive select list with replace-or-add semantics: schema columns
/// keep their positions — a column whose name is being derived is replaced
/// IN PLACE by its new expression — and genuinely new columns are appended
/// in the order given. Expressions are evaluated against the INPUT table, so
/// a replacement may reference the old value of the column it overwrites.
///
/// `derived` pairs are (output name, already-aliased expression).
pub fn merge_derived_columns(
    schema: &ArrowSchema,
    derived: Vec<(String, Expr)>,
) -> Vec<Expr> {
    // Option slots let a matched expression be moved out at its schema
    // position without cloning; the leftovers (genuinely new columns) drain
    // in insertion order below.
    let mut derived: Vec<Option<(String, Expr)>> = derived.into_iter().map(Some).collect();
    let mut out = Vec::with_capacity(schema.fields().len() + derived.len());
    for field in schema.fields() {
        // Python kwargs guarantee unique derived names, so first match wins.
        let slot = derived
            .iter_mut()
            .find(|slot| matches!(slot, Some((name, _)) if name == field.name()));
        match slot {
            Some(slot) => {
                let (_, expr) = slot.take().expect("find matched Some above");
                out.push(expr);
            }
            None => out.push(Expr::Column(Column::new_unqualified(field.name()))),
        }
    }
    out.extend(derived.into_iter().flatten().map(|(_, expr)| expr));
    out
}

/// Truncate sort specs at the first overwritten sort key: a replaced column
/// still exists in the output, but its values changed, so the declared order
/// is invalid from that key onward (same prefix semantics as select()'s
/// projection truncation in basic.rs).
pub fn truncate_specs_at_overwrite(
    specs: &[SortSpec],
    derived_names: &HashSet<String>,
) -> Vec<SortSpec> {
    specs
        .iter()
        .take_while(|spec| !derived_names.contains(&spec.column))
        .cloned()
        .collect()
}


// ============================================================================
// Test Utilities
// ============================================================================
