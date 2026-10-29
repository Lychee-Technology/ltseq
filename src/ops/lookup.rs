//! The join that resolves `lookup()` in `derive()`.
//!
//! A lookup fetches one value of the target for each input row, so it is a
//! left join that must find at most one target row per input row. A target
//! that holds a key in more than one row would repeat every input row that
//! matches it, and `derive()` would add rows (#351). So the join's target
//! side carries a check that fails the query, during execution, when a key
//! repeats.
//!
//! The check is part of the plan rather than a query run at the `derive()`
//! call, so `derive()` stays lazy (ADR 0005) and checks the target as it is
//! when the result executes. It groups the target by its key and counts the
//! keys held by more than one row, which reads the target's key column a
//! second time. Its one row is cross joined to the target, and a cross join
//! reads one side whole before it returns a row: the target's first row
//! reaches the lookup's join only after every key has been counted. So a
//! query that reads the target raises before it returns a row, `head(1)`
//! and `count()` included, whether or not an input row matches the repeated
//! key, as v0.5's `join(validate="m:1")` checks its right side (contract
//! §12.1). DataFusion may read the target first instead, when its
//! statistics say it is the smaller side; the target's rows then wait for
//! the check. A query with no input rows can return without reading the
//! target, and returns no rows.
//!
//! NULL keys are not checked: a NULL key matches no input row.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Int64Type, Schema as ArrowSchema};
use datafusion::common::utils::take_function_args;
use datafusion::common::{exec_err, DFSchema, Result, ScalarValue};
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlanBuilder, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::*;
use pyo3::prelude::*;

use crate::error::LtseqError;
use crate::ops::common::JoinType;
use crate::ops::join::prefixed_join;
use crate::transpiler::{binary_input_types, narrow_decimals};
use crate::LTSeqTable;

/// `lookup()`'s join: the left join of `table` with `other` on `key ==
/// target_key`, every column of `other` prefixed with `{alias}_`, as
/// `join_prefixed` builds it. The query fails during execution when
/// `target_key` holds a key in more than one row of `other`.
pub fn lookup_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    key: &str,
    target_key: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    let (_, input_schema) = table.require_df_and_schema()?;
    let (target, target_schema) = other.require_df_and_schema()?;
    let type_of = |schema: &ArrowSchema, name: &str| {
        schema
            .field_with_name(name)
            .map(|field| field.data_type().clone())
            .map_err(|_| LtseqError::ColumnNotFound(name.to_string()))
    };
    let compared = compared_key(
        ident(target_key),
        &type_of(input_schema, key)?,
        &type_of(target_schema, target_key)?,
    );
    let checked = with_one_row_per_key((**target).clone(), compared, target_key)
        .map_err(|e| LtseqError::Runtime(format!("Failed to plan the lookup's key check: {e}")))?;
    let checked = LTSeqTable::from_df_with_schema(
        Arc::clone(&other.session),
        checked,
        Arc::clone(target_schema),
        Vec::new(),
        None,
    );

    prefixed_join(
        table,
        &checked,
        &[key.to_string()],
        &[target_key.to_string()],
        JoinType::Left,
        alias,
    )
}

/// `target_key`, a key of type `target_type`, as the join compares it with
/// a key of type `key_type`: the join compares the two at the type
/// DataFusion coerces `key = target_key` to, after a narrow decimal widens
/// (#241). Two values the join finds equal there, such as the strings "1"
/// and "01" next to an integer key, would both match one input row, so
/// they are one key to the check.
///
/// When the pair has no common type the key is left as it is: the join
/// fails on the pair, and the check need not.
fn compared_key(target_key: Expr, key_type: &DataType, target_type: &DataType) -> Expr {
    let widens = narrow_decimals::widens([key_type, target_type]);
    let (target_key, key_type, target_type) = if widens {
        let wide = |t: &DataType| narrow_decimals::wide_type(t).unwrap_or_else(|| t.clone());
        (
            narrow_decimals::widened(target_key, target_type),
            wide(key_type),
            wide(target_type),
        )
    } else {
        (target_key, key_type.clone(), target_type.clone())
    };
    match binary_input_types(&key_type, &Operator::Eq, &target_type) {
        Ok((_, compared)) if compared != target_type => cast(target_key, compared),
        _ => target_key,
    }
}

/// `target` with the check that `compared`, its key as the join compares
/// it, holds each value in one row at most. `target_key` names the key in
/// the error.
fn with_one_row_per_key(target: DataFrame, compared: Expr, target_key: &str) -> Result<DataFrame> {
    // The check's two columns sit beside the target's in the cross join, so
    // their names must differ from all of them.
    let repeated = unused_name("__ltseq_repeated_keys", target.schema());
    let example = unused_name("__ltseq_repeated_key", target.schema());
    let check = target
        .clone()
        .aggregate(vec![compared.alias("key")], vec![count_all().alias("rows")])?
        .filter(ident("rows").gt(lit(1_i64)).and(ident("key").is_not_null()))?
        .aggregate(
            vec![],
            vec![
                count_all().alias(&repeated),
                first_value(ident("key"), vec![]).alias(&example),
            ],
        )?
        .filter(OneRowPerKey::call(
            ident(&repeated),
            ident(&example),
            target_key,
        ))?;

    let columns: Vec<Expr> = target
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let (state, target) = target.into_parts();
    let (_, check) = check.into_parts();
    let plan = LogicalPlanBuilder::from(check)
        .cross_join(target)?
        .project(columns)?
        .build()?;
    Ok(DataFrame::new(state, plan))
}

/// `base`, with underscores in front until no column of `schema` has it.
fn unused_name(base: &str, schema: &DFSchema) -> String {
    let mut name = base.to_string();
    while schema.has_column_with_unqualified_name(&name) {
        name.insert(0, '_');
    }
    name
}

/// The predicate over the check's one row: true when no key repeats, and
/// otherwise an execution error naming the key column and a repeated key.
#[derive(Debug, PartialEq, Eq, Hash)]
struct OneRowPerKey {
    /// The target's key column, as the user named it.
    column: String,
    signature: Signature,
}

impl OneRowPerKey {
    /// `repeated` is the number of repeated keys, `example` one of them.
    fn call(repeated: Expr, example: Expr, column: &str) -> Expr {
        ScalarUDF::new_from_impl(OneRowPerKey {
            column: column.to_string(),
            signature: Signature::any(2, Volatility::Immutable),
        })
        .call(vec![repeated, example])
    }

    fn error(&self, example: &ScalarValue, repeated: i64) -> String {
        let others = match repeated - 1 {
            0 => String::new(),
            1 => " (1 other key repeats too)".to_string(),
            n => format!(" ({n} other keys repeat too)"),
        };
        format!(
            "lookup(): the target's key column '{}' holds {} in more than one row{others}, \
             so each input row that matches it would repeat. A lookup takes one target row \
             per key: remove the repeated rows from the target, or use join() to get a row \
             for each match.",
            self.column,
            shown(example),
        )
    }
}

/// A key as the error shows it: a string in quotes, any other value as
/// DataFusion displays it.
fn shown(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Dictionary(_, value) => shown(value),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => {
            format!("'{s}'")
        }
        value => value.to_string(),
    }
}

impl ScalarUDFImpl for OneRowPerKey {
    fn name(&self) -> &str {
        "lookup_one_row_per_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let [repeated, example] = take_function_args(self.name(), args.args)?;
        let repeated = repeated.into_array(rows)?;
        let example = example.into_array(rows)?;
        let Some(repeated) = repeated.as_primitive_opt::<Int64Type>() else {
            return exec_err!(
                "{} counts in Int64, not {}",
                self.name(),
                repeated.data_type()
            );
        };
        for row in 0..rows {
            if repeated.is_valid(row) && repeated.value(row) > 0 {
                let key = ScalarValue::try_from_array(&example, row)?;
                return exec_err!("{}", self.error(&key, repeated.value(row)));
            }
        }
        let passed = BooleanArray::from(vec![true; rows]);
        Ok(ColumnarValue::Array(Arc::new(passed)))
    }
}
