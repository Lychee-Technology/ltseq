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
//! when the result executes. It sits between the target and the join and
//! checks the rows it passes on, which are the rows the join reads however
//! the target's plan evaluates: a key computed by `rand()` is checked with
//! the values the join matches. A check that read the target's plan a
//! second time could pass a volatile key that then repeated in the join.
//! The check remembers each key it passes and fails on a row whose key it
//! has passed, before that row reaches the join, so no input row repeats.
//!
//! Filters, limits and the join's dynamic filters do not cross the check, so
//! a query that reads the target to its end checks every key, whether or
//! not an input row matches the repeated one. Every terminal that reads the
//! whole result does, `count()` included. A query that stops early, such as
//! `head(1)`, may return before the check reaches a repeated key's second
//! row, and none of the rows it returns repeats. v0.5's
//! `join(validate="m:1")` raises there too (contract §12.1); a lookup could
//! do that only by holding the target until every key is checked. A query
//! with no input rows can return without reading the target, and returns
//! no rows.
//!
//! NULL keys are not checked: a NULL key matches no input row.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::{filter, is_not_null};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{exec_err, DFSchemaRef, Result, ScalarValue, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, Operator, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::group_values::{new_group_values, GroupValues};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    PlanProperties, ReplaceChildrenOptions, StatisticsArgs,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::*;
use futures_util::StreamExt;
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
    let checked = with_one_row_per_key((**target).clone(), compared, target_key);
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
fn with_one_row_per_key(target: DataFrame, compared: Expr, target_key: &str) -> DataFrame {
    let (state, input) = target.into_parts();
    let check = OneRowPerKey {
        input,
        key: compared,
        column: target_key.to_string(),
    };
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(check),
    });
    DataFrame::new(state, plan)
}

/// The target of a lookup, passed through unchanged, with the check that
/// `key` holds each non-NULL value in one row at most.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct OneRowPerKey {
    input: LogicalPlan,
    /// The target's key as the join compares it.
    key: Expr,
    /// The target's key column, as the user named it.
    column: String,
}

impl UserDefinedLogicalNodeCore for OneRowPerKey {
    fn name(&self) -> &str {
        "LookupOneRowPerKey"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.key.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LookupOneRowPerKey: {}", self.key)
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(OneRowPerKey {
            input: inputs.swap_remove(0),
            key: exprs.swap_remove(0),
            column: self.column.clone(),
        })
    }

    // Projection pruning crosses the check: it needs the columns asked of
    // it, and DataFusion adds those its key reads. Filters and limits keep
    // the defaults and stay above it, so the check sees every target row.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}

/// [`OneRowPerKey`] in the physical plan: its input's batches, unchanged,
/// until a row's key is one an earlier row of its partition held. It asks
/// for its input partitioned on the key, so all rows of a key pass
/// through one partition, and each partition remembers its own keys.
#[derive(Debug)]
struct OneRowPerKeyExec {
    input: Arc<dyn ExecutionPlan>,
    key: Arc<dyn PhysicalExpr>,
    column: String,
}

impl DisplayAs for OneRowPerKeyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LookupOneRowPerKeyExec: {}", self.key)
    }
}

impl ExecutionPlan for OneRowPerKeyExec {
    fn name(&self) -> &str {
        "LookupOneRowPerKeyExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::KeyPartitioned(vec![Arc::clone(&self.key)])]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        f(&self.key)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OneRowPerKeyExec {
            input: children.swap_remove(0),
            key: Arc::clone(&self.key),
            column: self.column.clone(),
        }))
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    // The join picks its build side by these, so they are the target's.
    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let key_field = Field::new("key", self.key.data_type(&schema)?, true);
        let mut keys = Keys {
            key: Arc::clone(&self.key),
            column: self.column.clone(),
            seen: new_group_values(
                Arc::new(ArrowSchema::new(vec![key_field])),
                &GroupOrdering::None,
            )?,
            groups: Vec::new(),
        };
        let checked = input.map(move |batch| {
            let batch = batch?;
            keys.pass(&batch)?;
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, checked)))
    }
}

/// The keys one partition of [`OneRowPerKeyExec`] has passed, held as a
/// hash aggregation holds its groups.
struct Keys {
    key: Arc<dyn PhysicalExpr>,
    column: String,
    seen: Box<dyn GroupValues>,
    /// The group of each row of the last batch, kept to reuse its space.
    groups: Vec<usize>,
}

impl Keys {
    /// Remembers the non-NULL keys of `batch`, or fails on its first row
    /// whose key an earlier row held, in this batch or before it.
    fn pass(&mut self, batch: &RecordBatch) -> Result<()> {
        let keys = self.key.evaluate(batch)?.into_array(batch.num_rows())?;
        let keys: ArrayRef = if keys.logical_null_count() > 0 {
            filter(&keys, &is_not_null(&keys)?)?
        } else {
            keys
        };
        let before = self.seen.len();
        self.seen
            .intern(std::slice::from_ref(&keys), &mut self.groups)?;
        // Each new key takes the next group id, so the batch repeats no key
        // when it adds as many groups as it has rows.
        let added = self.seen.len() - before;
        if added == keys.len() {
            return Ok(());
        }
        let mut first_seen = vec![false; added];
        for (row, &group) in self.groups.iter().enumerate() {
            let new = group
                .checked_sub(before)
                .is_some_and(|i| !std::mem::replace(&mut first_seen[i], true));
            if !new {
                let key = ScalarValue::try_from_array(&keys, row)?;
                return exec_err!("{}", repeated_key(&self.column, &key));
            }
        }
        Ok(())
    }
}

/// The error for a target whose key column `column` holds `key` in more
/// than one row.
fn repeated_key(column: &str, key: &ScalarValue) -> String {
    format!(
        "lookup(): the target's key column '{column}' holds {} in more than one row, \
         so each input row that matches it would repeat. A lookup takes one target row \
         per key: remove the repeated rows from the target, or use join() to get a row \
         for each match.",
        shown(key),
    )
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

/// Plans [`OneRowPerKey`] as [`OneRowPerKeyExec`]; the engine's query
/// planner holds it (`engine::QueryPlanner`).
pub(crate) struct OneRowPerKeyPlanner;

#[async_trait]
impl ExtensionPlanner for OneRowPerKeyPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<OneRowPerKey>() else {
            return Ok(None);
        };
        let key = planner.create_physical_expr(
            &node.key,
            logical_inputs[0].schema(),
            session,
            planning_ctx,
        )?;
        Ok(Some(Arc::new(OneRowPerKeyExec {
            input: Arc::clone(&physical_inputs[0]),
            key,
            column: node.column.clone(),
        })))
    }
}
