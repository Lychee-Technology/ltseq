//! Physical optimizer rules for GPU-accelerated execution.
//!
//! These rules inspect the physical plan tree and replace CPU execution nodes
//! (e.g., `FilterExec`, `ProjectionExec`) with GPU equivalents when the
//! expressions and data types are GPU-compatible.
//!
//! ## GpuFilterRule
//! Replaces `FilterExec` with `WgpuFilterExec` for:
//! - `BinaryExpr(Column, Op, Literal)` where Column is i32/i64/f32
//! - `BinaryExpr(Literal, Op, Column)` (operator is flipped)
//! - Op ∈ {Eq, NotEq, Lt, LtEq, Gt, GtEq}
//! - Compound: AND/OR/NOT
//!
//! ## GpuProjectionRule
//! Replaces `ProjectionExec` with `WgpuProjectionExec` when at least one
//! output expression contains GPU-compatible arithmetic (Add, Sub, Mul, Div, Mod)
//! on i32/i64/f32 columns and literals.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal, NotExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::InputOrderMode;
use std::sync::Arc;

use super::aggregate_exec::{AggColumnPlan, GpuAggFunc, KeyType, WgpuAggregateExec};
use super::codegen::{GpuArithOp, GpuDerivedColumn, GpuExprLiteral, GpuExprNode, GpuType};
use super::filter_exec::{CompareOp, GpuFilterOp, GpuFilterTree, GpuLiteral, WgpuFilterExec};
use super::fused_exec::FusedGpuPipelineExec;
use super::projection_exec::{OutputColumnPlan, WgpuProjectionExec};

/// Physical optimizer rule that replaces `FilterExec` nodes with `WgpuFilterExec`
/// when the filter predicate is GPU-compatible (simple column-vs-literal comparison
/// on i32/i64/f32 types, with AND/OR/NOT compound support).
///
/// Appended after DataFusion's default optimizer rules, so predicate pushdown,
/// filter reordering, and batch coalescing are already applied.
#[derive(Debug)]
pub struct GpuFilterRule;

impl PhysicalOptimizerRule for GpuFilterRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Walk the plan tree bottom-up, replacing eligible FilterExec nodes
        plan.transform_up(|node| try_replace_filter(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "GpuFilterRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace a single plan node if it is a GPU-eligible `FilterExec`.
///
/// Returns `Transformed::yes(new_plan)` if replaced, `Transformed::no(plan)` otherwise.
fn try_replace_filter(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(filter_exec) = plan.as_any().downcast_ref::<FilterExec>() else {
        return Ok(Transformed::no(plan));
    };

    let predicate = filter_exec.predicate();
    let input = filter_exec.input();
    let input_schema = input.schema();

    // Try to extract a GPU-compatible filter tree from the predicate
    let Some(gpu_filter) = try_extract_gpu_filter(predicate, &input_schema) else {
        return Ok(Transformed::no(plan));
    };

    Ok(Transformed::yes(Arc::new(WgpuFilterExec::new(
        input.clone(),
        gpu_filter,
        predicate.clone(),
    ))))
}

/// Try to extract a `GpuFilterTree` from a physical predicate expression.
///
/// Supports:
/// - `BinaryExpr(Column, Op, Literal)` → Leaf (direct)
/// - `BinaryExpr(Literal, Op, Column)` → Leaf (flip operator)
/// - `BinaryExpr(left, And, right)` → And(left_tree, right_tree)
/// - `BinaryExpr(left, Or, right)` → Or(left_tree, right_tree)
/// - `NotExpr(inner)` → Not(inner_tree)
///
/// Returns `None` if the predicate (or any sub-expression) is not GPU-compatible.
fn try_extract_gpu_filter(
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Option<GpuFilterTree> {
    // Case: NOT expression
    if let Some(not_expr) = predicate.as_any().downcast_ref::<NotExpr>() {
        let inner = not_expr.arg();
        let inner_tree = try_extract_gpu_filter(inner, schema)?;
        return Some(GpuFilterTree::Not(Box::new(inner_tree)));
    }

    let bin_expr = predicate.as_any().downcast_ref::<BinaryExpr>()?;

    // Case: AND / OR compound predicates
    match bin_expr.op() {
        Operator::And => {
            let left_tree = try_extract_gpu_filter(bin_expr.left(), schema)?;
            let right_tree = try_extract_gpu_filter(bin_expr.right(), schema)?;
            return Some(GpuFilterTree::And(
                Box::new(left_tree),
                Box::new(right_tree),
            ));
        }
        Operator::Or => {
            let left_tree = try_extract_gpu_filter(bin_expr.left(), schema)?;
            let right_tree = try_extract_gpu_filter(bin_expr.right(), schema)?;
            return Some(GpuFilterTree::Or(Box::new(left_tree), Box::new(right_tree)));
        }
        _ => {}
    }

    // Case: comparison operators (leaf predicates)
    let op = match bin_expr.op() {
        Operator::Eq => CompareOp::Eq,
        Operator::NotEq => CompareOp::NotEq,
        Operator::Lt => CompareOp::Lt,
        Operator::LtEq => CompareOp::LtEq,
        Operator::Gt => CompareOp::Gt,
        Operator::GtEq => CompareOp::GtEq,
        _ => return None, // arithmetic, etc. — not supported
    };

    let left = bin_expr.left();
    let right = bin_expr.right();

    // Case 1: Column op Literal
    if let (Some(col), Some(lit)) = (
        left.as_any().downcast_ref::<Column>(),
        right.as_any().downcast_ref::<Literal>(),
    ) {
        return extract_col_lit(col, lit, op, schema).map(GpuFilterTree::Leaf);
    }

    // Case 2: Literal op Column → flip
    if let (Some(lit), Some(col)) = (
        left.as_any().downcast_ref::<Literal>(),
        right.as_any().downcast_ref::<Column>(),
    ) {
        return extract_col_lit(col, lit, op.flip(), schema).map(GpuFilterTree::Leaf);
    }

    None
}

/// Extract column index and literal value, checking type compatibility.
fn extract_col_lit(
    col: &Column,
    lit: &Literal,
    op: CompareOp,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Option<GpuFilterOp> {
    let col_idx = col.index();
    let field = schema.field(col_idx);

    let literal = match (field.data_type(), lit.value()) {
        (DataType::Int32, ScalarValue::Int32(Some(v))) => GpuLiteral::Int32(*v),
        (DataType::Int64, ScalarValue::Int64(Some(v))) => GpuLiteral::Int64(*v),
        (DataType::Float32, ScalarValue::Float32(Some(v))) => GpuLiteral::Float32(*v),
        _ => return None, // Unsupported type or null literal
    };

    Some(GpuFilterOp {
        column_index: col_idx,
        op,
        literal,
    })
}

// ════════════════════════════════════════════════════════════════════════
//  GpuProjectionRule
// ════════════════════════════════════════════════════════════════════════

/// Physical optimizer rule that replaces `ProjectionExec` nodes with
/// `WgpuProjectionExec` when at least one output expression contains
/// GPU-compatible arithmetic (Add, Sub, Mul, Div, Mod on i32/i64/f32).
///
/// Column-only pass-through expressions are still evaluated on CPU;
/// only expressions with arithmetic operations are offloaded to the GPU.
#[derive(Debug)]
pub struct GpuProjectionRule;

impl PhysicalOptimizerRule for GpuProjectionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| try_replace_projection(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "GpuProjectionRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace a single plan node if it is a GPU-eligible `ProjectionExec`.
///
/// A projection is eligible when at least one output expression contains
/// arithmetic on GPU-compatible types (i32, i64, f32). Pure column-ref
/// projections are not worth sending to the GPU.
fn try_replace_projection(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(proj_exec) = plan.as_any().downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };

    let input = proj_exec.input();
    let input_schema = input.schema();
    let proj_exprs = proj_exec.expr();

    let mut output_plan = Vec::with_capacity(proj_exprs.len());
    let mut has_gpu_expr = false;

    for (out_idx, pe) in proj_exprs.iter().enumerate() {
        match try_extract_gpu_expr(&pe.expr, &input_schema) {
            Some(gpu_expr) if contains_arithmetic(&gpu_expr) => {
                has_gpu_expr = true;
                output_plan.push(OutputColumnPlan::GpuComputed(GpuDerivedColumn {
                    expr: gpu_expr,
                    output_index: out_idx,
                    output_name: pe.alias.clone(),
                }));
            }
            _ => {
                // Not GPU-compatible, or pure column ref → CPU pass-through
                output_plan.push(OutputColumnPlan::CpuPassthrough {
                    expr: pe.expr.clone(),
                    name: pe.alias.clone(),
                });
            }
        }
    }

    if !has_gpu_expr {
        return Ok(Transformed::no(plan));
    }

    // Build output schema from the projection expressions
    let output_fields: Vec<Field> = proj_exprs
        .iter()
        .map(|pe| {
            let dt = pe.expr.data_type(&input_schema).unwrap_or(DataType::Null);
            Field::new(
                &pe.alias,
                dt,
                pe.expr.nullable(&input_schema).unwrap_or(true),
            )
        })
        .collect();
    let output_schema = Arc::new(Schema::new(output_fields));

    Ok(Transformed::yes(Arc::new(WgpuProjectionExec::new(
        input.clone(),
        output_plan,
        proj_exprs.to_vec(),
        output_schema,
    ))))
}

/// Recursively extract a `GpuExprNode` from a DataFusion `PhysicalExpr`.
///
/// Returns `Some(node)` if the entire expression tree is GPU-compatible,
/// `None` otherwise. Supported nodes:
/// - `Column` with i32/i64/f32 data type
/// - `Literal` with Int32/Int64/Float32 scalar value
/// - `BinaryExpr` with Plus/Minus/Multiply/Divide/Modulo operator
///   (i64 Div/Mod are excluded — WGSL has no emulation for these)
fn try_extract_gpu_expr(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Option<GpuExprNode> {
    // Case: Column reference
    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
        let col_idx = col.index();
        let field = schema.field(col_idx);
        let gpu_type = match field.data_type() {
            DataType::Int32 => GpuType::Int32,
            DataType::Int64 => GpuType::Int64,
            DataType::Float32 => GpuType::Float32,
            _ => return None,
        };
        return Some(GpuExprNode::Column {
            index: col_idx,
            dtype: gpu_type,
        });
    }

    // Case: Literal value
    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
        let gpu_lit = match lit.value() {
            ScalarValue::Int32(Some(v)) => GpuExprLiteral::Int32(*v),
            ScalarValue::Int64(Some(v)) => GpuExprLiteral::Int64(*v),
            ScalarValue::Float32(Some(v)) => GpuExprLiteral::Float32(*v),
            _ => return None,
        };
        return Some(GpuExprNode::Literal(gpu_lit));
    }

    // Case: Binary arithmetic expression
    if let Some(bin_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let arith_op = match bin_expr.op() {
            Operator::Plus => GpuArithOp::Add,
            Operator::Minus => GpuArithOp::Sub,
            Operator::Multiply => GpuArithOp::Mul,
            Operator::Divide => GpuArithOp::Div,
            Operator::Modulo => GpuArithOp::Mod,
            _ => return None, // comparisons, boolean ops → not arithmetic
        };

        let left = try_extract_gpu_expr(bin_expr.left(), schema)?;
        let right = try_extract_gpu_expr(bin_expr.right(), schema)?;

        // Type compatibility: both operands must have the same GPU type
        let left_type = left.output_type();
        let right_type = right.output_type();
        if left_type != right_type {
            return None;
        }

        // i64 Div/Mod not supported in WGSL emulation
        if left_type == GpuType::Int64 && matches!(arith_op, GpuArithOp::Div | GpuArithOp::Mod) {
            return None;
        }

        return Some(GpuExprNode::BinaryOp {
            op: arith_op,
            left: Box::new(left),
            right: Box::new(right),
        });
    }

    None
}

/// Check whether a `GpuExprNode` contains at least one arithmetic operation.
///
/// We only want to offload to GPU when there is actual computation — pure
/// column references are cheaper to evaluate on CPU (zero-copy).
fn contains_arithmetic(expr: &GpuExprNode) -> bool {
    match expr {
        GpuExprNode::Column { .. } | GpuExprNode::Literal(_) => false,
        GpuExprNode::BinaryOp { .. } => true,
    }
}

// ════════════════════════════════════════════════════════════════════════
//  GpuAggregateRule
// ════════════════════════════════════════════════════════════════════════

/// Physical optimizer rule that replaces `AggregateExec` nodes with
/// `WgpuAggregateExec` when:
/// - Input is sorted by the (single) group-by key (`InputOrderMode::Sorted`)
/// - Group key type is i32 or i64
/// - All aggregates are sum/count/min/max/avg on i32 or i64 value columns
/// - No DISTINCT or FILTER clauses
/// - Mode is a first-stage aggregate (Partial / Single / SinglePartitioned)
///
/// Falls back silently for any case not matching the above criteria.
#[derive(Debug)]
pub struct GpuAggregateRule;

impl PhysicalOptimizerRule for GpuAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| try_replace_aggregate(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "GpuAggregateRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace a single plan node if it is a GPU-eligible `AggregateExec`.
fn try_replace_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Only handle first-stage aggregations (Partial / Single / SinglePartitioned).
    // Final/FinalPartitioned modes receive partially-aggregated state columns,
    // not raw value columns, so our GPU kernels cannot handle them.
    if !agg_exec.mode().is_first_stage() {
        return Ok(Transformed::no(plan));
    }

    // Require sorted input so we can use the contiguous-segment algorithm.
    // InputOrderMode::Sorted means ALL group keys are sorted (streaming mode).
    if agg_exec.input_order_mode() != &InputOrderMode::Sorted {
        return Ok(Transformed::no(plan));
    }

    // Phase 5: support exactly one group-by key (simplest case first).
    let group_by = agg_exec.group_expr();
    if group_by.expr().len() != 1 || group_by.has_grouping_set() {
        return Ok(Transformed::no(plan));
    }

    let input = agg_exec.input();
    let input_schema = input.schema();

    // Resolve the group key to a column index and key type.
    let (group_expr, _group_name) = &group_by.expr()[0];
    let Some(group_col) = group_expr.as_any().downcast_ref::<Column>() else {
        // Group key is not a plain column reference — fall back.
        return Ok(Transformed::no(plan));
    };
    let group_key_col = group_col.index();
    let group_key_type = match input_schema.field(group_key_col).data_type() {
        DataType::Int32 => KeyType::I32,
        DataType::Int64 => KeyType::I64,
        _ => return Ok(Transformed::no(plan)), // float/string/etc keys — fall back
    };

    // Reject if any aggregate has a FILTER clause or is DISTINCT.
    let filter_exprs = agg_exec.filter_expr();
    if filter_exprs.iter().any(|f| f.is_some()) {
        return Ok(Transformed::no(plan));
    }
    for agg_fn_expr in agg_exec.aggr_expr() {
        if agg_fn_expr.is_distinct() {
            return Ok(Transformed::no(plan));
        }
    }

    // Build the AggColumnPlan list. Fail-fast on any unsupported aggregate.
    let mut agg_plans: Vec<AggColumnPlan> = Vec::with_capacity(agg_exec.aggr_expr().len());

    // Build output schema fields: [group_key_field] + [agg_output_fields]
    let group_key_field = input_schema.field(group_key_col).clone();
    let mut output_fields: Vec<Field> = vec![group_key_field];

    for agg_fn_expr in agg_exec.aggr_expr() {
        let fn_name = agg_fn_expr.fun().name().to_lowercase();
        let output_name = agg_fn_expr.name().to_string();

        // Each aggregate must operate on exactly one plain column reference.
        let exprs = agg_fn_expr.expressions();

        // "count" with no arguments (COUNT(*)) is allowed — no column needed.
        let is_count_star = fn_name == "count" && exprs.is_empty();

        // For non-count-star aggregates, require exactly one Column expression.
        let value_col_index: Option<usize> = if is_count_star {
            None
        } else {
            if exprs.len() != 1 {
                return Ok(Transformed::no(plan));
            }
            let Some(col) = exprs[0].as_any().downcast_ref::<Column>() else {
                // Expression is not a bare column (e.g., CAST or arithmetic) — fall back.
                return Ok(Transformed::no(plan));
            };
            let col_idx = col.index();
            // Verify value column type is i32 or i64.
            match input_schema.field(col_idx).data_type() {
                DataType::Int32 | DataType::Int64 => {}
                _ => return Ok(Transformed::no(plan)),
            }
            Some(col_idx)
        };

        // Determine output type (must match DataFusion's conventions).
        let (agg_plan, output_dt) = match fn_name.as_str() {
            "sum" => {
                let col_idx = value_col_index.unwrap(); // sum always has a column
                                                        // DataFusion: sum(i32) → i64, sum(i64) → i64
                let out_dt = DataType::Int64;
                (
                    AggColumnPlan::GpuDirect {
                        func: GpuAggFunc::Sum,
                        value_col_index: col_idx,
                        output_name: output_name.clone(),
                    },
                    out_dt,
                )
            }
            "count" => {
                // count(*) or count(col) → i64
                let col_idx = value_col_index.unwrap_or(group_key_col); // dummy col for count*
                (
                    AggColumnPlan::GpuDirect {
                        func: GpuAggFunc::Count,
                        value_col_index: col_idx,
                        output_name: output_name.clone(),
                    },
                    DataType::Int64,
                )
            }
            "min" => {
                let col_idx = value_col_index.unwrap();
                let out_dt = input_schema.field(col_idx).data_type().clone();
                (
                    AggColumnPlan::GpuDirect {
                        func: GpuAggFunc::Min,
                        value_col_index: col_idx,
                        output_name: output_name.clone(),
                    },
                    out_dt,
                )
            }
            "max" => {
                let col_idx = value_col_index.unwrap();
                let out_dt = input_schema.field(col_idx).data_type().clone();
                (
                    AggColumnPlan::GpuDirect {
                        func: GpuAggFunc::Max,
                        value_col_index: col_idx,
                        output_name: output_name.clone(),
                    },
                    out_dt,
                )
            }
            "avg" => {
                let col_idx = value_col_index.unwrap();
                // avg always returns Float64 in DataFusion
                (
                    AggColumnPlan::GpuAvg {
                        value_col_index: col_idx,
                        output_name: output_name.clone(),
                    },
                    DataType::Float64,
                )
            }
            _ => {
                // Unsupported aggregate (median, var, stddev, etc.) — fall back entirely.
                return Ok(Transformed::no(plan));
            }
        };

        agg_plans.push(agg_plan);
        output_fields.push(Field::new(&output_name, output_dt, true));
    }

    let output_schema = Arc::new(Schema::new(output_fields));

    // Downcast plan to Arc<AggregateExec> for the fallback path stored in WgpuAggregateExec.
    let original_agg = plan
        .as_any()
        .downcast_ref::<AggregateExec>()
        .map(|_| {
            // SAFETY: we just confirmed it's an AggregateExec; clone the Arc by re-wrapping.
            // We need Arc<AggregateExec> but `plan` is Arc<dyn ExecutionPlan>.
            // Use unsafe transmute-free approach: rebuild from parts via try_new.
            // Instead, store the Arc<dyn ExecutionPlan> and cast at runtime.
            plan.clone()
        })
        .unwrap();

    // Downcast Arc<dyn ExecutionPlan> → Arc<AggregateExec>.
    // DataFusion's ExecutionPlan doesn't expose this directly; use Arc::downcast via Any.
    // We work around by storing it as Arc<dyn ExecutionPlan> and building a thin wrapper.
    // Actually reconstruct the Arc<AggregateExec> by cloning from the concrete ref.
    let agg_arc: Arc<AggregateExec> = {
        // Re-create an Arc<AggregateExec> using try_new (cheapest path without unsafe).
        Arc::new(AggregateExec::try_new(
            agg_exec.mode().clone(),
            agg_exec.group_expr().clone(),
            agg_exec.aggr_expr().to_vec(),
            agg_exec.filter_expr().to_vec(),
            agg_exec.input().clone(),
            agg_exec.input_schema(),
        )?)
    };
    let _ = original_agg; // drop the intermediate

    Ok(Transformed::yes(Arc::new(WgpuAggregateExec::new(
        input.clone(),
        group_key_col,
        group_key_type,
        agg_plans,
        agg_arc,
        output_schema,
    ))))
}

// ════════════════════════════════════════════════════════════════════════
//  GpuFusionRule
// ════════════════════════════════════════════════════════════════════════

/// Physical optimizer rule that collapses adjacent GPU operators into a fused plan.
///
/// Detects the pattern:
/// ```text
///   WgpuProjectionExec
///     └─ WgpuFilterExec
///          └─ child
/// ```
/// and replaces it with:
/// ```text
///   FusedGpuPipelineExec (filter_tree + projection_plan)
///     └─ child
/// ```
///
/// This eliminates the CPU round-trip between filter and projection: filter
/// produces a bitmask on GPU, projection evaluates on the already-filtered
/// batch, and we save one CPU→GPU upload per batch.
///
/// Must run AFTER `GpuFilterRule` and `GpuProjectionRule` have already
/// replaced the CPU nodes with GPU equivalents.
#[derive(Debug)]
pub struct GpuFusionRule;

impl PhysicalOptimizerRule for GpuFusionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| try_fuse_filter_projection(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "GpuFusionRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to fuse a `WgpuProjectionExec(WgpuFilterExec(...))` pattern into
/// a single `FusedGpuPipelineExec`.
fn try_fuse_filter_projection(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // Must be a WgpuProjectionExec
    let Some(proj_exec) = plan.as_any().downcast_ref::<WgpuProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Its child must be a WgpuFilterExec
    let children = plan.children();
    if children.len() != 1 {
        return Ok(Transformed::no(plan));
    }
    let Some(filter_exec) = children[0].as_any().downcast_ref::<WgpuFilterExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Extract filter tree + predicate from the WgpuFilterExec
    let filter_tree = filter_exec.gpu_filter().clone();
    let filter_predicate = filter_exec.predicate().clone();
    let filter_child = filter_exec.child().clone();

    // Extract projection plan + original exprs + output schema from WgpuProjectionExec
    let output_plan = proj_exec.output_plan().clone();
    let original_proj_exprs = proj_exec.original_exprs().clone();
    let output_schema = proj_exec.output_schema().clone();

    Ok(Transformed::yes(Arc::new(FusedGpuPipelineExec::new(
        filter_child,
        filter_tree,
        filter_predicate,
        output_plan,
        original_proj_exprs,
        output_schema,
    ))))
}
