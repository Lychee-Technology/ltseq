//! Physical optimizer rule that replaces CPU execution nodes with GPU equivalents.
//!
//! `HostToGpuRule` walks the physical plan tree via `transform_up` and replaces
//! eligible nodes with GPU-accelerated versions:
//!
//! - `FilterExec` → `GpuFilterExec` when the predicate is a simple
//!   `Column CMP Literal` on a supported numeric type (i32, i64, f32, f64).
//! - `AggregateExec` → `GpuHashAggregateExec` when group-by keys are simple
//!   numeric columns and aggregate functions are sum/count/min/max/avg.
//!   Also handles DISTINCT (group-by all columns with zero aggregates).
//! - `HashJoinExec` → `GpuHashJoinExec` for single numeric key equi-joins.
//! - `SortExec` → `GpuSortExec` when sort keys are numeric columns.
//!
//! The rule is **sort-order-aware**: it reads `EquivalenceProperties::output_ordering()`
//! from input nodes to determine if data is sorted. This enables Phase 1.2+
//! operators to choose between ordered and unordered algorithms:
//!
//! - Sorted group-by key → `GpuConsecutiveGroupExec` + `GpuSegmentedAggregateExec`
//! - Sorted join keys → `GpuMergeJoinExec`
//! - Sorted search column → `GpuBinarySearchExec`
//!
//! The rule also checks row count statistics: if the estimated row count is
//! below `GPU_MIN_ROWS_THRESHOLD`, the operation stays on CPU to avoid
//! H2D/D2H transfer overhead.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, Result};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;

use super::adjacent_distinct::{
    AdjacentDistinctKey, GpuAdjacentDistinctConfig, GpuAdjacentDistinctExec,
};
use super::filter_exec::{GpuCompoundPredicate, GpuFilterExec};
use super::hash_join::{is_hash_join_key_type_supported, GpuHashJoinConfig, GpuHashJoinExec};
use super::hash_ops::{GpuAggRequest, GpuGroupKey, GpuHashAggConfig, GpuHashAggregateExec};
use super::merge_join::GpuJoinType;
use super::ordered_ops::SegAggFunc;
use super::sort_aware::{extract_sort_columns, is_sorted_by};
use super::sort_exec::{is_sort_key_type_supported, GpuSortConfig, GpuSortExec, GpuSortKey};
use super::GPU_MIN_ROWS_THRESHOLD;

/// Physical optimizer rule that offloads eligible operations to GPU.
///
/// The rule is appended to DataFusion's default optimizer pipeline, so it runs
/// after all standard optimizations (predicate pushdown, coalesce batches, etc.)
/// have already been applied.
///
/// Sort-awareness: the rule reads `output_ordering()` from plan nodes to
/// choose between ordered and unordered GPU operator variants.
#[derive(Debug)]
pub struct HostToGpuRule;

impl PhysicalOptimizerRule for HostToGpuRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| try_replace_node(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "HostToGpuRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace a single plan node with a GPU-accelerated equivalent.
///
/// Currently handles:
/// - `FilterExec` → `GpuFilterExec` (simple Column CMP Literal)
/// - `AggregateExec` → `GpuAdjacentDistinctExec` (DISTINCT on sorted data)
/// - `AggregateExec` → `GpuHashAggregateExec` (numeric group-by + simple aggs, or DISTINCT)
/// - `HashJoinExec` → `GpuHashJoinExec` (single numeric key equi-join)
/// - `SortExec` → `GpuSortExec` (numeric sort keys)
fn try_replace_node(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // Try filter replacement first
    if let Some(result) = try_replace_filter(&plan)? {
        return Ok(result);
    }

    // Try adjacent distinct replacement (sorted DISTINCT → O(N) adjacent dedup).
    // Must be checked BEFORE try_replace_aggregate since both match AggregateExec,
    // but adjacent distinct is cheaper when the input is already sorted.
    if let Some(result) = try_replace_distinct(&plan)? {
        return Ok(result);
    }

    // Try aggregate replacement
    if let Some(result) = try_replace_aggregate(&plan)? {
        return Ok(result);
    }

    // Try join replacement
    if let Some(result) = try_replace_join(&plan)? {
        return Ok(result);
    }

    // Try sort replacement
    if let Some(result) = try_replace_sort(&plan)? {
        return Ok(result);
    }

    Ok(Transformed::no(plan))
}

/// Attempt to replace a `FilterExec` with `GpuFilterExec`.
fn try_replace_filter(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(filter_exec) = plan.as_any().downcast_ref::<FilterExec>() else {
        return Ok(None);
    };

    let input = filter_exec.input();
    let schema = input.schema();
    let predicate = filter_exec.predicate();

    let Some(gpu_pred) = GpuCompoundPredicate::try_from_physical_expr(predicate, &schema) else {
        return Ok(None);
    };

    // Check row count statistics — skip GPU for small datasets
    if let Ok(stats) = input.partition_statistics(None) {
        if let Some(num_rows) = stats.num_rows.get_value() {
            if *num_rows < GPU_MIN_ROWS_THRESHOLD {
                return Ok(None);
            }
        }
    }

    // Log sort metadata for debugging
    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        let sort_cols = extract_sort_columns(input);
        if sort_cols.is_empty() {
            eprintln!("[GPU] HostToGpuRule: FilterExec input is unsorted");
        } else {
            eprintln!(
                "[GPU] HostToGpuRule: FilterExec input sorted by [{}]",
                sort_cols.join(", ")
            );
        }
        eprintln!(
            "[GPU] HostToGpuRule: FilterExec → GpuFilterExec, predicate={}",
            gpu_pred.display_summary()
        );
    }

    let gpu_filter =
        GpuFilterExec::try_new_compound(gpu_pred, Arc::clone(predicate), Arc::clone(input))?;
    Ok(Some(Transformed::yes(Arc::new(gpu_filter))))
}

/// Attempt to replace a DISTINCT `AggregateExec` with `GpuAdjacentDistinctExec`.
///
/// This handler specifically targets DISTINCT operations (AggregateExec with
/// group-by keys and zero aggregate functions, or all-FIRST_VALUE aggregates)
/// when the input is already sorted by the key columns. In that case, O(N)
/// adjacent deduplication is much cheaper than hash-based deduplication.
///
/// Eligibility:
/// - Node is `AggregateExec`
/// - Mode is `Single` (no two-phase aggregation for sorted distinct)
/// - No GROUPING SETS
/// - All group-by keys are simple `Column` references
/// - Either zero aggregate functions (all-column DISTINCT) or all aggregates
///   are `FIRST_VALUE` (key-column DISTINCT)
/// - Input is sorted by ALL group-by key columns
/// - Row count ≥ GPU_MIN_ROWS_THRESHOLD
///
/// If the input is NOT sorted, returns `None` so `try_replace_aggregate` can
/// handle it via the hash-based `GpuHashAggregateExec` path.
fn try_replace_distinct(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };

    // Only handle Single mode — FinalPartitioned has partial aggregates that
    // don't guarantee sorted order between partitions.
    let mode = agg_exec.mode();
    if *mode != AggregateMode::Single {
        return Ok(None);
    }

    let group_by = agg_exec.group_by();

    // No GROUPING SETS
    if !group_by.is_single() {
        return Ok(None);
    }

    // Must have at least one group key
    if group_by.expr().is_empty() {
        return Ok(None);
    }

    // Check that this is a DISTINCT-shaped aggregate:
    // Either zero aggregates (all-column distinct) or all FIRST_VALUE (key-column distinct)
    let aggr_exprs = agg_exec.aggr_expr();
    let is_all_first_value = !aggr_exprs.is_empty()
        && aggr_exprs
            .iter()
            .all(|e| e.fun().name().to_lowercase() == "first_value");

    if !aggr_exprs.is_empty() && !is_all_first_value {
        return Ok(None); // Not a DISTINCT pattern
    }

    // Extract group key column names and indices
    let input = agg_exec.input();
    let input_schema = input.schema();
    let mut key_columns = Vec::new();

    for (expr, _alias) in group_by.expr() {
        let Some(col) = expr.as_any().downcast_ref::<Column>() else {
            return Ok(None); // Non-simple key expression
        };
        key_columns.push(AdjacentDistinctKey {
            column_index: col.index(),
            column_name: col.name().to_string(),
        });
    }

    // Check if input is sorted by ALL key columns
    let sort_cols = extract_sort_columns(input);
    let all_keys_sorted = key_columns
        .iter()
        .all(|k| sort_cols.iter().any(|sc| sc == &k.column_name));
    if !all_keys_sorted {
        return Ok(None); // Not sorted → fall through to hash aggregate
    }

    // Row count threshold check
    let stats = input.partition_statistics(None)?;
    let estimated_rows = stats.num_rows.value().unwrap_or(0);
    if estimated_rows > 0 && estimated_rows < GPU_MIN_ROWS_THRESHOLD {
        return Ok(None);
    }

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] HostToGpuRule: AggregateExec(DISTINCT) → GpuAdjacentDistinctExec, keys=[{}], first_value_aggs={}",
            key_columns.iter().map(|k| k.column_name.as_str()).collect::<Vec<_>>().join(", "),
            is_all_first_value,
        );
    }

    let config = GpuAdjacentDistinctConfig {
        keys: key_columns,
        is_key_distinct: is_all_first_value,
    };
    let output_schema = agg_exec.schema();
    let gpu_distinct = GpuAdjacentDistinctExec::try_new(config, Arc::clone(input), output_schema)?;
    Ok(Some(Transformed::yes(Arc::new(gpu_distinct))))
}

/// Attempt to replace an `AggregateExec` with `GpuHashAggregateExec`.
///
/// Eligibility criteria:
/// - Mode is `Single` (complete aggregation in one step)
/// - No GROUPING SETS / CUBE / ROLLUP
/// - Group-by keys are simple `Column` references with supported numeric types
/// - Aggregate functions are sum/count/min/max/avg (no DISTINCT, no FILTER, no ORDER BY)
/// - Row count ≥ GPU_MIN_ROWS_THRESHOLD
/// - Supports multi-key group-by and empty aggregates (DISTINCT case)
fn try_replace_aggregate(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] try_replace_aggregate: mode={:?}, groups={}, aggs={}",
            agg_exec.mode(),
            agg_exec.group_expr().expr().len(),
            agg_exec.aggr_expr().len(),
        );
    }

    // Accept Single mode directly, or FinalPartitioned mode (the top half of
    // DataFusion's two-phase aggregation). For FinalPartitioned, we'll dig
    // down through the child tree to find the Partial aggregate's original
    // input and replace the entire chain with a single GPU aggregate.
    let (effective_input, effective_agg) = match agg_exec.mode() {
        AggregateMode::Single => (Arc::clone(agg_exec.input()), agg_exec),
        AggregateMode::FinalPartitioned | AggregateMode::Final => {
            // Walk down through the child tree to find the Partial aggregate.
            // The typical plan shape is:
            //   FinalPartitioned(AggregateExec)
            //     └─ CoalesceBatches / RepartitionExec / ...
            //         └─ Partial(AggregateExec)
            //             └─ actual input
            match find_partial_aggregate_input(agg_exec.input()) {
                Some(input) => (input, agg_exec),
                None => {
                    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                        eprintln!(
                            "[GPU] try_replace_aggregate: SKIP — could not find Partial child"
                        );
                    }
                    return Ok(None);
                }
            }
        }
        _ => {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_aggregate: SKIP — unsupported mode {:?}",
                    agg_exec.mode()
                );
            }
            return Ok(None);
        }
    };

    let group_by = effective_agg.group_expr();

    // No GROUPING SETS
    if !group_by.is_single() {
        return Ok(None);
    }

    // Must have at least one group key (otherwise it's a global aggregate)
    if group_by.expr().is_empty() {
        return Ok(None);
    }

    // Extract group keys — must be simple Column refs with supported types
    let input_schema = effective_input.schema();
    let mut group_keys = Vec::new();

    for (expr, _alias) in group_by.expr() {
        let Some(col) = expr.as_any().downcast_ref::<Column>() else {
            return Ok(None); // Not a simple column reference
        };
        let data_type = input_schema.field(col.index()).data_type().clone();
        if !is_gpu_agg_supported_type(&data_type) {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_aggregate: SKIP — unsupported group key type: {:?}",
                    data_type
                );
            }
            return Ok(None);
        }
        group_keys.push(GpuGroupKey {
            column_index: col.index(),
            column_name: col.name().to_string(),
            data_type,
        });
    }

    // Extract aggregate functions — must be simple sum/count/min/max/avg
    // For DISTINCT, aggr_exprs is empty and this loop is a no-op.
    let mut agg_requests = Vec::new();
    let aggr_exprs = effective_agg.aggr_expr();
    let filter_exprs = effective_agg.filter_expr();

    for (i, agg_expr) in aggr_exprs.iter().enumerate() {
        // No per-aggregate FILTER
        if filter_exprs.get(i).is_some_and(|f| f.is_some()) {
            return Ok(None);
        }

        // No DISTINCT
        if agg_expr.is_distinct() {
            return Ok(None);
        }

        // No ORDER BY on aggregates
        if !agg_expr.order_bys().is_empty() {
            return Ok(None);
        }

        // Match function name to SegAggFunc
        let func_name = agg_expr.fun().name().to_lowercase();
        let seg_func = match func_name.as_str() {
            "count" => SegAggFunc::Count,
            "sum" => SegAggFunc::Sum,
            "min" => SegAggFunc::Min,
            "max" => SegAggFunc::Max,
            "avg" => SegAggFunc::Avg,
            "first_value" => SegAggFunc::FirstValue,
            _ => return Ok(None), // Unsupported aggregate function
        };

        // For count, we don't need a specific input column
        // For others, extract the input column
        let (column_index, data_type) = if seg_func == SegAggFunc::Count {
            (0, datafusion::arrow::datatypes::DataType::Int64)
        } else {
            let input_exprs = agg_expr.expressions();
            if input_exprs.len() != 1 {
                return Ok(None);
            }
            let Some(col) = input_exprs[0].as_any().downcast_ref::<Column>() else {
                return Ok(None);
            };
            let dt = input_schema.field(col.index()).data_type().clone();
            if !is_gpu_agg_supported_type(&dt) {
                return Ok(None);
            }
            (col.index(), dt)
        };

        agg_requests.push(GpuAggRequest {
            func: seg_func,
            column_index,
            data_type,
            output_name: agg_expr.name().to_string(),
        });
    }

    // Check row count statistics
    if let Ok(stats) = effective_input.partition_statistics(None) {
        if let Some(num_rows) = stats.num_rows.get_value() {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!("[GPU] try_replace_aggregate: row count stat = {}", num_rows);
            }
            if *num_rows < GPU_MIN_ROWS_THRESHOLD {
                if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                    eprintln!("[GPU] try_replace_aggregate: SKIP — below threshold");
                }
                return Ok(None);
            }
        }
    }

    // Check if input is sorted by group key(s)
    // For multi-key, we check if sorted by the first key only — this enables
    // the sorted fast path (skip sort step) when at least the primary key is ordered.
    let input_sorted = is_sorted_by(&effective_input, &group_keys[0].column_name);

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] HostToGpuRule: AggregateExec → GpuHashAggregateExec, keys=[{}], aggs={}, sorted={}",
            group_keys.iter().map(|k| k.column_name.as_str()).collect::<Vec<_>>().join(", "),
            agg_requests.len(),
            input_sorted,
        );
    }

    let config = GpuHashAggConfig {
        group_keys,
        agg_requests,
        input_sorted,
    };

    // Use the original AggregateExec output schema for transparent substitution
    let output_schema = effective_agg.schema();

    let gpu_agg = GpuHashAggregateExec::try_new(config, effective_input, output_schema)?;
    Ok(Some(Transformed::yes(Arc::new(gpu_agg))))
}

/// Attempt to replace a `HashJoinExec` with `GpuHashJoinExec`.
///
/// Eligibility criteria:
/// - Single equi-join key pair
/// - No non-equi `JoinFilter`
/// - Join type is Inner or Left only
/// - Key columns are simple `Column` references with matching supported numeric types
/// - Max row count of left/right ≥ GPU_MIN_ROWS_THRESHOLD
fn try_replace_join(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };

    // Only single equi-join key
    let on = hash_join.on();
    if on.len() != 1 {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!(
                "[GPU] try_replace_join: SKIP — {} join key pairs (need exactly 1)",
                on.len()
            );
        }
        return Ok(None);
    }

    // No non-equi filter
    if hash_join.filter().is_some() {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] try_replace_join: SKIP — has non-equi JoinFilter");
        }
        return Ok(None);
    }

    // Only Inner, Left, and Right join types
    // Right join is implemented as Left join with swapped inputs.
    let (gpu_join_type, swapped) = match hash_join.join_type() {
        JoinType::Inner => (GpuJoinType::Inner, false),
        JoinType::Left => (GpuJoinType::Left, false),
        JoinType::Right => (GpuJoinType::Left, true),
        other => {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_join: SKIP — unsupported join type {:?}",
                    other
                );
            }
            return Ok(None);
        }
    };

    // Extract key columns — must be simple Column references
    let (left_key_expr, right_key_expr) = &on[0];

    let Some(left_col) = left_key_expr.as_any().downcast_ref::<Column>() else {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] try_replace_join: SKIP — left key is not a Column ref");
        }
        return Ok(None);
    };

    let Some(right_col) = right_key_expr.as_any().downcast_ref::<Column>() else {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] try_replace_join: SKIP — right key is not a Column ref");
        }
        return Ok(None);
    };

    // Check key types: must be supported and matching
    let left_input = hash_join.left();
    let right_input = hash_join.right();
    let left_schema = left_input.schema();
    let right_schema = right_input.schema();

    let left_dt = left_schema.field(left_col.index()).data_type().clone();
    let right_dt = right_schema.field(right_col.index()).data_type().clone();

    if left_dt != right_dt {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!(
                "[GPU] try_replace_join: SKIP — key type mismatch: {:?} vs {:?}",
                left_dt, right_dt
            );
        }
        return Ok(None);
    }

    if !is_hash_join_key_type_supported(&left_dt) {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!(
                "[GPU] try_replace_join: SKIP — unsupported key type {:?}",
                left_dt
            );
        }
        return Ok(None);
    }

    // Check row count statistics — use max of left/right
    let mut max_rows: Option<usize> = None;
    if let Ok(stats) = left_input.partition_statistics(None) {
        if let Some(n) = stats.num_rows.get_value() {
            max_rows = Some(*n);
        }
    }
    if let Ok(stats) = right_input.partition_statistics(None) {
        if let Some(n) = stats.num_rows.get_value() {
            max_rows = Some(max_rows.map_or(*n, |prev| prev.max(*n)));
        }
    }
    if let Some(rows) = max_rows {
        if rows < GPU_MIN_ROWS_THRESHOLD {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_join: SKIP — max row count {} below threshold {}",
                    rows, GPU_MIN_ROWS_THRESHOLD
                );
            }
            return Ok(None);
        }
    }

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] HostToGpuRule: HashJoinExec → GpuHashJoinExec, left_key={}, right_key={}, type={:?}, key_type={:?}, swapped={}",
            left_col.name(), right_col.name(), gpu_join_type, left_dt, swapped
        );
    }

    // When swapped (Right join → Left join), swap left/right inputs and key info.
    // The GpuHashJoinExec always treats its `left` as probe and `right` as build.
    let (
        exec_left,
        exec_right,
        exec_left_key,
        exec_left_key_idx,
        exec_right_key,
        exec_right_key_idx,
    ) = if swapped {
        (
            Arc::clone(right_input),
            Arc::clone(left_input),
            right_col.name().to_string(),
            right_col.index(),
            left_col.name().to_string(),
            left_col.index(),
        )
    } else {
        (
            Arc::clone(left_input),
            Arc::clone(right_input),
            left_col.name().to_string(),
            left_col.index(),
            right_col.name().to_string(),
            right_col.index(),
        )
    };

    let config = GpuHashJoinConfig {
        left_key: exec_left_key,
        left_key_idx: exec_left_key_idx,
        right_key: exec_right_key,
        right_key_idx: exec_right_key_idx,
        key_type: left_dt,
        join_type: gpu_join_type,
        swapped,
    };

    // Reuse the original HashJoinExec schema for transparent substitution
    let output_schema = hash_join.schema();

    let gpu_join = GpuHashJoinExec::try_new(config, exec_left, exec_right, output_schema)?;

    Ok(Some(Transformed::yes(Arc::new(gpu_join))))
}

/// Attempt to replace a `SortExec` with `GpuSortExec`.
///
/// Eligibility criteria:
/// - All sort key expressions are simple `Column` references
/// - All sort key column types are supported (i32, i64, f32, f64)
/// - Row count ≥ GPU_MIN_ROWS_THRESHOLD
/// - No `preserve_partitioning` (GPU sort always produces a single partition)
fn try_replace_sort(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() else {
        return Ok(None);
    };

    // Skip if preserve_partitioning is true — GPU sort always produces 1 partition
    if sort_exec.preserve_partitioning() {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] try_replace_sort: SKIP — preserve_partitioning=true");
        }
        return Ok(None);
    }

    let input = sort_exec.input();
    let input_schema = input.schema();
    let sort_exprs = sort_exec.expr();

    if sort_exprs.is_empty() {
        return Ok(None);
    }

    // Extract sort keys — all must be simple Column refs with supported types
    let mut sort_keys = Vec::new();
    let mut physical_sort_exprs = Vec::new();

    for sort_expr in sort_exprs.iter() {
        let Some(col) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!("[GPU] try_replace_sort: SKIP — sort key is not a Column ref");
            }
            return Ok(None);
        };

        let data_type = input_schema.field(col.index()).data_type().clone();
        if !is_sort_key_type_supported(&data_type) {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_sort: SKIP — unsupported sort key type: {:?}",
                    data_type
                );
            }
            return Ok(None);
        }

        sort_keys.push(GpuSortKey {
            column_index: col.index(),
            column_name: col.name().to_string(),
            data_type,
            descending: sort_expr.options.descending,
            nulls_first: sort_expr.options.nulls_first,
        });

        physical_sort_exprs.push(sort_expr.clone());
    }

    // Check row count statistics
    if let Ok(stats) = input.partition_statistics(None) {
        if let Some(num_rows) = stats.num_rows.get_value() {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!("[GPU] try_replace_sort: row count stat = {}", num_rows);
            }
            if *num_rows < GPU_MIN_ROWS_THRESHOLD {
                if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                    eprintln!("[GPU] try_replace_sort: SKIP — below threshold");
                }
                return Ok(None);
            }
        }
    }

    let fetch = sort_exec.fetch();

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        let key_strs: Vec<String> = sort_keys
            .iter()
            .map(|k| {
                let dir = if k.descending { "DESC" } else { "ASC" };
                format!("{}:{}", k.column_name, dir)
            })
            .collect();
        eprintln!(
            "[GPU] HostToGpuRule: SortExec → GpuSortExec, keys=[{}], fetch={:?}",
            key_strs.join(", "),
            fetch
        );
    }

    let config = GpuSortConfig { sort_keys, fetch };
    let output_schema = sort_exec.schema();

    let gpu_sort = GpuSortExec::try_new(
        config,
        Arc::clone(input),
        output_schema,
        physical_sort_exprs,
    )?;

    Ok(Some(Transformed::yes(Arc::new(gpu_sort))))
}

/// Walk down the plan tree from a FinalPartitioned aggregate's child to find
/// the Partial aggregate's input. The typical chain is:
///   CoalesceBatches → RepartitionExec → Partial(AggregateExec)
/// but there may be other intermediate nodes.
fn find_partial_aggregate_input(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    // Check if this node is a Partial aggregate
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Some(Arc::clone(agg.input()));
        }
    }

    // Recurse into single-child nodes (CoalesceBatches, Repartition, etc.)
    let children = plan.children();
    if children.len() == 1 {
        return find_partial_aggregate_input(&Arc::clone(children[0]));
    }

    None
}

/// Check if a data type is supported for GPU aggregate operations.
fn is_gpu_agg_supported_type(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    matches!(
        dt,
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
    )
}
