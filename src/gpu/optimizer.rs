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
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::ExecutionPlan;

use super::adjacent_distinct::{
    AdjacentDistinctKey, GpuAdjacentDistinctConfig, GpuAdjacentDistinctExec,
};
use super::filter_exec::{GpuCompoundPredicate, GpuFilterExec};
use super::hash_join::{is_hash_join_key_type_supported, GpuHashJoinConfig, GpuHashJoinExec};
use super::hash_ops::{GpuAggRequest, GpuGroupKey, GpuHashAggConfig, GpuHashAggregateExec};
use super::merge_join::GpuJoinType;
use super::ordered_ops::SegAggFunc;
use super::sort_aware::{extract_sort_columns, input_already_sorted, is_sorted_by};
use super::sort_exec::{is_sort_key_type_supported, GpuSortConfig, GpuSortExec, GpuSortKey};
use super::window_ops::{
    is_window_gpu_supported_type, GpuWindowOp, GpuWindowShiftConfig, GpuWindowShiftExec,
    RollingAggFunc,
};
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
/// - `BoundedWindowAggExec` / `WindowAggExec` → `GpuWindowShiftExec` (lag/lead/diff)
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

    // Try window replacement (lag/lead/diff → GPU shift)
    if let Some(result) = try_replace_window(&plan)? {
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

    // Shuffle elimination: if input is already sorted by the requested keys,
    // eliminate the sort entirely. This avoids unnecessary repartition and
    // re-sorting when data is already in the desired order (e.g., pre-sorted
    // Parquet files, or output of a previous sort that covers this sort's keys).
    let input = sort_exec.input();
    let sort_exprs = sort_exec.expr();

    if !sort_exprs.is_empty() && input_already_sorted(input, sort_exprs) {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!(
                "[GPU] try_replace_sort: ELIMINATED — input already sorted by requested keys"
            );
        }
        return Ok(Some(Transformed::yes(Arc::clone(input))));
    }

    // Skip if preserve_partitioning is true — GPU sort always produces 1 partition
    if sort_exec.preserve_partitioning() {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!("[GPU] try_replace_sort: SKIP — preserve_partitioning=true");
        }
        return Ok(None);
    }

    let input_schema = input.schema();

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

// ============================================================================
// Window replacement (lag/lead/diff → GpuWindowShiftExec)
// ============================================================================

use datafusion::physical_expr::window::WindowExpr;

/// Attempt to replace a `BoundedWindowAggExec` or `WindowAggExec` with
/// `GpuWindowShiftExec` when all window expressions are simple lag/lead
/// operations on numeric columns without PARTITION BY.
///
/// Eligibility:
/// - Node is `BoundedWindowAggExec` or `WindowAggExec`
/// - ALL window expressions are lag or lead (identified by `name()` prefix)
/// - Source columns are numeric (i32/i64/f32/f64)
/// - No PARTITION BY (whole-table window)
/// - Row count ≥ GPU_MIN_ROWS_THRESHOLD
///
/// Note: diff() is expressed by DataFusion as `BinaryExpr(col, Minus, lag(col))`
/// at the physical level. Since the window function part is just a lag, the
/// GPU optimizer replaces the lag portion; DataFusion handles the subtraction
/// on CPU. For pure diff operations, the GpuWindowShiftExec handles both the
/// shift and the subtraction in a single GPU kernel pass — but this is only
/// used when the optimizer detects the diff pattern directly.
fn try_replace_window(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    // Try BoundedWindowAggExec first (more common for lag/lead)
    if let Some(bounded) = plan.as_any().downcast_ref::<BoundedWindowAggExec>() {
        return try_replace_window_inner(bounded.window_expr(), bounded.input(), plan);
    }

    // Also try WindowAggExec (for unbounded window frames)
    if let Some(window) = plan.as_any().downcast_ref::<WindowAggExec>() {
        return try_replace_window_inner(window.window_expr(), window.input(), plan);
    }

    Ok(None)
}

/// Inner implementation for window replacement that works with either
/// `BoundedWindowAggExec` or `WindowAggExec`.
fn try_replace_window_inner(
    window_exprs: &[Arc<dyn WindowExpr>],
    input: &Arc<dyn ExecutionPlan>,
    _plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Transformed<Arc<dyn ExecutionPlan>>>> {
    if window_exprs.is_empty() {
        return Ok(None);
    }

    let input_schema = input.schema();
    let mut ops = Vec::new();

    for w_expr in window_exprs {
        // Skip if window has PARTITION BY (not yet supported on GPU)
        if !w_expr.partition_by().is_empty() {
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_window: SKIP — has PARTITION BY (name={})",
                    w_expr.name()
                );
            }
            return Ok(None);
        }

        // Parse the window expression name to detect lag/lead, rolling aggregate,
        // or cumulative sum
        let name = w_expr.name();
        if let Some(op) = parse_lag_lead_expr(name, w_expr, &input_schema)? {
            ops.push(op);
        } else if let Some(op) = parse_rolling_agg_expr(name, w_expr, &input_schema)? {
            ops.push(op);
        } else if let Some(op) = parse_cumulative_sum_expr(name, w_expr, &input_schema)? {
            ops.push(op);
        } else {
            // Not a supported window function → bail out
            if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                eprintln!(
                    "[GPU] try_replace_window: SKIP — unsupported window function: {}",
                    name
                );
            }
            return Ok(None);
        }
    }

    // Check row count threshold
    if let Ok(stats) = input.partition_statistics(None) {
        if let Some(num_rows) = stats.num_rows.get_value() {
            if *num_rows < GPU_MIN_ROWS_THRESHOLD {
                if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                    eprintln!(
                        "[GPU] try_replace_window: SKIP — below threshold ({})",
                        num_rows
                    );
                }
                return Ok(None);
            }
        }
    }

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        let op_strs: Vec<String> = ops
            .iter()
            .map(|op| match op {
                GpuWindowOp::Shift {
                    column_name,
                    offset,
                    output_name,
                    ..
                } => {
                    format!("{}=shift({}, {})", output_name, column_name, offset)
                }
                GpuWindowOp::Diff {
                    column_name,
                    period,
                    output_name,
                    ..
                } => {
                    format!("{}=diff({}, {})", output_name, column_name, period)
                }
                GpuWindowOp::Rolling {
                    column_name,
                    window_size,
                    agg_func,
                    output_name,
                    ..
                } => {
                    format!(
                        "{}=rolling_{}({}, {})",
                        output_name, agg_func, column_name, window_size
                    )
                }
                GpuWindowOp::CumulativeSum {
                    column_name,
                    output_name,
                    ..
                } => {
                    format!("{}=cum_sum({})", output_name, column_name)
                }
            })
            .collect();
        eprintln!(
            "[GPU] HostToGpuRule: WindowExec → GpuWindowShiftExec, ops=[{}]",
            op_strs.join(", ")
        );
    }

    let config = GpuWindowShiftConfig { ops };
    let gpu_window = GpuWindowShiftExec::try_new(config, Arc::clone(input))?;
    Ok(Some(Transformed::yes(Arc::new(gpu_window))))
}

/// Parse a window expression name like `"lag(price,1,NULL)"` or `"lead(vol,2)"`
/// and extract the GPU window operation.
///
/// Returns `None` if the expression is not a supported lag/lead.
fn parse_lag_lead_expr(
    name: &str,
    w_expr: &Arc<dyn WindowExpr>,
    input_schema: &SchemaRef,
) -> Result<Option<GpuWindowOp>> {
    use datafusion::arrow::datatypes::SchemaRef;

    let name_lower = name.to_lowercase();

    // Match lag(...) or lead(...)
    let (is_lag, args_str) = if let Some(rest) = name_lower.strip_prefix("lag(") {
        (true, rest.trim_end_matches(')'))
    } else if let Some(rest) = name_lower.strip_prefix("lead(") {
        (false, rest.trim_end_matches(')'))
    } else {
        return Ok(None);
    };

    // Parse arguments: first arg is the column reference, second (optional) is offset
    let args: Vec<&str> = args_str.split(',').collect();
    if args.is_empty() {
        return Ok(None);
    }

    // First argument: column name (may have table qualifier like "t1.price")
    let col_ref = args[0].trim();
    // Extract just the column name (last part after any dots)
    let col_name = col_ref.rsplit('.').next().unwrap_or(col_ref);

    // Find the column index in the input schema
    let col_idx = input_schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name);
    let col_idx = match col_idx {
        Some(idx) => idx,
        None => return Ok(None), // Column not found
    };

    let data_type = input_schema.field(col_idx).data_type().clone();
    if !is_window_gpu_supported_type(&data_type) {
        return Ok(None);
    }

    // Second argument: offset (default 1 for lag, -1 for lead)
    let raw_offset = if args.len() > 1 {
        args[1].trim().parse::<i32>().unwrap_or(1)
    } else {
        1
    };

    // For lag, offset is positive (look backward).
    // For lead, offset is negative (look forward).
    let offset = if is_lag { raw_offset } else { -raw_offset };

    // Output column name — use the full expression name from DataFusion
    let output_name = w_expr.field()?.name().to_string();

    Ok(Some(GpuWindowOp::Shift {
        column_index: col_idx,
        column_name: col_name.to_string(),
        offset,
        output_name,
        data_type,
    }))
}

/// Parse a window expression name like `"avg(price)"`, `"sum(volume)"`, etc.
/// and check that it has a bounded ROWS frame suitable for rolling aggregation.
///
/// Returns `None` if the expression is not a supported rolling aggregate.
///
/// Recognized functions: sum, avg, min, max, count.
/// Required frame: `ROWS BETWEEN <n> PRECEDING AND CURRENT ROW`.
fn parse_rolling_agg_expr(
    name: &str,
    w_expr: &Arc<dyn WindowExpr>,
    input_schema: &SchemaRef,
) -> Result<Option<GpuWindowOp>> {
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};

    let name_lower = name.to_lowercase();

    // Match function_name(column_name) pattern
    // Supported: sum(...), avg(...), min(...), max(...), count(...)
    let (agg_func, args_str) = if let Some(rest) = name_lower.strip_prefix("sum(") {
        (RollingAggFunc::Sum, rest.trim_end_matches(')'))
    } else if let Some(rest) = name_lower.strip_prefix("avg(") {
        (RollingAggFunc::Avg, rest.trim_end_matches(')'))
    } else if let Some(rest) = name_lower.strip_prefix("min(") {
        (RollingAggFunc::Min, rest.trim_end_matches(')'))
    } else if let Some(rest) = name_lower.strip_prefix("max(") {
        (RollingAggFunc::Max, rest.trim_end_matches(')'))
    } else if let Some(rest) = name_lower.strip_prefix("count(") {
        (RollingAggFunc::Count, rest.trim_end_matches(')'))
    } else {
        return Ok(None);
    };

    // Check the window frame — must be ROWS BETWEEN <n> PRECEDING AND CURRENT ROW
    let frame = w_expr.get_window_frame();
    if frame.units != WindowFrameUnits::Rows {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!(
                "[GPU] parse_rolling_agg_expr: SKIP — frame units {:?} (need ROWS)",
                frame.units
            );
        }
        return Ok(None);
    }

    // End bound must be CURRENT ROW
    if frame.end_bound != WindowFrameBound::CurrentRow {
        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
            eprintln!(
                "[GPU] parse_rolling_agg_expr: SKIP — end bound {:?} (need CURRENT ROW)",
                frame.end_bound
            );
        }
        return Ok(None);
    }

    // Start bound must be <n> PRECEDING (not UNBOUNDED)
    let preceding_n = match &frame.start_bound {
        WindowFrameBound::Preceding(sv) => {
            if sv.is_null() {
                // UNBOUNDED PRECEDING — not a rolling window, it's a cumulative op
                return Ok(None);
            }
            match sv {
                ScalarValue::UInt64(Some(v)) => *v as usize,
                ScalarValue::Int64(Some(v)) => *v as usize,
                ScalarValue::UInt32(Some(v)) => *v as usize,
                ScalarValue::Int32(Some(v)) => *v as usize,
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };

    // window_size = preceding_n + 1 (frame includes current row)
    let window_size = preceding_n + 1;

    // Parse the column name from the function arguments
    let col_ref = args_str.trim();
    // Extract just the column name (last part after any dots)
    let col_name = col_ref.rsplit('.').next().unwrap_or(col_ref);

    // Find the column index in the input schema
    let col_idx = input_schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name);
    let col_idx = match col_idx {
        Some(idx) => idx,
        None => return Ok(None),
    };

    let data_type = input_schema.field(col_idx).data_type().clone();
    if !is_window_gpu_supported_type(&data_type) {
        return Ok(None);
    }

    // Output column name — use the full expression name from DataFusion
    let output_name = w_expr.field()?.name().to_string();

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] parse_rolling_agg_expr: MATCHED — {}({}) window_size={} → {}",
            agg_func, col_name, window_size, output_name
        );
    }

    Ok(Some(GpuWindowOp::Rolling {
        column_index: col_idx,
        column_name: col_name.to_string(),
        window_size,
        agg_func,
        output_name,
        data_type,
    }))
}

/// Parse a window expression like `"sum(col)"` with an UNBOUNDED PRECEDING frame
/// as a cumulative sum operation.
///
/// This is distinguished from rolling sum by the start bound:
/// - Rolling sum: `ROWS BETWEEN <n> PRECEDING AND CURRENT ROW` (bounded)
/// - Cumulative sum: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (unbounded)
///
/// Only `sum(col)` is supported (not avg/min/max/count — those with unbounded
/// frames are different operations not yet GPU-accelerated).
///
/// Returns `None` if the expression is not a cumulative sum.
fn parse_cumulative_sum_expr(
    name: &str,
    w_expr: &Arc<dyn WindowExpr>,
    input_schema: &SchemaRef,
) -> Result<Option<GpuWindowOp>> {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};

    let name_lower = name.to_lowercase();

    // Only match sum(col) — cumulative sum
    let args_str = if let Some(rest) = name_lower.strip_prefix("sum(") {
        rest.trim_end_matches(')')
    } else {
        return Ok(None);
    };

    // Check the window frame — must be ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    let frame = w_expr.get_window_frame();
    if frame.units != WindowFrameUnits::Rows {
        return Ok(None);
    }

    // End bound must be CURRENT ROW
    if frame.end_bound != WindowFrameBound::CurrentRow {
        return Ok(None);
    }

    // Start bound must be UNBOUNDED PRECEDING (Preceding(ScalarValue::Null))
    match &frame.start_bound {
        WindowFrameBound::Preceding(sv) => {
            if !sv.is_null() {
                // Bounded preceding — that's a rolling sum, not cumulative
                return Ok(None);
            }
        }
        _ => return Ok(None),
    }

    // Parse the column name
    let col_ref = args_str.trim();
    let col_name = col_ref.rsplit('.').next().unwrap_or(col_ref);

    // Find the column index in the input schema
    let col_idx = input_schema
        .fields()
        .iter()
        .position(|f| f.name() == col_name);
    let col_idx = match col_idx {
        Some(idx) => idx,
        None => return Ok(None),
    };

    let data_type = input_schema.field(col_idx).data_type().clone();
    if !is_window_gpu_supported_type(&data_type) {
        return Ok(None);
    }

    let output_name = w_expr.field()?.name().to_string();

    if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
        eprintln!(
            "[GPU] parse_cumulative_sum_expr: MATCHED — sum({}) UNBOUNDED → {}",
            col_name, output_name
        );
    }

    Ok(Some(GpuWindowOp::CumulativeSum {
        column_index: col_idx,
        column_name: col_name.to_string(),
        output_name,
        data_type,
    }))
}
