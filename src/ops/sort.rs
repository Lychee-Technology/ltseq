//! Sort operation for LTSeqTable
//!
//! `sort` orders rows by key columns in the order of contract §9.4 (#268):
//! stable, NULL placed by `nulls_last` in either direction, and floats by
//! their `order_key`, under which every NaN is above `+inf` and `-0.0`
//! equals `0.0`. `assume_sorted` declares such an order without sorting.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::gil::detached;
use crate::metadata::{sort_specs_to_file_sort_order, SortSpec};
use crate::order_key::{float_order_key, order_key, ORDER_KEY_NAME};
use crate::transpiler::{pyexpr_to_datafusion, Resolver};
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{
    Expr, LogicalPlan, Projection, SortExpr, TableProviderFilterPushDown,
};
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::{execute_stream_partitioned, ExecutionPlan};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures_util::lock::Mutex;
use futures_util::{stream, StreamExt, TryStreamExt};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Sort `df` by `order` unless its plan already ends in that order: a sort
/// whose keys start with `order`, under nothing but limits and projections
/// that only pick or rename columns.
///
/// Such a sort would be redundant. A slice is where #311 met one: DataFusion
/// 55 planned it beneath the slice's offset, which `KeepSortsAboveOffsets`
/// and `slice` now prevent for any sort (#339). The optimizer removes a
/// projection that keeps every column, so `sort("a").tail(5).select(...)`
/// ends in the slice's order as well.
pub(crate) fn sort_unless_ordered(
    df: DataFrame,
    order: Vec<SortExpr>,
) -> datafusion::error::Result<DataFrame> {
    if ends_in_order(df.logical_plan(), &order) {
        return Ok(df);
    }
    df.sort(order)
}

fn ends_in_order(plan: &LogicalPlan, order: &[SortExpr]) -> bool {
    match plan {
        LogicalPlan::Limit(limit) => ends_in_order(&limit.input, order),
        LogicalPlan::Projection(projection) => through_projection(projection, order)
            .is_some_and(|order| ends_in_order(&projection.input, &order)),
        LogicalPlan::Sort(sort) => {
            sort.expr.len() >= order.len()
                && sort
                    .expr
                    .iter()
                    .zip(order)
                    .all(|(have, want)| same_key(have, want))
        }
        _ => false,
    }
}

/// A sort key that is a column or a float column's `order_key`: the column,
/// and whether it is wrapped.
fn key_column(expr: &Expr) -> Option<(&Column, bool)> {
    match expr {
        Expr::Column(column) => Some((column, false)),
        Expr::ScalarFunction(call) if call.name() == ORDER_KEY_NAME => match call.args.as_slice() {
            [Expr::Column(column)] => Some((column, true)),
            _ => None,
        },
        _ => None,
    }
}

/// `order` in the column names of `projection`'s input, when the projection
/// only picks or renames columns and outputs each key once.
///
/// A projection that computes a column is not transparent: DataFusion may
/// repartition it, which loses the order beneath (#311).
fn through_projection(projection: &Projection, order: &[SortExpr]) -> Option<Vec<SortExpr>> {
    let sources = projection
        .expr
        .iter()
        .map(|expr| match expr {
            Expr::Alias(alias) => alias.expr.try_as_col(),
            expr => expr.try_as_col(),
        })
        .collect::<Option<Vec<&Column>>>()?;
    order
        .iter()
        .map(|want| {
            let (key, float) = key_column(&want.expr)?;
            let mut found = projection
                .schema
                .fields()
                .iter()
                .zip(&sources)
                .filter(|(field, _)| field.name() == &key.name)
                .map(|(_, source)| *source);
            let source = found.next()?;
            if found.next().is_some() {
                return None;
            }
            let source = Expr::Column(source.clone());
            Some(SortExpr {
                expr: if float { float_order_key(source) } else { source },
                ..want.clone()
            })
        })
        .collect()
}

/// The plan's sort keys are normalized to qualified columns; the ones built
/// from `SortSpec`s are not.
fn same_key(have: &SortExpr, want: &SortExpr) -> bool {
    let same_expr = match (key_column(&have.expr), key_column(&want.expr)) {
        (Some((have, have_float)), Some((want, want_float))) => {
            have.name == want.name && have_float == want_float
        }
        _ => have.expr == want.expr,
    };
    same_expr && have.asc == want.asc && have.nulls_first == want.nulls_first
}

/// Whether rows in `have`'s order are already in `want`'s: `want` is a
/// prefix of `have`. A stable sort by such keys leaves the rows in place.
fn satisfies(have: &[SortSpec], want: &[SortSpec]) -> bool {
    !want.is_empty() && have.len() >= want.len() && have.iter().zip(want).all(|(h, w)| h == w)
}

/// The flag of key `i` from one flag for every key, or one per key.
fn flag(flags: &[bool], i: usize, default: bool) -> bool {
    match flags {
        [one] => *one,
        flags => flags.get(i).copied().unwrap_or(default),
    }
}

/// Sort rows by one or more keys, in §9.4's order (#268).
///
/// The sort is stable: rows equal on every key keep their order in the
/// input. DataFusion's sort is not. Arrow sorts each run with an unstable
/// sort, and the merge of sorted partitions breaks ties round-robin, so a
/// sort is read from an input in table order with each row's position in
/// it (`in_table_order_with_position`), and that position is the last key.
/// No two rows then tie, so whichever way DataFusion plans the sort, at any
/// number of partitions, it returns the one stable order.
///
/// A float key sorts by its `order_key`, and NULL goes where `nulls_last`
/// says, in either direction. Column keys become the declared order
/// (`sort_specs`), which carries both, so every read that re-imposes the
/// order, and every window over it, orders the same way.
pub fn sort_impl(
    table: &LTSeqTable,
    sort_exprs: Vec<Bound<'_, PyDict>>,
    desc_flags: Vec<bool>,
    nulls_last_flags: Vec<bool>,
) -> PyResult<LTSeqTable> {
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }

    let schema = table.require_schema()?;

    // Capture full sort specs (column + direction + NULL placement) for
    // downstream window ops
    let mut captured_specs = Vec::new();
    let mut metadata_truncated = false;

    // Deserialize and transpile each sort expression
    let rx = Resolver::new(schema).map_err(LtseqError::Validation)?;
    let mut order = Vec::new();
    for (i, expr_dict) in sort_exprs.iter().enumerate() {
        let py_expr = dict_to_py_expr(expr_dict)?;
        let is_desc = flag(&desc_flags, i, false);
        let nulls_last = flag(&nulls_last_flags, i, true);
        let column = match &py_expr {
            PyExpr::Column(name) => Some(name.clone()),
            _ => None,
        };

        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(LtseqError::Validation)?;
        let data_type = rx.data_type(&df_expr).map_err(LtseqError::Validation)?;

        // Capture column name if this is a simple column reference. A
        // computed key cannot be represented in sort metadata — TRUNCATE
        // there (issue #125): capturing later columns would record a
        // non-prefix of the true order, e.g. sort("a", expr, "c") → [a, c].
        match column {
            Some(name) if !metadata_truncated => {
                captured_specs.push(SortSpec::new(name, is_desc, nulls_last, &data_type))
            }
            Some(_) => {}
            None => metadata_truncated = true,
        }

        order.push(SortExpr {
            expr: order_key(df_expr, &data_type),
            asc: !is_desc,
            nulls_first: !nulls_last,
        });
    }

    // SAFETY: require_schema() validated schema above
    let schema = Arc::clone(table.schema.as_ref().expect("schema validated by require_schema"));

    // Rows in table order that already satisfy every key stay where they
    // are: their table order is the stable sort's result. This keeps a sort
    // by the keys an `assume_sorted` Parquet table declares free.
    if !metadata_truncated && satisfies(&table.sort_specs, &captured_specs) {
        let in_order = table.ordered_df()?;
        let result = LTSeqTable::from_df_with_schema(
            Arc::clone(&table.session),
            in_order,
            schema,
            captured_specs,
            None,
        );
        return Ok(if table.plan_in_order {
            result.with_plan_in_order()
        } else {
            result
        });
    }

    // Under D10a a sort demands its keys on every row of its input, so a
    // filter after it may not skip rows whose key can fail (X4, #273).
    let keys = order.iter().map(|key| key.expr.clone()).collect::<Vec<_>>();
    let keys_can_fail =
        crate::rewrite_guard::can_fail_over(table.require_df()?.logical_plan(), &keys);
    let (positioned, position) = in_table_order_with_position(table, keys_can_fail)?;
    order.push(SortExpr {
        expr: Expr::Column(Column::new_unqualified(&position)),
        asc: true,
        nulls_first: false,
    });
    let columns: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    // Plan building only; nothing executes here.
    let sorted_df = positioned
        .sort(order)
        .and_then(|df| df.select(columns))
        .map_err(|e| LtseqError::Runtime(format!("Sort execution failed: {}", e)))?;

    // Return new LTSeqTable with sorted data and captured sort specs
    // (schema unchanged). The physical sort defines a new row order, so the
    // raw-file fast-path token must not survive: the Parquet file's physical
    // order no longer matches the table's logical order.
    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        sorted_df,
        schema,
        captured_specs,
        None,
    ))
}

/// `table`'s rows in table order, with a `UInt64` column numbering them from
/// 0 in that order, and the name of that column (one the table does not
/// have).
///
/// Table order is what `in_table_order` plans: the declared order, or the
/// order of one partition. A position must count rows in that order, so the
/// input is read as one stream ([`TableOrder`]), which plans
/// `in_table_order(table)` when it is executed and numbers the rows it
/// returns. Planning there keeps `sort` plan-only, and keeps the input's
/// single partition from spreading to the sort and the operators above it,
/// which plan at the session's partition count as usual ([`Hub`]). The
/// cost is that the input's operators run on one partition, as
/// `assume_sorted`'s collection does, and that a plan shows the stream
/// rather than them.
fn in_table_order_with_position(
    table: &LTSeqTable,
    keys_can_fail: bool,
) -> Result<(DataFrame, String), LtseqError> {
    let (state, plan) = in_table_order(table)?.into_parts();
    let input = Arc::clone(plan.schema().inner());
    let position = (0..)
        .map(|i| format!("__ltseq_position_{i}"))
        .find(|name| input.field_with_name(name).is_err())
        .expect("an unbounded range has a free name");
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(&position, DataType::UInt64, false));
    let schema: SchemaRef = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        input.metadata().clone(),
    ));
    let source = TableOrder {
        schema,
        state,
        plan,
        keys_can_fail,
    };
    let df = table
        .session
        .read_table(Arc::new(source))
        .map_err(|e| LtseqError::with_context("Failed to read the table in order", e))?;
    Ok((df, position))
}

/// A table's rows in table order, numbered: the columns of `plan`, then a
/// position column, the last field of `schema`.
///
/// It takes the filters and the projection DataFusion pushes into a scan and
/// applies them to `plan` before numbering, so a filter after a `sort` still
/// reaches the input's own scan (Parquet pruning included). A filter keeps
/// the surviving rows in their order, so numbering after it orders them as
/// numbering before it would. Where a key of the sort above can fail, it
/// takes no filter: the sort demands its keys on every row (D10a, #273).
#[derive(Clone)]
struct TableOrder {
    schema: SchemaRef,
    state: SessionState,
    plan: LogicalPlan,
    keys_can_fail: bool,
}

impl std::fmt::Debug for TableOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableOrder")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl TableOrder {
    fn position(&self) -> usize {
        self.schema.fields().len() - 1
    }
}

/// `expr` with every column unqualified: a filter DataFusion pushes into a
/// scan names the scan's columns, and the input plan has its own
/// qualifiers.
fn unqualified(expr: &Expr) -> datafusion::error::Result<Expr> {
    expr.clone()
        .transform(|e| match e {
            Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                column.name,
            )))),
            e => Ok(Transformed::no(e)),
        })
        .map(|t| t.data)
}

#[async_trait]
impl TableProvider for TableOrder {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        let position = self.schema.field(self.position()).name();
        Ok(filters
            .iter()
            .map(|filter| {
                let names_position = filter
                    .column_refs()
                    .iter()
                    .any(|column| column.name == *position);
                if self.keys_can_fail || names_position || filter.is_volatile() {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Exact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let position = self.position();
        let indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..=position).collect(),
        };
        let mut df = DataFrame::new(self.state.clone(), self.plan.clone());
        for filter in filters {
            df = df.filter(unqualified(filter)?)?;
        }
        // The input columns the output needs, in the order the stream
        // returns them; a projection of no input column still reads one, so
        // that its batches carry their row count.
        let mut read: Vec<usize> = indices.iter().copied().filter(|&i| i < position).collect();
        if read.is_empty() && position > 0 {
            read.push(0);
        }
        let layout = indices
            .iter()
            .map(|&i| read.iter().position(|&r| r == i))
            .collect();
        let columns = read
            .iter()
            .map(|&i| Expr::Column(Column::new_unqualified(self.schema.field(i).name())))
            .collect::<Vec<_>>();
        let (state, plan) = df.select(columns)?.into_parts();
        let schema = Arc::new(self.schema.project(&indices)?);
        let hub = Arc::new(Hub {
            source: Positioned {
                schema: Arc::clone(&schema),
                state,
                plan,
                layout,
            },
            partitions: session.config().target_partitions().max(1),
            current: std::sync::Mutex::new(None),
        });
        let partitions = (0..hub.partitions)
            .map(|index| {
                Arc::new(Share {
                    hub: Arc::clone(&hub),
                    index,
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            Vec::new(),
            false,
            None,
        )?))
    }
}

/// The rows of `plan`, planned and executed under `state` on every
/// execution, its partitions read one after another. Output column `k` is
/// `plan`'s column `layout[k]`, or the row's position among them where
/// `layout[k]` is `None`.
struct Positioned {
    schema: SchemaRef,
    state: SessionState,
    plan: LogicalPlan,
    layout: Vec<Option<usize>>,
}

impl Positioned {
    fn execute(&self) -> SendableRecordBatchStream {
        let state = self.state.clone();
        let plan = self.plan.clone();
        let rows = stream::once(async move {
            let physical = state.create_physical_plan(&plan).await?;
            // An operator with several inputs can return one partition per
            // input, in their order (`in_table_order`): read them in turn.
            let partitions = execute_stream_partitioned(physical, state.task_ctx())?;
            Ok::<_, datafusion::error::DataFusionError>(stream::iter(partitions).flatten())
        })
        .try_flatten();
        let schema = Arc::clone(&self.schema);
        let layout = self.layout.clone();
        let mut next: u64 = 0;
        let numbered = rows.map(move |batch| {
            let batch = batch?;
            let rows = batch.num_rows();
            let start = next;
            next += rows as u64;
            let columns = layout
                .iter()
                .map(|column| match column {
                    Some(j) => Arc::clone(batch.column(*j)),
                    None => Arc::new(UInt64Array::from_iter_values(start..next)) as ArrayRef,
                })
                .collect();
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )?)
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            numbered,
        ))
    }
}

/// One execution's numbered stream, which its partitions take turns to read.
type SharedStream = Arc<Mutex<SendableRecordBatchStream>>;

/// The one numbered stream of an execution, shared by its partitions.
///
/// DataFusion sorts an input of one partition on one thread: it
/// parallelizes a sort only over an input that has several, and drops a
/// repartition above one. So the scan has the session's number of
/// partitions, each taking the next batch of one shared stream when it is
/// ready for one. The rows are numbered before they are shared out, so the
/// position key orders them whichever partition sorts them, and the merge
/// of the sorted partitions gives the one stable order.
struct Hub {
    source: Positioned,
    partitions: usize,
    /// The stream of the execution under way, and which of its partitions
    /// have started. A partition that starts again starts a new execution.
    current: std::sync::Mutex<Option<(SharedStream, Vec<bool>)>>,
}

impl Hub {
    fn stream_for(&self, index: usize) -> SharedStream {
        let mut current = self.current.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((stream, started)) = current.as_mut() {
            if !started[index] {
                started[index] = true;
                return Arc::clone(stream);
            }
        }
        let stream = Arc::new(Mutex::new(self.source.execute()));
        let mut started = vec![false; self.partitions];
        started[index] = true;
        *current = Some((Arc::clone(&stream), started));
        stream
    }
}

/// Partition `index` of a [`Hub`].
struct Share {
    hub: Arc<Hub>,
    index: usize,
}

impl std::fmt::Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Share")
            .field("schema", &self.hub.source.schema)
            .field("layout", &self.hub.source.layout)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for Share {
    fn schema(&self) -> &SchemaRef {
        &self.hub.source.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let shared = self.hub.stream_for(self.index);
        let batches = stream::unfold(shared, |shared| async move {
            let next = shared.lock().await.next().await;
            next.map(|batch| (batch, shared))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.hub.source.schema),
            batches,
        ))
    }
}

/// `table`'s plan, set to return its rows in table order, in partition
/// order: for `assume_sorted` to collect, and for `sort` to number.
///
/// A declared order is table order (`ordered_df`). A table without one has
/// an order only at one partition: across several, a scan of several files
/// or byte ranges, a split MemTable and a repartitioned filter or projection
/// return their partitions in arrival order, so the declaration would
/// describe rows that are not in it (#148). Planned at one partition, every
/// source is read in file-then-row order. An operator with several inputs
/// can still return one partition per input: a union returns its inputs'
/// partitions in their order.
fn in_table_order(table: &LTSeqTable) -> Result<DataFrame, LtseqError> {
    let df = table.ordered_df()?;
    if !table.sort_specs.is_empty() {
        return Ok(df);
    }
    let (mut state, plan) = df.into_parts();
    state.config_mut().options_mut().execution.target_partitions = 1;
    Ok(DataFrame::new(state, plan))
}

/// Declare sort order metadata without physically sorting the data.
///
/// Use this when the data is already sorted (e.g., pre-sorted Parquet files).
/// Unlike the old implementation that only set metadata, this function now
/// actually communicates the sort order to DataFusion's optimizer so that
/// downstream `.sort()` calls can be elided (no redundant SortExec).
///
/// **Parquet path (lazy):** If the table was loaded from Parquet, builds
/// the scan of the files its reader listed again with
/// `ListingOptions::file_sort_order` set (`source::with_file_sort_order`).
/// This flows through `ListingTable` → `EquivalenceProperties`, enabling
/// the `enforce_sorting` optimizer pass to skip the sort.
///
/// **Non-Parquet fallback (materializing):** Collects the DataFrame into
/// batches in table order (`in_table_order`) and creates a `MemTable` with
/// `with_sort_order()`.
///
/// # Safety
/// The caller is responsible for ensuring the data is actually sorted in the
/// declared order. Incorrect metadata will produce wrong results in window
/// functions and shift operations.
pub fn assume_sorted_impl(
    py: Python<'_>,
    table: &LTSeqTable,
    sort_exprs: Vec<Bound<'_, PyDict>>,
    desc_flags: Vec<bool>,
    nulls_last_flags: Vec<bool>,
) -> PyResult<LTSeqTable> {
    // A never-loaded table yields another never-loaded table
    if table.dataframe.is_none() {
        return Ok(LTSeqTable::unloaded(Arc::clone(&table.session)));
    }

    let (df, schema) = table.require_df_and_schema()?;

    // Capture sort specs (same logic as sort_impl, but skip the actual
    // sort). Truncate at the first computed key — see sort_impl. A float
    // key declares its §9.4 order, `order_key(column)`.
    let mut captured_specs = Vec::new();
    for (i, expr_dict) in sort_exprs.iter().enumerate() {
        let py_expr = dict_to_py_expr(expr_dict)?;

        if let PyExpr::Column(ref col_name) = py_expr {
            captured_specs.push(SortSpec::of_schema(
                col_name.clone(),
                flag(&desc_flags, i, false),
                flag(&nulls_last_flags, i, true),
                schema,
            ));
        } else {
            break;
        }
    }

    // Build DataFusion sort order for the optimizer
    let sort_order = sort_specs_to_file_sort_order(&captured_specs);

    // Parquet path (fully lazy, no materialization): the files the reader
    // listed at its call (#262), with the declared order. It builds a plan
    // and reads nothing, so it keeps the GIL.
    if table.source_parquet_path.is_some() {
        let declared = crate::source::with_file_sort_order(&table.session, df, sort_order.clone())
            .map_err(|e| {
                LtseqError::Runtime(format!("Failed to declare the Parquet sort order: {}", e))
            })?;
        if let Some(result_df) = declared {
            let result_schema = LTSeqTable::schema_from_df(result_df.schema());
            return Ok(LTSeqTable::from_df_with_schema(
                Arc::clone(&table.session),
                result_df,
                result_schema,
                captured_specs,
                table.source_parquet_path.clone(),
            ));
        }
    }

    // Non-Parquet fallback: collect batches and create MemTable with sort
    // order. The declaration is about the rows in their current table order.
    let in_order = in_table_order(table)?;
    let batches = detached(py, || {
        RUNTIME
            .block_on(in_order.collect_partitioned())
            .map(|partitions| partitions.into_iter().flatten().collect::<Vec<_>>())
            .map_err(|e| {
                LtseqError::Runtime(format!("Failed to collect data for assume_sorted: {}", e))
            })
    })?;

    // A filtered-to-empty table must stay queryable (PR #126 review):
    // assume_sorted() is the one-line migration for the declared-order
    // requirement, so it cannot degrade to a schema-only stub that raises
    // "No data loaded" on len()/cum_sum(). Keep a zero-row MemTable.
    let batch_schema = match batches.first() {
        Some(batch) => batch.schema(),
        None => LTSeqTable::schema_from_df(df.schema()),
    };
    let partitions = if batches.is_empty() {
        vec![Vec::new()]
    } else {
        vec![batches]
    };
    let mem_table =
        MemTable::try_new(Arc::clone(&batch_schema), partitions)
            .map_err(|e| {
                LtseqError::Runtime(format!(
                    "Failed to create MemTable: {}",
                    e
                ))
            })?
            .with_sort_order(sort_order);

    let result_df = table
        .session
        .read_table(Arc::new(mem_table))
        .map_err(|e| {
            LtseqError::Runtime(format!(
                "Failed to read sorted MemTable: {}",
                e
            ))
        })?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        result_df,
        batch_schema,
        captured_specs,
        None,
    ))
}
