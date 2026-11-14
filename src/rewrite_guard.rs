//! The guards of the rewrites the rule inventory rates Restricted (#273).
//!
//! Row-wise demand (contract §20.2) says an execution error is raised if
//! and only if a demanded value fails, and that a rewrite whose moved or
//! dropped expression can fail MUST NOT be applied where its errors cannot
//! be suppressed. DataFusion's rules know nothing of demand. The trade-off
//! record's rule inventory (`docs/proposals/v0.5/review/trade-offs.md`)
//! rates each of them, and the ones it rates Restricted run here behind a
//! guard that checks the rating's condition on the plan in front of it:
//!
//! - [`Guarded`] wraps a logical rule. [`Guard::KeepFailures`] accepts the
//!   rule's rewrite of a plan only if every expression that can fail and
//!   was demanded before is still demanded after (`eliminate_filter`,
//!   `eliminate_limit`, `propagate_empty_relation`, `eliminate_join`,
//!   `eliminate_group_by_constant`). For `simplify_expressions` it keeps
//!   every expression that can fail in the node's own expressions: where
//!   simplifying one whole would drop or add one, it keeps that
//!   expression's root and simplifies its parts, which stops the identities
//!   and absorption of assessment hazard X2. [`Guard::KeepBarriers`] keeps
//!   `push_down_filter` from moving a filter below a [`DemandBarrier`].
//!   [`Guard::SkipFallible`] does not run the rule on a node whose own
//!   expressions can fail (`eliminate_outer_join`).
//! - [`GuardedPhysical`] wraps a physical rule, and runs it only on a plan
//!   in which no expression can fail (`LimitedDistinctAggregation`,
//!   `LimitPushPastWindows`, `PushdownSort` and the dynamic filters of
//!   `FilterPushdown(Post)`).
//! - [`KeepDemandedKeys`] guards `push_down_filter` and the physical
//!   `FilterPushdown` (hazard X4, D10a option (a)): a sort, grouping,
//!   `distinct` or aggregate demands its keys and arguments on every row of
//!   its input, and a join on every row of its right input, so a filter
//!   above one whose keys can fail is stopped by a [`DemandBarrier`] where
//!   it would leave a key unevaluated.
//!
//! Whether an expression can fail is [`fallible_node`]'s conservative
//! reading of DataFusion 55's kernels: a `cast()` that can fail (an
//! [`OpaqueCast`]; a `Cast` is the engine's), a division or remainder by
//! anything but a safe constant, a function not known to be total, a
//! pattern match whose pattern is not a literal. A constant is
//! fallible only if evaluating it fails. Checked arithmetic (M5) makes more
//! operations fallible, and extends it.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatchOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::expr::{
    AggregateFunction, Like, ScalarFunction, WindowFunction, WindowFunctionParams,
};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::{expr_to_columns, merge_schema};
use datafusion::logical_expr::{
    BinaryExpr, Distinct, Expr, Extension, JoinType, LogicalPlan, Operator, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore, Volatility,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::transpiler::{planned_type, OpaqueCast};

/// Scalar functions DataFusion 55 evaluates without raising for any input
/// of the types they accept, by name. Every other function is fallible.
const TOTAL_FUNCTIONS: &[&str] = &[
    "coalesce",
    "nvl",
    "nvl2",
    "nullif",
    "ifnull",
    "upper",
    "lower",
    "btrim",
    "ltrim",
    "rtrim",
    "trim",
    "length",
    "char_length",
    "character_length",
    "octet_length",
    "bit_length",
    "concat",
    "concat_ws",
    "starts_with",
    "ends_with",
    "isnan",
    "iszero",
    "get_field",
    "named_struct",
    // Folded into a literal of the query's start time.
    "now",
    "current_date",
    "current_time",
    crate::order_key::ORDER_KEY_NAME,
];

/// Whether evaluating `expr` itself, its children aside, can raise.
///
/// `schema` is the one `expr` is evaluated against; where a type cannot be
/// read from it, the node counts as fallible.
pub(crate) fn fallible_node(expr: &Expr, schema: &DFSchema) -> bool {
    let fallible = match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::And
            | Operator::Or
            // Integer arithmetic wraps in DataFusion 55 (X9); M5 checks it.
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::StringConcat
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseXor => false,
            Operator::Divide | Operator::Modulo => {
                !(float_typed(left, schema) && float_typed(right, schema)) && !safe_divisor(right)
            }
            _ => true,
        },
        // A `Cast` is the engine's: coercion inserts it, and so does LTSeq's
        // lowering, to compare or combine two types. A cast the program
        // writes that can fail is an `OpaqueCast` (#260, X1). Where an
        // engine cast next to a literal could fail, as `µs` to `ns` can,
        // the comparison the contract defines is the exact one, which
        // `unwrap_cast` gives by removing the cast; its error is no value
        // the program demands.
        Expr::Cast(_) => false,
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            if let Some(cast) = func.inner().downcast_ref::<OpaqueCast>() {
                match args.as_slice() {
                    [Expr::Literal(value, _)] => value.cast_to(cast.target()).is_err(),
                    _ => true,
                }
            } else {
                !TOTAL_FUNCTIONS.contains(&func.name())
            }
        }
        Expr::Like(Like { pattern, .. }) | Expr::SimilarTo(Like { pattern, .. }) => {
            !matches!(pattern.as_ref(), Expr::Literal(..))
        }
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_) => true,
        // The result of an aggregate or a window function is computed per
        // group or frame; its arguments are what a row demands (§20.2).
        // Its own overflow is M5's and M13's.
        _ => false,
    };
    fallible && !constant_that_evaluates(expr)
}

/// A float divided by anything gives a float or NaN, never an error.
fn float_typed(expr: &Expr, schema: &DFSchema) -> bool {
    planned_type(expr, schema).is_some_and(|t| t.is_floating())
}

/// A divisor of a constant other than 0, -1 and NULL cannot fail.
fn safe_divisor(expr: &Expr) -> bool {
    let Expr::Literal(value, _) = expr else {
        return false;
    };
    if value.is_null() {
        return false;
    }
    let zero = ScalarValue::new_zero(&value.data_type());
    let minus_one = ScalarValue::new_negative_one(&value.data_type());
    zero.is_ok_and(|zero| &zero != value) && minus_one.map_or(true, |m| &m != value)
}

/// Whether `expr` reads no column and evaluates without an error: then the
/// constant evaluator folds it, and it cannot fail.
fn constant_that_evaluates(expr: &Expr) -> bool {
    let mut constant = true;
    let _ = expr.apply(|e| {
        let varies = match e {
            Expr::Column(_)
            | Expr::OuterReferenceColumn(..)
            | Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
            | Expr::Placeholder(_)
            | Expr::ScalarVariable(..)
            | Expr::Unnest(_)
            | Expr::GroupingSet(_) => true,
            Expr::ScalarFunction(f) => f.func.signature().volatility == Volatility::Volatile,
            _ => false,
        };
        constant &= !varies;
        Ok(if varies {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    if !constant {
        return false;
    }
    let schema = DFSchema::empty();
    let Ok(physical) = create_physical_expr(
        expr,
        &schema,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    ) else {
        return false;
    };
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    let Ok(batch) = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
    else {
        return false;
    };
    physical.evaluate(&batch).is_ok()
}

/// `expr` with columns unqualified and aliases removed, as a string: the
/// same expression keeps its key wherever a rewrite moves it.
fn key(expr: &Expr) -> String {
    expr.clone()
        .transform_up(|e| {
            Ok(match e {
                Expr::Column(c) => Transformed::yes(Expr::Column(Column::new_unqualified(c.name))),
                Expr::Alias(alias) => Transformed::yes(*alias.expr),
                e => Transformed::no(e),
            })
        })
        .map_or_else(|_| expr.to_string(), |t| t.data.to_string())
}

/// The keys of the fallible subexpressions of `expr`, `expr` included.
fn raw_atoms(expr: &Expr, schema: &DFSchema, out: &mut BTreeSet<String>) {
    let _ = expr.apply(|e| {
        if fallible_node(e, schema) {
            out.insert(key(e));
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

type Atoms = Rc<BTreeSet<String>>;

/// The fallible expressions that the values of a plan's columns are
/// computed from, through the projections, aggregates and windows below.
#[derive(Default)]
struct Inliner {
    memo: RefCell<HashMap<(usize, String), Atoms>>,
}

impl Inliner {
    /// The fallible expressions `exprs` evaluate, over `plan`'s inputs.
    fn exprs(&self, plan: &LogicalPlan, exprs: &[Expr]) -> BTreeSet<String> {
        let schema = input_schema(plan);
        let mut out = BTreeSet::new();
        for expr in exprs {
            raw_atoms(expr, &schema, &mut out);
            let mut columns = std::collections::HashSet::new();
            let _ = expr_to_columns(expr, &mut columns);
            for column in &columns {
                for input in plan.inputs() {
                    if let Some(index) = index_of(input.schema(), column) {
                        out.extend(self.column(input, index).iter().cloned());
                        break;
                    }
                }
            }
        }
        out
    }

    /// The fallible expressions column `index` of `plan` is computed from.
    fn column(&self, plan: &LogicalPlan, index: usize) -> Atoms {
        let memo_key = (plan as *const LogicalPlan as usize, index.to_string());
        if let Some(atoms) = self.memo.borrow().get(&memo_key) {
            return Rc::clone(atoms);
        }
        let atoms = Rc::new(self.compute_column(plan, index));
        self.memo.borrow_mut().insert(memo_key, Rc::clone(&atoms));
        atoms
    }

    fn compute_column(&self, plan: &LogicalPlan, index: usize) -> BTreeSet<String> {
        let defining = |expr: &Expr| self.exprs(plan, std::slice::from_ref(expr));
        match plan {
            LogicalPlan::Projection(p) => p.expr.get(index).map(defining).unwrap_or_default(),
            LogicalPlan::Aggregate(a) => {
                let groups = a.group_expr_len().unwrap_or(a.group_expr.len());
                if index < groups && a.group_expr.len() == groups {
                    defining(&a.group_expr[index])
                } else if index >= groups && index - groups < a.aggr_expr.len() {
                    defining(&a.aggr_expr[index - groups])
                } else {
                    self.exprs(plan, &plan.expressions())
                }
            }
            LogicalPlan::Window(w) => {
                let passed = w.input.schema().fields().len();
                if index < passed {
                    self.column(&w.input, index).as_ref().clone()
                } else {
                    w.window_expr
                        .get(index - passed)
                        .map(defining)
                        .unwrap_or_default()
                }
            }
            LogicalPlan::Union(u) => {
                let mut out = BTreeSet::new();
                for input in &u.inputs {
                    if index < input.schema().fields().len() {
                        out.extend(self.column(input, index).iter().cloned());
                    }
                }
                out
            }
            LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => BTreeSet::new(),
            plan => {
                let column = Column::from(plan.schema().qualified_field(index));
                for input in plan.inputs() {
                    if let Some(i) = index_of(input.schema(), &column)
                        .or_else(|| same_position(plan, input, index))
                    {
                        return self.column(input, i).as_ref().clone();
                    }
                }
                BTreeSet::new()
            }
        }
    }

    /// The fallible expressions `plan` demands on its input's rows, with
    /// those of the columns they read (§20.2): the predicate of a filter,
    /// the keys of a sort, grouping, `distinct` or join, the arguments of
    /// an aggregate or window function, an extension's expressions.
    fn demanded_by(&self, plan: &LogicalPlan) -> BTreeSet<String> {
        let exprs: Vec<Expr> = match plan {
            LogicalPlan::Filter(f) => vec![f.predicate.clone()],
            LogicalPlan::Sort(s) => s.expr.iter().map(|s| s.expr.clone()).collect(),
            LogicalPlan::Aggregate(a) => a
                .group_expr
                .iter()
                .cloned()
                .chain(a.aggr_expr.iter().flat_map(arguments))
                .collect(),
            LogicalPlan::Window(w) => w.window_expr.iter().flat_map(arguments).collect(),
            // A barrier marks values an operator above demands on every
            // row: the keys of a sort a filter crossed, for one.
            LogicalPlan::Distinct(Distinct::All(input)) => all_columns(input),
            plan if is_barrier(plan) => all_columns(plan.inputs()[0]),
            LogicalPlan::Join(_)
            | LogicalPlan::Distinct(Distinct::On(_))
            | LogicalPlan::Extension(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Limit(_) => plan.expressions(),
            _ => Vec::new(),
        };
        self.exprs(plan, &exprs)
    }

    /// What every node of `plan` demands.
    ///
    /// A filter that reads no column is the exception: `push_down_filter`
    /// moves such a conjunct through every extension node, a
    /// [`DemandBarrier`] included, so below one it may stand where a sort
    /// above demands what it would remove. Everything that can fail below
    /// it counts as demanded.
    fn demanded_in(&self, plan: &LogicalPlan) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let _ = plan.apply(|node| {
            match node {
                LogicalPlan::Filter(f) if f.predicate.column_refs().is_empty() => {
                    let _ = f.input.apply(|below| {
                        let schema = input_schema(below);
                        for expr in below.expressions() {
                            raw_atoms(&expr, &schema, &mut out);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    });
                }
                node => out.extend(self.demanded_by(node)),
            }
            Ok(TreeNodeRecursion::Continue)
        });
        out
    }
}

fn all_columns(plan: &LogicalPlan) -> Vec<Expr> {
    plan.schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect()
}

/// Whether evaluating `exprs` over the rows of `input` can fail, counting
/// the expressions the columns they read are computed from. An operator
/// that reads its input through a provider of its own, such as `sort`'s
/// table-order stream, asks this to know whether a filter may skip rows
/// whose keys it demands (X4).
pub(crate) fn can_fail_over(input: &LogicalPlan, exprs: &[Expr]) -> bool {
    let inliner = Inliner::default();
    let schema = input.schema();
    exprs.iter().any(|expr| {
        let mut atoms = BTreeSet::new();
        raw_atoms(expr, schema, &mut atoms);
        let mut columns = std::collections::HashSet::new();
        let _ = expr_to_columns(expr, &mut columns);
        !atoms.is_empty()
            || columns.iter().any(|column| {
                index_of(schema, column).is_some_and(|i| !inliner.column(input, i).is_empty())
            })
    })
}

/// The arguments, filter and ordering of an aggregate or window function,
/// or the expression itself if it is neither.
fn arguments(expr: &Expr) -> Vec<Expr> {
    let mut expr = expr;
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    match expr {
        Expr::AggregateFunction(AggregateFunction { params, .. }) => params
            .args
            .iter()
            .cloned()
            .chain(params.filter.as_deref().cloned())
            .chain(params.order_by.iter().map(|s| s.expr.clone()))
            .collect(),
        Expr::WindowFunction(window) => {
            let WindowFunction {
                params:
                    WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        filter,
                        ..
                    },
                ..
            } = window.as_ref();
            args.iter()
                .chain(partition_by)
                .cloned()
                .chain(filter.as_deref().cloned())
                .chain(order_by.iter().map(|s| s.expr.clone()))
                .collect()
        }
        expr => vec![expr.clone()],
    }
}

fn input_schema(plan: &LogicalPlan) -> DFSchemaRef {
    match plan {
        LogicalPlan::TableScan(scan) => {
            DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())
                .map(Arc::new)
                .unwrap_or_else(|_| Arc::clone(plan.schema()))
        }
        _ if plan.inputs().is_empty() => Arc::clone(plan.schema()),
        _ => Arc::new(merge_schema(&plan.inputs())),
    }
}

fn index_of(schema: &DFSchema, column: &Column) -> Option<usize> {
    schema.index_of_column(column).ok().or_else(|| {
        schema
            .index_of_column(&Column::new_unqualified(&column.name))
            .ok()
    })
}

/// For a node that passes its input's columns through in order.
fn same_position(plan: &LogicalPlan, input: &LogicalPlan, index: usize) -> Option<usize> {
    (plan.inputs().len() == 1 && input.schema().fields().len() == plan.schema().fields().len())
        .then_some(index)
}

/// How a [`Guarded`] rule is held to its rating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Guard {
    /// Accept a rewrite only if it keeps every fallible expression the
    /// plan demanded. With `own`, the expressions are the node's own,
    /// every one of them, as `simplify_expressions` rewrites only those,
    /// and the rewrite may not add one either.
    KeepFailures { own: bool },
    /// Run the rule only on a node whose own expressions cannot fail.
    SkipFallible,
    /// Accept a rewrite only if it moves no filter below a
    /// [`DemandBarrier`]. `push_down_filter` moves a conjunct that reads
    /// no column through every extension node.
    KeepBarriers,
}

/// A DataFusion logical rule held to its rating by `guard`. It keeps the
/// rule's name, so the inventory and the optimizer's messages name it.
pub(crate) struct Guarded {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
    guard: Guard,
}

impl Guarded {
    pub(crate) fn new(inner: Arc<dyn OptimizerRule + Send + Sync>, guard: Guard) -> Self {
        Guarded { inner, guard }
    }
}

impl fmt::Debug for Guarded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Guarded({}, {:?})", self.inner.name(), self.guard)
    }
}

impl OptimizerRule for Guarded {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.inner.apply_order()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match self.guard {
            Guard::SkipFallible => {
                // `eliminate_outer_join` rewrites a filter over a join.
                if matches!(plan, LogicalPlan::Filter(_))
                    && !Inliner::default()
                        .exprs(&plan, &plan.expressions())
                        .is_empty()
                {
                    return Ok(Transformed::no(plan));
                }
                self.inner.rewrite(plan, config)
            }
            Guard::KeepBarriers => {
                let before = filtered_below_barriers(&plan);
                let rewritten = self.inner.rewrite(plan.clone(), config)?;
                if rewritten.transformed
                    && !filtered_below_barriers(&rewritten.data).is_subset(&before)
                {
                    return Ok(Transformed::no(plan));
                }
                Ok(rewritten)
            }
            Guard::KeepFailures { own } => {
                let demanded = |plan: &LogicalPlan| {
                    let inliner = Inliner::default();
                    if own {
                        inliner.exprs(plan, &plan.expressions())
                    } else {
                        inliner.demanded_in(plan)
                    }
                };
                let rewritten = self.inner.rewrite(plan.clone(), config)?;
                if !rewritten.transformed {
                    return Ok(rewritten);
                }
                let before = demanded(&plan);
                if before.is_empty() && !own {
                    return Ok(rewritten);
                }
                let after = if own {
                    // `simplify_expressions` may put a projection above an
                    // aggregate it rewrote; what it computes is below.
                    let mut after = demanded(&rewritten.data);
                    if let LogicalPlan::Projection(p) = &rewritten.data {
                        if !matches!(plan, LogicalPlan::Projection(_)) {
                            after.extend(demanded(&p.input));
                        }
                    }
                    after
                } else {
                    demanded(&rewritten.data)
                };
                // A rewrite of a node's own expressions neither drops nor
                // adds one that can fail.
                if own && before != after {
                    // Simplify what does keep them, expression by expression
                    // and subexpression by subexpression. Refusing the
                    // node's rewrite whole would also refuse rewrites
                    // DataFusion requires: `coalesce` and `now()` fail at
                    // execution unless they are simplified.
                    return simplify_keeping_failures(plan, config);
                }
                Ok(if before.is_subset(&after) {
                    rewritten
                } else {
                    Transformed::no(plan)
                })
            }
        }
    }
}

/// `simplify_expressions` on one node, keeping every expression that can
/// fail: each expression is simplified whole where that keeps them, and
/// otherwise its root is kept and its children are simplified the same
/// way. A function at a kept root still gets its own `simplify` hook
/// (`coalesce` to `CASE`) where that keeps them. This follows
/// `SimplifyExpressions::optimize_internal` in DataFusion 55, whose
/// schema, context and name preservation it repeats.
fn simplify_keeping_failures(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    use datafusion::logical_expr::expr_rewriter::NamePreserver;
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;

    let context = || {
        SimplifyContext::builder()
            .with_schema(input_schema(&plan))
            .with_config_options(config.options())
            .with_query_execution_start_time(config.query_execution_start_time())
            .build()
    };
    let simplifier = ExprSimplifier::new(context());
    let simplifier = if matches!(plan, LogicalPlan::Join(_)) {
        simplifier.with_canonicalize(false)
    } else {
        simplifier
    };
    let info = context();
    let inliner = Inliner::default();
    let original = plan.clone();
    let atoms = |e: &Expr| inliner.exprs(&original, std::slice::from_ref(e));

    fn keep(
        expr: Expr,
        simplify: &dyn Fn(Expr) -> Result<Expr>,
        atoms: &dyn Fn(&Expr) -> BTreeSet<String>,
        info: &SimplifyContext,
    ) -> Result<Expr> {
        let before = atoms(&expr);
        let simplified = simplify(expr.clone())?;
        if atoms(&simplified) == before {
            return Ok(simplified);
        }
        let expr = expr
            .map_children(|child| keep(child, simplify, atoms, info).map(Transformed::yes))?
            .data;
        if let Expr::ScalarFunction(f) = &expr {
            if let ExprSimplifyResult::Simplified(hooked) = f.func.simplify(f.args.clone(), info)? {
                if atoms(&hooked) == atoms(&expr) {
                    return Ok(hooked);
                }
            }
        }
        Ok(expr)
    }

    let simplify = |e: Expr| simplifier.simplify(e);
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        let kept = if let Expr::GroupingSet(_) = &expr {
            expr.clone()
                .map_children(|e| keep(e, &simplify, &atoms, &info).map(Transformed::yes))?
                .data
        } else {
            keep(expr.clone(), &simplify, &atoms, &info)?
        };
        // An aggregate's expressions stay aggregate functions; a hook that
        // returns another expression needs the projection DataFusion's rule
        // adds, so that one is not taken here.
        let aggregate = |e: &Expr| {
            let mut e = e;
            while let Expr::Alias(alias) = e {
                e = &alias.expr;
            }
            matches!(e, Expr::AggregateFunction(_))
        };
        if aggregate(&expr) && !aggregate(&kept) {
            return Ok(Transformed::no(expr));
        }
        Ok(if kept == expr {
            Transformed::no(expr)
        } else {
            Transformed::yes(name.restore(kept))
        })
    })
}

/// A DataFusion physical rule that runs only on a plan in which no
/// expression can fail. It keeps the rule's name.
pub(crate) struct GuardedPhysical {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl GuardedPhysical {
    pub(crate) fn new(inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>) -> Self {
        GuardedPhysical { inner }
    }
}

impl fmt::Debug for GuardedPhysical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GuardedPhysical({})", self.inner.name())
    }
}

impl PhysicalOptimizerRule for GuardedPhysical {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if physical_plan_can_fail(&plan) {
            Ok(plan)
        } else {
            self.inner.optimize(plan, config)
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

/// Whether any expression of `plan` can fail, as [`fallible_node`] reads
/// the physical expressions DataFusion plans from logical ones.
pub(crate) fn physical_plan_can_fail(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_expr::expressions::{BinaryExpr, LikeExpr, Literal};
    let mut fails = false;
    let _ = plan.apply(|node| {
        let schema = node
            .children()
            .first()
            .map_or_else(|| node.schema(), |child| child.schema());
        let _ = node.apply_expressions(&mut |root| {
            root.apply(|e| {
                let fallible = if let Some(binary) = e.downcast_ref::<BinaryExpr>() {
                    match binary.op() {
                        Operator::Divide | Operator::Modulo => {
                            let float = |e: &Arc<dyn PhysicalExpr>| {
                                e.data_type(&schema).is_ok_and(|t| t.is_floating())
                            };
                            let safe = binary.right().downcast_ref::<Literal>().is_some_and(|l| {
                                safe_divisor(&Expr::Literal(l.value().clone(), None))
                            });
                            !(float(binary.left()) && float(binary.right())) && !safe
                        }
                        op => {
                            let total = [
                                Operator::Eq,
                                Operator::NotEq,
                                Operator::Lt,
                                Operator::LtEq,
                                Operator::Gt,
                                Operator::GtEq,
                                Operator::IsDistinctFrom,
                                Operator::IsNotDistinctFrom,
                                Operator::And,
                                Operator::Or,
                                Operator::Plus,
                                Operator::Minus,
                                Operator::Multiply,
                                Operator::StringConcat,
                                Operator::BitwiseAnd,
                                Operator::BitwiseOr,
                                Operator::BitwiseXor,
                            ];
                            !total.contains(op)
                        }
                    }
                } else if let Some(function) = e.downcast_ref::<ScalarFunctionExpr>() {
                    function.name().starts_with("opaque_cast_")
                        || !TOTAL_FUNCTIONS.contains(&function.name())
                } else if let Some(like) = e.downcast_ref::<LikeExpr>() {
                    like.pattern().downcast_ref::<Literal>().is_none()
                } else {
                    false
                };
                fails |= fallible;
                Ok(if fallible {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            })
        });
        Ok(if fails {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    fails
}

/// A node that passes its input through unchanged and that no filter
/// crosses, logically or physically (X4). [`KeepDemandedKeys`] plans it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct DemandBarrier {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for DemandBarrier {
    fn name(&self) -> &str {
        "DemandBarrier"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DemandBarrier")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(DemandBarrier {
            input: inputs.swap_remove(0),
        })
    }

    // Projection pruning crosses it: it needs from its input exactly the
    // columns asked of it.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
}

/// The predicates of the filters and scans below a [`DemandBarrier`].
fn filtered_below_barriers(plan: &LogicalPlan) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let _ = plan.apply(|node| {
        if is_barrier(node) {
            let _ = node.apply(|below| {
                match below {
                    LogicalPlan::Filter(f) => {
                        out.insert(key(&f.predicate));
                    }
                    LogicalPlan::TableScan(scan) => out.extend(scan.filters.iter().map(key)),
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            });
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

fn is_barrier(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Extension(Extension { node }) if node.as_any().is::<DemandBarrier>())
}

fn barrier(plan: LogicalPlan) -> LogicalPlan {
    if is_barrier(&plan) {
        return plan;
    }
    LogicalPlan::Extension(Extension {
        node: Arc::new(DemandBarrier { input: plan }),
    })
}

/// Guards `push_down_filter` and the physical `FilterPushdown` (X4).
///
/// Under D10a option (a), a sort, grouping, `distinct` or aggregate demands
/// its keys and arguments on every row of its input, and a join on every
/// row of its right input (both inputs for `right` and `full`), including
/// rows a later filter removes (§20.2). `push_down_filter` moves a filter
/// below such an operator, and the operator then evaluates its keys only
/// on the rows the filter keeps: `derive(k=10 // r.x).sort("k").filter(r.x
/// != 0)` over `x = [0, 2, 5]` returned two rows instead of raising.
///
/// Where a filter sits above such an operator, this rule puts a
/// [`DemandBarrier`] where the filter has to stop: above the operator if it
/// computes a fallible key itself, or on its input (the right input of a
/// join) if a key it reads is computed below from an expression that can
/// fail. Where no key can fail, it leaves the plan alone, so the filter
/// still moves into the scan and prunes. M0 priced the barrier on the input
/// (#251, m4b, "keys"). It runs first in every optimizer pass.
#[derive(Debug)]
pub(crate) struct KeepDemandedKeys;

impl OptimizerRule for KeepDemandedKeys {
    fn name(&self) -> &str {
        "ltseq_keep_demanded_keys"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        guard_keys(plan, false, false)
    }
}

/// `plan` with barriers below the filters above it (`under_filter`).
/// `barred` says `plan` is the input of a barrier already.
fn guard_keys(
    plan: LogicalPlan,
    under_filter: bool,
    barred: bool,
) -> Result<Transformed<LogicalPlan>> {
    if is_barrier(&plan) {
        return plan.map_children(|child| guard_keys(child, false, true));
    }
    // A limit, a scan and an extension stop a filter; a filter starts one.
    let below = match &plan {
        LogicalPlan::Limit(_) | LogicalPlan::Extension(_) => false,
        LogicalPlan::Filter(_) => true,
        _ => under_filter,
    };
    let guarded = if under_filter && !barred {
        keys_guard(&plan)
    } else {
        KeysGuard::None
    };
    let plan = match guarded {
        KeysGuard::Inputs(ref sides) => {
            let mut i = 0;
            plan.map_children(|child| {
                let side = i;
                i += 1;
                let child = guard_keys(child, below, sides.contains(&side))?;
                Ok(if sides.contains(&side) && !is_barrier(&child.data) {
                    Transformed::yes(barrier(child.data))
                } else {
                    child
                })
            })?
        }
        _ => plan.map_children(|child| guard_keys(child, below, false))?,
    };
    Ok(match guarded {
        KeysGuard::Above => plan.update_data(barrier).update_transformed(true),
        _ => plan,
    })
}

trait UpdateTransformed {
    fn update_transformed(self, yes: bool) -> Self;
}

impl<T> UpdateTransformed for Transformed<T> {
    fn update_transformed(mut self, yes: bool) -> Self {
        self.transformed |= yes;
        self
    }
}

enum KeysGuard {
    None,
    Above,
    Inputs(Vec<usize>),
}

/// Where a filter above `plan` has to stop, by the keys `plan` demands.
fn keys_guard(plan: &LogicalPlan) -> KeysGuard {
    let demanding = match plan {
        LogicalPlan::Sort(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => {
            vec![0]
        }
        LogicalPlan::Join(join) => match join.join_type {
            JoinType::Right | JoinType::Full | JoinType::RightSemi | JoinType::RightAnti => {
                vec![0, 1]
            }
            _ => vec![1],
        },
        _ => return KeysGuard::None,
    };
    let inliner = Inliner::default();
    let schema = input_schema(plan);
    let mut own = BTreeSet::new();
    for expr in plan.expressions() {
        for expr in arguments(&expr) {
            raw_atoms(&expr, &schema, &mut own);
        }
    }
    if !own.is_empty() {
        return KeysGuard::Above;
    }
    if inliner.demanded_by(plan).is_empty() {
        return KeysGuard::None;
    }
    let sides: Vec<usize> = demanding
        .into_iter()
        .filter(|&side| {
            !plan
                .inputs()
                .get(side)
                .is_some_and(|input| is_barrier(input))
        })
        .collect();
    if sides.is_empty() {
        KeysGuard::None
    } else {
        KeysGuard::Inputs(sides)
    }
}

/// A [`DemandBarrier`] in the physical plan: its input's batches,
/// unchanged. Like any operator that does not say otherwise, it takes no
/// filter from above, so the physical `FilterPushdown` stops here too.
#[derive(Debug)]
struct DemandBarrierExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for DemandBarrierExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DemandBarrierExec")
    }
}

impl ExecutionPlan for DemandBarrierExec {
    fn name(&self) -> &str {
        "DemandBarrierExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DemandBarrierExec {
            input: children.swap_remove(0),
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// Plans a [`DemandBarrier`] as a [`DemandBarrierExec`]; the engine's
/// query planner holds it.
pub(crate) struct DemandBarrierPlanner;

#[async_trait]
impl ExtensionPlanner for DemandBarrierPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn datafusion::catalog::Session,
        _planning_ctx: &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(node.as_any().downcast_ref::<DemandBarrier>().map(|_| {
            Arc::new(DemandBarrierExec {
                input: Arc::clone(&physical_inputs[0]),
            }) as Arc<dyn ExecutionPlan>
        }))
    }
}

/// How the session holds the logical rule `name` to its rating, if it is
/// rated Restricted.
pub(crate) fn logical_guard(name: &str) -> Option<Guard> {
    match name {
        "simplify_expressions" => Some(Guard::KeepFailures { own: true }),
        "eliminate_filter"
        | "eliminate_limit"
        | "propagate_empty_relation"
        | "eliminate_join"
        | "eliminate_group_by_constant" => Some(Guard::KeepFailures { own: false }),
        "eliminate_outer_join" => Some(Guard::SkipFallible),
        "push_down_filter" => Some(Guard::KeepBarriers),
        _ => None,
    }
}

/// Whether the session runs the physical rule `name` only on plans in
/// which nothing can fail.
pub(crate) fn physical_guarded(name: &str) -> bool {
    matches!(
        name,
        "LimitedDistinctAggregation"
            | "LimitPushPastWindows"
            | "PushdownSort"
            | "FilterPushdown(Post)"
    )
}

/// Plan-guard tests: one minimal plan per rule the inventory rates
/// Restricted or Disabled, with the optimized logical or physical plan
/// asserted. Each `plan_guard_*` test is named in the inventory's guard
/// column, and `rule_inventory` checks that it exists. A DataFusion
/// upgrade that changes, adds or re-enables a rewrite fails here.
#[cfg(all(test, not(feature = "m0")))]
mod tests {
    use std::num::NonZeroUsize;

    use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::functions_aggregate::expr_fn::{count, max};
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::{ExprFunctionExt, JoinType};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{col, lit, DataFrame, SessionContext};

    use super::*;
    use crate::engine::{session_context_with, LayoutOverrides, RUNTIME};
    use datafusion::logical_expr::Cast;

    fn session() -> Arc<SessionContext> {
        session_context_with(LayoutOverrides {
            target_partitions: NonZeroUsize::new(2),
            batch_size: None,
        })
    }

    fn batch(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
        let schema = Schema::new(
            columns
                .iter()
                .map(|(name, _)| Field::new(*name, DataType::Int64, false))
                .collect::<Vec<_>>(),
        );
        let arrays = columns
            .iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values.clone())) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    /// A table of non-nullable `int64` columns.
    fn table(session: &SessionContext, columns: &[(&str, Vec<i64>)]) -> DataFrame {
        let batch = batch(columns);
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        session.read_table(Arc::new(table)).unwrap()
    }

    /// The same table read from a Parquet file, which takes the filters a
    /// memory table does not.
    fn parquet(session: &SessionContext, name: &str, columns: &[(&str, Vec<i64>)]) -> DataFrame {
        let dir =
            std::env::temp_dir().join(format!("ltseq-rewrite-guard-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.parquet");
        let batch = batch(columns);
        let file = std::fs::File::create(&path).unwrap();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        RUNTIME
            .block_on(session.read_parquet(path.to_str().unwrap(), Default::default()))
            .unwrap()
    }

    fn parquet_file(name: &str, batch: &RecordBatch) -> String {
        let dir =
            std::env::temp_dir().join(format!("ltseq-rewrite-guard-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    fn logical(df: &DataFrame) -> String {
        df.clone()
            .into_optimized_plan()
            .unwrap()
            .display_indent()
            .to_string()
    }

    fn physical(df: &DataFrame) -> String {
        let plan = RUNTIME.block_on(df.clone().create_physical_plan()).unwrap();
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        shown
    }

    fn collect(df: &DataFrame) -> Result<usize> {
        RUNTIME
            .block_on(df.clone().collect())
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
    }

    /// `10 / x`, which fails where `x` is 0.
    fn fallible() -> Expr {
        lit(10_i64) / col("x")
    }

    /// The line of `plan` holding `needle`, and its depth.
    fn depth(plan: &str, needle: &str) -> usize {
        let line = plan
            .lines()
            .find(|line| line.contains(needle))
            .unwrap_or_else(|| panic!("no {needle} in\n{plan}"));
        line.len() - line.trim_start().len()
    }

    #[test]
    fn the_classifier_reads_failures_conservatively() {
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
            Field::new("i", DataType::Int32, false),
        ]))
        .unwrap();
        let fails = |e: Expr| fallible_node(&e, &schema);
        assert!(fails(lit(10_i64) / col("x")));
        assert!(fails(col("x") % col("x")));
        assert!(fails(lit(1_i64) / lit(0_i64)));
        assert!(fails(col("x") / lit(-1_i64)));
        assert!(!fails(col("x") / lit(2_i64)));
        assert!(!fails(lit(10_i64) / lit(2_i64)));
        assert!(!fails(col("f") / col("f")));
        assert!(!fails(col("x") + col("x")));
        let opaque = |e: Expr| OpaqueCast::call(e, DataType::Int32);
        assert!(fails(opaque(col("x"))));
        assert!(fails(opaque(lit("abc"))));
        assert!(!fails(opaque(lit("12"))));
        // A `Cast` is the engine's, which no program demands.
        assert!(!fails(Expr::Cast(Cast::new(
            Box::new(col("x")),
            DataType::Int32
        ))));
    }

    /// X2: identities gated on non-nullability, and absorption, which has
    /// no gate, keep an operand that can fail.
    #[test]
    fn plan_guard_simplify_expressions() {
        let session = session();
        let t = table(&session, &[("x", vec![0, 2]), ("b", vec![1, 0])]);
        let a = col("b").eq(lit(1_i64));
        for (name, expr) in [
            ("times zero", fallible() * lit(0_i64)),
            ("is null", fallible().is_null()),
            ("self equal", fallible().eq(fallible())),
            (
                "absorption",
                a.clone().or(a.clone().and(fallible().gt(lit(1_i64)))),
            ),
            ("constant", (lit(1_i64) / lit(0_i64)).is_null()),
        ] {
            let df = t.clone().select(vec![expr.alias("v")]).unwrap();
            let plan = logical(&df);
            assert!(plan.contains(" / "), "{name}: the division is gone\n{plan}");
            assert!(collect(&df).is_err(), "{name}: no error\n{plan}");
        }
        // A rewrite that drops nothing that can fail still applies.
        let df = t.select(vec![(col("x") * lit(0_i64)).alias("v")]).unwrap();
        assert!(logical(&df).contains("Int64(0) AS v"), "{}", logical(&df));
    }

    /// A filter of a literal FALSE above a sort whose key can fail keeps
    /// the sort, which demands the key on every row (D10a).
    #[test]
    fn plan_guard_eliminate_filter() {
        let session = session();
        let t = table(&session, &[("x", vec![0, 2])]);
        let sorted = |key: Expr| {
            t.clone()
                .with_column("k", key)
                .unwrap()
                .sort(vec![col("k").sort(true, true)])
                .unwrap()
                .filter(lit(false))
                .unwrap()
        };
        let guarded = sorted(fallible());
        assert!(logical(&guarded).contains("Sort:"), "{}", logical(&guarded));
        assert!(collect(&guarded).is_err(), "{}", physical(&guarded));
        let free = sorted(col("x") + lit(1_i64));
        assert!(
            logical(&free).contains("EmptyRelation"),
            "{}",
            logical(&free)
        );
    }

    /// `head(0)` of a sort whose key can fail keeps the sort.
    #[test]
    fn plan_guard_eliminate_limit() {
        let session = session();
        let t = table(&session, &[("x", vec![0, 2])]);
        let limited = |key: Expr| {
            t.clone()
                .with_column("k", key)
                .unwrap()
                .sort(vec![col("k").sort(true, true)])
                .unwrap()
                .limit(0, Some(0))
                .unwrap()
        };
        let guarded = limited(fallible());
        assert!(logical(&guarded).contains("Sort:"), "{}", logical(&guarded));
        let free = limited(col("x"));
        assert!(
            logical(&free).contains("EmptyRelation"),
            "{}",
            logical(&free)
        );
    }

    /// An inner join with an empty input keeps the other input when that
    /// one's grouping key can fail.
    #[test]
    fn plan_guard_propagate_empty_relation() {
        let session = session();
        let left = |key: Expr| {
            table(&session, &[("x", vec![0, 2])])
                .aggregate(vec![key.alias("k")], vec![count(lit(1_i64)).alias("n")])
                .unwrap()
        };
        let right = table(&session, &[("y", vec![1])])
            .filter(lit(false))
            .unwrap();
        let join = |left: DataFrame| {
            left.join(right.clone(), JoinType::Inner, &["k"], &["y"], None)
                .unwrap()
        };
        // The plan keeps the grouping. A hash join whose build side is
        // empty does not read its probe side, so executing it raises
        // nothing: that is the join's execution, not this rule (M16).
        let guarded = join(left(fallible()));
        assert!(
            logical(&guarded).contains("Aggregate:"),
            "{}",
            logical(&guarded)
        );
        let free = join(left(col("x")));
        assert!(!logical(&free).contains("Aggregate:"), "{}", logical(&free));
    }

    /// A left join whose right side is unused and unique keeps the right
    /// side when its key can fail: a join demands it on every right row.
    #[test]
    fn plan_guard_eliminate_join() {
        let session = session();
        let right = |key: Expr| {
            table(&session, &[("x", vec![0, 2])])
                .aggregate(vec![key.alias("k")], vec![])
                .unwrap()
        };
        let join = |right: DataFrame| {
            table(&session, &[("a", vec![1, 5])])
                .join(right, JoinType::Left, &["a"], &["k"], None)
                .unwrap()
                .select(vec![col("a")])
                .unwrap()
        };
        let guarded = join(right(fallible()));
        assert!(logical(&guarded).contains("Join:"), "{}", logical(&guarded));
        assert!(collect(&guarded).is_err());
        let free = join(right(col("x")));
        assert!(!logical(&free).contains("Join:"), "{}", logical(&free));
    }

    /// A constant grouping key that fails stays a grouping key.
    #[test]
    fn plan_guard_eliminate_group_by_constant() {
        let session = session();
        let t = table(&session, &[("x", vec![0, 2])]);
        let grouped = |constant: Expr| {
            t.clone()
                .aggregate(
                    vec![col("x"), constant.alias("c")],
                    vec![count(lit(1_i64)).alias("n")],
                )
                .unwrap()
        };
        let guarded = grouped(lit(1_i64) / lit(0_i64));
        let plan = logical(&guarded);
        assert!(
            plan.lines()
                .any(|l| l.contains("Aggregate: groupBy") && l.contains("Int64(1) / Int64(0)")),
            "{plan}"
        );
        let free = grouped(lit(1_i64));
        let plan = logical(&free);
        assert!(
            !plan
                .lines()
                .any(|l| l.contains("Aggregate: groupBy") && l.contains("Int64(1) AS c")),
            "{plan}"
        );
    }

    /// A filter that rejects NULL on the right of a left join does not
    /// turn it into an inner join when the filter can fail: on a padded
    /// row its other conjunct is NULL, and it is demanded there (D10).
    #[test]
    fn plan_guard_eliminate_outer_join() {
        let session = session();
        let joined = |predicate: Expr| {
            table(&session, &[("x", vec![0, 2])])
                .join(
                    table(&session, &[("y", vec![2]), ("b", vec![1])]),
                    JoinType::Left,
                    &["x"],
                    &["y"],
                    None,
                )
                .unwrap()
                .filter(predicate)
                .unwrap()
        };
        // NULL-rejecting on `b`, but not split by `push_down_filter`.
        let guarded = joined(
            fallible()
                .gt(lit(1_i64))
                .and(col("b").gt(lit(0_i64)))
                .or(col("b").gt(lit(5_i64))),
        );
        assert!(
            logical(&guarded).contains("Left Join"),
            "{}",
            logical(&guarded)
        );
        assert!(collect(&guarded).is_err());
        let free = joined(col("b").gt(lit(0_i64)).or(col("b").gt(lit(5_i64))));
        assert!(logical(&free).contains("Inner Join"), "{}", logical(&free));
    }

    /// X4: a filter after a sort, grouping, `distinct` or join stops above
    /// the input whose keys can fail, and moves into the scan otherwise.
    #[test]
    fn plan_guard_push_down_filter() {
        let session = session();
        let t = || table(&session, &[("x", vec![0, 2, 5])]);
        let keyed = |key: Expr| t().with_column("k", key).unwrap();
        type Operation<'a> = Box<dyn Fn(Expr) -> DataFrame + 'a>;
        let after: [(&str, Operation); 4] = [
            (
                "sort",
                Box::new(|key| keyed(key).sort(vec![col("k").sort(true, true)]).unwrap()),
            ),
            ("distinct", Box::new(|key| keyed(key).distinct().unwrap())),
            (
                "grouping",
                Box::new(|key| {
                    keyed(key)
                        .aggregate(vec![col("x"), col("k")], vec![])
                        .unwrap()
                }),
            ),
            (
                "join",
                Box::new(|key| {
                    table(&session, &[("a", vec![0, 2, 5])])
                        .join(keyed(key), JoinType::Inner, &["a"], &["k"], None)
                        .unwrap()
                }),
            ),
        ];
        for (name, op) in &after {
            let guarded = op(fallible()).filter(col("x").not_eq(lit(0_i64))).unwrap();
            let plan = logical(&guarded);
            assert!(
                depth(&plan, "Filter:") < depth(&plan, "DemandBarrier"),
                "{name}: the filter crossed the barrier\n{plan}"
            );
            assert!(collect(&guarded).is_err(), "{name}: no error\n{plan}");
            let free = op(col("x") + lit(1_i64))
                .filter(col("x").not_eq(lit(0_i64)))
                .unwrap();
            let plan = logical(&free);
            assert!(!plan.contains("DemandBarrier"), "{name}\n{plan}");
            assert!(collect(&free).is_ok(), "{name}\n{plan}");
        }
    }

    /// X4 in the physical plan: the filter stays above the barrier and out
    /// of the Parquet scan, which takes it where no key can fail.
    #[test]
    fn plan_guard_filter_pushdown() {
        let session = session();
        let t = parquet(&session, "pushdown", &[("x", vec![0, 2, 5])]);
        let sorted = |key: Expr| {
            t.clone()
                .with_column("k", key)
                .unwrap()
                .sort(vec![col("k").sort(true, true)])
                .unwrap()
                .filter(col("x").not_eq(lit(0_i64)))
                .unwrap()
        };
        let plan = physical(&sorted(fallible()));
        assert!(
            depth(&plan, "FilterExec") < depth(&plan, "DemandBarrierExec"),
            "{plan}"
        );
        assert!(!plan.contains("predicate=x@0 != 0"), "{plan}");
        let plan = physical(&sorted(col("x") + lit(1_i64)));
        assert!(plan.contains("predicate=x@0 != 0"), "{plan}");
    }

    /// The dynamic filter of a TopK reaches the scan only where nothing
    /// in the plan can fail.
    #[test]
    fn plan_guard_filter_pushdown_post() {
        let session = session();
        let t = parquet(
            &session,
            "post",
            &[("x", vec![0, 2, 5]), ("y", vec![1, 2, 3])],
        );
        let top = |value: Expr| {
            t.clone()
                .with_column("v", value)
                .unwrap()
                .sort(vec![col("y").sort(true, true)])
                .unwrap()
                .limit(0, Some(1))
                .unwrap()
        };
        assert!(!physical(&top(fallible())).contains("DynamicFilter"));
        let plan = physical(&top(col("x") + lit(1_i64)));
        assert!(plan.contains("DynamicFilter"), "{plan}");
    }

    /// A `distinct` under a limit stops reading once it has enough groups
    /// only where no key can fail.
    #[test]
    fn plan_guard_limited_distinct_aggregation() {
        let session = session();
        let distinct = |key: Expr| {
            table(&session, &[("x", vec![0, 2, 5])])
                .aggregate(vec![key.alias("k")], vec![])
                .unwrap()
                .limit(0, Some(1))
                .unwrap()
        };
        assert!(!physical(&distinct(fallible())).contains("lim=["));
        let plan = physical(&distinct(col("x") + lit(1_i64)));
        assert!(plan.contains("lim=["), "{plan}");
    }

    /// A limit moves below a window only where nothing can fail.
    #[test]
    fn plan_guard_limit_push_past_windows() {
        let session = session();
        let windowed = |value: Expr| {
            table(&session, &[("x", vec![0, 2, 5]), ("y", vec![1, 2, 3])])
                .with_column("v", value)
                .unwrap()
                .sort(vec![col("y").sort(true, true)])
                .unwrap()
                .with_column(
                    "n",
                    row_number()
                        .order_by(vec![col("y").sort(true, true)])
                        .build()
                        .unwrap(),
                )
                .unwrap()
                .limit(0, Some(1))
                .unwrap()
        };
        let guarded = physical(&windowed(fallible()));
        let free = physical(&windowed(col("x") + lit(1_i64)));
        assert!(
            depth(&guarded, "GlobalLimitExec") < depth(&guarded, "BoundedWindowAggExec")
                || !guarded.contains("fetch=1"),
            "{guarded}"
        );
        assert_ne!(guarded, free.replace("x@0 + 1", "10 / x@0"), "{free}");
    }

    /// A float `max` over Parquet is not answered from statistics, which
    /// leave NaN out (#253, #291); an integer one still is.
    #[test]
    fn plan_guard_aggregate_statistics() {
        let session = session();
        let floats = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)])),
            vec![Arc::new(Float64Array::from(vec![1.0, f64::NAN, 2.0]))],
        )
        .unwrap();
        let path = parquet_file("statistics", &floats);
        let t = RUNTIME
            .block_on(session.read_parquet(path.as_str(), Default::default()))
            .unwrap();
        let plan = physical(&t.aggregate(vec![], vec![max(col("f"))]).unwrap());
        assert!(plan.contains("AggregateExec"), "{plan}");
        let t = parquet(&session, "statistics_int", &[("x", vec![1, 3])]);
        let plan = physical(&t.aggregate(vec![], vec![max(col("x"))]).unwrap());
        assert!(!plan.contains("AggregateExec"), "{plan}");
    }

    /// A sort in the reverse of a scan's declared order is pushed into the
    /// scan, which then reads row groups in reverse and may stop early,
    /// only where nothing in the plan can fail.
    #[test]
    fn plan_guard_pushdown_sort() {
        use datafusion::prelude::ParquetReadOptions;
        let session = session();
        let path = parquet_file("sort", &batch(&[("x", vec![0, 2, 5])]));
        let options =
            ParquetReadOptions::default().file_sort_order(vec![vec![col("x").sort(true, true)]]);
        let t = RUNTIME
            .block_on(session.read_parquet(path.as_str(), options))
            .unwrap();
        let top = |value: Expr| {
            t.clone()
                .with_column("v", value)
                .unwrap()
                .sort(vec![col("x").sort(false, false)])
                .unwrap()
                .limit(0, Some(1))
                .unwrap()
        };
        let guarded = physical(&top(fallible()));
        let free = physical(&top(col("x") + lit(1_i64)));
        assert_ne!(
            guarded.replace("10 / x@0", "x@0 + 1"),
            free,
            "the guard changed nothing:\n{free}"
        );
        assert!(free.contains("reverse"), "{free}");
        assert!(!guarded.contains("reverse"), "{guarded}");
    }

    /// The rules LTSeq turns off by their flags leave the plans they would
    /// rewrite alone.
    #[test]
    fn plan_guard_disabled_rules() {
        let session = session();
        let t = || table(&session, &[("x", vec![0, 2, 5]), ("y", vec![1, 2, 3])]);
        // unions_to_filter: the branches of a UNION DISTINCT stay filters
        // of their own, not one `p OR q`.
        let union = t()
            .filter(col("x").gt(lit(1_i64)))
            .unwrap()
            .union_distinct(t().filter(col("y").gt(lit(2_i64))).unwrap())
            .unwrap();
        let plan = logical(&union);
        assert!(plan.contains("Union"), "{plan}");
        assert!(!plan.contains(" OR "), "{plan}");
        // WindowTopN: a filter on a row number stays a filter over the
        // window.
        let top = t()
            .with_column(
                "n",
                row_number()
                    .partition_by(vec![col("x")])
                    .order_by(vec![col("y").sort(true, true)])
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .filter(col("n").lt_eq(lit(1_u64)))
            .unwrap();
        let plan = physical(&top);
        assert!(plan.contains("FilterExec"), "{plan}");
        assert!(!plan.contains("PartitionedTopK"), "{plan}");
        // filter_null_join_keys: no IS NOT NULL filter on nullable keys.
        let nullable = |name: &str| {
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
            )
            .unwrap();
            session
                .read_table(Arc::new(
                    MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
                ))
                .unwrap()
        };
        let join = nullable("a")
            .join(nullable("b"), JoinType::Inner, &["a"], &["b"], None)
            .unwrap();
        let plan = logical(&join);
        assert!(!plan.contains("IS NOT NULL"), "{plan}");
        // HashJoinBuffering: no buffer above the probe side.
        let plan = physical(&join);
        assert!(!plan.contains("BufferExec"), "{plan}");
        // The aggregates' dynamic filter (#253): a `max` puts none in the
        // scan.
        let t = parquet(&session, "max", &[("x", vec![0, 2, 5])]);
        let plan = physical(&t.aggregate(vec![], vec![max(col("x"))]).unwrap());
        assert!(!plan.contains("DynamicFilter"), "{plan}");
    }
}
