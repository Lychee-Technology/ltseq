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
//!   and absorption of assessment hazard X2. Both count occurrences
//!   ([`Demand`]), each expression of a node on its own: two occurrences of
//!   one expression are demanded on different rows. An occurrence in a
//!   `CASE` branch is counted apart from one evaluated on every row
//!   ([`Reach`]), so a rewrite that moves one out of a branch (`CASE` to
//!   `AND`) is refused too, and a bitwise operation is never simplified, as
//!   its identities are wrong in value ([`has_bitwise`]). [`Guard::KeepBarriers`]
//!   keeps `push_down_filter` from moving a filter below a
//!   [`DemandBarrier`], and [`Guard::KeepJoinShape`] keeps
//!   `eliminate_cross_join` from changing a join's inputs (X5).
//!   [`Guard::SkipFallible`] does not run the rule on a node whose own
//!   expressions can fail (`eliminate_outer_join`).
//! - [`GuardedPhysical`] wraps a physical rule, and runs it only on a plan
//!   in which no expression can fail (`LimitedDistinctAggregation`,
//!   `LimitPushPastWindows`, `PushdownSort` and the dynamic filters of
//!   `FilterPushdown(Post)`), runs `join_selection` without swapping a
//!   join's inputs, or does not run `OptimizeAggregateOrder` (X5).
//! - [`KeepDemandedKeys`] guards `push_down_filter` and the physical
//!   `FilterPushdown` (hazard X4, D10a option (a)): a sort, grouping,
//!   `distinct` or aggregate demands its keys and arguments on every row of
//!   its input, and a join on every row of its right input, so a filter
//!   above one whose keys can fail is stopped by a [`DemandBarrier`] where
//!   it would leave a key unevaluated. Chained filters are kept apart the
//!   same way where either predicate can fail.
//! - [`KeepStableSorts`] keeps `EnsureRequirements` from replacing a stable
//!   sort with an unstable one by a prefix of its keys (X5).
//!
//! Whether an expression can fail is [`fallible_node`]'s conservative
//! reading of DataFusion 55's kernels: a `cast()` that can fail (an
//! [`OpaqueCast`], from a lambda or `filter_where`'s SQL; a `Cast` is the
//! engine's), a division or remainder by
//! anything but a safe constant, a function not known to be total, a
//! pattern match whose pattern is not a literal. A constant is
//! fallible only if evaluating it fails. Checked arithmetic (M5) makes more
//! operations fallible, and extends it.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
        // writes that can fail is an `OpaqueCast` (#260, X1), whether
        // `cast()` or `filter_where`'s SQL writes it (R2-F3). Where an
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

/// Where an occurrence of an expression that can fail is evaluated (R3-F2
/// of #273).
///
/// `Every` is on every row its expression is evaluated on. `Branch` is in a
/// `CASE`'s `THEN` or `ELSE`, or a condition after its first, or in an
/// argument after the first of a function that evaluates it only where the
/// first is NULL (`coalesce`): there it is evaluated only on the rows that
/// reach it. A rewrite that moves an occurrence from a branch to where it
/// is always evaluated, as turning `CASE WHEN c THEN false ELSE e END` into
/// `NOT c AND e` does, raises its errors on rows the `CASE` did not demand
/// them on. `AND` and `OR` count as `Every`: DataFusion evaluates their
/// right operand on every row of a batch unless the left one decides it for
/// the whole batch, which M16's masking will make row-wise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Reach {
    Every,
    Branch,
}

/// The fallible expressions a plan evaluates, by where they are evaluated
/// and their key, each with the number of places it is evaluated in (R2-F1
/// of #273).
///
/// A set would not do: an expression that can fail is demanded where its
/// occurrence is, and two occurrences of the same expression are demanded
/// on different rows. `v = a | (a & (c > 0))` and `w = if_else(a, c, 0)`
/// both evaluate `c`, but `v` demands it where `a` is NULL and `w` only
/// where `a` is TRUE, so a rewrite that drops the occurrence in `v` drops a
/// demanded failure although `c` is still evaluated in `w`. Counting the
/// occurrences catches it. Their [`Reach`] keeps an occurrence in a branch
/// apart from one evaluated on every row, so a rewrite that moves one out
/// of a branch changes the count too (R3-F2).
pub(crate) type Demand = BTreeMap<(Reach, String), u64>;

fn add(out: &mut Demand, from: &Demand, times: u64) {
    for (key, count) in from {
        let entry = out.entry(key.clone()).or_default();
        *entry = entry.saturating_add(count.saturating_mul(times));
    }
}

/// Whether `after` evaluates every occurrence `before` does.
fn covers(after: &Demand, before: &Demand) -> bool {
    before
        .iter()
        .all(|(key, count)| after.get(key).is_some_and(|n| n >= count))
}

/// Functions that evaluate an argument after the first only where the
/// first is NULL. DataFusion simplifies each into a `CASE`.
const LAZY_FUNCTIONS: &[&str] = &["coalesce", "nvl", "nvl2", "ifnull"];

/// The occurrences of the fallible subexpressions of `expr`, `expr`
/// included, each with its [`Reach`].
fn raw_atoms(expr: &Expr, schema: &DFSchema, out: &mut Demand) {
    // `apply` visits a node before its children, so a node marks the
    // children it evaluates only on some rows before they are visited.
    let mut branches: std::collections::HashSet<*const Expr> = std::collections::HashSet::new();
    let _ = expr.apply(|e| {
        let reach = if branches.contains(&(e as *const Expr)) {
            Reach::Branch
        } else {
            Reach::Every
        };
        if fallible_node(e, schema) {
            *out.entry((reach, key(e))).or_default() += 1;
        }
        let mut branch = |child: &Expr| {
            branches.insert(child as *const Expr);
        };
        match (reach, e) {
            (Reach::Branch, e) => {
                let _ = e.apply_children(|child| {
                    branch(child);
                    Ok(TreeNodeRecursion::Continue)
                });
            }
            (Reach::Every, Expr::Case(case)) => {
                for (i, (when, then)) in case.when_then_expr.iter().enumerate() {
                    if i > 0 {
                        branch(when);
                    }
                    branch(then);
                }
                if let Some(otherwise) = &case.else_expr {
                    branch(otherwise);
                }
            }
            (Reach::Every, Expr::ScalarFunction(f)) if LAZY_FUNCTIONS.contains(&f.name()) => {
                f.args.iter().skip(1).for_each(&mut branch);
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// Whether `expr` holds a bitwise operator (R3-F1 of #273).
///
/// The simplifier's bitwise identities are wrong in value: `-A & A`, `-A |
/// A` and `-A ^ A` become 0, -1 and -1 (`-2 & 2` is 2), `-(A & B)` becomes
/// `-A | -B`, and `A ^ A` and `A ^ B ^ A` drop `A` with no nullability
/// gate, so a NULL `A` gives 0 or `B`. LTSeq's lambdas lower no bitwise
/// operator, but `filter_where`'s SQL writes them. The guarded simplifier
/// keeps every bitwise operation, and simplifies its operands on their own.
fn has_bitwise(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::BitwiseAnd
                    | Operator::BitwiseOr
                    | Operator::BitwiseXor
                    | Operator::BitwiseShiftLeft
                    | Operator::BitwiseShiftRight,
                ..
            })
        ))
    })
    .unwrap_or(true)
}

/// How many times `expr` reads each column.
fn column_occurrences(expr: &Expr) -> Vec<(Column, u64)> {
    let mut out: Vec<(Column, u64)> = Vec::new();
    let _ = expr.apply(|e| {
        if let Expr::Column(column) = e {
            match out.iter_mut().find(|(c, _)| c == column) {
                Some((_, n)) => *n += 1,
                None => out.push((column.clone(), 1)),
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

type Atoms = Rc<Demand>;

/// The fallible expressions that the values of a plan's columns are
/// computed from, through the projections, aggregates and windows below.
#[derive(Default)]
struct Inliner {
    memo: RefCell<HashMap<(usize, String), Atoms>>,
}

impl Inliner {
    /// The fallible expressions `exprs` evaluate, over `plan`'s inputs: the
    /// ones they hold, and those of the columns they read, once for each
    /// time they read the column.
    fn exprs(&self, plan: &LogicalPlan, exprs: &[Expr]) -> Demand {
        let schema = input_schema(plan);
        let mut out = Demand::new();
        for expr in exprs {
            raw_atoms(expr, &schema, &mut out);
            for (column, times) in column_occurrences(expr) {
                for input in plan.inputs() {
                    if let Some(index) = index_of(input.schema(), &column) {
                        add(&mut out, &self.column(input, index), times);
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

    fn compute_column(&self, plan: &LogicalPlan, index: usize) -> Demand {
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
                let mut out = Demand::new();
                for input in &u.inputs {
                    if index < input.schema().fields().len() {
                        add(&mut out, &self.column(input, index), 1);
                    }
                }
                out
            }
            LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => Demand::new(),
            plan => {
                let column = Column::from(plan.schema().qualified_field(index));
                for input in plan.inputs() {
                    if let Some(i) = index_of(input.schema(), &column)
                        .or_else(|| same_position(plan, input, index))
                    {
                        return self.column(input, i).as_ref().clone();
                    }
                }
                Demand::new()
            }
        }
    }

    /// The fallible expressions `plan` demands on its input's rows, with
    /// those of the columns they read (§20.2): the predicate of a filter,
    /// the keys of a sort, grouping, `distinct` or join, the arguments of
    /// an aggregate or window function, an extension's expressions.
    fn demanded_by(&self, plan: &LogicalPlan) -> Demand {
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
    fn demanded_in(&self, plan: &LogicalPlan) -> Demand {
        let mut out = Demand::new();
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
                node => add(&mut out, &self.demanded_by(node), 1),
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
        let mut atoms = Demand::new();
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
    /// Accept a rewrite only if every join keeps its inputs, in their
    /// places (`eliminate_cross_join`, X5).
    KeepJoinShape,
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
            Guard::KeepJoinShape => {
                let rewritten = self.inner.rewrite(plan.clone(), config)?;
                if rewritten.transformed && join_shape(&rewritten.data) != join_shape(&plan) {
                    return Ok(Transformed::no(plan));
                }
                Ok(rewritten)
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
            Guard::KeepFailures { own: false } => {
                let rewritten = self.inner.rewrite(plan.clone(), config)?;
                if !rewritten.transformed {
                    return Ok(rewritten);
                }
                // Every occurrence of a demanded expression that can fail
                // is still demanded: one left elsewhere in the plan does
                // not stand in for one the rewrite removed (R2-F1).
                let before = Inliner::default().demanded_in(&plan);
                if before.is_empty() {
                    return Ok(rewritten);
                }
                let after = Inliner::default().demanded_in(&rewritten.data);
                Ok(if covers(&after, &before) {
                    rewritten
                } else {
                    Transformed::no(plan)
                })
            }
            Guard::KeepFailures { own: true } => {
                let rewritten = self.inner.rewrite(plan.clone(), config)?;
                if !rewritten.transformed
                    || (keeps_each_expression(&plan, &rewritten.data)
                        && !plan.expressions().iter().any(has_bitwise))
                {
                    return Ok(rewritten);
                }
                // Simplify what does keep them, expression by expression
                // and subexpression by subexpression. Refusing the node's
                // rewrite whole would also refuse rewrites DataFusion
                // requires: `coalesce` and `now()` fail at execution unless
                // they are simplified.
                simplify_keeping_failures(plan, config)
            }
        }
    }
}

/// The joins of `plan` and what each joins, as a string: a join is
/// `(left JOIN right)`, and any other input is the names of its columns,
/// which stay the same through the filters and projections that
/// `eliminate_cross_join` moves.
///
/// A join returns its rows in an order its inputs decide (the probe side's
/// for a hash join), so a rule that flattens a tree of inner joins and
/// builds it again in another shape changes the order of the rows below
/// the root sort, and of a table without a declared order. With the same
/// shape, it only moves equalities between the filter and the join keys.
fn join_shape(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Join(join) => format!(
            "({} {} JOIN {})",
            join_shape(&join.left),
            join.join_type,
            join_shape(&join.right)
        ),
        plan if plan.inputs().len() == 1 && !matches!(plan, LogicalPlan::Aggregate(_)) => {
            join_shape(plan.inputs()[0])
        }
        plan if plan.inputs().is_empty() || !plan_has_join(plan) => {
            let mut names: Vec<&str> = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            names.sort_unstable();
            format!("[{}]", names.join(","))
        }
        plan => plan
            .inputs()
            .into_iter()
            .map(join_shape)
            .collect::<Vec<_>>()
            .join(" | "),
    }
}

fn plan_has_join(plan: &LogicalPlan) -> bool {
    plan.exists(|p| Ok(matches!(p, LogicalPlan::Join(_))))
        .unwrap_or(true)
}

/// Whether `rewritten`, `simplify_expressions`' rewrite of `plan`, keeps
/// every occurrence of an expression that can fail in each of the node's
/// expressions, and adds none (R2-F1).
///
/// Each expression is compared with its own rewrite: the occurrences of
/// two expressions are demanded on different rows, so one expression's
/// occurrence does not stand in for another's. A rewrite that changes the
/// node's shape, as one that puts a projection above an aggregate does, is
/// accepted only where neither side evaluates anything that can fail.
fn keeps_each_expression(plan: &LogicalPlan, rewritten: &LogicalPlan) -> bool {
    let inliner = Inliner::default();
    let each = |plan: &LogicalPlan| -> Vec<Demand> {
        plan.expressions()
            .iter()
            .map(|e| inliner.exprs(plan, std::slice::from_ref(e)))
            .collect()
    };
    let before = each(plan);
    let after = each(rewritten);
    if std::mem::discriminant(plan) == std::mem::discriminant(rewritten)
        && before.len() == after.len()
    {
        return before == after;
    }
    // A projection the rewrite put above the node computes from below.
    let mut after: Vec<Demand> = after;
    if let LogicalPlan::Projection(p) = rewritten {
        after.extend(each(&p.input));
    }
    before.iter().chain(&after).all(Demand::is_empty)
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

    // An expression is simplified whole only where that keeps every
    // occurrence of what can fail in it, and adds none: a set would let a
    // kept occurrence stand in for a dropped one (R2-F1).
    fn keep(
        expr: Expr,
        simplify: &dyn Fn(Expr) -> Result<Expr>,
        atoms: &dyn Fn(&Expr) -> Demand,
        info: &SimplifyContext,
    ) -> Result<Expr> {
        // A bitwise operation is kept, its operands simplified on their
        // own: the simplifier's bitwise identities are wrong in value
        // (R3-F1).
        if !has_bitwise(&expr) {
            let simplified = simplify(expr.clone())?;
            if atoms(&simplified) == atoms(&expr) {
                return Ok(simplified);
            }
        }
        let expr = expr
            .map_children(|child| keep(child, simplify, atoms, info).map(Transformed::yes))?
            .data;
        if let Expr::ScalarFunction(f) = &expr {
            if let ExprSimplifyResult::Simplified(hooked) = f.func.simplify(f.args.clone(), info)? {
                // A hook may evaluate an argument twice (`coalesce(a, b)`
                // becomes `CASE WHEN a IS NOT NULL THEN a ELSE b END`), the
                // second time in a branch, on rows where the first one did
                // not fail. It may not drop one, add another, or move one
                // out of a branch.
                let (hooked_atoms, atoms) = (atoms(&hooked), atoms(&expr));
                let added_in_a_branch = |(reach, key): &(Reach, String)| {
                    atoms.contains_key(&(*reach, key.clone()))
                        || (*reach == Reach::Branch
                            && atoms.contains_key(&(Reach::Every, key.clone())))
                };
                if covers(&hooked_atoms, &atoms) && hooked_atoms.keys().all(added_in_a_branch) {
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

/// How a [`GuardedPhysical`] rule is held to its rating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhysicalGuard {
    /// Run the rule only on a plan in which no expression can fail.
    NoFailures,
    /// Run the rule with `join_reordering` off, so that it chooses how to
    /// execute a join without swapping its inputs (`join_selection`, X5).
    NoSwap,
    /// Do not run the rule (`OptimizeAggregateOrder`, X5).
    Off,
}

/// A DataFusion physical rule held to its rating by `guard`. It keeps the
/// rule's name.
pub(crate) struct GuardedPhysical {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    guard: PhysicalGuard,
}

impl GuardedPhysical {
    pub(crate) fn new(
        inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
        guard: PhysicalGuard,
    ) -> Self {
        GuardedPhysical { inner, guard }
    }
}

impl fmt::Debug for GuardedPhysical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GuardedPhysical({}, {:?})", self.inner.name(), self.guard)
    }
}

impl PhysicalOptimizerRule for GuardedPhysical {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.guard {
            PhysicalGuard::NoFailures if physical_plan_can_fail(&plan) => Ok(plan),
            PhysicalGuard::NoFailures => self.inner.optimize(plan, config),
            PhysicalGuard::NoSwap => {
                let mut config = config.clone();
                config.optimizer.join_reordering = false;
                self.inner.optimize(plan, &config)
            }
            PhysicalGuard::Off => Ok(plan),
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

pub(crate) fn barrier(plan: LogicalPlan) -> LogicalPlan {
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
        guard_keys(plan, None, false)
    }
}

/// `plan` with barriers below the filters above it. `under_filter` is
/// `Some` below a filter that `push_down_filter` could move down to `plan`,
/// and says whether its predicate can fail. `barred` says `plan` is the
/// input of a barrier already.
fn guard_keys(
    plan: LogicalPlan,
    under_filter: Option<bool>,
    barred: bool,
) -> Result<Transformed<LogicalPlan>> {
    if is_barrier(&plan) {
        return plan.map_children(|child| guard_keys(child, None, true));
    }
    // A limit, a scan and an extension stop a filter; a filter starts one.
    let below = match &plan {
        LogicalPlan::Limit(_) | LogicalPlan::Extension(_) => None,
        LogicalPlan::Filter(f) => Some(can_fail_over(&f.input, std::slice::from_ref(&f.predicate))),
        _ => under_filter,
    };
    let guarded = match under_filter {
        Some(upper_fails) if !barred => match &plan {
            // Chained filters (R2-F2). Each demands its predicate on every
            // row it reads, so the lower one on rows the upper one would
            // remove, and the upper one only on rows the lower one keeps.
            // `push_down_filter` merges them into `lower AND upper` and puts
            // the cheaper conjunct first, after which either suppresses the
            // other's errors where it is FALSE, and evaluates the upper one
            // on rows the lower one removes. Where either can fail, the
            // upper one stops above the lower one.
            LogicalPlan::Filter(_) if upper_fails || below == Some(true) => KeysGuard::Above,
            _ => keys_guard(&plan),
        },
        _ => KeysGuard::None,
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
    let mut own = Demand::new();
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

/// Keeps the order of every stable sort LTSeq plans (X5, R2-F4 of #273).
///
/// `sort` is stable: its last key is each row's position in the input
/// (`sort_impl`), so no two rows tie. Above it, an operator that requires an
/// order asks only for the declared keys: a window `ORDER BY k`, or the
/// root sort that re-imposes the declared order. `EnsureRequirements` then
/// replaces the stable sort with a sort by those keys, or removes it under
/// the root's, as either meets the requirement; DataFusion's sort is not
/// stable, so rows equal on `k` came back in any order, and `shift(1)` over
/// them read whichever row the sort put before, even on one partition.
///
/// This rule runs before `EnsureRequirements` and puts a
/// [`StableOrderExec`] above each such sort. It requires its input in the
/// sort's full order, on one partition, so `EnsureRequirements` has to keep
/// a sort by every key, position included; above it the rows are in that
/// order, which meets any requirement of a prefix of its keys.
#[derive(Debug)]
pub(crate) struct KeepStableSorts;

impl PhysicalOptimizerRule for KeepStableSorts {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_expr::expressions::Column as PhysicalColumn;
        use datafusion::physical_plan::sorts::sort::SortExec;
        plan.transform_up(|node| {
            let Some(sort) = node.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(node));
            };
            let stable = sort.expr().last().is_some_and(|key| {
                key.expr
                    .downcast_ref::<PhysicalColumn>()
                    .is_some_and(|c| c.name().starts_with(crate::ops::sort::POSITION_PREFIX))
            });
            if !stable {
                return Ok(Transformed::no(node));
            }
            let ordering = sort.expr().clone();
            Ok(Transformed::yes(Arc::new(StableOrderExec {
                input: node,
                ordering,
            }) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_keep_stable_sorts"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Its input's batches, unchanged, required in `ordering` on one partition
/// ([`KeepStableSorts`]).
#[derive(Debug)]
pub(crate) struct StableOrderExec {
    input: Arc<dyn ExecutionPlan>,
    ordering: datafusion::physical_expr::LexOrdering,
}

impl DisplayAs for StableOrderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StableOrderExec: [{}]", self.ordering)
    }
}

impl ExecutionPlan for StableOrderExec {
    fn name(&self) -> &str {
        "StableOrderExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition]
    }

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        vec![Some(datafusion::physical_expr::OrderingRequirements::from(
            self.ordering.clone(),
        ))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    // A limit above it may still reach the sort, which then keeps only
    // the first rows (TopK).
    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for key in self.ordering.iter() {
            if f(&key.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StableOrderExec {
            input: children.swap_remove(0),
            ordering: self.ordering.clone(),
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
        "eliminate_cross_join" => Some(Guard::KeepJoinShape),
        _ => None,
    }
}

/// How the session holds the physical rule `name` to its rating, if it is
/// rated Restricted or Disabled and wrapped.
pub(crate) fn physical_guard(name: &str) -> Option<PhysicalGuard> {
    match name {
        "LimitedDistinctAggregation"
        | "LimitPushPastWindows"
        | "PushdownSort"
        | "FilterPushdown(Post)" => Some(PhysicalGuard::NoFailures),
        "join_selection" => Some(PhysicalGuard::NoSwap),
        "OptimizeAggregateOrder" => Some(PhysicalGuard::Off),
        _ => None,
    }
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
        // R2-F1: the occurrence absorption would drop is demanded where `a`
        // is NULL, and the one `w` keeps only where `a` is TRUE, so `v`
        // keeps its own.
        let df = t
            .clone()
            .select(vec![
                a.clone()
                    .or(a.clone().and(fallible().gt(lit(1_i64))))
                    .alias("v"),
                datafusion::logical_expr::when(a.clone(), fallible())
                    .otherwise(lit(0_i64))
                    .unwrap()
                    .alias("w"),
            ])
            .unwrap();
        let plan = logical(&df);
        let v = plan
            .lines()
            .find(|l| l.contains(" AS v"))
            .unwrap_or_else(|| panic!("{plan}"));
        assert_eq!(v.matches("Int64(10) / ").count(), 2, "{plan}");
        // The same within one expression: two occurrences stay two.
        let df = t
            .clone()
            .select(vec![(datafusion::logical_expr::when(a.clone(), fallible())
                .otherwise(lit(0_i64))
                .unwrap()
                + datafusion::logical_expr::when(
                    a.clone().or(a.clone().and(fallible().gt(lit(1_i64)))),
                    lit(1_i64),
                )
                .otherwise(lit(0_i64))
                .unwrap())
            .alias("v")])
            .unwrap();
        let plan = logical(&df);
        assert_eq!(plan.matches("Int64(10) / ").count(), 2, "{plan}");
        // A rewrite that drops nothing that can fail still applies.
        let df = t.select(vec![(col("x") * lit(0_i64)).alias("v")]).unwrap();
        assert!(logical(&df).contains("Int64(0) AS v"), "{}", logical(&df));
    }

    /// R3-F1: the simplifier's bitwise identities are wrong in value
    /// (`-2 & 2` is 2, not 0), and `filter_where`'s SQL writes bitwise
    /// operators, so the guarded simplifier keeps every bitwise operation.
    /// Unguarded, `-x & x = 0` became TRUE and the filter was removed.
    #[test]
    fn plan_guard_bitwise_identities() {
        use datafusion::logical_expr::binary_expr;
        let session = session();
        let t = table(&session, &[("x", vec![2, 3, 4])]);
        let negated = || Expr::Negative(Box::new(col("x")));
        let op = |l: Expr, op: Operator, r: Expr| binary_expr(l, op, r);
        for (name, expr, rows) in [
            (
                "-x & x = 0",
                op(negated(), Operator::BitwiseAnd, col("x")).eq(lit(0_i64)),
                0,
            ),
            (
                "-x & x = 2",
                op(negated(), Operator::BitwiseAnd, col("x")).eq(lit(2_i64)),
                1,
            ),
            (
                "x | -x = -1",
                op(col("x"), Operator::BitwiseOr, negated()).eq(lit(-1_i64)),
                // `3 | -3` is -1; `2 | -2` is -2.
                1,
            ),
            (
                "-x ^ x = -1",
                op(negated(), Operator::BitwiseXor, col("x")).eq(lit(-1_i64)),
                0,
            ),
            (
                "-(x & 3) = -2",
                Expr::Negative(Box::new(op(col("x"), Operator::BitwiseAnd, lit(3_i64))))
                    .eq(lit(-2_i64)),
                1,
            ),
        ] {
            let df = t.clone().filter(expr).unwrap();
            let plan = logical(&df);
            assert!(
                plan.contains("Filter:"),
                "{name}: the filter is gone\n{plan}"
            );
            assert_eq!(collect(&df).unwrap(), rows, "{name}\n{plan}");
        }
    }

    /// R3-F2: a `CASE` evaluates a branch only on the rows that reach it.
    /// The simplifier turned `CASE WHEN x = 0 THEN false ELSE 10 / x > 1
    /// END` into `x != 0 AND 10 / x > 1`, which keeps the one occurrence of
    /// the division but evaluates it on every row, and raised on `x = 0`.
    /// The guarded simplifier keeps the `CASE` where a branch can fail.
    #[test]
    fn plan_guard_case_rewrites() {
        use datafusion::logical_expr::when;
        let session = session();
        let t = table(&session, &[("x", vec![0, 2])]);
        let guarded = |value: Expr| {
            when(col("x").eq(lit(0_i64)), lit(false))
                .otherwise(value)
                .unwrap()
        };
        let df = t
            .clone()
            .filter(guarded(fallible().gt(lit(1_i64))))
            .unwrap();
        let plan = logical(&df);
        assert!(plan.contains("CASE WHEN"), "{plan}");
        assert_eq!(collect(&df).unwrap(), 1, "{plan}");
        // The same in a projection, and nested in a branch.
        let df = t
            .clone()
            .select(vec![guarded(guarded(fallible().gt(lit(1_i64)))).alias("v")])
            .unwrap();
        let plan = logical(&df);
        assert!(plan.contains("CASE WHEN"), "{plan}");
        assert_eq!(collect(&df).unwrap(), 2, "{plan}");
        // Where no branch can fail, the rewrite still applies.
        let df = t.filter(guarded(col("x").gt(lit(1_i64)))).unwrap();
        let plan = logical(&df);
        assert!(!plan.contains("CASE"), "{plan}");
        assert_eq!(collect(&df).unwrap(), 1, "{plan}");
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
        // R2-F2: chained filters are not merged where either can fail. The
        // lower one demands its predicate on every row, the upper one only
        // on the rows the lower one keeps.
        let chained = |lower: Expr, upper: Expr| t().filter(lower).unwrap().filter(upper).unwrap();
        let guarded = chained(fallible().gt(lit(1_i64)), col("x").not_eq(lit(0_i64)));
        let plan = logical(&guarded);
        assert_eq!(plan.matches("Filter:").count(), 2, "{plan}");
        assert!(
            depth(&plan, "x != Int64(0)") < depth(&plan, "DemandBarrier"),
            "{plan}"
        );
        assert!(collect(&guarded).is_err(), "{plan}");
        let guarded = chained(col("x").not_eq(lit(0_i64)), fallible().gt(lit(1_i64)));
        let plan = logical(&guarded);
        assert!(plan.contains("DemandBarrier"), "{plan}");
        assert_eq!(collect(&guarded).unwrap(), 2, "{plan}");
        let free = chained(col("x").gt(lit(0_i64)), col("x").not_eq(lit(2_i64)));
        let plan = logical(&free);
        assert!(!plan.contains("DemandBarrier"), "{plan}");
        assert_eq!(plan.matches("Filter:").count(), 1, "{plan}");
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

    /// X5: a hash join returns its rows in its probe side's order, so
    /// swapping the inputs changes the order of an inner join's rows.
    /// `join_selection` still picks how a join runs, but keeps its sides
    /// where DataFusion's rule would build on the smaller one.
    #[test]
    fn plan_guard_join_selection() {
        let joined = |session: &SessionContext| {
            let big = table(session, &[("a", (0..1000).collect())]);
            let small = table(session, &[("k", vec![1, 2, 3])]);
            physical(
                &big.join(small, JoinType::Inner, &["a"], &["k"], None)
                    .unwrap(),
            )
        };
        let guarded = joined(&session());
        assert!(guarded.contains("on=[(a@0, k@0)]"), "{guarded}");
        // Unguarded, DataFusion builds on the small right input.
        let swapped = joined(&SessionContext::new());
        assert!(swapped.contains("on=[(k@0, a@0)]"), "{swapped}");
    }

    /// X5: over an input in descending order, DataFusion turns a
    /// `first_value` ordered ascending into a `last_value` ordered
    /// descending, which takes the other end of a run of ties. LTSeq keeps
    /// the aggregate it planned.
    #[test]
    fn plan_guard_optimize_aggregate_order() {
        use datafusion::functions_aggregate::expr_fn::first_value;
        let aggregated = |session: &SessionContext| {
            let batch = batch(&[("x", vec![3, 2, 2, 1]), ("v", vec![1, 2, 3, 4])]);
            let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
                .unwrap()
                .with_sort_order(vec![vec![col("x").sort(false, false)]]);
            let t = session.read_table(Arc::new(table)).unwrap();
            physical(
                &t.aggregate(
                    vec![],
                    vec![first_value(col("v"), vec![col("x").sort(true, false)])],
                )
                .unwrap(),
            )
        };
        let guarded = aggregated(&session());
        assert!(!guarded.contains("last_value"), "{guarded}");
        let reversed = aggregated(&SessionContext::new());
        assert!(reversed.contains("last_value"), "{reversed}");
    }

    /// X5: a filter over a tree of inner joins is rebuilt in the shape
    /// `eliminate_cross_join` chooses, which changes which side each join
    /// probes. LTSeq keeps a rewrite only where every join keeps its
    /// inputs.
    #[test]
    fn plan_guard_eliminate_cross_join() {
        let shaped = |session: &SessionContext| {
            let a = table(session, &[("a", vec![1, 2])]);
            let b = table(session, &[("b", vec![1, 2]), ("b2", vec![1, 2])]);
            let c = table(session, &[("c", vec![1, 2])]);
            let right = b.join(c, JoinType::Inner, &["b2"], &["c"], None).unwrap();
            let df = a
                .join(right, JoinType::Inner, &["a"], &["b"], None)
                .unwrap()
                .filter(col("a").gt(lit(0_i64)))
                .unwrap();
            let plan = df.into_optimized_plan().unwrap();
            join_shape(&plan)
        };
        let guarded = shaped(&session());
        assert_eq!(
            guarded, "([a] Inner JOIN ([b,b2] Inner JOIN [c]))",
            "the joins keep their inputs"
        );
        let rebuilt = shaped(&SessionContext::new());
        assert_ne!(rebuilt, guarded, "DataFusion's rule reshapes the tree");
    }

    /// X5: `EnsureRequirements` meets a window's `ORDER BY k` over a stable
    /// sort by `(k, position)` with a sort by `k` alone, which is not
    /// stable. `ltseq_keep_stable_sorts` keeps the sort by every key.
    #[test]
    fn plan_guard_ensure_requirements() {
        use datafusion::functions_window::expr_fn::lag;
        let windowed = |session: &SessionContext| {
            let position = format!("{}0", crate::ops::sort::POSITION_PREFIX);
            let t = table(
                session,
                &[("k", vec![1, 0, 1, 0]), ("i", vec![0, 1, 2, 3]), (&position, vec![0, 1, 2, 3])],
            );
            let sorted = t
                .sort(vec![col("k").sort(true, false), col(position.as_str()).sort(true, false)])
                .unwrap()
                .select(vec![col("k"), col("i")])
                .unwrap();
            physical(
                &sorted
                    .with_column(
                        "p",
                        lag(col("i"), Some(1), None)
                            .order_by(vec![col("k").sort(true, false)])
                            .build()
                            .unwrap(),
                    )
                    .unwrap(),
            )
        };
        let guarded = windowed(&session());
        assert!(guarded.contains("StableOrderExec"), "{guarded}");
        assert!(guarded.contains("__ltseq_position_0@2 ASC"), "{guarded}");
        let coarsened = windowed(&SessionContext::new());
        assert!(!coarsened.contains("__ltseq_position_0@2 ASC"), "{coarsened}");
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
