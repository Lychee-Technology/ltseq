//! m4: either operand of `&` and `|` guards the other (D10, M16, #252).
//!
//! The contract makes `a & b` FALSE wherever either operand is FALSE, and
//! `a | b` TRUE wherever either is TRUE, even where the other operand would
//! raise. DataFusion's `BinaryExpr` evaluates its left operand on every row
//! and pre-selects the right one only when the left has no NULL and leaves
//! at most 20% of the rows open, so today whether `fallible & guard` raises
//! depends on operand order and on the data.
//!
//! This rule replaces each `AND`/`OR` of a filter predicate with a fallible
//! operand (one that calls a checked function of `super::checked`, so m4
//! runs with `checked` on) by [`MaskedLogical`], which evaluates it the way
//! [`Masked`] says. With one fallible operand, `Select` evaluates the other
//! first and the fallible one only on the rows it leaves open, whichever
//! side it is written on. With two, no order works on its own, so the first
//! is evaluated with its errors kept as a mask (`Errors` below) and the
//! second on the rows the first leaves open or failed on. `Errors`
//! evaluates both on every row with errors kept as masks; `Optimistic`
//! evaluates as today and redoes a batch as `Errors` only when it raises.
//! A row raises only when an operand failed there and the other did not
//! decide it.
//!
//! Errors are kept as masks only for the expression shapes m4 measures: a
//! checked function, and a binary operator over expressions that are
//! either infallible or of these shapes. Any other fallible expression is
//! evaluated as today and may raise. That includes an `AND` or `OR` nested
//! in another: the rule rewrites the inner one into its own
//! [`MaskedLogical`], which raises for a row its own operands leave
//! undecided instead of passing the error out, so the outer operand never
//! gets to decide that row. Each m4 pipeline has one `&` or `|`, so m4
//! prices that flat form only; M16 has to carry errors through nested
//! logical operators before raising.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::compute::kernels::boolean::{and_kleene, or_kleene};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_expr_common::utils::scatter;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::ExecutionPlan;

use super::checked::{self, Op, OVERFLOW};
use super::Masked;

/// Whether evaluating `expr` can raise: it calls a checked function.
fn fallible(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.exists(|e| {
        Ok(e.downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|f| Op::of(f.name()).is_some()))
    })
    .unwrap_or(false)
}

/// Replaces the `AND`s and `OR`s with a fallible operand in each filter.
#[derive(Debug)]
pub(crate) struct MaskedFilters {
    pub(crate) mode: Masked,
}

impl PhysicalOptimizerRule for MaskedFilters {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(filter) = node.downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(node));
            };
            let predicate = Arc::clone(filter.predicate()).transform_up(|e| {
                let Some(binary) = e.downcast_ref::<BinaryExpr>() else {
                    return Ok(Transformed::no(e));
                };
                if !matches!(binary.op(), Operator::And | Operator::Or) {
                    return Ok(Transformed::no(e));
                }
                let (left, right) = (binary.left(), binary.right());
                if !fallible(left) && !fallible(right) {
                    return Ok(Transformed::no(e));
                }
                Ok(Transformed::yes(Arc::new(MaskedLogical {
                    op: *binary.op(),
                    left: Arc::clone(left),
                    right: Arc::clone(right),
                    mode: self.mode,
                })
                    as Arc<dyn PhysicalExpr>))
            })?;
            if !predicate.transformed {
                return Ok(Transformed::no(node));
            }
            let rebuilt = FilterExecBuilder::from(filter)
                .with_predicate(predicate.data)
                .build()?;
            Ok(Transformed::yes(Arc::new(rebuilt) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ltseq_m0_masked_filters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Values of an expression with the rows whose evaluation failed.
struct Kept {
    values: ColumnarValue,
    errors: Option<BooleanBuffer>,
}

fn union(a: Option<BooleanBuffer>, b: Option<BooleanBuffer>) -> Option<BooleanBuffer> {
    match (a, b) {
        (Some(a), Some(b)) => Some(&a | &b),
        (a, b) => a.or(b),
    }
}

/// `expr` over `batch` with errors kept as a mask (see the module comment
/// for the shapes this covers).
fn evaluate_kept(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<Kept> {
    let rows = batch.num_rows();
    if !fallible(expr) {
        return Ok(Kept {
            values: expr.evaluate(batch)?,
            errors: None,
        });
    }
    if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
        if let (Some(op), [left, right]) = (Op::of(function.name()), function.args()) {
            let (left, right) = (evaluate_kept(left, batch)?, evaluate_kept(right, batch)?);
            let (values, overflowed) =
                checked::wrapping_with_overflow(op, &left.values, &right.values, rows)?;
            return Ok(Kept {
                values: ColumnarValue::Array(Arc::new(values)),
                errors: union(union(left.errors, right.errors), overflowed),
            });
        }
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        // Evaluate the fallible children with their errors kept, append
        // their values to the batch, and apply the operator to the
        // appended columns in their place.
        let schema = batch.schema();
        let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
        let mut columns = batch.columns().to_vec();
        let mut errors = None;
        let mut child = |e: &Arc<dyn PhysicalExpr>| -> Result<Arc<dyn PhysicalExpr>> {
            if !fallible(e) {
                return Ok(Arc::clone(e));
            }
            let kept = evaluate_kept(e, batch)?;
            errors = union(errors.take(), kept.errors);
            let array = kept.values.into_array(rows)?;
            let name = format!("__m0_kept_{}", columns.len());
            fields.push(Arc::new(Field::new(&name, array.data_type().clone(), true)));
            columns.push(array);
            Ok(Arc::new(Column::new(&name, columns.len() - 1)))
        };
        let (left, right) = (child(binary.left())?, child(binary.right())?);
        let extended = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let values = BinaryExpr::new(left, *binary.op(), right).evaluate(&extended)?;
        return Ok(Kept { values, errors });
    }
    Ok(Kept {
        values: expr.evaluate(batch)?,
        errors: None,
    })
}

/// A boolean operand's values, with its failed rows.
struct Operand {
    values: BooleanArray,
    errors: Option<BooleanBuffer>,
}

impl Operand {
    fn of(kept: Kept, rows: usize) -> Result<Operand> {
        Ok(Operand {
            values: kept.values.into_array(rows)?.as_boolean().clone(),
            errors: kept.errors,
        })
    }

    /// Rows where this operand is `decider` (FALSE for `&`, TRUE for `|`)
    /// and did not fail: the other operand is not needed there.
    fn decided(&self, decider: bool) -> BooleanBuffer {
        let values = self.values.values();
        let hit = if decider { values.clone() } else { !values };
        let hit = match self.values.nulls() {
            Some(nulls) => &hit & nulls.inner(),
            None => hit,
        };
        match &self.errors {
            Some(errors) => &hit & &!errors,
            None => hit,
        }
    }

    /// The values with failed rows turned NULL, for the Kleene combine.
    fn without_errors(self) -> BooleanArray {
        match self.errors {
            None => self.values,
            Some(errors) => {
                let ok = NullBuffer::new(!&errors);
                let nulls = NullBuffer::union(self.values.nulls(), Some(&ok));
                BooleanArray::new(self.values.values().clone(), nulls)
            }
        }
    }
}

/// `left AND right` or `left OR right`, evaluated as [`Masked`] says.
#[derive(Debug, Eq)]
pub(crate) struct MaskedLogical {
    op: Operator,
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    mode: Masked,
}

impl std::hash::Hash for MaskedLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        self.left.hash(state);
        self.right.hash(state);
        self.mode.hash(state);
    }
}

impl PartialEq for MaskedLogical {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
            && self.left.eq(&other.left)
            && self.right.eq(&other.right)
            && self.mode == other.mode
    }
}

impl MaskedLogical {
    /// The value that decides the result on its own.
    fn decider(&self) -> bool {
        self.op == Operator::Or
    }

    fn combine(&self, left: BooleanArray, right: BooleanArray) -> Result<ArrayRef> {
        let result = match self.op {
            Operator::And => and_kleene(&left, &right)?,
            _ => or_kleene(&left, &right)?,
        };
        Ok(Arc::new(result))
    }

    /// `other` evaluated, errors kept, only on the rows `open` marks, and
    /// scattered back to the batch's rows (NULL elsewhere).
    fn evaluate_open(
        other: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
        open: &BooleanBuffer,
        keep_errors: bool,
    ) -> Result<Operand> {
        let rows = batch.num_rows();
        let selected = open.count_set_bits();
        let evaluate = |b: &RecordBatch| -> Result<Operand> {
            let kept = if keep_errors {
                evaluate_kept(other, b)?
            } else {
                Kept {
                    values: other.evaluate(b)?,
                    errors: None,
                }
            };
            Operand::of(kept, b.num_rows())
        };
        if selected == rows {
            return evaluate(batch);
        }
        let mask = BooleanArray::new(open.clone(), None);
        if selected == 0 {
            let values = scatter(&mask, &BooleanArray::new_null(0))?;
            return Ok(Operand {
                values: values.as_boolean().clone(),
                errors: None,
            });
        }
        let part = evaluate(&filter_record_batch(batch, &mask)?)?;
        let values = scatter(&mask, &part.values)?.as_boolean().clone();
        let errors = match part.errors {
            Some(errors) => {
                let spread = scatter(&mask, &BooleanArray::new(errors, None))?;
                let spread = spread.as_boolean();
                let set = match spread.nulls() {
                    Some(nulls) => spread.values() & nulls.inner(),
                    None => spread.values().clone(),
                };
                Some(set)
            }
            None => None,
        };
        Ok(Operand { values, errors })
    }

    /// Raise when an operand failed on a row the other did not decide.
    fn result(&self, left: Operand, right: Operand) -> Result<ArrayRef> {
        let failed = union(left.errors.clone(), right.errors.clone());
        if let Some(failed) = failed {
            let decided = &left.decided(self.decider()) | &right.decided(self.decider());
            let raised = &failed & &!&decided;
            if let Some(row) = raised.set_indices().next() {
                return datafusion::common::exec_err!(
                    "{OVERFLOW}: an operand of {} overflows on row {row} of a batch, \
                     where the other operand does not decide the result",
                    self.op
                );
            }
        }
        self.combine(left.without_errors(), right.without_errors())
    }

    fn select(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let rows = batch.num_rows();
        let decider = self.decider();
        let (left_fallible, right_fallible) = (fallible(&self.left), fallible(&self.right));
        if left_fallible && right_fallible {
            let first = Operand::of(evaluate_kept(&self.left, batch)?, rows)?;
            let open = !&first.decided(decider);
            let second = Self::evaluate_open(&self.right, batch, &open, true)?;
            return self.result(first, second);
        }
        // Evaluate the infallible operand first, wherever it is written.
        let (guard, other) = if left_fallible {
            (&self.right, &self.left)
        } else {
            (&self.left, &self.right)
        };
        let guard = Operand::of(
            Kept {
                values: guard.evaluate(batch)?,
                errors: None,
            },
            rows,
        )?;
        let open = !&guard.decided(decider);
        let other = Self::evaluate_open(other, batch, &open, false)?;
        // AND and OR commute, so the operands' order does not matter here.
        self.combine(guard.values, other.values)
    }

    fn errors(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let rows = batch.num_rows();
        let left = Operand::of(evaluate_kept(&self.left, batch)?, rows)?;
        let right = Operand::of(evaluate_kept(&self.right, batch)?, rows)?;
        self.result(left, right)
    }
}

impl fmt::Display for MaskedLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "m0_masked({} {} {})", self.left, self.op, self.right)
    }
}

impl PhysicalExpr for MaskedLogical {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let result = match self.mode {
            Masked::Off => {
                BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right))
                    .evaluate(batch)
            }
            Masked::Select => self.select(batch).map(ColumnarValue::Array),
            Masked::Errors => self.errors(batch).map(ColumnarValue::Array),
            Masked::Optimistic => {
                match BinaryExpr::new(Arc::clone(&self.left), self.op, Arc::clone(&self.right))
                    .evaluate(batch)
                {
                    Err(e) if e.to_string().contains(OVERFLOW) => {
                        self.errors(batch).map(ColumnarValue::Array)
                    }
                    other => other,
                }
            }
        }?;
        Ok(result)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [left, right] = <[_; 2]>::try_from(children)
            .map_err(|_| datafusion::common::DataFusionError::Internal("two children".into()))?;
        Ok(Arc::new(MaskedLogical {
            op: self.op,
            left,
            right,
            mode: self.mode,
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl, Signature, Volatility};
    use datafusion::physical_expr::expressions::lit;

    fn batch(g: Vec<i64>, a: Vec<i64>, b: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(g)),
                Arc::new(Int64Array::from(a)),
                Arc::new(Int64Array::from(b)),
            ],
        )
        .unwrap()
    }

    /// `name * 2 > 0` through the checked multiply, as the planner builds it.
    fn doubled_positive(name: &str, index: usize, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct Mul(Signature);
        impl ScalarUDFImpl for Mul {
            fn name(&self) -> &str {
                "m0_checked_mul"
            }
            fn signature(&self) -> &Signature {
                &self.0
            }
            fn return_type(&self, _: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int64)
            }
            fn invoke_with_args(
                &self,
                args: datafusion::logical_expr::ScalarFunctionArgs,
            ) -> Result<ColumnarValue> {
                let (values, overflowed) = checked::wrapping_with_overflow(
                    Op::Mul,
                    &args.args[0],
                    &args.args[1],
                    args.number_rows,
                )?;
                if overflowed.is_some() {
                    return datafusion::common::exec_err!("{OVERFLOW}");
                }
                Ok(ColumnarValue::Array(Arc::new(values)))
            }
        }
        let udf = Arc::new(ScalarUDF::new_from_impl(Mul(Signature::any(
            2,
            Volatility::Immutable,
        ))));
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(name, index));
        let product = Arc::new(ScalarFunctionExpr::new(
            "m0_checked_mul",
            udf,
            vec![column, lit(2i64)],
            Arc::new(Field::new("p", DataType::Int64, true)),
            Arc::new(ConfigOptions::default()),
        ));
        let _ = schema;
        Arc::new(BinaryExpr::new(product, Operator::Gt, lit(0i64)))
    }

    fn evaluate(
        mode: Masked,
        op: Operator,
        left: &str,
        right: &str,
        b: &RecordBatch,
    ) -> Result<Vec<Option<bool>>> {
        let schema = b.schema();
        let operand = |name: &str| -> Arc<dyn PhysicalExpr> {
            match name {
                "guard" => Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("g", 0)),
                    Operator::Gt,
                    lit(0i64),
                )),
                "a" => doubled_positive("a", 1, &schema),
                _ => doubled_positive("b", 2, &schema),
            }
        };
        let expr = MaskedLogical {
            op,
            left: operand(left),
            right: operand(right),
            mode,
        };
        let out = expr.evaluate(b)?.into_array(b.num_rows())?;
        Ok(out.as_boolean().iter().collect())
    }

    const BIG: i64 = i64::MAX / 2 + 1;

    #[test]
    fn a_fallible_operand_is_needed_only_where_the_other_does_not_decide() {
        // Row 0: guard TRUE, a fine. Row 1: guard FALSE, a overflows.
        let b = batch(vec![1, 0], vec![3, BIG], vec![1, 1]);
        for mode in [Masked::Select, Masked::Errors, Masked::Optimistic] {
            for (l, r) in [("guard", "a"), ("a", "guard")] {
                assert_eq!(
                    evaluate(mode, Operator::And, l, r, &b).unwrap(),
                    vec![Some(true), Some(false)],
                    "{mode:?} {l} & {r}"
                );
            }
        }
        // Today's evaluation raises for `a & guard`.
        assert!(evaluate(Masked::Off, Operator::And, "a", "guard", &b).is_err());
        // Where the guard does not decide, the overflow raises.
        let b = batch(vec![1, 1], vec![3, BIG], vec![1, 1]);
        for mode in [Masked::Select, Masked::Errors, Masked::Optimistic] {
            assert!(
                evaluate(mode, Operator::And, "guard", "a", &b).is_err(),
                "{mode:?}"
            );
        }
    }

    #[test]
    fn two_fallible_operands_guard_each_other() {
        // Row 0: both TRUE. Row 1: a overflows, b FALSE. Row 2: a FALSE,
        // b overflows.
        let b = batch(vec![0, 0, 0], vec![3, BIG, -1], vec![1, -1, BIG]);
        for mode in [Masked::Select, Masked::Errors, Masked::Optimistic] {
            assert_eq!(
                evaluate(mode, Operator::And, "a", "b", &b).unwrap(),
                vec![Some(true), Some(false), Some(false)],
                "{mode:?}"
            );
        }
        // `|` with both overflowing on one row raises.
        let b = batch(vec![0], vec![BIG], vec![BIG]);
        for mode in [Masked::Select, Masked::Errors] {
            assert!(evaluate(mode, Operator::Or, "a", "b", &b).is_err());
        }
    }
}
