//! m1: checked integer arithmetic (M5, #221).
//!
//! DataFusion 55 wraps on integer `+ - *` (its `BinaryExpr` can check, but
//! nothing turns that on) and in `sum`; `/`, `%`, `abs` and ltseq's `//`
//! already raise. The contract (§17.1) raises `ArithmeticOverflowError`
//! instead. This rule rewrites, on Int64 operands:
//!
//! - `a + b`, `a - b`, `a * b` into `m0_checked_{add,sub,mul}`;
//! - `power(CAST(a AS Float64), k)` for an integer literal `0 <= k` into
//!   `CAST(m0_checked_pow(a, k) AS Float64)`: v0.4 has no integer `**`, so
//!   its float `power` is the nearest thing to time against, and the cast
//!   back keeps the plan's types, which an optimizer rule may not change;
//! - `sum(a)`, in an aggregate or a window, into `m0_checked_sum`, which
//!   adds exactly (in i128) and checks only the values it returns. The
//!   contract judges a sum by its value, so a check per addition would
//!   raise for some input orders and not for others.
//!
//! [`Checked::Arrow`] computes each value with arrow's checked kernels,
//! which stop at the first overflow; [`Checked::Fast`] computes the wrapped
//! values and an overflow word per batch in one branch-free loop, and looks
//! for the overflowing row only when the word says there is one.
//!
//! Narrower integer types are left alone: widening them makes the check a
//! compare, cheaper than Int64's.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Int64Array, PrimitiveArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, BinaryExpr, Cast, ColumnarValue, EmitTo, Expr,
    ExprSchemable, GroupsAccumulator, LogicalPlan, Operator, ReversedUDAF, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowFunctionDefinition,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use super::Checked;

/// The error the contract names, as the message prefix a test can match.
pub(crate) const OVERFLOW: &str = "ArithmeticOverflowError";

fn overflow<T>(what: impl std::fmt::Display) -> Result<T> {
    exec_err!("{OVERFLOW}: {what} overflows Int64")
}

/// An integer operation the prototype checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Add,
    Sub,
    Mul,
    /// `base ** exponent`, the exponent a literal.
    Pow,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Add => "m0_checked_add",
            Op::Sub => "m0_checked_sub",
            Op::Mul => "m0_checked_mul",
            Op::Pow => "m0_checked_pow",
        }
    }

    /// The operation a checked function of this prototype computes.
    pub(crate) fn of(name: &str) -> Option<Op> {
        [Op::Add, Op::Sub, Op::Mul, Op::Pow]
            .into_iter()
            .find(|op| op.name() == name)
    }

    fn symbol(self) -> &'static str {
        match self {
            Op::Add => "+",
            Op::Sub => "-",
            Op::Mul => "*",
            Op::Pow => "**",
        }
    }

    fn checked(self, a: i64, b: i64) -> Option<i64> {
        match self {
            Op::Add => a.checked_add(b),
            Op::Sub => a.checked_sub(b),
            Op::Mul => a.checked_mul(b),
            Op::Pow => a.checked_pow(u32::try_from(b).ok()?),
        }
    }
}

/// One operand of a binary kernel.
enum Side<'a> {
    Array(&'a Int64Array),
    Scalar(Option<i64>),
}

impl<'a> Side<'a> {
    fn of(value: &'a ColumnarValue) -> Result<Side<'a>> {
        match value {
            ColumnarValue::Array(array) => match array.as_primitive_opt::<Int64Type>() {
                Some(array) => Ok(Side::Array(array)),
                None => exec_err!("checked arithmetic takes Int64, not {}", array.data_type()),
            },
            ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(Side::Scalar(*v)),
            ColumnarValue::Scalar(other) => {
                exec_err!("checked arithmetic takes Int64, not {}", other.data_type())
            }
        }
    }

    fn at(&self, i: usize) -> i64 {
        match self {
            Side::Array(a) => a.value(i),
            Side::Scalar(v) => v.unwrap_or_default(),
        }
    }
}

/// The wrapped result of `a op b` and a word whose sign bit is set when the
/// true result does not fit. Branch-free, so a loop over it vectorizes.
#[inline(always)]
fn add_word(a: i64, b: i64) -> (i64, i64) {
    let s = a.wrapping_add(b);
    (s, (a ^ s) & (b ^ s))
}

#[inline(always)]
fn sub_word(a: i64, b: i64) -> (i64, i64) {
    let s = a.wrapping_sub(b);
    (s, (a ^ b) & (a ^ s))
}

#[inline(always)]
fn mul_word(a: i64, b: i64) -> (i64, i64) {
    let (s, o) = a.overflowing_mul(b);
    (s, -(o as i64))
}

/// Wrapped values over `len` rows and the OR of their overflow words.
fn words<F: Fn(i64, i64) -> (i64, i64)>(
    left: &Side,
    right: &Side,
    len: usize,
    f: F,
) -> (Vec<i64>, i64) {
    let mut any = 0i64;
    let values = match (left, right) {
        (Side::Array(a), Side::Array(b)) => a
            .values()
            .iter()
            .zip(b.values().iter())
            .map(|(&a, &b)| {
                let (v, o) = f(a, b);
                any |= o;
                v
            })
            .collect(),
        (Side::Array(a), Side::Scalar(b)) => {
            let b = b.unwrap_or_default();
            a.values()
                .iter()
                .map(|&a| {
                    let (v, o) = f(a, b);
                    any |= o;
                    v
                })
                .collect()
        }
        (Side::Scalar(a), Side::Array(b)) => {
            let a = a.unwrap_or_default();
            b.values()
                .iter()
                .map(|&b| {
                    let (v, o) = f(a, b);
                    any |= o;
                    v
                })
                .collect()
        }
        (Side::Scalar(a), Side::Scalar(b)) => {
            let (v, o) = f(a.unwrap_or_default(), b.unwrap_or_default());
            any |= o;
            vec![v; len]
        }
    };
    (values, any)
}

fn nulls_of(left: &Side, right: &Side, len: usize) -> Option<NullBuffer> {
    let side = |s: &Side| match s {
        Side::Array(a) => a.nulls().cloned(),
        Side::Scalar(Some(_)) => None,
        Side::Scalar(None) => Some(NullBuffer::new_null(len)),
    };
    NullBuffer::union(side(left).as_ref(), side(right).as_ref())
}

/// `left op right` over `len` rows, wrapping, with the rows whose true
/// value does not fit (`None`: no row). Rows that are NULL never overflow.
/// [`Masked::Errors`](super::Masked::Errors) builds on this: it keeps the
/// mask instead of raising.
pub(crate) fn wrapping_with_overflow(
    op: Op,
    left: &ColumnarValue,
    right: &ColumnarValue,
    len: usize,
) -> Result<(Int64Array, Option<BooleanBuffer>)> {
    let (l, r) = (Side::of(left)?, Side::of(right)?);
    let (values, any) = match op {
        Op::Add => words(&l, &r, len, add_word),
        Op::Sub => words(&l, &r, len, sub_word),
        Op::Mul => words(&l, &r, len, mul_word),
        Op::Pow => {
            let Side::Scalar(Some(k)) = r else {
                return exec_err!("m0_checked_pow takes a literal exponent");
            };
            let k = u32::try_from(k).unwrap_or(u32::MAX);
            words(&l, &Side::Scalar(Some(0)), len, |a, _| {
                let (v, o) = a.overflowing_pow(k);
                (v, -(o as i64))
            })
        }
    };
    let nulls = nulls_of(&l, &r, len);
    let mask = (any < 0).then(|| {
        BooleanBuffer::collect_bool(len, |i| {
            nulls.as_ref().is_none_or(|n| n.is_valid(i)) && op.checked(l.at(i), r.at(i)).is_none()
        })
    });
    let mask = mask.filter(|m| m.count_set_bits() > 0);
    Ok((Int64Array::new(ScalarBuffer::from(values), nulls), mask))
}

fn evaluate(op: Op, mode: Checked, args: &[ColumnarValue], len: usize) -> Result<ArrayRef> {
    let [left, right] = args else {
        return exec_err!("{} takes two arguments", op.name());
    };
    match (mode, op) {
        (Checked::Arrow, Op::Pow) => {
            let (Side::Array(a), Side::Scalar(Some(k))) = (Side::of(left)?, Side::of(right)?)
            else {
                return exec_err!("m0_checked_pow takes a column and a literal exponent");
            };
            let k = u32::try_from(k).unwrap_or(u32::MAX);
            let result: PrimitiveArray<Int64Type> = a.try_unary(|x| {
                x.checked_pow(k).ok_or_else(|| {
                    datafusion::arrow::error::ArrowError::ComputeError(OVERFLOW.to_string())
                })
            })?;
            Ok(Arc::new(result))
        }
        (Checked::Arrow, _) => {
            let datum = |v: &ColumnarValue| -> Result<Box<dyn datafusion::arrow::array::Datum>> {
                Ok(match v {
                    ColumnarValue::Array(a) => Box::new(Arc::clone(a)),
                    ColumnarValue::Scalar(s) => Box::new(s.to_scalar()?),
                })
            };
            let (l, r) = (datum(left)?, datum(right)?);
            let result = match op {
                Op::Add => numeric::add(l.as_ref(), r.as_ref()),
                Op::Sub => numeric::sub(l.as_ref(), r.as_ref()),
                _ => numeric::mul(l.as_ref(), r.as_ref()),
            };
            result.or_else(|e| overflow(format_args!("`{}` ({e})", op.symbol())))
        }
        _ => {
            let (values, overflowed) = wrapping_with_overflow(op, left, right, len)?;
            if let Some(mask) = overflowed {
                let row = mask.set_indices().next().unwrap_or_default();
                let (l, r) = (Side::of(left)?, Side::of(right)?);
                return overflow(format_args!("{} {} {}", l.at(row), op.symbol(), r.at(row)));
            }
            Ok(Arc::new(values))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CheckedUdf {
    op: Op,
    mode: Checked,
    signature: Signature,
}

impl ScalarUDFImpl for CheckedUdf {
    fn name(&self) -> &str {
        self.op.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let len = if scalar { 1 } else { args.number_rows };
        let result = evaluate(self.op, self.mode, &args.args, len)?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

/// Splits each Int64 into a signed high and an unsigned low half and sums
/// the halves in i64 lanes, which vectorizes, then joins them in i128.
fn sum_halves(values: &[i64]) -> i128 {
    // 2^31 halves of < 2^32 cannot overflow a lane.
    values
        .chunks(1 << 31)
        .map(|chunk| {
            let (mut high, mut low) = (0i64, 0u64);
            for &v in chunk {
                high += v >> 32;
                low += (v as u64) & 0xFFFF_FFFF;
            }
            ((high as i128) << 32) + low as i128
        })
        .sum()
}

/// The exact sum of the valid values, and how many there are.
fn sum_exact(values: &Int64Array, mode: Checked) -> (i128, u64) {
    let valid = (values.len() - values.null_count()) as u64;
    let sum = match values.nulls() {
        None if mode == Checked::Fast => sum_halves(values.values()),
        None => values.values().iter().map(|&v| v as i128).sum(),
        Some(nulls) => nulls.valid_indices().map(|i| values.value(i) as i128).sum(),
    };
    (sum, valid)
}

fn fit(sum: i128) -> Result<i64> {
    i64::try_from(sum).or_else(|_| overflow(format_args!("sum {sum}")))
}

const STATE: DataType = DataType::Decimal128(38, 0);

fn state_array(sums: Vec<i128>, seen: Option<NullBuffer>) -> Result<ArrayRef> {
    Ok(Arc::new(
        Decimal128Array::new(ScalarBuffer::from(sums), seen).with_precision_and_scale(38, 0)?,
    ))
}

#[derive(Debug)]
struct SumAccumulator {
    mode: Checked,
    sum: i128,
    count: u64,
}

impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let (sum, count) = sum_exact(values[0].as_primitive::<Int64Type>(), self.mode);
        self.sum += sum;
        self.count += count;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let (sum, count) = sum_exact(values[0].as_primitive::<Int64Type>(), self.mode);
        self.sum -= sum;
        self.count -= count;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(if self.count == 0 {
            None
        } else {
            Some(fit(self.sum)?)
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sum = (self.count > 0).then_some(self.sum);
        Ok(vec![ScalarValue::Decimal128(sum, 38, 0)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states = states[0].as_primitive::<Decimal128Type>();
        for i in 0..states.len() {
            if states.is_valid(i) {
                self.sum += states.value(i);
                self.count += 1;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct SumGroups {
    sums: Vec<i128>,
    seen: Vec<bool>,
}

impl SumGroups {
    fn grow(&mut self, groups: usize) {
        self.sums.resize(groups, 0);
        self.seen.resize(groups, false);
    }

    fn take(&mut self, emit_to: EmitTo) -> (Vec<i128>, Option<NullBuffer>) {
        let sums = emit_to.take_needed(&mut self.sums);
        let seen = emit_to.take_needed(&mut self.seen);
        let nulls = NullBuffer::from(seen);
        let nulls = (nulls.null_count() > 0).then_some(nulls);
        (sums, nulls)
    }
}

impl GroupsAccumulator for SumGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let values = values[0].as_primitive::<Int64Type>();
        if values.null_count() == 0 && opt_filter.is_none() {
            for (&g, &v) in group_indices.iter().zip(values.values().iter()) {
                self.sums[g] += v as i128;
                self.seen[g] = true;
            }
        } else {
            for (i, &g) in group_indices.iter().enumerate() {
                let kept = opt_filter.is_none_or(|f| f.is_valid(i) && f.value(i));
                if kept && values.is_valid(i) {
                    self.sums[g] += values.value(i) as i128;
                    self.seen[g] = true;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (sums, nulls) = self.take(emit_to);
        let values = sums
            .iter()
            .enumerate()
            .map(|(i, &s)| {
                if nulls.as_ref().is_none_or(|n| n.is_valid(i)) {
                    fit(s)
                } else {
                    Ok(0)
                }
            })
            .collect::<Result<Vec<i64>>>()?;
        Ok(Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (sums, nulls) = self.take(emit_to);
        Ok(vec![state_array(sums, nulls)?])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let states = values[0].as_primitive::<Decimal128Type>();
        for (i, &g) in group_indices.iter().enumerate() {
            if states.is_valid(i) {
                self.sums[g] += states.value(i);
                self.seen[g] = true;
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<Int64Type>();
        let sums = values.values().iter().map(|&v| v as i128).collect();
        let kept = opt_filter.map(|f| {
            NullBuffer::from(
                f.values()
                    & &f.nulls()
                        .map_or_else(|| BooleanBuffer::new_set(f.len()), |n| n.inner().clone()),
            )
        });
        Ok(vec![state_array(
            sums,
            NullBuffer::union(values.nulls(), kept.as_ref()),
        )?])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * size_of::<i128>() + self.seen.capacity()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CheckedSum {
    mode: Checked,
    signature: Signature,
}

impl AggregateUDFImpl for CheckedSum {
    fn name(&self) -> &str {
        "m0_checked_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator {
            mode: self.mode,
            sum: 0,
            count: 0,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "sum"),
            STATE,
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::<SumGroups>::default())
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

/// Rewrites a plan's Int64 `+ - *`, integer `power` and `sum` into the
/// checked functions above.
#[derive(Debug)]
pub(crate) struct CheckedArithmetic {
    add: Arc<ScalarUDF>,
    sub: Arc<ScalarUDF>,
    mul: Arc<ScalarUDF>,
    pow: Arc<ScalarUDF>,
    sum: Arc<AggregateUDF>,
}

impl CheckedArithmetic {
    pub(crate) fn new(mode: Checked) -> Self {
        let scalar = |op| {
            Arc::new(ScalarUDF::new_from_impl(CheckedUdf {
                op,
                mode,
                signature: Signature::exact(vec![DataType::Int64; 2], Volatility::Immutable),
            }))
        };
        CheckedArithmetic {
            add: scalar(Op::Add),
            sub: scalar(Op::Sub),
            mul: scalar(Op::Mul),
            pow: scalar(Op::Pow),
            sum: Arc::new(AggregateUDF::new_from_impl(CheckedSum {
                mode,
                signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            })),
        }
    }

    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        let int64 = |e: &Expr| matches!(e.get_type(schema), Ok(DataType::Int64));
        Ok(match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::Plus | Operator::Minus | Operator::Multiply)
                    && int64(&left)
                    && int64(&right) =>
            {
                let udf = match op {
                    Operator::Plus => &self.add,
                    Operator::Minus => &self.sub,
                    _ => &self.mul,
                };
                Transformed::yes(udf.call(vec![*left, *right]))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) if func.name() == "power" => {
                match integer_power(&args, &int64) {
                    Some((base, k)) => Transformed::yes(Expr::Cast(Cast::new(
                        Box::new(
                            self.pow
                                .call(vec![base, Expr::Literal(ScalarValue::Int64(Some(k)), None)]),
                        ),
                        DataType::Float64,
                    ))),
                    None => Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args })),
                }
            }
            Expr::AggregateFunction(AggregateFunction { func, params })
                if func.name() == "sum" && !params.distinct && params.args.iter().all(int64) =>
            {
                Transformed::yes(Expr::AggregateFunction(AggregateFunction {
                    func: Arc::clone(&self.sum),
                    params,
                }))
            }
            Expr::WindowFunction(window) => {
                let WindowFunction { fun, params } = *window;
                match fun {
                    WindowFunctionDefinition::AggregateUDF(f)
                        if f.name() == "sum"
                            && !params.distinct
                            && params.args.iter().all(int64) =>
                    {
                        Transformed::yes(Expr::WindowFunction(Box::new(WindowFunction {
                            fun: WindowFunctionDefinition::AggregateUDF(Arc::clone(&self.sum)),
                            params,
                        })))
                    }
                    fun => Transformed::no(Expr::WindowFunction(Box::new(WindowFunction {
                        fun,
                        params,
                    }))),
                }
            }
            other => Transformed::no(other),
        })
    }
}

/// `(base, k)` when `args` is `power(CAST(base AS Float64), k)` for an
/// Int64 `base` and an integer literal `0 <= k <= u32::MAX`.
fn integer_power(args: &[Expr], int64: &dyn Fn(&Expr) -> bool) -> Option<(Expr, i64)> {
    let [Expr::Cast(Cast { expr: base, field }), exponent] = args else {
        return None;
    };
    if field.data_type() != &DataType::Float64 || !int64(base) {
        return None;
    }
    let k = match exponent {
        Expr::Literal(ScalarValue::Float64(Some(k)), _) if k.fract() == 0.0 => *k as i64,
        Expr::Literal(ScalarValue::Int64(Some(k)), _) => *k,
        Expr::Cast(Cast { expr, .. }) => match expr.as_ref() {
            Expr::Literal(ScalarValue::Int64(Some(k)), _) => *k,
            _ => return None,
        },
        _ => return None,
    };
    (0..=i64::from(u32::MAX))
        .contains(&k)
        .then(|| ((**base).clone(), k))
}

impl OptimizerRule for CheckedArithmetic {
    fn name(&self) -> &str {
        "ltseq_m0_checked_arithmetic"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Expressions read the columns of the node's one input; nodes with
        // none or several (scans, joins) are left as they are.
        let schema = match plan.inputs().as_slice() {
            [input] => Arc::clone(input.schema()),
            _ => return Ok(Transformed::no(plan)),
        };
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = names.save(&expr);
            Ok(expr
                .transform_up(|e| self.rewrite_expr(e, &schema))?
                .update_data(|e| name.restore(e)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(values: Vec<Option<i64>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int64Array::from(values)))
    }

    fn run(
        op: Op,
        mode: Checked,
        l: Vec<Option<i64>>,
        r: Vec<Option<i64>>,
    ) -> Result<Vec<Option<i64>>> {
        let len = l.len();
        let out = evaluate(op, mode, &[column(l), column(r)], len)?;
        Ok(out.as_primitive::<Int64Type>().iter().collect())
    }

    #[test]
    fn both_modes_raise_exactly_where_a_value_overflows() {
        for mode in [Checked::Arrow, Checked::Fast] {
            for (op, a, b) in [
                (Op::Add, i64::MAX, 1),
                (Op::Add, i64::MIN, -1),
                (Op::Sub, i64::MIN, 1),
                (Op::Sub, 0, i64::MIN),
                (Op::Mul, i64::MAX, 2),
                (Op::Mul, i64::MIN, -1),
            ] {
                let err =
                    run(op, mode, vec![Some(1), Some(a)], vec![Some(2), Some(b)]).unwrap_err();
                assert!(err.to_string().contains(OVERFLOW), "{mode:?} {op:?}: {err}");
                // The same values in a NULL row do not raise.
                let ok = run(op, mode, vec![Some(1), Some(a)], vec![Some(2), None]).unwrap();
                assert_eq!(ok[1], None);
                assert_eq!(ok[0], op.checked(1, 2));
            }
            let ok = run(
                Op::Mul,
                mode,
                vec![Some(-3_037_000_499)],
                vec![Some(3_037_000_499)],
            )
            .unwrap();
            assert_eq!(ok, vec![Some(-3_037_000_499i64 * 3_037_000_499)]);
        }
    }

    #[test]
    fn pow_checks_and_keeps_nulls() {
        let base = column(vec![Some(3), None, Some(-2)]);
        let k = ColumnarValue::Scalar(ScalarValue::Int64(Some(3)));
        for mode in [Checked::Arrow, Checked::Fast] {
            let out = evaluate(Op::Pow, mode, &[base.clone(), k.clone()], 3).unwrap();
            let got: Vec<_> = out.as_primitive::<Int64Type>().iter().collect();
            assert_eq!(got, vec![Some(27), None, Some(-8)]);
            let big = column(vec![Some(3_000_000)]);
            assert!(evaluate(Op::Pow, mode, &[big, k.clone()], 1).is_err());
        }
    }

    #[test]
    fn the_overflow_mask_marks_only_valid_overflowing_rows() {
        let l = column(vec![Some(i64::MAX), Some(1), Some(i64::MAX), None]);
        let r = column(vec![Some(1), Some(1), None, Some(1)]);
        let (values, mask) = wrapping_with_overflow(Op::Add, &l, &r, 4).unwrap();
        assert_eq!(values.value(1), 2);
        assert_eq!(
            mask.unwrap().iter().collect::<Vec<_>>(),
            vec![true, false, false, false]
        );
        let (_, none) = wrapping_with_overflow(Op::Add, &r, &r, 4).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn sums_check_the_total_not_each_addition() {
        let values: Vec<i64> = vec![i64::MAX, 5, -10, i64::MIN, 3];
        let exact: i128 = values.iter().map(|&v| v as i128).sum();
        assert_eq!(sum_halves(&values), exact);
        for mode in [Checked::Arrow, Checked::Fast] {
            let mut acc = SumAccumulator {
                mode,
                sum: 0,
                count: 0,
            };
            // A running i64 sum overflows after the first two values; the
            // total fits.
            acc.update_batch(&[Arc::new(Int64Array::from(values.clone()))])
                .unwrap();
            assert_eq!(
                acc.evaluate().unwrap(),
                ScalarValue::Int64(Some(exact as i64))
            );
            acc.update_batch(&[Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX]))])
                .unwrap();
            assert!(acc.evaluate().unwrap_err().to_string().contains(OVERFLOW));
            acc.retract_batch(&[Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX]))])
                .unwrap();
            assert_eq!(
                acc.evaluate().unwrap(),
                ScalarValue::Int64(Some(exact as i64))
            );
        }
        let mut groups = SumGroups::default();
        let batch: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            Some(1),
            None,
            Some(-1),
        ]));
        groups
            .update_batch(&[batch], &[0, 1, 2, 0], None, 3)
            .unwrap();
        let out = groups.evaluate(EmitTo::All).unwrap();
        let got: Vec<_> = out.as_primitive::<Int64Type>().iter().collect();
        assert_eq!(got, vec![Some(i64::MAX - 1), Some(1), None]);
    }
}
