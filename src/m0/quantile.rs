//! m8: exact `quantile` and `median` (M13, #306).
//!
//! Contract §14.2 makes `quantile(q)` the exact value of
//! `x[j] + t * (x[j+1] - x[j])` over the values sorted ascending, rounded
//! once to `float64`, with `h = q̂ * (n - 1)`, `j = floor(h)` and `t = h - j`;
//! `q̂` is the decimal `q` was written as (`Fraction(repr(q))`), and every
//! input takes part at its exact value. `median` is `quantile(0.5)`.
//! DataFusion 55's `percentile_cont` coerces an integer input to `float64`,
//! computes `h` in `float64` and interpolates there with `t` cut to six
//! decimals; its `median` returns the input type; LTSeq's `percentile` is
//! today the t-digest `approx_percentile_cont`.
//!
//! This rule rewrites `median(x)` over `float64` and
//! `approx_percentile_cont(x, q)` over any numeric `x` (DataFusion casts
//! it to `float64`; an `int64` operand is taken from under that cast) into
//! one of:
//!
//! - [`Quantile::Cont`]: DataFusion's `percentile_cont(x, q)`, the baseline
//!   #306 names. It is given no `WITHIN GROUP` ordering, which would make
//!   the aggregate demand sorted input.
//! - [`Quantile::Select`]: this module's accumulators and selection, with
//!   `h`, `t` and the interpolation in `float64` as DataFusion computes
//!   them (without the six-decimal cut). Not the contract: it separates
//!   the cost of selecting from that of the exact interpolation.
//! - [`Quantile::Exact`]: the same selection, `j` and `t` from `q̂` in
//!   integer arithmetic, and the interpolation exact (`BigInt`) and rounded
//!   once. `int64` inputs stay `int64`.
//!
//! Both prototypes hold every value, as DataFusion's `percentile_cont` and
//! `median` do (§21.1 lets `quantile` hold its input), select `x[j]` with
//! `select_nth_unstable` and take `x[j+1]` as the least value above it,
//! where DataFusion selects a second time. A NaN taking part makes the
//! result NaN (§14.2); an infinity among the two values interpolated gives
//! `(1 - t) * x[j] + t * x[j+1]`'s infinity or NaN.
//!
//! Left alone: `median` over an integer type (DataFusion's returns the input
//! type, which an optimizer rule may not change), decimals, `DISTINCT`, and
//! windows. `q` is limited to 19 decimals, so that `q̂ * (n - 1)` fits `u128`.

use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, Float64Array, ListArray,
    PrimitiveArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::ArrowNativeType;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_err, plan_err, DFSchema, Result, ScalarValue};
use datafusion::functions_aggregate::percentile_cont::percentile_cont_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Cast, EmitTo, Expr, GroupsAccumulator,
    LogicalPlan, ReversedUDAF, Signature, Volatility,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use num_bigint::{BigInt, BigUint, Sign};
use num_traits::Zero;

use super::exact_acc::div_round;
use super::Quantile;
use crate::transpiler::planned_type;

/// A value type the prototype takes: `int64` or `float64`.
trait Value: ArrowNativeType + Copy + Send + Sync + Debug + 'static {
    type Arrow: ArrowPrimitiveType<Native = Self>;
    const MAY_BE_NAN: bool;
    fn order(a: &Self, b: &Self) -> Ordering;
    fn is_nan(self) -> bool;
    fn is_infinite(self) -> bool;
    /// The nearest `float64`, rounded once.
    fn to_f64(self) -> f64;
    /// `(m, e)` with the value `m * 2^e`, for a finite value.
    fn scaled(self) -> (BigInt, i64);
}

impl Value for i64 {
    type Arrow = Int64Type;
    const MAY_BE_NAN: bool = false;
    fn order(a: &Self, b: &Self) -> Ordering {
        a.cmp(b)
    }
    fn is_nan(self) -> bool {
        false
    }
    fn is_infinite(self) -> bool {
        false
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn scaled(self) -> (BigInt, i64) {
        (BigInt::from(self), 0)
    }
}

impl Value for f64 {
    type Arrow = Float64Type;
    const MAY_BE_NAN: bool = true;
    fn order(a: &Self, b: &Self) -> Ordering {
        // NaN never reaches a comparison that decides a result: a NaN
        // taking part makes the result NaN before selection.
        a.total_cmp(b)
    }
    fn is_nan(self) -> bool {
        f64::is_nan(self)
    }
    fn is_infinite(self) -> bool {
        f64::is_infinite(self)
    }
    fn to_f64(self) -> f64 {
        self
    }
    fn scaled(self) -> (BigInt, i64) {
        let bits = self.to_bits();
        let exponent = ((bits >> 52) & 0x7FF) as i64;
        let fraction = bits & ((1 << 52) - 1);
        let (m, e) = if exponent == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1 << 52), exponent - 1075)
        };
        let m = BigInt::from(m);
        (if self.is_sign_negative() { -m } else { m }, e)
    }
}

/// `q` and its decimal `q̂ = num / den`, `den = 10^k`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Rank {
    q: f64,
    num: u128,
    den: u128,
}

impl Rank {
    fn new(q: f64) -> Result<Rank> {
        if !(0.0..=1.0).contains(&q) {
            return plan_err!("quantile q must be in [0, 1], got {q}");
        }
        // Rust prints a float as the shortest decimal that reads back as it,
        // never in exponent notation.
        let text = format!("{q}");
        let (whole, decimals) = text.split_once('.').unwrap_or((&text, ""));
        let k = decimals.len() as u32;
        if k > 19 {
            return plan_err!("the m0 quantile takes at most 19 decimals in q, not {text}");
        }
        let den = 10u128.pow(k);
        let num = whole.parse::<u128>().expect("digits") * den
            + if k == 0 {
                0
            } else {
                decimals.parse::<u128>().expect("digits")
            };
        Ok(Rank { q, num, den })
    }
}

/// `x[j]` and, when `upper`, `x[j+1]` of `values` sorted ascending.
fn select_two<T: Value>(values: &mut [T], j: usize, upper: bool) -> (T, Option<T>) {
    let (_, lo, right) = values.select_nth_unstable_by(j, T::order);
    let lo = *lo;
    let hi = if upper {
        right.iter().copied().min_by(T::order)
    } else {
        None
    };
    (lo, hi)
}

/// `lo + r / den * (hi - lo)`, exact, rounded once; `0 < r < den`.
fn interpolate<T: Value>(lo: T, hi: T, r: u128, den: u128) -> f64 {
    if lo.is_infinite() || hi.is_infinite() {
        // (1 - t) * lo + t * hi with 0 < t < 1.
        let (l, h) = (lo.to_f64(), hi.to_f64());
        return match (lo.is_infinite(), hi.is_infinite()) {
            (true, true) if l != h => f64::NAN,
            (true, _) => l,
            _ => h,
        };
    }
    let (ml, el) = lo.scaled();
    let (mh, eh) = hi.scaled();
    let e = match (ml.is_zero(), mh.is_zero()) {
        (true, true) => return 0.0,
        (true, false) => eh,
        (false, true) => el,
        (false, false) => el.min(eh),
    };
    let align = |m: BigInt, from: i64| {
        if m.is_zero() {
            m
        } else {
            m << (from - e) as usize
        }
    };
    let (ml, mh) = (align(ml, el), align(mh, eh));
    let n = &ml * den + (mh - &ml) * r;
    if n.is_zero() {
        return 0.0;
    }
    div_round(
        n.sign() == Sign::Minus,
        n.magnitude(),
        &BigUint::from(den),
        e,
    )
}

/// The quantile of `values` (reordered), `None` when there are none.
fn quantile<T: Value>(values: &mut [T], rank: Rank, mode: Quantile) -> Option<f64> {
    let n = values.len();
    if n == 0 {
        return None;
    }
    if T::MAY_BE_NAN && values.iter().any(|v| v.is_nan()) {
        return Some(f64::NAN);
    }
    Some(match mode {
        Quantile::Exact => {
            let a = rank.num * (n as u128 - 1);
            let (j, r) = ((a / rank.den) as usize, a % rank.den);
            match select_two(values, j, r != 0) {
                // + 0.0 makes a zero +0.0.
                (lo, None) => lo.to_f64() + 0.0,
                (lo, Some(hi)) => interpolate(lo, hi, r, rank.den),
            }
        }
        _ => {
            let h = rank.q * (n - 1) as f64;
            let j = h.floor() as usize;
            let t = h - j as f64;
            match select_two(values, j, t > 0.0) {
                (lo, None) => lo.to_f64(),
                (lo, Some(hi)) => {
                    let l = lo.to_f64();
                    l + (hi.to_f64() - l) * t
                }
            }
        }
    })
}

fn list_of<T: Value>(groups: Vec<Vec<T>>) -> ArrayRef {
    let mut offsets = Vec::with_capacity(groups.len() + 1);
    offsets.push(0i32);
    let mut end = 0i32;
    for g in &groups {
        end += g.len() as i32;
        offsets.push(end);
    }
    let values: Vec<T> = groups.into_iter().flatten().collect();
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(T::Arrow::DATA_TYPE, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(PrimitiveArray::<T::Arrow>::new(
            ScalarBuffer::from(values),
            None,
        )),
        None,
    ))
}

fn push_valid<T: Value>(into: &mut Vec<T>, array: &ArrayRef) {
    let values = array.as_primitive::<T::Arrow>();
    if values.null_count() == 0 {
        into.extend_from_slice(values.values());
    } else {
        into.extend(values.iter().flatten());
    }
}

#[derive(Debug)]
struct QuantileAcc<T: Value> {
    values: Vec<T>,
    rank: Rank,
    mode: Quantile,
}

impl<T: Value> Accumulator for QuantileAcc<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        push_valid(&mut self.values, &values[0]);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let lists = states[0].as_list::<i32>();
        for list in lists.iter().flatten() {
            push_valid(&mut self.values, &list);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // The values move into the list without a copy, as DataFusion's
        // percentile_cont hands them on.
        let values = std::mem::take(&mut self.values);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
        let list = ListArray::new(
            Arc::new(Field::new_list_field(T::Arrow::DATA_TYPE, true)),
            offsets,
            Arc::new(PrimitiveArray::<T::Arrow>::new(
                ScalarBuffer::from(values),
                None,
            )),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(quantile(
            &mut self.values,
            self.rank,
            self.mode,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<T>()
    }
}

#[derive(Debug)]
struct QuantileGroups<T: Value> {
    groups: Vec<Vec<T>>,
    rank: Rank,
    mode: Quantile,
}

impl<T: Value> GroupsAccumulator for QuantileGroups<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups.resize_with(total_num_groups, Vec::new);
        let values = values[0].as_primitive::<T::Arrow>();
        if values.null_count() == 0 && opt_filter.is_none() {
            for (&g, &v) in group_indices.iter().zip(values.values().iter()) {
                self.groups[g].push(v);
            }
        } else {
            for (i, &g) in group_indices.iter().enumerate() {
                let kept = opt_filter.is_none_or(|f| f.is_valid(i) && f.value(i));
                if kept && values.is_valid(i) {
                    self.groups[g].push(values.value(i));
                }
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups.resize_with(total_num_groups, Vec::new);
        let lists = values[0].as_list::<i32>();
        for (&g, list) in group_indices.iter().zip(lists.iter()) {
            if let Some(list) = list {
                push_valid(&mut self.groups[g], &list);
            }
        }
        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![list_of(emit_to.take_needed(&mut self.groups))])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let mut groups = emit_to.take_needed(&mut self.groups);
        let out: Float64Array = groups
            .iter_mut()
            .map(|g| quantile(g, self.rank, self.mode))
            .collect();
        Ok(Arc::new(out))
    }

    /// Each row as a group of its own: a one-value list, NULL where the
    /// value is NULL or filtered out.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<T::Arrow>();
        let kept = opt_filter.map(|f| {
            let set = f
                .nulls()
                .map_or_else(|| BooleanBuffer::new_set(f.len()), |n| n.inner().clone());
            NullBuffer::from(f.values() & &set)
        });
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, values.len()));
        Ok(vec![Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(T::Arrow::DATA_TYPE, true)),
            offsets,
            Arc::new(PrimitiveArray::<T::Arrow>::new(
                values.values().clone(),
                None,
            )),
            NullBuffer::union(values.nulls(), kept.as_ref()),
        ))])
    }

    fn size(&self) -> usize {
        self.groups
            .iter()
            .map(|g| g.capacity() * size_of::<T>())
            .sum::<usize>()
            + self.groups.capacity() * size_of::<Vec<T>>()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct QuantileUdaf {
    mode: Quantile,
    /// `q`'s bits (`f64` is not `Hash`).
    q: u64,
    input: DataType,
    signature: Signature,
}

impl QuantileUdaf {
    fn rank(&self) -> Result<Rank> {
        Rank::new(f64::from_bits(self.q))
    }
}

impl AggregateUDFImpl for QuantileUdaf {
    fn name(&self) -> &str {
        "m0_quantile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "values"),
            DataType::List(Arc::new(Field::new_list_field(self.input.clone(), true))),
            true,
        ))])
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let (rank, mode) = (self.rank()?, self.mode);
        Ok(match self.input {
            DataType::Int64 => Box::new(QuantileAcc::<i64> {
                values: vec![],
                rank,
                mode,
            }),
            DataType::Float64 => Box::new(QuantileAcc::<f64> {
                values: vec![],
                rank,
                mode,
            }),
            ref other => return exec_err!("m0_quantile takes int64 or float64, not {other}"),
        })
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let (rank, mode) = (self.rank()?, self.mode);
        Ok(match self.input {
            DataType::Int64 => Box::new(QuantileGroups::<i64> {
                groups: vec![],
                rank,
                mode,
            }),
            DataType::Float64 => Box::new(QuantileGroups::<f64> {
                groups: vec![],
                rank,
                mode,
            }),
            ref other => return exec_err!("m0_quantile takes int64 or float64, not {other}"),
        })
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

/// Rewrites a plan's `median` and `approx_percentile_cont` into the
/// quantile of [`Quantile`].
#[derive(Debug)]
pub(crate) struct ExactQuantile {
    mode: Quantile,
}

impl ExactQuantile {
    pub(crate) fn new(mode: Quantile) -> Self {
        ExactQuantile { mode }
    }

    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
            return Ok(Transformed::no(expr));
        };
        let float64 = |e: &Expr| planned_type(e, schema) == Some(DataType::Float64);
        let target = match (func.name(), params.args.as_slice()) {
            _ if params.distinct => None,
            ("median", [x]) if float64(x) => Some((x.clone(), 0.5)),
            ("approx_percentile_cont", [x, Expr::Literal(ScalarValue::Float64(Some(q)), _)])
                if float64(x) =>
            {
                Some((x.clone(), *q))
            }
            _ => None,
        };
        let Some((x, q)) = target else {
            return Ok(Transformed::no(Expr::AggregateFunction(
                AggregateFunction { func, params },
            )));
        };
        let (udaf, x) = match self.mode {
            Quantile::Cont => (percentile_cont_udaf(), x),
            _ => {
                // An int64 operand DataFusion cast to float64 takes part as
                // itself.
                let (x, input) = match x {
                    Expr::Cast(Cast { expr, field })
                        if field.data_type() == &DataType::Float64
                            && planned_type(&expr, schema) == Some(DataType::Int64) =>
                    {
                        (*expr, DataType::Int64)
                    }
                    x => (x, DataType::Float64),
                };
                Rank::new(q)?;
                let udaf = AggregateUDF::new_from_impl(QuantileUdaf {
                    mode: self.mode,
                    q: q.to_bits(),
                    signature: Signature::exact(vec![input.clone()], Volatility::Immutable),
                    input,
                });
                (Arc::new(udaf), x)
            }
        };
        let args = match self.mode {
            Quantile::Cont => vec![x, Expr::Literal(ScalarValue::Float64(Some(q)), None)],
            _ => vec![x],
        };
        Ok(Transformed::yes(Expr::AggregateFunction(
            AggregateFunction::new_udf(
                udaf,
                args,
                false,
                params.filter,
                vec![],
                params.null_treatment,
            ),
        )))
    }
}

impl OptimizerRule for ExactQuantile {
    fn name(&self) -> &str {
        "ltseq_m0_exact_quantile"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !matches!(plan, LogicalPlan::Aggregate(_)) {
            return Ok(Transformed::no(plan));
        }
        let schema = Arc::clone(plan.inputs()[0].schema());
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

    fn exact<T: Value>(mut values: Vec<T>, q: f64) -> Option<f64> {
        quantile(&mut values, Rank::new(q).unwrap(), Quantile::Exact)
    }

    /// The cases contract row A2 names.
    #[test]
    fn the_contracts_cases() {
        let big = 2i64.pow(53) + 1;
        assert_eq!(exact(vec![1i64, big], 0.5), Some(4503599627370497.0));
        // As a float, 2^53 + 1 is 2^53, and the median 2^52 + 0.5 rounds to
        // even.
        assert_eq!(exact(vec![1.0, big as f64], 0.5), Some(4503599627370496.0));
        // Two 0 and nine 10^18: h = 0.1 * 10 = 1 exactly, so the second value.
        let mut v = vec![0i64, 0];
        v.extend([10i64.pow(18); 9]);
        assert_eq!(exact(v, 0.1), Some(0.0));
        assert_eq!(exact(vec![1.0, f64::INFINITY], 0.5), Some(f64::INFINITY));
        assert!(exact(vec![f64::NEG_INFINITY, f64::INFINITY], 0.5)
            .unwrap()
            .is_nan());
        assert!(exact(vec![1.0, f64::NAN, 2.0], 0.5).unwrap().is_nan());
        assert_eq!(exact::<f64>(vec![], 0.5), None);
    }

    #[test]
    fn ranks_and_zeros() {
        assert_eq!(exact(vec![5i64, 1, 3, 2, 4], 0.0), Some(1.0));
        assert_eq!(exact(vec![5i64, 1, 3, 2, 4], 1.0), Some(5.0));
        assert_eq!(exact(vec![5i64, 1, 3, 2, 4], 0.9), Some(4.6));
        assert_eq!(exact(vec![1.0, 2.0, 3.0, 4.0], 0.5), Some(2.5));
        let zero = exact(vec![-0.0f64, -0.0], 0.5).unwrap();
        assert!(zero == 0.0 && zero.is_sign_positive());
        let zero = exact(vec![-1.0f64, 1.0], 0.5).unwrap();
        assert!(zero == 0.0 && zero.is_sign_positive());
        // Values far apart in magnitude interpolate exactly.
        assert_eq!(exact(vec![1e-300, 1e300], 0.5), Some(5e299));
        assert_eq!(exact(vec![i64::MIN, i64::MAX], 0.5), Some(-0.5));
    }

    #[test]
    fn decimals_of_q() {
        let r = Rank::new(0.1).unwrap();
        assert_eq!((r.num, r.den), (1, 10));
        let r = Rank::new(1.0).unwrap();
        assert_eq!((r.num, r.den), (1, 1));
        let r = Rank::new(0.30000000000000004).unwrap();
        assert_eq!((r.num, r.den), (30000000000000004, 10u128.pow(17)));
        assert!(Rank::new(1.5).is_err());
    }

    /// Exact agrees with a float interpolation wherever that one is exact.
    #[test]
    fn exact_matches_floats_on_small_integers() {
        let values: Vec<i64> = (0..1000).map(|i| (i * 7919) % 1009).collect();
        for q in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let mut a = values.clone();
            let mut b = values.clone();
            assert_eq!(
                quantile(&mut a, Rank::new(q).unwrap(), Quantile::Exact),
                quantile(&mut b, Rank::new(q).unwrap(), Quantile::Select),
                "{q}"
            );
        }
    }
}
