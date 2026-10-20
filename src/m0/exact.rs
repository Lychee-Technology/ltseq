//! m2: exact float statistics (contract §14.2; M13, M4, OD-W).
//!
//! DataFusion accumulates `sum`, `avg`, `var`, `stddev`, `covar_samp` and
//! `corr` in f64 (Welford's updates for the moments), so a result depends
//! on the order rows arrive in, which a parallel aggregate does not fix.
//! The contract rounds each statistic once from exact sums. This rule
//! replaces those functions on Float64, in aggregates and in windows, by
//! one UDAF per statistic over [`Sums`]:
//!
//! - [`FloatSums::Exact`] keeps Σx, Σx², Σab in [`ExactAcc`] superaccumulators
//!   (`super::exact_acc`) and rounds once with its finalizers; windows
//!   retract by exact subtraction.
//! - [`FloatSums::Compensated`] keeps them as double-doubles: deterministic for
//!   one row order and far more accurate than f64, but neither exact nor
//!   order-independent, and it does not follow the contract's non-finite
//!   rules. It prices a weaker answer to OD-W (#276), not the contract.
//!
//! Partial aggregates exchange their sums serialized, one binary column
//! per sum.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, BooleanArray, Float64Array, UInt64Array,
};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, UInt64Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator, LogicalPlan,
    ReversedUDAF, Signature, Volatility, WindowFunctionDefinition,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use super::exact_acc::{exact_corr, exact_cov, exact_mean, exact_std, exact_var, ExactAcc};
use super::FloatSums;
use crate::transpiler::planned_type;

/// A statistic and the sums it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Stat {
    /// Σx.
    Sum,
    /// Σx.
    Mean,
    /// Σx, Σx².
    Var,
    /// Σx, Σx².
    Std,
    /// Σab, Σa, Σb.
    Cov,
    /// Σab, Σa, Σb, Σa², Σb².
    Corr,
}

impl Stat {
    /// The statistic a DataFusion aggregate function computes, by name.
    fn of(name: &str) -> Option<Stat> {
        Some(match name {
            "sum" => Stat::Sum,
            "avg" | "mean" => Stat::Mean,
            "var" | "var_samp" | "var_sample" => Stat::Var,
            "stddev" | "stddev_samp" => Stat::Std,
            "covar_samp" | "covar" => Stat::Cov,
            "corr" => Stat::Corr,
            _ => return None,
        })
    }

    fn arity(self) -> usize {
        match self {
            Stat::Cov | Stat::Corr => 2,
            _ => 1,
        }
    }

    fn sums(self) -> usize {
        match self {
            Stat::Sum | Stat::Mean => 1,
            Stat::Var | Stat::Std => 2,
            Stat::Cov => 3,
            Stat::Corr => 5,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Stat::Sum => "m0_exact_sum",
            Stat::Mean => "m0_exact_mean",
            Stat::Var => "m0_exact_var",
            Stat::Std => "m0_exact_std",
            Stat::Cov => "m0_exact_cov",
            Stat::Corr => "m0_exact_corr",
        }
    }
}

/// Running sums of values and of products of values.
///
/// `add`, `sub`, `add_product`, `sub_product` and `merge` return the heap
/// bytes they allocated, which `heap_bytes` then counts, so that a caller
/// holding many sums keeps their total without walking them.
pub(crate) trait Sums: Clone + Default + Debug + Send + Sync + 'static {
    fn add(&mut self, x: f64) -> usize;
    fn sub(&mut self, x: f64) -> usize;
    fn add_product(&mut self, a: f64, b: f64) -> usize;
    fn sub_product(&mut self, a: f64, b: f64) -> usize;
    fn add_slice(&mut self, xs: &[f64]) {
        for &x in xs {
            self.add(x);
        }
    }
    fn sub_slice(&mut self, xs: &[f64]) {
        for &x in xs {
            self.sub(x);
        }
    }
    fn add_products(&mut self, a: &[f64], b: &[f64]) {
        for (&a, &b) in a.iter().zip(b) {
            self.add_product(a, b);
        }
    }
    fn sub_products(&mut self, a: &[f64], b: &[f64]) {
        for (&a, &b) in a.iter().zip(b) {
            self.sub_product(a, b);
        }
    }
    fn add_squares(&mut self, xs: &[f64]) {
        self.add_products(xs, xs)
    }
    fn sub_squares(&mut self, xs: &[f64]) {
        self.sub_products(xs, xs)
    }
    fn merge(&mut self, other: &Self) -> usize;
    fn serialize(&self, out: &mut Vec<u8>);
    fn deserialize(bytes: &[u8]) -> Result<Self>;
    fn heap_bytes(&self) -> usize;

    fn sum(s: &Self) -> f64;
    fn mean(s: &Self, n: u64) -> Option<f64>;
    fn var(s1: &Self, s2: &Self, n: u64) -> Option<f64>;
    fn std(s1: &Self, s2: &Self, n: u64) -> Option<f64>;
    fn cov(sab: &Self, sa: &Self, sb: &Self, n: u64) -> Option<f64>;
    fn corr(sab: &Self, sa: &Self, sb: &Self, saa: &Self, sbb: &Self, n: u64) -> Option<f64>;
}

impl Sums for ExactAcc {
    fn add(&mut self, x: f64) -> usize {
        ExactAcc::add(self, x)
    }
    fn sub(&mut self, x: f64) -> usize {
        ExactAcc::sub(self, x)
    }
    fn add_product(&mut self, a: f64, b: f64) -> usize {
        ExactAcc::add_product(self, a, b)
    }
    fn sub_product(&mut self, a: f64, b: f64) -> usize {
        ExactAcc::sub_product(self, a, b)
    }
    fn add_slice(&mut self, xs: &[f64]) {
        ExactAcc::add_slice(self, xs)
    }
    fn sub_slice(&mut self, xs: &[f64]) {
        ExactAcc::sub_slice(self, xs)
    }
    fn add_products(&mut self, a: &[f64], b: &[f64]) {
        ExactAcc::add_products(self, a, b)
    }
    fn sub_products(&mut self, a: &[f64], b: &[f64]) {
        ExactAcc::sub_products(self, a, b)
    }
    fn add_squares(&mut self, xs: &[f64]) {
        ExactAcc::add_squares(self, xs)
    }
    fn sub_squares(&mut self, xs: &[f64]) {
        ExactAcc::sub_squares(self, xs)
    }
    fn merge(&mut self, other: &Self) -> usize {
        ExactAcc::merge(self, other)
    }
    fn serialize(&self, out: &mut Vec<u8>) {
        ExactAcc::serialize(self, out)
    }
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        match ExactAcc::try_deserialize(bytes) {
            Some(acc) => Ok(acc),
            None => exec_err!("malformed exact sum state"),
        }
    }
    fn heap_bytes(&self) -> usize {
        ExactAcc::heap_bytes(self)
    }
    fn sum(s: &Self) -> f64 {
        s.round()
    }
    fn mean(s: &Self, n: u64) -> Option<f64> {
        exact_mean(s, n)
    }
    fn var(s1: &Self, s2: &Self, n: u64) -> Option<f64> {
        exact_var(s1, s2, n)
    }
    fn std(s1: &Self, s2: &Self, n: u64) -> Option<f64> {
        exact_std(s1, s2, n)
    }
    fn cov(sab: &Self, sa: &Self, sb: &Self, n: u64) -> Option<f64> {
        exact_cov(sab, sa, sb, n)
    }
    fn corr(sab: &Self, sa: &Self, sb: &Self, saa: &Self, sbb: &Self, n: u64) -> Option<f64> {
        exact_corr(sab, sa, sb, saa, sbb, n)
    }
}

/// A double-double: the unevaluated sum `hi + lo`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct DoubleDouble {
    hi: f64,
    lo: f64,
}

#[inline(always)]
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let v = s - a;
    (s, (a - (s - v)) + (b - v))
}

#[inline(always)]
fn quick_two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    (s, b - (s - a))
}

/// Dekker's split, so the product's error is exact without an FMA.
#[inline(always)]
fn split(a: f64) -> (f64, f64) {
    let t = 134_217_729.0 * a;
    let hi = t - (t - a);
    (hi, a - hi)
}

#[inline(always)]
fn two_prod(a: f64, b: f64) -> (f64, f64) {
    let p = a * b;
    let ((ah, al), (bh, bl)) = (split(a), split(b));
    (p, ((ah * bh - p) + ah * bl + al * bh) + al * bl)
}

impl DoubleDouble {
    fn of(hi: f64) -> Self {
        DoubleDouble { hi, lo: 0.0 }
    }

    #[inline(always)]
    fn plus(self, other: DoubleDouble) -> Self {
        let (s, e) = two_sum(self.hi, other.hi);
        let (hi, lo) = quick_two_sum(s, e + self.lo + other.lo);
        DoubleDouble { hi, lo }
    }

    fn neg(self) -> Self {
        DoubleDouble {
            hi: -self.hi,
            lo: -self.lo,
        }
    }

    fn times(self, other: DoubleDouble) -> Self {
        let (p, e) = two_prod(self.hi, other.hi);
        let (hi, lo) = quick_two_sum(p, e + self.hi * other.lo + self.lo * other.hi);
        DoubleDouble { hi, lo }
    }

    fn value(self) -> f64 {
        if self.hi.is_finite() {
            self.hi + self.lo
        } else {
            self.hi
        }
    }

    /// `n Σab − Σa Σb` for the comoment of a covariance or variance.
    fn comoment(sab: Self, sa: Self, sb: Self, n: u64) -> Self {
        sab.times(DoubleDouble::of(n as f64))
            .plus(sa.times(sb).neg())
    }
}

impl Sums for DoubleDouble {
    #[inline(always)]
    fn add(&mut self, x: f64) -> usize {
        let (s, e) = two_sum(self.hi, x);
        let (hi, lo) = quick_two_sum(s, e + self.lo);
        *self = DoubleDouble { hi, lo };
        0
    }
    fn sub(&mut self, x: f64) -> usize {
        self.add(-x)
    }
    #[inline(always)]
    fn add_product(&mut self, a: f64, b: f64) -> usize {
        let (p, e) = two_prod(a, b);
        *self = self.plus(DoubleDouble { hi: p, lo: e });
        0
    }
    fn sub_product(&mut self, a: f64, b: f64) -> usize {
        self.add_product(-a, b)
    }
    fn merge(&mut self, other: &Self) -> usize {
        *self = self.plus(*other);
        0
    }
    fn serialize(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.hi.to_le_bytes());
        out.extend_from_slice(&self.lo.to_le_bytes());
    }
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        let (Some(hi), Some(lo)) = (bytes.get(0..8), bytes.get(8..16)) else {
            return exec_err!("malformed double-double state");
        };
        Ok(DoubleDouble {
            hi: f64::from_le_bytes(hi.try_into().expect("8 bytes")),
            lo: f64::from_le_bytes(lo.try_into().expect("8 bytes")),
        })
    }
    fn heap_bytes(&self) -> usize {
        0
    }
    fn sum(s: &Self) -> f64 {
        s.value()
    }
    fn mean(s: &Self, n: u64) -> Option<f64> {
        (n > 0).then(|| s.value() / n as f64)
    }
    fn var(s1: &Self, s2: &Self, n: u64) -> Option<f64> {
        (n > 1).then(|| {
            (DoubleDouble::comoment(*s2, *s1, *s1, n).value() / (n as f64 * (n - 1) as f64))
                .max(0.0)
        })
    }
    fn std(s1: &Self, s2: &Self, n: u64) -> Option<f64> {
        Self::var(s1, s2, n).map(f64::sqrt)
    }
    fn cov(sab: &Self, sa: &Self, sb: &Self, n: u64) -> Option<f64> {
        (n > 1).then(|| {
            DoubleDouble::comoment(*sab, *sa, *sb, n).value() / (n as f64 * (n - 1) as f64)
        })
    }
    fn corr(sab: &Self, sa: &Self, sb: &Self, saa: &Self, sbb: &Self, n: u64) -> Option<f64> {
        if n < 2 {
            return None;
        }
        let num = DoubleDouble::comoment(*sab, *sa, *sb, n).value();
        let da = DoubleDouble::comoment(*saa, *sa, *sa, n).value();
        let db = DoubleDouble::comoment(*sbb, *sb, *sb, n).value();
        Some(num / (da * db).sqrt())
    }
}

fn finish<S: Sums>(stat: Stat, s: &[S], n: u64) -> Option<f64> {
    match stat {
        Stat::Sum => (n > 0).then(|| S::sum(&s[0])),
        Stat::Mean => S::mean(&s[0], n),
        Stat::Var => S::var(&s[0], &s[1], n),
        Stat::Std => S::std(&s[0], &s[1], n),
        Stat::Cov => S::cov(&s[0], &s[1], &s[2], n),
        Stat::Corr => S::corr(&s[0], &s[1], &s[2], &s[3], &s[4], n),
    }
}

/// The rows of `values` that take part (every argument non-NULL and the
/// filter TRUE), as contiguous slices' owners.
fn taking_part(
    values: &[ArrayRef],
    opt_filter: Option<&BooleanArray>,
) -> Result<(Vec<Float64Array>, Option<BooleanArray>)> {
    let arrays: Vec<Float64Array> = values
        .iter()
        .map(|v| v.as_primitive::<Float64Type>().clone())
        .collect();
    let mut keep: Option<BooleanBuffer> = None;
    for a in &arrays {
        if let Some(nulls) = a.nulls() {
            keep = Some(match keep {
                Some(k) => &k & nulls.inner(),
                None => nulls.inner().clone(),
            });
        }
    }
    if let Some(f) = opt_filter {
        let set = match f.nulls() {
            Some(n) => f.values() & n.inner(),
            None => f.values().clone(),
        };
        keep = Some(match keep {
            Some(k) => &k & &set,
            None => set,
        });
    }
    Ok((arrays, keep.map(|k| BooleanArray::new(k, None))))
}

/// Add (or, with `retract`, subtract) the rows of `values` to `sums`.
fn apply<S: Sums>(stat: Stat, sums: &mut [S], values: &[ArrayRef], retract: bool) -> Result<u64> {
    let (arrays, keep) = taking_part(values, None)?;
    let arrays: Vec<Float64Array> = match keep {
        Some(k) => arrays
            .iter()
            .map(|a| Ok(filter(a, &k)?.as_primitive::<Float64Type>().clone()))
            .collect::<Result<_>>()?,
        None => arrays,
    };
    let a = arrays[0].values().as_ref();
    let b = arrays.get(1).map(|b| b.values().as_ref());
    type Slice<S> = fn(&mut S, &[f64]);
    type Products<S> = fn(&mut S, &[f64], &[f64]);
    let (slice, squares, products): (Slice<S>, Slice<S>, Products<S>) = if retract {
        (S::sub_slice, S::sub_squares, S::sub_products)
    } else {
        (S::add_slice, S::add_squares, S::add_products)
    };
    match (stat, b) {
        (Stat::Sum | Stat::Mean, _) => slice(&mut sums[0], a),
        (Stat::Var | Stat::Std, _) => {
            slice(&mut sums[0], a);
            squares(&mut sums[1], a);
        }
        (Stat::Cov, Some(b)) => {
            products(&mut sums[0], a, b);
            slice(&mut sums[1], a);
            slice(&mut sums[2], b);
        }
        (Stat::Corr, Some(b)) => {
            products(&mut sums[0], a, b);
            slice(&mut sums[1], a);
            slice(&mut sums[2], b);
            squares(&mut sums[3], a);
            squares(&mut sums[4], b);
        }
        _ => return exec_err!("{} takes two arguments", stat.name()),
    }
    Ok(a.len() as u64)
}

#[derive(Debug)]
struct StatAccumulator<S: Sums> {
    stat: Stat,
    sums: Vec<S>,
    n: u64,
}

impl<S: Sums> Accumulator for StatAccumulator<S> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.n += apply(self.stat, &mut self.sums, values, false)?;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.n -= apply(self.stat, &mut self.sums, values, true)?;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(finish(self.stat, &self.sums, self.n)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .sums
                .iter()
                .map(|s| size_of::<S>() + s.heap_bytes())
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut out: Vec<ScalarValue> = self
            .sums
            .iter()
            .map(|s| {
                let mut bytes = Vec::new();
                s.serialize(&mut bytes);
                ScalarValue::Binary(Some(bytes))
            })
            .collect();
        out.push(ScalarValue::UInt64(Some(self.n)));
        Ok(out)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let k = self.sums.len();
        let counts = states[k].as_primitive::<UInt64Type>();
        for row in 0..counts.len() {
            if counts.is_null(row) {
                continue;
            }
            for (j, sum) in self.sums.iter_mut().enumerate() {
                sum.merge(&S::deserialize(states[j].as_binary::<i32>().value(row))?);
            }
            self.n += counts.value(row);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct StatGroups<S: Sums> {
    stat: Stat,
    /// One vector per sum, indexed by group.
    sums: Vec<Vec<S>>,
    n: Vec<u64>,
    /// The heap bytes the sums own. DataFusion asks for `size()` after every
    /// batch, so this adds up what each change allocates instead of walking
    /// every group there.
    heap: usize,
}

impl<S: Sums> StatGroups<S> {
    fn new(stat: Stat) -> Self {
        StatGroups {
            stat,
            sums: vec![Vec::new(); stat.sums()],
            n: Vec::new(),
            heap: 0,
        }
    }

    fn grow(&mut self, groups: usize) {
        for s in &mut self.sums {
            s.resize(groups, S::default());
        }
        self.n.resize(groups, 0);
    }

    fn take(&mut self, emit_to: EmitTo) -> (Vec<Vec<S>>, Vec<u64>) {
        let sums: Vec<Vec<S>> = self
            .sums
            .iter_mut()
            .map(|s| emit_to.take_needed(s))
            .collect();
        self.heap -= sums.iter().flatten().map(S::heap_bytes).sum::<usize>();
        (sums, emit_to.take_needed(&mut self.n))
    }
}

fn binary_column<S: Sums>(sums: &[S]) -> ArrayRef {
    let mut builder = BinaryBuilder::with_capacity(sums.len(), sums.len() * 24);
    let mut bytes = Vec::new();
    for s in sums {
        bytes.clear();
        s.serialize(&mut bytes);
        builder.append_value(&bytes);
    }
    Arc::new(builder.finish())
}

impl<S: Sums> GroupsAccumulator for StatGroups<S> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let (arrays, keep) = taking_part(values, opt_filter)?;
        let a = arrays[0].values();
        let b = arrays.get(1).map(|b| b.values());
        let kept = |i: usize| keep.as_ref().is_none_or(|k| k.value(i));
        macro_rules! each_row {
            (|$g:ident, $i:ident| $body:block) => {
                for ($i, &$g) in group_indices.iter().enumerate() {
                    if kept($i) {
                        self.n[$g] += 1;
                        $body
                    }
                }
            };
        }
        let mut heap = 0;
        match (self.stat, b) {
            (Stat::Sum | Stat::Mean, _) => each_row!(|g, i| {
                heap += self.sums[0][g].add(a[i]);
            }),
            (Stat::Var | Stat::Std, _) => each_row!(|g, i| {
                heap += self.sums[0][g].add(a[i]);
                heap += self.sums[1][g].add_product(a[i], a[i]);
            }),
            (Stat::Cov, Some(b)) => each_row!(|g, i| {
                heap += self.sums[0][g].add_product(a[i], b[i]);
                heap += self.sums[1][g].add(a[i]);
                heap += self.sums[2][g].add(b[i]);
            }),
            (Stat::Corr, Some(b)) => each_row!(|g, i| {
                heap += self.sums[0][g].add_product(a[i], b[i]);
                heap += self.sums[1][g].add(a[i]);
                heap += self.sums[2][g].add(b[i]);
                heap += self.sums[3][g].add_product(a[i], a[i]);
                heap += self.sums[4][g].add_product(b[i], b[i]);
            }),
            _ => return exec_err!("{} takes two arguments", self.stat.name()),
        }
        self.heap += heap;
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (sums, n) = self.take(emit_to);
        let mut row = Vec::with_capacity(sums.len());
        let values: Float64Array = (0..n.len())
            .map(|g| {
                row.clear();
                row.extend(sums.iter().map(|s| s[g].clone()));
                finish(self.stat, &row, n[g])
            })
            .collect();
        Ok(Arc::new(values))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (sums, n) = self.take(emit_to);
        let mut out: Vec<ArrayRef> = sums.iter().map(|s| binary_column(s)).collect();
        out.push(Arc::new(UInt64Array::from(n)));
        Ok(out)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let k = self.sums.len();
        let counts = values[k].as_primitive::<UInt64Type>();
        let states: Vec<&BinaryArray> = values[..k].iter().map(|v| v.as_binary::<i32>()).collect();
        for (row, &g) in group_indices.iter().enumerate() {
            if counts.is_null(row) {
                continue;
            }
            for (j, column) in states.iter().enumerate() {
                let other = S::deserialize(column.value(row))?;
                self.heap += self.sums[j][g].merge(&other);
            }
            self.n[g] += counts.value(row);
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let rows = values[0].len();
        let mut groups = StatGroups::<S>::new(self.stat);
        let identity: Vec<usize> = (0..rows).collect();
        groups.update_batch(values, &identity, opt_filter, rows)?;
        groups.state(EmitTo::All)
    }

    fn size(&self) -> usize {
        self.sums
            .iter()
            .map(|s| s.capacity() * size_of::<S>())
            .sum::<usize>()
            + self.n.capacity() * 8
            + self.heap
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StatUdaf {
    stat: Stat,
    mode: FloatSums,
    signature: Signature,
}

impl AggregateUDFImpl for StatUdaf {
    fn name(&self) -> &str {
        self.stat.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let k = self.stat.sums();
        Ok(match self.mode {
            FloatSums::Compensated => Box::new(StatAccumulator::<DoubleDouble> {
                stat: self.stat,
                sums: vec![DoubleDouble::default(); k],
                n: 0,
            }),
            _ => Box::new(StatAccumulator::<ExactAcc> {
                stat: self.stat,
                sums: vec![ExactAcc::default(); k],
                n: 0,
            }),
        })
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields: Vec<FieldRef> = (0..self.stat.sums())
            .map(|j| {
                Arc::new(Field::new(
                    format_state_name(args.name, &format!("s{j}")),
                    DataType::Binary,
                    true,
                ))
            })
            .collect();
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, "n"),
            DataType::UInt64,
            true,
        )));
        Ok(fields)
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(match self.mode {
            FloatSums::Compensated => Box::new(StatGroups::<DoubleDouble>::new(self.stat)),
            _ => Box::new(StatGroups::<ExactAcc>::new(self.stat)),
        })
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

/// Rewrites Float64 `sum`, `avg`, `var`, `stddev`, `covar_samp` and `corr`
/// into the UDAFs above.
#[derive(Debug)]
pub(crate) struct ExactFloat {
    udafs: Vec<(Stat, Arc<AggregateUDF>)>,
}

impl ExactFloat {
    pub(crate) fn new(mode: FloatSums) -> Self {
        let udafs = [
            Stat::Sum,
            Stat::Mean,
            Stat::Var,
            Stat::Std,
            Stat::Cov,
            Stat::Corr,
        ]
        .into_iter()
        .map(|stat| {
            let signature =
                Signature::exact(vec![DataType::Float64; stat.arity()], Volatility::Immutable);
            (
                stat,
                Arc::new(AggregateUDF::new_from_impl(StatUdaf {
                    stat,
                    mode,
                    signature,
                })),
            )
        })
        .collect();
        ExactFloat { udafs }
    }

    /// The UDAF replacing `func` over `args`, when it is one this rule covers.
    fn replacement(
        &self,
        name: &str,
        args: &[Expr],
        distinct: bool,
        schema: &DFSchema,
    ) -> Option<Arc<AggregateUDF>> {
        let stat = Stat::of(name)?;
        let float = |e: &Expr| planned_type(e, schema) == Some(DataType::Float64);
        if distinct || args.len() != stat.arity() || !args.iter().all(float) {
            return None;
        }
        self.udafs
            .iter()
            .find(|(s, _)| *s == stat)
            .map(|(_, f)| Arc::clone(f))
    }

    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        Ok(match expr {
            Expr::AggregateFunction(AggregateFunction { func, params }) => {
                match self.replacement(func.name(), &params.args, params.distinct, schema) {
                    Some(func) => Transformed::yes(Expr::AggregateFunction(AggregateFunction {
                        func,
                        params,
                    })),
                    None => {
                        Transformed::no(Expr::AggregateFunction(AggregateFunction { func, params }))
                    }
                }
            }
            Expr::WindowFunction(window) => {
                let WindowFunction { fun, params } = *window;
                let replaced = match &fun {
                    WindowFunctionDefinition::AggregateUDF(f) => {
                        self.replacement(f.name(), &params.args, params.distinct, schema)
                    }
                    _ => None,
                };
                match replaced {
                    Some(f) => Transformed::yes(Expr::WindowFunction(Box::new(WindowFunction {
                        fun: WindowFunctionDefinition::AggregateUDF(f),
                        params,
                    }))),
                    None => Transformed::no(Expr::WindowFunction(Box::new(WindowFunction {
                        fun,
                        params,
                    }))),
                }
            }
            other => Transformed::no(other),
        })
    }
}

impl OptimizerRule for ExactFloat {
    fn name(&self) -> &str {
        "ltseq_m0_exact_float"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !matches!(plan, LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)) {
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

    fn column(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn evaluate<S: Sums>(stat: Stat, args: &[&[f64]]) -> Option<f64> {
        let mut acc = StatAccumulator::<S> {
            stat,
            sums: vec![S::default(); stat.sums()],
            n: 0,
        };
        let arrays: Vec<ArrayRef> = args.iter().map(|a| column(a)).collect();
        acc.update_batch(&arrays).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn exact_sums_round_once_where_f64_loses_the_answer() {
        let xs = [1e16, 1.0, -1e16];
        assert_eq!(xs.iter().sum::<f64>(), 0.0);
        assert_eq!(evaluate::<ExactAcc>(Stat::Sum, &[&xs]), Some(1.0));
        assert_eq!(evaluate::<DoubleDouble>(Stat::Sum, &[&xs]), Some(1.0));
        // Var of values with a large offset: f64's one-pass formula fails.
        let ys = [1e9 + 4.0, 1e9 + 7.0, 1e9 + 13.0, 1e9 + 16.0];
        assert_eq!(evaluate::<ExactAcc>(Stat::Var, &[&ys]), Some(30.0));
        assert_eq!(evaluate::<ExactAcc>(Stat::Mean, &[&ys]), Some(1e9 + 10.0));
        let a = [1.0, 2.0, 3.0];
        let b = [2.0, 4.0, 6.0];
        assert_eq!(evaluate::<ExactAcc>(Stat::Corr, &[&a, &b]), Some(1.0));
        assert_eq!(evaluate::<ExactAcc>(Stat::Cov, &[&a, &b]), Some(2.0));
        assert_eq!(evaluate::<ExactAcc>(Stat::Std, &[&[1.0, 1.0]]), Some(0.0));
        assert_eq!(evaluate::<ExactAcc>(Stat::Var, &[&[1.0]]), None);
    }

    #[test]
    fn groups_and_partial_states_agree_with_one_accumulator() {
        let xs: Vec<f64> = (0..1000).map(|i| (i as f64) * 0.1 + 1e8).collect();
        let whole = evaluate::<ExactAcc>(Stat::Var, &[&xs]);
        let mut groups = StatGroups::<ExactAcc>::new(Stat::Var);
        // Two partial states for group 0, merged.
        let half = xs.len() / 2;
        let left = groups
            .convert_to_state(&[column(&xs[..half])], None)
            .unwrap();
        let mut merged = StatGroups::<ExactAcc>::new(Stat::Var);
        merged.merge_batch(&left, &vec![0; half], 1).unwrap();
        groups
            .update_batch(&[column(&xs[half..])], &vec![0; xs.len() - half], None, 1)
            .unwrap();
        let right = groups.state(EmitTo::All).unwrap();
        merged.merge_batch(&right, &[0], 1).unwrap();
        let out = merged.evaluate(EmitTo::All).unwrap();
        assert_eq!(out.as_primitive::<Float64Type>().value(0), whole.unwrap());
    }

    /// `size()` as a walk over every group's sums.
    fn walked_size<S: Sums>(groups: &StatGroups<S>) -> usize {
        let heap: usize = groups.sums.iter().flatten().map(S::heap_bytes).sum();
        groups
            .sums
            .iter()
            .map(|s| s.capacity() * size_of::<S>())
            .sum::<usize>()
            + groups.n.capacity() * 8
            + heap
    }

    #[test]
    fn group_sizes_follow_growth_merges_and_emits() {
        // Exponents far apart grow a superaccumulator's chunks; an infinity
        // allocates its non-finite state.
        let a = [1e-300, 1e300, 3.5, f64::INFINITY, -2e150, 1e-10];
        let b = [2.0, -1e200, 1e-200, 4.0, 5.0, f64::NAN];
        let columns = [column(&a), column(&b)];
        let mut groups = StatGroups::<ExactAcc>::new(Stat::Corr);
        let mut other = StatGroups::<ExactAcc>::new(Stat::Corr);
        for round in 0..4 {
            let indices: Vec<usize> = (0..a.len()).map(|i| (i + round) % 5).collect();
            groups.update_batch(&columns, &indices, None, 5).unwrap();
            assert_eq!(groups.size(), walked_size(&groups), "update {round}");
            other
                .update_batch(&columns, &[0, 1, 0, 1, 0, 1], None, 2)
                .unwrap();
            let state = other.state(EmitTo::First(1)).unwrap();
            assert_eq!(other.size(), walked_size(&other), "state {round}");
            groups.merge_batch(&state, &[4], 5).unwrap();
            assert_eq!(groups.size(), walked_size(&groups), "merge {round}");
            groups.evaluate(EmitTo::First(2)).unwrap();
            assert_eq!(groups.size(), walked_size(&groups), "evaluate {round}");
        }
        assert!(groups.heap > 0);
        groups.evaluate(EmitTo::All).unwrap();
        assert_eq!(groups.heap, 0);
    }

    #[test]
    fn windows_retract_exactly() {
        let xs = [1e16, 1.0, -1e16, 3.0, 5.0];
        let mut acc = StatAccumulator::<ExactAcc> {
            stat: Stat::Sum,
            sums: vec![ExactAcc::default()],
            n: 0,
        };
        acc.update_batch(&[column(&xs)]).unwrap();
        acc.retract_batch(&[column(&xs[..2])]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Float64(Some(-1e16 + 8.0))
        );
        let nulls: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.0), None]));
        acc.update_batch(&[nulls]).unwrap();
        assert_eq!(acc.n, 4);
    }
}
