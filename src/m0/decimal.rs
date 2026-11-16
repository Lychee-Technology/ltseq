//! m7: decimal `/` and `%` through a 256-bit intermediate (M6, #306).
//!
//! arrow-rs computes decimal `/` and `%` in `i128`, and rescales an operand
//! there before dividing: `/` multiplies the dividend by `10^(s - s1 + s2)`
//! (`s` the result's scale), `%` brings both operands to the larger scale.
//! That product overflows where the contract's result fits (#276's N5): a
//! `decimal128(38, 10)` 1e15 `/` `decimal128(38, 10)` 1 raises, though its
//! `decimal(38, 14)` quotient holds 1e15. This rule rewrites `a / b` and
//! `a % b` on two `decimal128` operands into `m0_decimal_div` and
//! `m0_decimal_rem`, which compute the contract's value (§17.3):
//!
//! - `/` truncates the exact quotient toward zero at the result's scale;
//! - `%` takes the divisor's sign (floored), where arrow's takes the
//!   dividend's;
//! - a zero divisor raises `DivisionByZeroError`, and a result that does not
//!   fit the contract's precision raises `ArithmeticOverflowError`.
//!
//! [`Decimal::Wide`] computes every row in `i256`. [`Decimal::Guarded`]
//! computes a row in `i128` and takes the `i256` path only for a row whose
//! rescaling overflows `i128`. `i256` always suffices: `%` rescales to at
//! most 38 digits more, and a `/` whose rescaled dividend overflows `i256`
//! (above 5.7e76) has a quotient of more than 38 digits, which raises.
//!
//! The result keeps the plan's type, which an optimizer rule may not change.
//! For `/` that is the contract's type (DataFusion's formula). For `%` the
//! contract's precision is `min(p2 - s2 + s, 38)` where DataFusion plans
//! `min(p1 - s1, p2 - s2) + s`, because a floored remainder can be as large
//! as the divisor; the prototype checks the contract's precision and emits
//! DataFusion's type, so a value can exceed the precision its type states.
//! Neither changes the work per row, which is what this prices.
//!
//! Only `decimal128` with `decimal128` is rewritten: an integer operand
//! reaches the operator already cast to a decimal by DataFusion's coercion,
//! and `decimal256` operands already compute in `i256`. Nodes with no input or
//! several are left alone, as in [`super::checked`].

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{i256, DataType, Decimal128Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, LogicalPlan, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use super::checked::OVERFLOW;
use super::Decimal;
use crate::transpiler::planned_type;

/// The error the contract names for a zero divisor.
pub(crate) const ZERO_DIVISOR: &str = "DivisionByZeroError";

const MAX_PRECISION: u8 = 38;

/// `10^k` for `k <= 38` in `i128`, and every `k <= 76` in `i256`.
fn pow10_i128(k: u32) -> Option<i128> {
    10i128.checked_pow(k)
}

fn pow10_i256(k: u32) -> i256 {
    i256::from_i128(10).wrapping_pow(k)
}

/// The operator a decimal function computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Op {
    Div,
    Rem,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Div => "m0_decimal_div",
            Op::Rem => "m0_decimal_rem",
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Op::Div => "/",
            Op::Rem => "%",
        }
    }
}

/// What one row computes, from the operands' and the result's types.
#[derive(Debug, Clone, Copy)]
struct Plan {
    op: Op,
    /// Multipliers of the dividend and the divisor (`%` only) in `i256`,
    /// and in `i128` where they fit.
    l_mul: i256,
    r_mul: i256,
    l_mul_128: Option<i128>,
    r_mul_128: Option<i128>,
    /// Exclusive bound on a result's magnitude: `10^precision`.
    bound: i128,
}

impl Plan {
    /// `(p1, s1) op (p2, s2)` with the result at scale `s`.
    fn new(op: Op, (p1, s1): (u8, i8), (p2, s2): (u8, i8), s: i8) -> Result<Plan> {
        if s1 < 0 || s2 < 0 || s < 0 {
            return exec_err!("{} takes no negative scale", op.name());
        }
        let (s1, s2, s) = (s1 as u32, s2 as u32, s as u32);
        let (l_k, r_k, precision) = match op {
            // The quotient l / r at scale s is (l * 10^(s - s1 + s2)) / r,
            // of precision `min(p1 - s1 + s2 + s, 38)`.
            Op::Div if s + s2 >= s1 => (
                s + s2 - s1,
                0,
                (p1 as u32 + s2 + s)
                    .saturating_sub(s1)
                    .min(MAX_PRECISION as u32),
            ),
            Op::Div => return exec_err!("m0_decimal_div: result scale below the dividend's"),
            // Both operands at scale s = max(s1, s2); the remainder is
            // smaller than the divisor, so `p2 - s2 + s` digits hold it.
            Op::Rem => (
                s.saturating_sub(s1),
                s.saturating_sub(s2),
                (p2 as u32 - s2 + s).min(MAX_PRECISION as u32),
            ),
        };
        if l_k > 76 || r_k > 76 {
            return exec_err!("{}: a rescaling beyond 76 digits", op.name());
        }
        Ok(Plan {
            op,
            l_mul: pow10_i256(l_k),
            r_mul: pow10_i256(r_k),
            l_mul_128: pow10_i128(l_k),
            r_mul_128: pow10_i128(r_k),
            bound: 10i128.pow(precision),
        })
    }

    fn overflow<T>(&self, l: i128, r: i128) -> Result<T> {
        exec_err!(
            "{OVERFLOW}: decimal {l} {} {r} does not fit its result type",
            self.op.symbol()
        )
    }

    /// The row in `i128`, or `None` when a rescaling overflows `i128`.
    #[inline(always)]
    fn narrow(&self, l: i128, r: i128) -> Option<i128> {
        let l = l.checked_mul(self.l_mul_128?)?;
        let r = r.checked_mul(self.r_mul_128?)?;
        Some(match self.op {
            Op::Div => l.checked_div(r)?,
            Op::Rem => {
                let m = l.checked_rem(r)?;
                if m != 0 && (m < 0) != (r < 0) {
                    m + r
                } else {
                    m
                }
            }
        })
    }

    /// The row in `i256`; `None` when the quotient cannot fit.
    #[inline(always)]
    fn wide(&self, l: i128, r: i128) -> Option<i128> {
        let l = i256::from_i128(l).checked_mul(self.l_mul)?;
        // `r_mul` is at most 10^38, so the divisor never overflows.
        let r = i256::from_i128(r).wrapping_mul(self.r_mul);
        let v = match self.op {
            Op::Div => l.checked_div(r)?,
            Op::Rem => {
                let m = l.checked_rem(r)?;
                if m != i256::ZERO && m.is_negative() != r.is_negative() {
                    m.wrapping_add(r)
                } else {
                    m
                }
            }
        };
        v.to_i128()
    }

    /// One valid row: a zero divisor and a result beyond the precision raise.
    #[inline(always)]
    fn row(&self, mode: Decimal, l: i128, r: i128) -> Result<i128> {
        if r == 0 {
            return exec_err!("{ZERO_DIVISOR}: decimal {l} {} 0", self.op.symbol());
        }
        let v = match mode {
            Decimal::Guarded => match self.narrow(l, r) {
                Some(v) => Some(v),
                None => self.wide(l, r),
            },
            _ => self.wide(l, r),
        };
        match v {
            Some(v) if v.unsigned_abs() < self.bound as u128 => Ok(v),
            _ => self.overflow(l, r),
        }
    }
}

/// One operand: an array, or a scalar for every row.
enum Side<'a> {
    Array(&'a Decimal128Array),
    Scalar(Option<i128>),
}

impl Side<'_> {
    #[inline(always)]
    fn at(&self, i: usize) -> i128 {
        match self {
            Side::Array(a) => a.value(i),
            Side::Scalar(v) => v.unwrap_or_default(),
        }
    }

    fn nulls(&self, len: usize) -> Option<NullBuffer> {
        match self {
            Side::Array(a) => a.nulls().cloned(),
            Side::Scalar(Some(_)) => None,
            Side::Scalar(None) => Some(NullBuffer::new_null(len)),
        }
    }
}

fn side(value: &ColumnarValue) -> Result<(Side<'_>, (u8, i8))> {
    match value {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Decimal128(p, s) => {
                Ok((Side::Array(a.as_primitive::<Decimal128Type>()), (*p, *s)))
            }
            other => exec_err!("decimal arithmetic takes decimal128, not {other}"),
        },
        ColumnarValue::Scalar(ScalarValue::Decimal128(v, p, s)) => Ok((Side::Scalar(*v), (*p, *s))),
        ColumnarValue::Scalar(other) => {
            exec_err!(
                "decimal arithmetic takes decimal128, not {}",
                other.data_type()
            )
        }
    }
}

/// `left op right` over `len` rows into `out`'s precision and scale.
fn evaluate(
    op: Op,
    mode: Decimal,
    out: (u8, i8),
    left: &ColumnarValue,
    right: &ColumnarValue,
    len: usize,
) -> Result<Decimal128Array> {
    let (l, l_type) = side(left)?;
    let (r, r_type) = side(right)?;
    let plan = Plan::new(op, l_type, r_type, out.1)?;
    let nulls = NullBuffer::union(l.nulls(len).as_ref(), r.nulls(len).as_ref());
    let mut values = vec![0i128; len];
    match &nulls {
        None => {
            for (i, v) in values.iter_mut().enumerate() {
                *v = plan.row(mode, l.at(i), r.at(i))?;
            }
        }
        Some(n) => {
            for i in n.valid_indices() {
                values[i] = plan.row(mode, l.at(i), r.at(i))?;
            }
        }
    }
    Ok(Decimal128Array::new(ScalarBuffer::from(values), nulls)
        .with_precision_and_scale(out.0, out.1)?)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalUdf {
    op: Op,
    mode: Decimal,
    /// The plan's type for the expression this call replaces.
    out: (u8, i8),
    signature: Signature,
}

impl ScalarUDFImpl for DecimalUdf {
    fn name(&self) -> &str {
        self.op.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(self.out.0, self.out.1))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("{} takes two arguments", self.op.name());
        };
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let len = if scalar { 1 } else { args.number_rows };
        let result: ArrayRef = Arc::new(evaluate(self.op, self.mode, self.out, left, right, len)?);
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

/// Rewrites a plan's `decimal128` `/` and `%` into the functions above.
#[derive(Debug)]
pub(crate) struct WideDecimal {
    mode: Decimal,
}

impl WideDecimal {
    pub(crate) fn new(mode: Decimal) -> Self {
        WideDecimal { mode }
    }

    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        let decimal = |e: &Expr| match planned_type(e, schema) {
            Some(DataType::Decimal128(p, s)) => Some((p, s)),
            _ => None,
        };
        Ok(match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::Divide | Operator::Modulo)
                    && decimal(&left).is_some()
                    && decimal(&right).is_some() =>
            {
                let whole = Expr::BinaryExpr(BinaryExpr { left, op, right });
                let Some(out) = decimal(&whole) else {
                    return Ok(Transformed::no(whole));
                };
                let Expr::BinaryExpr(BinaryExpr { left, op, right }) = whole else {
                    unreachable!("built above");
                };
                let op = if op == Operator::Divide {
                    Op::Div
                } else {
                    Op::Rem
                };
                let udf = ScalarUDF::new_from_impl(DecimalUdf {
                    op,
                    mode: self.mode,
                    out,
                    signature: Signature::any(2, Volatility::Immutable),
                });
                Transformed::yes(Arc::new(udf).call(vec![*left, *right]))
            }
            other => Transformed::no(other),
        })
    }
}

impl OptimizerRule for WideDecimal {
    fn name(&self) -> &str {
        "ltseq_m0_wide_decimal"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
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

    fn column(values: Vec<Option<i128>>, p: u8, s: i8) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        ))
    }

    fn run(
        op: Op,
        mode: Decimal,
        out: (u8, i8),
        l: (Vec<Option<i128>>, u8, i8),
        r: (Vec<Option<i128>>, u8, i8),
    ) -> Result<Vec<Option<i128>>> {
        let len = l.0.len();
        let out = evaluate(
            op,
            mode,
            out,
            &column(l.0, l.1, l.2),
            &column(r.0, r.1, r.2),
            len,
        )?;
        Ok(out.iter().collect())
    }

    const MODES: [Decimal; 2] = [Decimal::Wide, Decimal::Guarded];
    const E10: i128 = 10_000_000_000;

    /// #306's two probes: arrow's rescaling overflows, the contract's result
    /// fits, and both modes give it.
    #[test]
    fn the_issues_probes_fit() {
        for mode in MODES {
            // decimal128(38, 10) 1e15 / 1 is decimal(38, 14) 1e15.
            let e15 = 10i128.pow(15) * E10;
            let got = run(
                Op::Div,
                mode,
                (38, 14),
                (vec![Some(e15)], 38, 10),
                (vec![Some(E10)], 38, 10),
            );
            assert_eq!(got.unwrap(), vec![Some(10i128.pow(29))], "{mode:?}");
            // decimal128(38, 0) 10^37 % decimal128(38, 10) 7 is 3.
            let got = run(
                Op::Rem,
                mode,
                (38, 10),
                (vec![Some(10i128.pow(37))], 38, 0),
                (vec![Some(7 * E10)], 38, 10),
            );
            assert_eq!(got.unwrap(), vec![Some(3 * E10)], "{mode:?}");
        }
    }

    #[test]
    fn division_truncates_and_remainders_take_the_divisors_sign() {
        for mode in MODES {
            // 7 / -2 at scale 4 (s1 = s2 = 0): -3.5000.
            let got = run(
                Op::Div,
                mode,
                (24, 4),
                (vec![Some(7), Some(-1)], 20, 0),
                (vec![Some(-2), Some(3)], 20, 0),
            );
            assert_eq!(got.unwrap(), vec![Some(-35_000), Some(-3_333)]);
            // decimal(4, 2) -10.50 % decimal(5, 2) 360.00 is 349.50.
            let got = run(
                Op::Rem,
                mode,
                (5, 2),
                (vec![Some(-1050), Some(1050)], 4, 2),
                (vec![Some(36000), Some(-36000)], 5, 2),
            );
            assert_eq!(got.unwrap(), vec![Some(34950), Some(-34950)]);
        }
    }

    #[test]
    fn zero_divisors_raise_and_nulls_never_do() {
        for mode in MODES {
            for op in [Op::Div, Op::Rem] {
                let err = run(
                    op,
                    mode,
                    (38, 4),
                    (vec![Some(1)], 20, 0),
                    (vec![Some(0)], 20, 0),
                )
                .unwrap_err();
                assert!(err.to_string().contains(ZERO_DIVISOR), "{err}");
                let ok = run(
                    op,
                    mode,
                    (38, 4),
                    (vec![None, Some(1)], 20, 0),
                    (vec![Some(0), None], 20, 0),
                )
                .unwrap();
                assert_eq!(ok, vec![None, None]);
            }
        }
    }

    #[test]
    fn a_quotient_beyond_the_precision_raises() {
        for mode in MODES {
            // (10^38 - 1) / 0.1 is about 10^39: 43 digits at scale 4, beyond 38.
            let err = run(
                Op::Div,
                mode,
                (38, 4),
                (vec![Some(10i128.pow(38) - 1)], 38, 0),
                (vec![Some(1)], 38, 1),
            )
            .unwrap_err();
            assert!(err.to_string().contains(OVERFLOW), "{err}");
        }
    }

    /// Every row the guarded mode computes in `i128` agrees with `i256`.
    #[test]
    fn the_narrow_and_wide_paths_agree() {
        let values = [
            1i128,
            -1,
            7,
            -7,
            12_345_678_901_234,
            -98_765_432_109_876_543_210,
            10i128.pow(30),
        ];
        for op in [Op::Div, Op::Rem] {
            for (s1, s2) in [(0i8, 0i8), (10, 10), (0, 10), (10, 0), (2, 6)] {
                let s = if op == Op::Div {
                    (s1 + 4).min(38)
                } else {
                    s1.max(s2)
                };
                let plan = Plan::new(op, (38, s1), (38, s2), s).unwrap();
                for &l in &values {
                    for &r in &values {
                        if let Some(narrow) = plan.narrow(l, r) {
                            assert_eq!(
                                Some(narrow),
                                plan.wide(l, r),
                                "{op:?} {l} {r} at {s1},{s2}"
                            );
                        }
                    }
                }
            }
        }
    }
}
