//! Floor division (`//`) with Python's semantics.
//!
//! DataFusion has no floor-division operator, and its `/` truncates
//! integers toward zero: `-7 / 2 == -3` where Python's `-7 // 2 == -4`.
//! Every evaluator that runs `//` goes through this module — the
//! `floor_div` UDF in DataFusion plans, [`floor_div_arrays`] in the
//! hand-written linear-scan and search_pattern evaluators — so the
//! semantics are defined once:
//!
//! - Integer operands floor in integer arithmetic (no float round trip, so
//!   values beyond 2^53 stay exact) and produce Int64, or UInt64 when both
//!   operands are unsigned.
//! - A float operand makes both Float64, computed with CPython's float
//!   floor-division algorithm (`1.0 // 0.1 == 9.0`, signed zeros, inf/NaN).
//! - A zero divisor is an error for integers and floats alike, as Python
//!   raises ZeroDivisionError (`/` on floats returns inf instead).
//! - Integer overflow (`i64::MIN // -1`) is an error, as it is for `/`.
//! - NULL in either operand gives NULL, never an error.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray};
use datafusion::arrow::compute::{cast_with_options, try_binary, CastOptions};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::utils::take_function_args;
use datafusion::common::{internal_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

static FLOOR_DIV: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(FloorDivUdf {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
});

/// `left // right` as a DataFusion expression.
pub(crate) fn floor_div(left: Expr, right: Expr) -> Expr {
    FLOOR_DIV.call(vec![left, right])
}

/// `left // right` over two arrays of any supported types.
pub(crate) fn floor_div_arrays(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    let target = common_type(left.data_type(), right.data_type())?;
    // A value that does not fit the common type must error, not become
    // NULL (`safe: true`, arrow's default, would silently null it).
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let left = cast_with_options(left, &target, &options)?;
    let right = cast_with_options(right, &target, &options)?;
    let result: ArrayRef = match target {
        DataType::Int64 => Arc::new(try_binary::<_, _, _, Int64Type>(
            left.as_primitive::<Int64Type>(),
            right.as_primitive::<Int64Type>(),
            floor_div_int,
        )?),
        DataType::UInt64 => Arc::new(try_binary::<_, _, _, UInt64Type>(
            left.as_primitive::<UInt64Type>(),
            right.as_primitive::<UInt64Type>(),
            floor_div_int,
        )?),
        DataType::Float64 => Arc::new(try_binary::<_, _, _, Float64Type>(
            left.as_primitive::<Float64Type>(),
            right.as_primitive::<Float64Type>(),
            floor_div_float,
        )?),
        other => return internal_err!("floor_div: unexpected common type {other}"),
    };
    Ok(result)
}

/// The type both operands are cast to, which is also the result type.
fn common_type(lhs: &DataType, rhs: &DataType) -> Result<DataType> {
    let int_or_float = |t: &DataType| t.is_integer() || t.is_floating();
    match (lhs, rhs) {
        // Dictionary-encoded columns (pandas categoricals, dictionary Parquet
        // pages) divide as their values, as they do for `/`.
        (DataType::Dictionary(_, value), other) | (other, DataType::Dictionary(_, value)) => {
            common_type(value, other)
        }
        (DataType::Null, DataType::Null) => Ok(DataType::Int64),
        (DataType::Null, other) | (other, DataType::Null) => common_type(other, other),
        (l, r) if !int_or_float(l) || !int_or_float(r) => {
            plan_err!("floor division (//) needs integer or float operands, got {lhs} and {rhs}")
        }
        (l, r) if l.is_floating() || r.is_floating() => Ok(DataType::Float64),
        (l, r) if l.is_unsigned_integer() && r.is_unsigned_integer() => Ok(DataType::UInt64),
        _ => Ok(DataType::Int64),
    }
}

/// Integer `a // b`. `div_checked` truncates toward zero (and rejects a zero
/// divisor and `MIN / -1`); the floor is one lower when the division was
/// inexact and the remainder's sign disagrees with the divisor's.
fn floor_div_int<T: ArrowNativeTypeOp>(a: T, b: T) -> Result<T, ArrowError> {
    let quotient = a.div_checked(b)?;
    let remainder = a.mod_wrapping(b);
    if !remainder.is_zero() && (remainder < T::ZERO) != (b < T::ZERO) {
        Ok(quotient.sub_wrapping(T::ONE))
    } else {
        Ok(quotient)
    }
}

/// Float `a // b`, ported from CPython's `_float_div_mod`. `floor(a / b)`
/// is not the same: `1.0 / 0.1` rounds to exactly 10.0, but Python's
/// `1.0 // 0.1` is 9.0 because `0.1` is slightly more than one tenth.
fn floor_div_float(a: f64, b: f64) -> Result<f64, ArrowError> {
    if b == 0.0 {
        return Err(ArrowError::DivideByZero);
    }
    // `%` on f64 is C's fmod: exact, with the sign of `a`, so `a - modulo`
    // is mathematically a multiple of `b`.
    let modulo = a % b;
    let mut div = (a - modulo) / b;
    if modulo != 0.0 && (b < 0.0) != (modulo < 0.0) {
        div -= 1.0;
    }
    if div == 0.0 {
        // A zero quotient takes the sign of the true quotient.
        return Ok(0.0_f64.copysign(a / b));
    }
    // `div` is within rounding error of an integer; snap to it.
    let floor = div.floor();
    Ok(if div - floor > 0.5 {
        floor + 1.0
    } else {
        floor
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FloorDivUdf {
    signature: Signature,
}

impl ScalarUDFImpl for FloorDivUdf {
    fn name(&self) -> &str {
        "floor_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [lhs, rhs] = take_function_args(self.name(), arg_types)?;
        common_type(lhs, rhs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let common = self.return_type(arg_types)?;
        Ok(vec![common.clone(), common])
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = take_function_args(self.name(), &arrays)?;
        let result = floor_div_arrays(left, right)?;
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, DictionaryArray, Float64Array, Int32Array, Int64Array, Int8Array, NullArray,
        StringArray, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::datatypes::Int8Type;

    // Expected values were computed with CPython's `//`.

    #[test]
    fn int_matches_python() {
        const MIN: i64 = i64::MIN;
        const MAX: i64 = i64::MAX;
        let table = [
            (7, 2, 3),
            (-7, 2, -4),
            (7, -2, -4),
            (-7, -2, 3),
            (-3, 2, -2),
            (3, -2, -2),
            (6, 3, 2),
            (-6, 3, -2),
            (0, 5, 0),
            (0, -5, 0),
            (MIN, 1, MIN),
            (MIN, 2, -4611686018427387904),
            (MAX, -1, -MAX),
            (MIN + 1, -1, MAX),
            (-1, MAX, -1),
            (1, MIN, -1),
            (MIN, MAX, -2),
            // Beyond 2^53: a Float64 round trip would lose the last digit.
            (9007199254740993, 1, 9007199254740993),
            (9007199254740993, 2, 4503599627370496),
        ];
        for (a, b, expected) in table {
            assert_eq!(floor_div_int(a, b).unwrap(), expected, "{a} // {b}");
        }
    }

    #[test]
    fn unsigned_is_truncation() {
        assert_eq!(floor_div_int(7_u64, 2).unwrap(), 3);
        assert_eq!(floor_div_int(u64::MAX, 1).unwrap(), u64::MAX);
    }

    #[test]
    fn int_zero_divisor_and_overflow_error() {
        assert!(matches!(
            floor_div_int(1_i64, 0),
            Err(ArrowError::DivideByZero)
        ));
        assert!(matches!(
            floor_div_int(0_u64, 0),
            Err(ArrowError::DivideByZero)
        ));
        assert!(matches!(
            floor_div_int(i64::MIN, -1),
            Err(ArrowError::ArithmeticOverflow(_))
        ));
    }

    #[test]
    fn float_matches_python() {
        let inf = f64::INFINITY;
        let table = [
            (7.5, 2.0, 3.0),
            (-7.5, 2.0, -4.0),
            (7.5, -2.0, -4.0),
            (-7.5, -2.0, 3.0),
            (7.0, 2.0, 3.0),
            (-7.0, 2.5, -3.0),
            (1.0, 0.1, 9.0),
            (-1.0, 0.1, -10.0),
            (1.0, inf, 0.0),
            (-1.0, inf, -1.0),
            (5.0, -inf, -1.0),
            (-5.0, -inf, 0.0),
        ];
        for (a, b, expected) in table {
            assert_eq!(floor_div_float(a, b).unwrap(), expected, "{a} // {b}");
        }
    }

    #[test]
    fn float_zero_sign_matches_python() {
        // (a, b, expected sign is negative)
        let table = [(0.0, 3.0, false), (-0.0, 3.0, true), (0.0, -3.0, true)];
        for (a, b, negative) in table {
            let got = floor_div_float(a, b).unwrap();
            assert_eq!(got, 0.0, "{a} // {b}");
            assert_eq!(got.is_sign_negative(), negative, "{a} // {b} sign");
        }
    }

    #[test]
    fn float_nan_and_inf_dividend_give_nan() {
        for (a, b) in [(f64::INFINITY, 1.0), (f64::NAN, 1.0), (1.0, f64::NAN)] {
            assert!(floor_div_float(a, b).unwrap().is_nan(), "{a} // {b}");
        }
    }

    #[test]
    fn float_zero_divisor_errors() {
        for (a, b) in [(1.0, 0.0), (1.0, -0.0), (0.0, 0.0)] {
            assert!(
                matches!(floor_div_float(a, b), Err(ArrowError::DivideByZero)),
                "{a} // {b}"
            );
        }
    }

    #[test]
    fn common_type_table() {
        use DataType::*;
        let table = [
            (Int64, Int64, Int64),
            (Int32, Int64, Int64),
            (Int32, Int32, Int64),
            (Int8, UInt8, Int64),
            (UInt32, UInt64, UInt64),
            (UInt8, UInt8, UInt64),
            (Int64, UInt64, Int64),
            (Int64, Float64, Float64),
            (Float32, Int32, Float64),
            (Float32, Float32, Float64),
            (Null, Int32, Int64),
            (Float64, Null, Float64),
            (Null, Null, Int64),
            (Dictionary(Box::new(Int8), Box::new(Int64)), Int64, Int64),
            (
                Int8,
                Dictionary(Box::new(Int32), Box::new(Float32)),
                Float64,
            ),
            (
                Dictionary(Box::new(Int8), Box::new(UInt32)),
                Dictionary(Box::new(Int8), Box::new(UInt8)),
                UInt64,
            ),
        ];
        for (lhs, rhs, expected) in table {
            assert_eq!(common_type(&lhs, &rhs).unwrap(), expected, "{lhs} // {rhs}");
        }
    }

    #[test]
    fn common_type_rejects_non_int_or_float() {
        use DataType::*;
        for (lhs, rhs) in [
            (Utf8, Int64),
            (Int64, Boolean),
            (Decimal128(10, 2), Int64),
            (Date32, Int64),
            (Dictionary(Box::new(Int8), Box::new(Utf8)), Int64),
        ] {
            let err = common_type(&lhs, &rhs).unwrap_err().to_string();
            assert!(err.contains("needs integer or float operands"), "{err}");
        }
    }

    #[test]
    fn arrays_mixed_types_and_nulls() {
        let left: ArrayRef = Arc::new(Int32Array::from(vec![Some(-7), None, Some(9), Some(5)]));
        // The NULL divisor sits on a valid dividend: it must give NULL, not
        // a divide-by-zero error from the value under the null slot.
        let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(3), None, Some(-2)]));
        let got = floor_div_arrays(&left, &right).unwrap();
        let got = got.as_primitive::<Int64Type>();
        assert_eq!(
            got.iter().collect::<Vec<_>>(),
            [Some(-4), None, None, Some(-3)]
        );
    }

    #[test]
    fn arrays_int_and_float_give_float() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![7, -7]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 2.0]));
        let got = floor_div_arrays(&left, &right).unwrap();
        assert_eq!(got.data_type(), &DataType::Float64);
        assert_eq!(got.as_primitive::<Float64Type>().values(), &[3.0, -4.0]);
    }

    #[test]
    fn arrays_unsigned_stay_unsigned() {
        let left: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        let right: ArrayRef = Arc::new(UInt32Array::from(vec![2]));
        let got = floor_div_arrays(&left, &right).unwrap();
        assert_eq!(got.as_primitive::<UInt64Type>().value(0), u64::MAX / 2);
    }

    #[test]
    fn arrays_out_of_range_cast_errors() {
        // UInt64 above i64::MAX against a signed operand: the Int64 cast
        // must fail rather than null the row.
        let left: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        assert!(floor_div_arrays(&left, &right).is_err());
    }

    #[test]
    fn arrays_null_typed_operand() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let right: ArrayRef = Arc::new(NullArray::new(2));
        let got = floor_div_arrays(&left, &right).unwrap();
        assert_eq!(got.data_type(), &DataType::Int64);
        assert_eq!(got.null_count(), 2);
    }

    #[test]
    fn arrays_dictionary_operand() {
        let keys = Int8Array::from(vec![0, 1, 0]);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![-7, 9]));
        let left: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
        let right: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, -2]));
        let got = floor_div_arrays(&left, &right).unwrap();
        assert_eq!(got.as_primitive::<Int64Type>().values(), &[-4, 4, 3]);
    }

    #[test]
    fn arrays_reject_strings() {
        let left: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        assert!(floor_div_arrays(&left, &right).is_err());
    }
}
