//! True division (`/`) of two integers with Python's semantics (#218).
//!
//! DataFusion's `/` on two integer operands truncates toward zero and keeps
//! an integer type: `7 / 2 == 3`, where Python's `7 / 2 == 3.5`. Every
//! evaluator that runs `/` on two integers goes through this module (the
//! `true_div` UDF in DataFusion plans, [`true_div_arrays`] in the linear-scan
//! and search_pattern evaluators), so the semantics are defined once:
//!
//! - The result is Float64: the exact quotient rounded once to the nearest
//!   binary64, as CPython's `int / int` rounds it. Operands beyond 2^53 are
//!   divided in integer arithmetic, not converted to Float64 first, which
//!   would round them before the division rounds again.
//! - A zero divisor is an error, as Python raises ZeroDivisionError. A float
//!   cast would give `inf` or NaN instead. Nothing else is an error: every
//!   quotient of two 64-bit integers is a finite binary64.
//! - NULL in either operand gives NULL, never an error.
//!
//! Only integer pairs come here ([`integer_pair`]): a float operand keeps
//! DataFusion's IEEE division (`1 / 0.0` is `inf`) and a decimal keeps
//! decimal division.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::compute::{cast_with_options, try_binary, CastOptions};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::utils::take_function_args;
use datafusion::common::{internal_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

static TRUE_DIV: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(TrueDivUdf {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
});

/// `left / right` as a DataFusion expression, for two integer operands
/// ([`integer_pair`]).
pub(crate) fn true_div(left: Expr, right: Expr) -> Expr {
    TRUE_DIV.call(vec![left, right])
}

/// The type of the values of `t`, without dictionary or run-end encoding.
fn value_type(t: &DataType) -> &DataType {
    match t {
        DataType::Dictionary(_, value) => value_type(value),
        DataType::RunEndEncoded(_, values) => value_type(values.data_type()),
        other => other,
    }
}

/// Whether `left / right` on operands of these types is integer true
/// division: both are integers, or one is and the other is NULL-typed (a
/// `None` literal, which DataFusion reads as the other operand's type).
/// Encoded operands are read by their value type.
pub(crate) fn integer_pair(left: &DataType, right: &DataType) -> bool {
    match (value_type(left), value_type(right)) {
        (DataType::Null, DataType::Null) => false,
        (DataType::Null, other) | (other, DataType::Null) => other.is_integer(),
        (l, r) => l.is_integer() && r.is_integer(),
    }
}

/// The type an integer operand is cast to: UInt64 stays as it is, every
/// other integer type (and NULL) widens to Int64, which holds it exactly.
fn operand_type(t: &DataType) -> Result<DataType> {
    match value_type(t) {
        DataType::UInt64 => Ok(DataType::UInt64),
        DataType::Null => Ok(DataType::Int64),
        other if other.is_integer() => Ok(DataType::Int64),
        _ => plan_err!("integer true division (/) needs integer operands, got {t}"),
    }
}

/// `left / right` over two integer arrays (any integer types, encoded or
/// NULL-typed), as Float64.
pub(crate) fn true_div_arrays(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    let left_type = operand_type(left.data_type())?;
    let right_type = operand_type(right.data_type())?;
    // These casts only widen or decode, so none can fail. `safe: false` is a
    // backstop: arrow's default would turn a value that did not fit into
    // NULL instead of an error.
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let left = cast_with_options(left, &left_type, &options)?;
    let right = cast_with_options(right, &right_type, &options)?;
    // `try_binary` calls the kernel only where both operands are valid, so a
    // NULL divisor whose slot holds a zero is NULL, not an error.
    let result = match (&left_type, &right_type) {
        (DataType::Int64, DataType::Int64) => try_binary::<_, _, _, Float64Type>(
            left.as_primitive::<Int64Type>(),
            right.as_primitive::<Int64Type>(),
            |a, b| true_div_int(a.into(), b.into()),
        )?,
        (DataType::UInt64, DataType::UInt64) => try_binary::<_, _, _, Float64Type>(
            left.as_primitive::<UInt64Type>(),
            right.as_primitive::<UInt64Type>(),
            |a, b| true_div_int(a.into(), b.into()),
        )?,
        (DataType::Int64, DataType::UInt64) => try_binary::<_, _, _, Float64Type>(
            left.as_primitive::<Int64Type>(),
            right.as_primitive::<UInt64Type>(),
            |a, b| true_div_int(a.into(), b.into()),
        )?,
        (DataType::UInt64, DataType::Int64) => try_binary::<_, _, _, Float64Type>(
            left.as_primitive::<UInt64Type>(),
            right.as_primitive::<Int64Type>(),
            |a, b| true_div_int(a.into(), b.into()),
        )?,
        other => return internal_err!("true_div: unexpected operand types {other:?}"),
    };
    Ok(Arc::new(result))
}

/// `a / b` for two integers that fit 64 bits (signed or unsigned), as the
/// exact quotient rounded once to the nearest binary64, ties to even.
///
/// When both magnitudes are at most 2^53 they are exact in Float64 and IEEE
/// division rounds their quotient once, so that is the answer. Otherwise the
/// dividend is scaled by a power of two until the integer quotient has at
/// least 56 significant bits; a nonzero remainder is folded into its lowest
/// bit (a sticky bit, below the rounding position), so converting that
/// quotient to Float64 rounds as the exact quotient would round. Scaling
/// back by the power of two is exact, because a quotient of two 64-bit
/// integers is never subnormal.
fn true_div_int(a: i128, b: i128) -> Result<f64, ArrowError> {
    const EXACT: u128 = 1 << 53;
    if b == 0 {
        return Err(ArrowError::DivideByZero);
    }
    let (magnitude_a, magnitude_b) = (a.unsigned_abs(), b.unsigned_abs());
    if magnitude_a <= EXACT && magnitude_b <= EXACT {
        // Also gives Python's signed zero: `0 / -5` is `-0.0`.
        return Ok(a as f64 / b as f64);
    }
    let negative = (a < 0) != (b < 0);
    let magnitude = if magnitude_a == 0 {
        0.0
    } else {
        let bits = |v: u128| u128::BITS - v.leading_zeros();
        // At most 56 + 65 bits after the shift, as both magnitudes are at
        // most 2^64: well inside u128.
        let shift = (56 + bits(magnitude_b)).saturating_sub(bits(magnitude_a));
        let scaled = magnitude_a << shift;
        let quotient = scaled / magnitude_b;
        let sticky = u128::from(scaled % magnitude_b != 0);
        // `as` rounds an integer to the nearest float, ties to even.
        (quotient | sticky) as f64 * 2f64.powi(-(shift as i32))
    };
    Ok(if negative { -magnitude } else { magnitude })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrueDivUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TrueDivUdf {
    fn name(&self) -> &str {
        "true_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [lhs, rhs] = take_function_args(self.name(), arg_types)?;
        operand_type(lhs)?;
        operand_type(rhs)?;
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [lhs, rhs] = take_function_args(self.name(), arg_types)?;
        Ok(vec![operand_type(lhs)?, operand_type(rhs)?])
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
        let result = true_div_arrays(left, right)?;
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
        StringArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::Int8Type;

    // Expected values were computed with CPython's `/`.

    #[test]
    fn signs_match_python() {
        let table = [
            (7, 2, 3.5),
            (-7, 2, -3.5),
            (7, -2, -3.5),
            (-7, -2, 3.5),
            (6, 3, 2.0),
            (1, 3, 0.3333333333333333),
            (-1, 3, -0.3333333333333333),
        ];
        for (a, b, expected) in table {
            assert_eq!(true_div_int(a, b).unwrap(), expected, "{a} / {b}");
        }
    }

    #[test]
    fn zero_dividend_keeps_python_sign() {
        let positive = true_div_int(0, 5).unwrap();
        let negative = true_div_int(0, -5).unwrap();
        assert!(positive == 0.0 && positive.is_sign_positive());
        assert!(negative == 0.0 && negative.is_sign_negative());
        // The slow path too: a divisor beyond 2^53.
        let big = 1 << 60;
        assert!(true_div_int(0, big).unwrap().is_sign_positive());
        assert!(true_div_int(0, -big).unwrap().is_sign_negative());
    }

    #[test]
    fn zero_divisor_errors() {
        for a in [0, 1, -1, i64::MIN.into(), u64::MAX.into()] {
            assert!(
                matches!(true_div_int(a, 0), Err(ArrowError::DivideByZero)),
                "{a} / 0"
            );
        }
    }

    /// Operands beyond 2^53 round once, as CPython does; converting them to
    /// Float64 first would round twice.
    #[test]
    fn large_operands_round_once() {
        const MIN: i128 = i64::MIN as i128;
        const MAX: i128 = i64::MAX as i128;
        const UMAX: i128 = u64::MAX as i128;
        let table: [(i128, i128, f64); 12] = [
            // float(2**53 + 1) / 1 is 2**53; the exact value ties and goes
            // to even, which is 2**53 too.
            (9007199254740993, 1, 9007199254740992.0),
            // 2**53 + 3 ties between 2**53 + 2 and 2**53 + 4: even is + 4.
            (9007199254740995, 1, 9007199254740996.0),
            // (2**53 + 3) / 2: exact 4503599627370497.5, Python rounds to
            // ...498 (ties to even); float(2**53 + 3) / 2 is ...498 too.
            (9007199254740995, 2, 4503599627370498.0),
            // Double rounding: float(2**54 + 3) is 2**54 + 4, so the float
            // path gives (2**54 + 4) / 3 rounded; Python divides exactly.
            (18014398509481987, 3, 6004799503160662.0),
            (MAX, 1, 9.223372036854776e18),
            (MIN, 1, -9.223372036854776e18),
            (MIN, -1, 9.223372036854776e18),
            (MAX, MIN, -1.0),
            (MIN, MAX, -1.0),
            (UMAX, 3, 6.148914691236517e18),
            (UMAX, -UMAX, -1.0),
            (1, UMAX, 5.421010862427522e-20),
        ];
        for (a, b, expected) in table {
            assert_eq!(true_div_int(a, b).unwrap(), expected, "{a} / {b}");
        }
    }

    #[test]
    fn integer_pairs() {
        use DataType::*;
        let dict = Dictionary(Box::new(Int8), Box::new(Int32));
        assert!(integer_pair(&Int64, &Int64));
        assert!(integer_pair(&Int32, &UInt64));
        assert!(integer_pair(&UInt8, &Int16));
        assert!(integer_pair(&dict, &Int64));
        assert!(integer_pair(&Null, &Int64));
        assert!(integer_pair(&UInt64, &Null));
        assert!(!integer_pair(&Null, &Null));
        assert!(!integer_pair(&Int64, &Float64));
        assert!(!integer_pair(&Float32, &Int64));
        assert!(!integer_pair(&Int64, &Decimal128(10, 2)));
        assert!(!integer_pair(&Utf8, &Int64));
        assert!(!integer_pair(&Null, &Float64));
    }

    #[test]
    fn arrays_mixed_types_and_nulls() {
        let left: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), Some(-7), None, Some(1)]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(2), Some(0), None]));
        let result = true_div_arrays(&left, &right).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.value(0), 3.5);
        assert_eq!(result.value(1), -3.5);
        assert!(result.is_null(2));
        assert!(result.is_null(3));
    }

    #[test]
    fn arrays_null_divisor_slot_holding_zero_is_null() {
        // A NULL slot's value is zero here; it must not raise.
        let left: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4)]));
        let result = true_div_arrays(&left, &right).unwrap();
        assert!(result.is_null(0));
        assert_eq!(result.as_primitive::<Float64Type>().value(1), 0.5);
    }

    #[test]
    fn arrays_zero_divisor_errors() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![1, 0]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![1, 0]));
        let err = true_div_arrays(&left, &right).unwrap_err();
        assert!(err.to_string().contains("Divide by zero"), "{err}");
    }

    #[test]
    fn arrays_uint64_against_signed() {
        let left: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX, 7]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![-1, -2]));
        let result = true_div_arrays(&left, &right).unwrap();
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), -1.8446744073709552e19);
        assert_eq!(result.value(1), -3.5);
        let swapped = true_div_arrays(&right, &left).unwrap();
        assert_eq!(swapped.as_primitive::<Float64Type>().value(1), -2.0 / 7.0);
    }

    #[test]
    fn arrays_null_typed_and_dictionary_operands() {
        let nulls: ArrayRef = Arc::new(NullArray::new(2));
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let result = true_div_arrays(&ints, &nulls).unwrap();
        assert_eq!(result.data_type(), &DataType::Float64);
        assert_eq!(result.null_count(), 2);

        let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![0, 1, 0]),
            Arc::new(Int64Array::from(vec![7, -7])),
        ));
        let twos: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, -2]));
        let result = true_div_arrays(&dict, &twos).unwrap();
        let values: Vec<f64> = result.as_primitive::<Float64Type>().values().to_vec();
        assert_eq!(values, vec![3.5, -3.5, -3.5]);
    }

    #[test]
    fn arrays_reject_non_integers() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        assert!(true_div_arrays(&strings, &ints).is_err());
        assert!(true_div_arrays(&ints, &floats).is_err());
    }
}
