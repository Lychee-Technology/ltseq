//! The value a float sorts by in the contract's order (§9.4, #268).
//!
//! In §9.4's order every NaN, whatever its sign bit and payload, is greater
//! than `+inf`, and `-0.0` equals `0.0`. Arrow and DataFusion sort floats by
//! IEEE totalOrder, which puts a NaN with the sign bit set (the NaN
//! `inf - inf` gives on x86-64) below `-inf`, and `-0.0` before `0.0`. So a
//! float key is sorted by [`order_key`] of it: the column with every NaN
//! replaced by one positive NaN and `-0.0` by `0.0` (§18's representatives,
//! `ops::common::equal_floats_as_one`), on which totalOrder is §9.4's order
//! and equal values are equal bit for bit, so a stable sort keeps them in
//! input order.
//!
//! Every order LTSeq builds from a key goes through this: `sort`, the
//! declared order a terminal re-imposes (`SortSpec::to_df_sort_expr`), the
//! order a source declares to DataFusion, window `ORDER BY` and ranking. The
//! same function on the same column gives DataFusion the same expression
//! everywhere, so it still sees that a sorted input satisfies a later
//! requirement and plans no second sort.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::utils::take_function_args;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::ops::common::equal_floats_as_one;

/// The name the function has in a plan.
pub(crate) const ORDER_KEY_NAME: &str = "ltseq_order_key";

static ORDER_KEY: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(OrderKey::new())));

/// The float type `data_type` holds, decoded: a float type itself, or the
/// values of a dictionary or run-end-encoded column of floats.
fn float_values(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(data_type),
        DataType::Dictionary(_, values) => float_values(values),
        DataType::RunEndEncoded(_, values) => float_values(values.data_type()),
        _ => None,
    }
}

/// `expr`, of type `data_type`, as the value it sorts by under §9.4:
/// [`order_key`]`(expr)` for a float, decoded first if it is encoded, and
/// `expr` itself for every other type, whose DataFusion order is §9.4's.
pub(crate) fn order_key(expr: Expr, data_type: &DataType) -> Expr {
    if float_values(data_type).is_some() {
        float_order_key(expr)
    } else {
        expr
    }
}

/// [`order_key`] of `expr`, known to be a float.
pub(crate) fn float_order_key(expr: Expr) -> Expr {
    ORDER_KEY.call(vec![expr])
}

/// Whether a key of `data_type` sorts through [`order_key`].
pub(crate) fn needs_order_key(data_type: &DataType) -> bool {
    float_values(data_type).is_some()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct OrderKey {
    signature: Signature,
}

impl OrderKey {
    fn new() -> Self {
        OrderKey {
            // `Any` takes the input as it is: no coercion of its own.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for OrderKey {
    fn name(&self) -> &str {
        ORDER_KEY_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [input] = take_function_args(self.name(), arg_types)?;
        Ok(float_values(input).unwrap_or(input).clone())
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = take_function_args(self.name(), args.args)?;
        let Some(to) = float_values(&input.data_type()).cloned() else {
            return Ok(input);
        };
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let array = input.into_array(1)?;
        let decoded = if array.data_type() == &to {
            array
        } else {
            cast(&array, &to)?
        };
        let key = equal_floats_as_one(decoded);
        if scalar {
            ScalarValue::try_from_array(&key, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(key))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array};
    use datafusion::arrow::datatypes::{Field, Float64Type};
    use datafusion::config::ConfigOptions;

    fn keys(values: ArrayRef) -> Vec<u64> {
        let rows = values.len();
        let field = Arc::new(Field::new("x", values.data_type().clone(), true));
        let out = OrderKey::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(values)],
                arg_fields: vec![Arc::clone(&field)],
                number_rows: rows,
                return_field: Arc::new(Field::new("k", DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .into_array(rows)
            .unwrap();
        out.as_primitive::<Float64Type>()
            .values()
            .iter()
            .map(|v| v.to_bits())
            .collect()
    }

    #[test]
    fn every_nan_is_one_positive_nan_and_zeros_are_one_zero() {
        let negative_nan = f64::from_bits(0xfff8_0000_0000_0001);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            negative_nan,
            f64::NAN,
            -0.0,
            0.0,
            -1.5,
        ]));
        let nan = f64::NAN.to_bits();
        assert_eq!(keys(values), vec![nan, nan, 0, 0, (-1.5f64).to_bits()]);
        assert_eq!(nan >> 63, 0, "the representative NaN sorts above +inf");
    }

    #[test]
    fn a_dictionary_of_floats_is_decoded() {
        use datafusion::arrow::array::{DictionaryArray, Int8Array};
        let dict: ArrayRef = Arc::new(
            DictionaryArray::try_new(
                Int8Array::from(vec![0, 1, 0]),
                Arc::new(Float64Array::from(vec![-0.0, f64::from_bits(0xfff8_0000_0000_0000)])),
            )
            .unwrap(),
        );
        let nan = f64::NAN.to_bits();
        assert_eq!(keys(dict), vec![0, nan, 0]);
    }

    #[test]
    fn only_floats_get_a_key() {
        let x = datafusion::prelude::col("x");
        assert_eq!(order_key(x.clone(), &DataType::Int64), x);
        assert_ne!(order_key(x.clone(), &DataType::Float32), x);
        assert!(needs_order_key(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Float16)
        )));
    }
}
