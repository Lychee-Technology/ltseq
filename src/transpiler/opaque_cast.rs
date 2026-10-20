//! `cast()` in a lambda, as a value DataFusion computes (#260).
//!
//! DataFusion's simplifier rewrites `CAST(a AS t) <op> literal`, and
//! `CAST(a AS t) IN (literals)`, to compare `a` itself whenever every
//! literal converts exactly to the type of `a` (`unwrap_cast_in_comparison`).
//! It does so in the logical optimizer, and again in the physical
//! simplifier that a Parquet scan applies to the filter pushed into it. The
//! rewrite is sound only for a cast that keeps every value of its input.
//! For any other cast it changes the answer: a narrowing cast's failure
//! disappears (`int64` 2^40 cast to `int32` and compared with 5 keeps the
//! row, compared unconverted), and a decimal's truncation is undone (`5.5`
//! cast to `int64` no longer equals 5). Neither simplifier can be told to
//! leave a cast alone.
//!
//! So [`cast`] gives DataFusion a `Cast` only where the rewrite is sound,
//! and otherwise an [`OpaqueCast`]: a UDF that runs the cast DataFusion's
//! `CastExpr` runs, with the same kernel and options, but that no rewrite
//! sees through. A lossless cast (`int32` to `int64`) stays a `Cast`, so a
//! filter on it still prunes Parquet row groups by their statistics.

use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::utils::take_function_args;
use datafusion::common::Result;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use super::exact::{cast_class, CastClass};
use super::Resolver;

/// `expr` cast to `to`, as `cast()` in a lambda writes it.
///
/// It is a DataFusion `Cast` when DataFusion may see through it, or never
/// meets it next to a column:
///
/// - the cast keeps every value of its input exactly ([`cast_class`] is
///   `Exact`), so comparing the input instead gives the same answer. A
///   decimal of negative scale is the exception: DataFusion 55 scales the
///   literal by ten to the power of the scale read as a `u32`
///   (`try_cast_numeric_literal`, the overflow of #226 and
///   apache/datafusion#24896), which panics in a debug build and scales
///   the literal to 0 in a release build;
/// - the input is a constant, which DataFusion folds into a literal, cast
///   included, before it rewrites any comparison. A constant that fails to
///   cast is a planning error there: assessment hazard X1, which #273
///   resolves;
/// - Arrow has no such cast, or the input has no type: DataFusion reports
///   the error when it plans the query, as it always has.
///
/// Every other cast is an [`OpaqueCast`].
pub(crate) fn cast(expr: Expr, to: DataType, rx: &Resolver<'_>) -> Expr {
    let opaque = !Resolver::query_constant(&expr)
        && rx
            .data_type(&expr)
            .is_ok_and(|from| can_cast_types(&from, &to) && !transparent(&from, &to));
    if opaque {
        OpaqueCast::call(expr, to)
    } else {
        Expr::Cast(Cast::new(Box::new(expr), to))
    }
}

/// Whether DataFusion may compare a column instead of its cast to `to`.
fn transparent(from: &DataType, to: &DataType) -> bool {
    cast_class(from, to) == CastClass::Exact && !negative_scale(from)
}

/// Whether the values of `t` are decimals of negative scale.
fn negative_scale(t: &DataType) -> bool {
    match t {
        DataType::Dictionary(_, values) => negative_scale(values),
        DataType::RunEndEncoded(_, values) => negative_scale(values.data_type()),
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale < 0,
        _ => false,
    }
}

/// A cast that no DataFusion rewrite sees through.
///
/// DataFusion's rewrites match `Cast` and `TryCast` nodes, and a UDF call
/// is neither. Otherwise it is the `Cast` it replaces: it runs the kernel
/// and options of DataFusion's `CastExpr`, so it gives the same values and
/// raises the same errors; it is NULL exactly where its input is; and it
/// names the column it makes after its input, as a `Cast` does. One
/// difference remains, for an input that becomes a constant only in the
/// optimizer: DataFusion folds it where it casts, and leaves it for
/// execution where it fails, which for a `Cast` is a planning error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpaqueCast {
    /// `opaque_cast_<to>`, so that a plan shows the target type.
    name: String,
    to: DataType,
    signature: Signature,
}

impl OpaqueCast {
    fn call(expr: Expr, to: DataType) -> Expr {
        ScalarUDF::new_from_impl(OpaqueCast {
            name: format!("opaque_cast_{to}"),
            to,
            // `Any` takes the input as it is: no coercion of its own.
            signature: Signature::any(1, Volatility::Immutable),
        })
        .call(vec![expr])
    }
}

impl ScalarUDFImpl for OpaqueCast {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.to.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            self.to.clone(),
            input.is_nullable(),
        )))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [input] = take_function_args(self.name(), args)?;
        Ok(input.schema_name().to_string())
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = take_function_args(self.name(), args.args)?;
        input.cast_to(&self.to, Some(&DEFAULT_CAST_OPTIONS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Schema, TimeUnit};
    use datafusion::logical_expr::table_scan;
    use datafusion::prelude::{col, lit};

    fn schema(from: DataType, nullable: bool) -> Schema {
        Schema::new(vec![Field::new("a", from, nullable)])
    }

    fn cast_column(from: DataType, to: DataType) -> Expr {
        let schema = schema(from, true);
        cast(col("a"), to, &Resolver::new(&schema).unwrap())
    }

    fn is_opaque(expr: &Expr) -> bool {
        matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<OpaqueCast>())
    }

    #[test]
    fn a_cast_that_keeps_every_value_stays_a_cast() {
        use DataType::*;
        let dictionary = Dictionary(Box::new(Int32), Box::new(Int64));
        for (from, to) in [
            (Int32, Int64),
            (Int64, Int64),
            (UInt32, Int64),
            (Int16, Int32),
            (Int32, Float64),
            (Decimal128(9, 0), Int32),
            (dictionary, Int64),
        ] {
            let expr = cast_column(from.clone(), to.clone());
            assert!(matches!(expr, Expr::Cast(_)), "{from} -> {to}: {expr}");
        }
    }

    #[test]
    fn a_cast_that_can_fail_or_round_is_opaque() {
        use DataType::*;
        let seconds = Timestamp(TimeUnit::Second, None);
        for (from, to) in [
            (Int64, Int32),
            (UInt64, Int64),
            (UInt32, Int32),
            (Int64, Float64),
            (Float64, Int32),
            (Decimal128(10, 2), Int64),
            (Decimal128(38, 0), Int64),
            (Int64, Date32),
            (Date64, Date32),
            (seconds, Int32),
            (Utf8, Int32),
            (Int64, Utf8),
            (Utf8View, Utf8),
            // Lossless, but DataFusion 55 cannot rewrite it (#226).
            (Decimal128(5, -2), Int32),
        ] {
            let expr = cast_column(from.clone(), to.clone());
            assert!(is_opaque(&expr), "{from} -> {to}: {expr}");
        }
    }

    #[test]
    fn constants_and_casts_arrow_lacks_stay_casts() {
        let schema = schema(DataType::new_list(DataType::Int64, true), true);
        let rx = Resolver::new(&schema).unwrap();
        for (expr, to) in [
            (lit(1_i64 << 40), DataType::Int32),
            (lit("abc"), DataType::Int32),
            (col("a"), DataType::Int32),
        ] {
            let cast = cast(expr, to, &rx);
            assert!(matches!(cast, Expr::Cast(_)), "{cast}");
        }
    }

    #[test]
    fn an_opaque_cast_types_and_names_its_value_as_a_cast_does() {
        for nullable in [true, false] {
            let schema = schema(DataType::Int64, nullable);
            let rx = Resolver::new(&schema).unwrap();
            let input = || col("a") + lit(1_i64);
            let opaque = cast(input(), DataType::Int32, &rx);
            assert!(is_opaque(&opaque), "{opaque}");
            let plain = Expr::Cast(Cast::new(Box::new(input()), DataType::Int32));
            // The column a projection of each makes.
            let column = |expr: Expr| {
                let plan = table_scan(Some("t"), &schema, None)
                    .and_then(|scan| scan.project(vec![expr]))
                    .and_then(|projection| projection.build())
                    .unwrap();
                Arc::clone(plan.schema().field(0))
            };
            let (opaque, plain) = (column(opaque), column(plain));
            assert_eq!(opaque.name(), plain.name());
            assert_eq!(opaque.data_type(), &DataType::Int32);
            assert_eq!(opaque.is_nullable(), nullable);
            assert_eq!(plain.is_nullable(), nullable);
        }
    }
}
