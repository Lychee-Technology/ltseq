//! `lpad` / `rpad` (`s.pad_left` / `s.pad_right`) that return an error where
//! DataFusion's kernels would panic or abort (#192).
//!
//! DataFusion 55's `lpad` and `rpad` check each row's width against
//! `i32::MAX`, but not the batch: a Utf8 result whose bytes add up to more
//! than `i32::MAX` overflows arrow's offsets, and the builder panics with
//! `byte array offset overflow`. The panic crosses the FFI as
//! `PanicException`, which `except Exception` does not catch. Two rows padded
//! to `2**31 - 1` are enough, and so are 8192 rows (one default batch) padded
//! to `2**18`.
//!
//! The kernels also reserve their output buffer from the widths alone: the
//! sum of every non-NULL width in the batch, on the path taken for a width
//! column, a fill column or a literal width above 16384. That sum counts rows
//! whose string is NULL, rows whose empty fill pads nothing, and reads a
//! negative width as a huge `usize`, so the reservation can be far larger than
//! the result, or fail: an allocation failure aborts the process.
//!
//! The wrapper keeps DataFusion's kernels and semantics and fixes both before
//! delegating to them:
//!
//! - It computes the exact byte length of the result (cheaply first, as an
//!   upper bound, and exactly only when that bound exceeds the limit) and
//!   returns an execution error when it does not fit the result's offsets.
//! - On the kernels' reserving path it rewrites the width argument to one
//!   that gives the same result but reserves no more than the result needs:
//!   NULL where the row's result is NULL, 0 for a negative width (which the
//!   kernels read as 0), and the string's byte length where an empty fill
//!   leaves a shorter string as it is.
//!
//! The wrapper keeps the name `lpad` / `rpad`, so plans and default column
//! names read as before.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, AsArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

/// The widest a literal width may be and still take DataFusion's scalar fast
/// path, which reserves `rows * width` bytes and never the width sum
/// (`MAX_SCALAR_TARGET_LEN` in `datafusion-functions` 55).
const MAX_SCALAR_TARGET_LEN: i64 = 16384;

static LPAD: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| checked(datafusion::functions::unicode::lpad()));
static RPAD: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| checked(datafusion::functions::unicode::rpad()));

fn checked(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(CheckedPad { inner }))
}

/// `lpad(string, width[, fill])`, checked as the module describes.
pub(crate) fn lpad(args: Vec<Expr>) -> Expr {
    LPAD.call(args)
}

/// `rpad(string, width[, fill])`, checked as the module describes.
pub(crate) fn rpad(args: Vec<Expr>) -> Expr {
    RPAD.call(args)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CheckedPad {
    inner: Arc<ScalarUDF>,
}

impl ScalarUDFImpl for CheckedPad {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let limit = match args.return_field.data_type() {
            DataType::LargeUtf8 => i64::MAX as u128,
            _ => i32::MAX as u128,
        };
        if let Some(width) = guard(self.name(), &args.args, args.number_rows, limit)? {
            args.args[1] = width;
        }
        self.inner.invoke_with_args(args)
    }
}

/// A string argument: one value for every row, or a column.
enum Strs<'a> {
    Scalar(Option<&'a str>),
    Utf8(&'a datafusion::arrow::array::StringArray),
    LargeUtf8(&'a datafusion::arrow::array::LargeStringArray),
    Utf8View(&'a datafusion::arrow::array::StringViewArray),
}

impl<'a> Strs<'a> {
    /// `None` for a type the kernels do not take either; they report it.
    fn of(value: &'a ColumnarValue) -> Option<Self> {
        Some(match value {
            ColumnarValue::Scalar(s) => Strs::Scalar(s.try_as_str()?),
            ColumnarValue::Array(a) => match a.data_type() {
                DataType::Utf8 => Strs::Utf8(a.as_string::<i32>()),
                DataType::LargeUtf8 => Strs::LargeUtf8(a.as_string::<i64>()),
                DataType::Utf8View => Strs::Utf8View(a.as_string_view()),
                _ => return None,
            },
        })
    }

    fn get(&self, row: usize) -> Option<&'a str> {
        match self {
            Strs::Scalar(s) => *s,
            Strs::Utf8(a) => a.is_valid(row).then(|| a.value(row)),
            Strs::LargeUtf8(a) => a.is_valid(row).then(|| a.value(row)),
            Strs::Utf8View(a) => a.is_valid(row).then(|| a.value(row)),
        }
    }
}

/// The width argument: one value for every row, or an Int64 column.
enum Widths<'a> {
    Scalar(Option<i64>),
    Array(&'a Int64Array),
}

impl<'a> Widths<'a> {
    fn of(value: &'a ColumnarValue) -> Option<Self> {
        Some(match value {
            ColumnarValue::Scalar(ScalarValue::Int64(w)) => Widths::Scalar(*w),
            ColumnarValue::Array(a) => Widths::Array(a.as_primitive_opt::<Int64Type>()?),
            _ => return None,
        })
    }

    fn get(&self, row: usize) -> Option<i64> {
        match self {
            Widths::Scalar(w) => *w,
            Widths::Array(a) => a.is_valid(row).then(|| a.value(row)),
        }
    }
}

/// Checks that the result fits `limit` bytes, and returns the width argument
/// to pass the kernel instead of the original when the kernel would reserve
/// its buffer from the widths. `None` leaves the arguments as they are, also
/// for argument types the kernel itself rejects.
fn guard(
    name: &str,
    args: &[ColumnarValue],
    number_rows: usize,
    limit: u128,
) -> Result<Option<ColumnarValue>> {
    let (Some(string_arg), Some(width_arg)) = (args.first(), args.get(1)) else {
        return Ok(None);
    };
    let fill_arg = args.get(2);
    let (Some(strings), Some(widths)) = (Strs::of(string_arg), Widths::of(width_arg)) else {
        return Ok(None);
    };
    let fills = match fill_arg {
        None => Strs::Scalar(Some(" ")),
        Some(f) => match Strs::of(f) {
            Some(fills) => fills,
            None => return Ok(None),
        },
    };
    let all_scalar = args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
    let rows = if all_scalar { 1 } else { number_rows };
    let row = |i: usize| match (strings.get(i), widths.get(i), fills.get(i)) {
        (Some(s), Some(w), Some(f)) => Some((s, w, f)),
        _ => None,
    };

    // A cheap upper bound first, then the exact length only near the limit.
    let mut bound: u128 = 0;
    for i in 0..rows {
        let Some((s, w, f)) = row(i) else { continue };
        if w > i32::MAX as i64 {
            return exec_err!(
                "{name} requested length {w} too large, maximum allowed length is {}",
                i32::MAX
            );
        }
        bound += s.len() as u128 + w.max(0) as u128 * if f.is_ascii() { 1 } else { 4 };
    }
    if bound > limit {
        let exact: u128 = (0..rows)
            .filter_map(row)
            .map(|(s, w, f)| padded_len(s, w.max(0) as usize, f))
            .sum();
        if exact > limit {
            return exec_err!(
                "{name} would build {exact} bytes of strings in one batch of {rows} rows, \
                 more than the {limit} a {} column holds; use a smaller width",
                if limit == i32::MAX as u128 {
                    "Utf8"
                } else {
                    "LargeUtf8"
                }
            );
        }
    }

    // The kernels' scalar fast path reserves `rows * width` for a literal
    // width of at most 16384 and a literal fill; leave it alone.
    let fast_path = matches!(widths, Widths::Scalar(Some(w)) if w <= MAX_SCALAR_TARGET_LEN)
        && matches!(fills, Strs::Scalar(Some(_)));
    if fast_path {
        return Ok(None);
    }
    let reserved = |i: usize| {
        row(i).map(|(s, w, f)| {
            let w = w.max(0);
            if f.is_empty() {
                // An empty fill pads nothing: a width at or beyond the
                // string's byte length (and so its length in characters)
                // leaves the string as it is.
                w.min(s.len() as i64)
            } else {
                w
            }
        })
    };
    Ok(Some(if all_scalar {
        ColumnarValue::Scalar(ScalarValue::Int64(reserved(0)))
    } else {
        ColumnarValue::Array(Arc::new((0..rows).map(reserved).collect::<Int64Array>()))
    }))
}

/// The byte length of `lpad(s, width, fill)` (and of `rpad`, which places
/// the same characters on the other side): `s` cut to `width` characters, or
/// `s` with `fill` repeated until the result has `width` characters. An
/// empty fill leaves a shorter `s` as it is.
fn padded_len(s: &str, width: usize, fill: &str) -> u128 {
    if width == 0 {
        return 0;
    }
    let chars = if s.is_ascii() {
        if width <= s.len() {
            return width as u128;
        }
        s.len()
    } else {
        match s.char_indices().nth(width) {
            Some((cut, _)) => return cut as u128,
            None => s.chars().count(),
        }
    };
    if fill.is_empty() || chars >= width {
        return s.len() as u128;
    }
    s.len() as u128 + fill_len(fill, width - chars)
}

/// The byte length of the first `n` characters of `fill` repeated.
fn fill_len(fill: &str, n: usize) -> u128 {
    if fill.is_ascii() {
        return n as u128;
    }
    let per_cycle = fill.chars().count();
    let partial = fill
        .char_indices()
        .nth(n % per_cycle)
        .map_or(fill.len(), |(b, _)| b);
    (n / per_cycle) as u128 * fill.len() as u128 + partial as u128
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn invoke(udf: &ScalarUDF, args: Vec<ColumnarValue>, rows: usize) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("a", a.data_type(), true)))
            .collect::<Vec<_>>();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn strings(values: &[Option<&str>]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(values.to_vec())) as ArrayRef)
    }

    fn widths(values: &[Option<i64>]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int64Array::from(values.to_vec())) as ArrayRef)
    }

    fn width(w: i64) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(w)))
    }

    fn fill(f: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(f.to_string())))
    }

    fn values(result: ColumnarValue) -> Vec<Option<String>> {
        let array = result.into_array(1).unwrap();
        array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    /// The lengths [`padded_len`] predicts are the lengths the kernels build.
    #[test]
    fn padded_len_matches_the_kernels() {
        let inputs = ["", "a", "abc", "héllo", "日本語", "a😀b"];
        let fills = ["", " ", "xy", "é", "日😀", "aé"];
        for udf in [&*LPAD, &*RPAD] {
            for s in inputs {
                for f in fills {
                    for w in 0..9usize {
                        let out = values(
                            invoke(udf, vec![strings(&[Some(s)]), width(w as i64), fill(f)], 1)
                                .unwrap(),
                        );
                        let built = out[0].as_deref().unwrap();
                        assert_eq!(
                            padded_len(s, w, f),
                            built.len() as u128,
                            "{}({s:?}, {w}, {f:?}) = {built:?}",
                            udf.name()
                        );
                    }
                }
            }
        }
    }

    /// Two rows padded to `2**31 - 1` overflow Utf8's offsets: an error, not
    /// the kernels' panic, and raised before anything is allocated.
    #[test]
    fn oversized_result_is_an_error() {
        for udf in [&*LPAD, &*RPAD] {
            let err = invoke(
                udf,
                vec![strings(&[Some("a"), Some("b")]), width(i32::MAX as i64)],
                2,
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("Utf8 column holds"), "{err}");
        }
    }

    /// The bound is on the result, not on rows times width: NULL strings and
    /// an empty fill add nothing, so this result is small and must build.
    #[test]
    fn nulls_and_empty_fill_do_not_count() {
        let big = 1i64 << 30;
        let out = invoke(
            &LPAD,
            vec![
                strings(&[Some("a"), None, Some("b"), None]),
                width(big),
                fill(""),
            ],
            4,
        )
        .unwrap();
        assert_eq!(
            values(out),
            [Some("a".into()), None, Some("b".into()), None]
        );
    }

    /// A width column with negative widths reads them as 0, as the kernels
    /// do; the kernels' own reservation would read them as a huge `usize`.
    #[test]
    fn negative_width_column_pads_to_nothing() {
        for udf in [&*LPAD, &*RPAD] {
            let out = invoke(
                udf,
                vec![
                    strings(&[Some("ab"), Some("cd"), None]),
                    widths(&[Some(-1), Some(3), Some(i64::MIN)]),
                ],
                3,
            )
            .unwrap();
            let expected = if udf.name() == "lpad" { " cd" } else { "cd " };
            assert_eq!(
                values(out),
                [Some(String::new()), Some(expected.into()), None]
            );
        }
    }

    /// A width beyond `i32::MAX` keeps the kernels' error message.
    #[test]
    fn width_beyond_i32_is_an_error() {
        let err = invoke(
            &LPAD,
            vec![strings(&[Some("a")]), width(1 << 40), fill("")],
            1,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("too large"), "{err}");
    }
}
