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
//! The kernels also size their work from the arguments rather than the
//! result: they reserve the sum of the widths (or `rows * width`, four times
//! that for a non-ASCII string or fill), counting rows whose string is NULL
//! and empty fills that pad nothing, and reading a negative width as a huge
//! `usize`; and they first repeat each literal string argument into a column
//! of the batch's length, which can itself overflow the offsets however small
//! the result. An allocation failure aborts the process.
//!
//! So this wrapper keeps DataFusion's signature, types and semantics, and
//! pads itself:
//!
//! - It reads each argument as it comes, a literal as one value, and
//!   computes the exact byte length of the result before building anything;
//!   it returns an execution error when that does not fit the result's
//!   offsets, and reserves exactly that length otherwise.
//! - A literal width above `i32::MAX` is an error before any row is read, as
//!   in DataFusion, even when every string is NULL; a width column is checked
//!   on the rows whose string, width and fill are all non-NULL.
//! - When every argument is a literal, the result is one value that the
//!   projection repeats for every row of the batch, so the check covers the
//!   batch's rows times that value. A call whose arguments reference no
//!   column is planned as volatile ([`lpad`]): DataFusion would otherwise
//!   fold it to a literal while planning, checked against a single row, and
//!   repeat that literal into each batch with a panic on overflow.
//!
//! The wrapper keeps the name `lpad` / `rpad`, so plans and default column
//! names read as before.

use std::fmt::Write;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericStringBuilder, Int64Array, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

static LPAD: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    checked(
        datafusion::functions::unicode::lpad(),
        Side::Left,
        Volatility::Immutable,
    )
});
static RPAD: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    checked(
        datafusion::functions::unicode::rpad(),
        Side::Right,
        Volatility::Immutable,
    )
});
static LPAD_CONSTANT: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    checked(
        datafusion::functions::unicode::lpad(),
        Side::Left,
        Volatility::Volatile,
    )
});
static RPAD_CONSTANT: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    checked(
        datafusion::functions::unicode::rpad(),
        Side::Right,
        Volatility::Volatile,
    )
});

fn checked(inner: Arc<ScalarUDF>, side: Side, volatility: Volatility) -> Arc<ScalarUDF> {
    let signature = Signature {
        volatility,
        ..inner.signature().clone()
    };
    Arc::new(ScalarUDF::new_from_impl(CheckedPad {
        inner,
        side,
        signature,
    }))
}

/// Whether a call on `args` is constant: no argument references a column.
/// The optimizer would fold such a call to a literal, which the projection
/// repeats for every row without a check, so it is planned as volatile and
/// evaluated per batch, where the check sees the batch's rows.
fn constant(args: &[Expr]) -> bool {
    !args.iter().any(Expr::any_column_refs)
}

/// `lpad(string, width[, fill])`, checked as the module describes.
pub(crate) fn lpad(args: Vec<Expr>) -> Expr {
    if constant(&args) {
        LPAD_CONSTANT.call(args)
    } else {
        LPAD.call(args)
    }
}

/// `rpad(string, width[, fill])`, checked as the module describes.
pub(crate) fn rpad(args: Vec<Expr>) -> Expr {
    if constant(&args) {
        RPAD_CONSTANT.call(args)
    } else {
        RPAD.call(args)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Side {
    Left,
    Right,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CheckedPad {
    inner: Arc<ScalarUDF>,
    side: Side,
    signature: Signature,
}

impl ScalarUDFImpl for CheckedPad {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(operands) = Operands::of(&args.args) else {
            // A type the kernels do not take either; they report it.
            return self.inner.invoke_with_args(args);
        };
        let pad = Pad {
            name: self.name(),
            side: self.side,
            operands,
            number_rows: args.number_rows,
        };
        match args.return_field.data_type() {
            DataType::LargeUtf8 => pad.run::<i64>(),
            _ => pad.run::<i32>(),
        }
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
    /// `None` for a type the kernels do not take either.
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

/// The three arguments, read without copying a literal into a column.
struct Operands<'a> {
    strings: Strs<'a>,
    widths: Widths<'a>,
    fills: Strs<'a>,
    /// Every argument is a literal: the result is one value.
    all_scalar: bool,
}

impl<'a> Operands<'a> {
    fn of(args: &'a [ColumnarValue]) -> Option<Self> {
        if !(2..=3).contains(&args.len()) {
            return None;
        }
        Some(Operands {
            strings: Strs::of(&args[0])?,
            widths: Widths::of(&args[1])?,
            fills: match args.get(2) {
                None => Strs::Scalar(Some(" ")),
                Some(f) => Strs::of(f)?,
            },
            all_scalar: args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_))),
        })
    }

    /// The row's string, width and fill, or `None` where the result is NULL.
    fn row(&self, i: usize) -> Option<(&'a str, i64, &'a str)> {
        match (self.strings.get(i), self.widths.get(i), self.fills.get(i)) {
            (Some(s), Some(w), Some(f)) => Some((s, w, f)),
            _ => None,
        }
    }
}

struct Pad<'a> {
    name: &'a str,
    side: Side,
    operands: Operands<'a>,
    number_rows: usize,
}

impl Pad<'_> {
    fn check_width(&self, width: i64) -> Result<()> {
        if width > i32::MAX as i64 {
            return exec_err!(
                "{} requested length {width} too large, maximum allowed length is {}",
                self.name,
                i32::MAX
            );
        }
        Ok(())
    }

    fn run<O: OffsetSizeTrait>(&self) -> Result<ColumnarValue> {
        let ops = &self.operands;
        // A literal width is checked before any row, NULL or not, as in
        // DataFusion.
        if let Widths::Scalar(Some(w)) = ops.widths {
            self.check_width(w)?;
        }
        let rows = if ops.all_scalar { 1 } else { self.number_rows };

        let mut bytes: u128 = 0;
        for i in 0..rows {
            if let Some((s, w, f)) = ops.row(i) {
                self.check_width(w)?;
                bytes += padded_len(s, w.max(0) as usize, f);
            }
        }
        // One literal result is repeated for each row of the batch.
        let batch_bytes = if ops.all_scalar {
            bytes * self.number_rows.max(1) as u128
        } else {
            bytes
        };
        let (limit, kind) = if O::IS_LARGE {
            (i64::MAX as u128, "LargeUtf8")
        } else {
            (i32::MAX as u128, "Utf8")
        };
        if batch_bytes > limit {
            return exec_err!(
                "{} would build {batch_bytes} bytes of strings in one batch of {} rows, \
                 more than the {limit} a {kind} column holds; use a smaller width",
                self.name,
                self.number_rows
            );
        }

        let capacity = usize::try_from(bytes)
            .map_err(|_| exec_datafusion_err!("{} result of {bytes} bytes", self.name))?;
        let mut builder = GenericStringBuilder::<O>::with_capacity(rows, capacity);
        // A literal fill is cycled from one buffer for every row.
        let shared = match ops.fills {
            Strs::Scalar(Some(f)) => Some(Cycle::new(f, usize::MAX)),
            _ => None,
        };
        for i in 0..rows {
            let Some((s, w, f)) = ops.row(i) else {
                builder.append_null();
                continue;
            };
            let width = w.max(0) as usize;
            let (kept, pad_chars) = cut(s, width, f);
            if pad_chars == 0 {
                builder.append_value(kept);
                continue;
            }
            let own;
            let cycle = match &shared {
                Some(c) => c,
                None => {
                    own = Cycle::new(f, pad_chars);
                    &own
                }
            };
            let written = match self.side {
                Side::Left => cycle
                    .write(&mut builder, pad_chars)
                    .and_then(|()| builder.write_str(kept)),
                Side::Right => builder
                    .write_str(kept)
                    .and_then(|()| cycle.write(&mut builder, pad_chars)),
            };
            written.map_err(|e| exec_datafusion_err!("{}: {e}", self.name))?;
            builder.append_value("");
        }
        let array = Arc::new(builder.finish()) as ArrayRef;
        Ok(if ops.all_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0)?)
        } else {
            ColumnarValue::Array(array)
        })
    }
}

/// The part of `s` that `lpad` / `rpad` keep for `width`, and the number of
/// fill characters they add: `s` cut to `width` characters, or `s` and the
/// characters missing to `width` (none for an empty fill).
fn cut<'s>(s: &'s str, width: usize, fill: &str) -> (&'s str, usize) {
    // Only the first `width` characters matter, so only the first `width`
    // bytes are read when they are ASCII.
    let chars = if s.as_bytes()[..s.len().min(width)].is_ascii() {
        if width <= s.len() {
            return (&s[..width], 0);
        }
        s.len()
    } else {
        match s.char_indices().nth(width) {
            Some((at, _)) => return (&s[..at], 0),
            None => s.chars().count(),
        }
    };
    let pad = if fill.is_empty() {
        0
    } else {
        width.saturating_sub(chars)
    };
    (s, pad)
}

/// A fill repeated: whole cycles of it in one buffer of about 4 KiB at most,
/// written as often as a padding needs.
struct Cycle {
    block: String,
    /// Characters in `block`.
    block_chars: usize,
    ascii: bool,
}

impl Cycle {
    const BLOCK_BYTES: usize = 4096;

    /// A cycle of `fill` for paddings of up to about `chars` characters.
    /// Reads no more of `fill` than such a padding holds.
    fn new(fill: &str, chars: usize) -> Self {
        if let Some((at, _)) = fill.char_indices().nth(chars) {
            let block = &fill[..at];
            return Cycle {
                block: block.to_string(),
                block_chars: chars,
                ascii: block.is_ascii(),
            };
        }
        let fill_chars = fill.chars().count();
        if fill_chars == 0 {
            return Cycle {
                block: String::new(),
                block_chars: 0,
                ascii: true,
            };
        }
        let wanted = chars.div_ceil(fill_chars).max(1);
        let fit = (Self::BLOCK_BYTES / fill.len()).max(1);
        let reps = wanted.min(fit);
        Cycle {
            block: fill.repeat(reps),
            block_chars: reps * fill_chars,
            ascii: fill.is_ascii(),
        }
    }

    /// Writes the first `n` characters of the fill repeated.
    fn write(&self, out: &mut impl Write, mut n: usize) -> std::fmt::Result {
        if n == 0 || self.block_chars == 0 {
            return Ok(());
        }
        while n >= self.block_chars {
            out.write_str(&self.block)?;
            n -= self.block_chars;
        }
        let at = if self.ascii {
            n
        } else {
            self.block
                .char_indices()
                .nth(n)
                .map_or(self.block.len(), |(b, _)| b)
        };
        out.write_str(&self.block[..at])
    }
}

/// The byte length of `lpad(s, width, fill)` (and of `rpad`, which places
/// the same characters on the other side).
fn padded_len(s: &str, width: usize, fill: &str) -> u128 {
    let (kept, pad) = cut(s, width, fill);
    kept.len() as u128 + if pad == 0 { 0 } else { fill_len(fill, pad) }
}

/// The byte length of the first `n` characters of `fill` repeated. Reads
/// no more of `fill` than the padding holds.
fn fill_len(fill: &str, n: usize) -> u128 {
    if fill.is_empty() {
        return 0;
    }
    if let Some((at, _)) = fill.char_indices().nth(n) {
        return at as u128;
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
    use datafusion::arrow::array::{LargeStringArray, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::DFSchema;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::simplify::SimplifyContext;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;

    fn invoke(udf: &ScalarUDF, args: Vec<ColumnarValue>, rows: usize) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("a", a.data_type(), true)))
            .collect::<Vec<_>>();
        let types = args.iter().map(|a| a.data_type()).collect::<Vec<_>>();
        let return_type = udf.return_type(&types)?;
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: rows,
            return_field: Arc::new(Field::new("r", return_type, true)),
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

    /// The values of a result, `rows` of them for a scalar one.
    fn values_of(result: ColumnarValue, rows: usize) -> (DataType, Vec<Option<String>>) {
        let array = result.into_array(rows).unwrap();
        let data_type = array.data_type().clone();
        let values = match &data_type {
            DataType::Utf8 => array
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(str::to_string))
                .collect(),
            DataType::LargeUtf8 => array
                .as_string::<i64>()
                .iter()
                .map(|v| v.map(str::to_string))
                .collect(),
            other => panic!("unexpected result type {other}"),
        };
        (data_type, values)
    }

    fn values(result: ColumnarValue) -> Vec<Option<String>> {
        values_of(result, 1).1
    }

    fn pads() -> [(&'static ScalarUDF, Arc<ScalarUDF>); 2] {
        [
            (&LPAD, datafusion::functions::unicode::lpad()),
            (&RPAD, datafusion::functions::unicode::rpad()),
        ]
    }

    /// The wrapper builds what DataFusion's kernels build, for every layout
    /// of the arguments: each string type, literals and columns, NULLs,
    /// negative widths, empty and multi-byte fills, and the two-argument
    /// form.
    #[test]
    fn matches_datafusion() {
        let texts = [
            Some(""),
            Some("a"),
            Some("abc"),
            Some("héllo"),
            Some("日本語"),
            Some("a😀b"),
            None,
        ];
        let string_args = |layout: usize| -> ColumnarValue {
            match layout {
                0 => strings(&texts),
                1 => ColumnarValue::Array(Arc::new(LargeStringArray::from(texts.to_vec()))),
                2 => ColumnarValue::Array(Arc::new(StringViewArray::from(texts.to_vec()))),
                _ => ColumnarValue::Scalar(ScalarValue::Utf8(Some("héllo".into()))),
            }
        };
        let width_args = [
            width(0),
            width(2),
            width(5),
            width(-3),
            width(9000),
            ColumnarValue::Scalar(ScalarValue::Int64(None)),
            // No negative width: DataFusion reads it as a huge reservation
            // here (`negative_width_column_pads_to_nothing`).
            widths(&[
                Some(0),
                Some(4),
                Some(1),
                Some(9000),
                None,
                Some(3),
                Some(8),
            ]),
        ];
        let fill_args = [
            None,
            Some(fill("")),
            Some(fill(" ")),
            Some(fill("xy")),
            Some(fill("é")),
            Some(fill("日😀a")),
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            Some(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(
                "ab".into(),
            )))),
            Some(strings(&[
                Some("xy"),
                Some(""),
                Some("é"),
                None,
                Some("日😀a"),
                Some("-"),
                Some("ab"),
            ])),
        ];
        let rows = texts.len();
        for (ours, theirs) in pads() {
            for layout in 0..4 {
                for w in &width_args {
                    for f in &fill_args {
                        let mut args = vec![string_args(layout), w.clone()];
                        args.extend(f.clone());
                        // DataFusion reads a negative width as 0, but off its
                        // literal-fill path it reserves it as a huge size and
                        // panics: ask it for width 0 there.
                        let literal_fill = match f {
                            None => true,
                            Some(ColumnarValue::Scalar(v)) => !v.is_null(),
                            Some(_) => false,
                        };
                        let mut theirs_args = args.clone();
                        if !literal_fill
                            && matches!(w, ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) if *n < 0)
                        {
                            theirs_args[1] = width(0);
                        }
                        let expected = values_of(invoke(&theirs, theirs_args, rows).unwrap(), rows);
                        let got = values_of(invoke(ours, args.clone(), rows).unwrap(), rows);
                        assert_eq!(got, expected, "{}({args:?})", ours.name());
                    }
                }
            }
        }
    }

    /// The lengths [`padded_len`] predicts are the lengths DataFusion's
    /// kernels build.
    #[test]
    fn padded_len_matches_the_kernels() {
        let inputs = ["", "a", "abc", "héllo", "日本語", "a😀b"];
        let fills = ["", " ", "xy", "é", "日😀", "aé"];
        for (_, udf) in pads() {
            for s in inputs {
                for f in fills {
                    for w in 0..9usize {
                        let out = values(
                            invoke(&udf, vec![strings(&[Some(s)]), width(w as i64), fill(f)], 1)
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

    /// A call on literals only gives one value, which the projection repeats
    /// for each row of the batch: the check counts the batch's rows, so
    /// 8192 rows of `2**18` bytes are refused although one row fits.
    #[test]
    fn literal_result_counts_the_batch_rows() {
        for udf in [&*LPAD, &*RPAD, &*LPAD_CONSTANT, &*RPAD_CONSTANT] {
            let args = || {
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))),
                    width(1 << 18),
                    fill(" "),
                ]
            };
            let err = invoke(udf, args(), 8192).unwrap_err().to_string();
            assert!(
                err.contains("2147483648 bytes of strings in one batch of 8192 rows"),
                "{err}"
            );
            let one = invoke(udf, args(), 1).unwrap();
            assert!(matches!(one, ColumnarValue::Scalar(_)));
            assert_eq!(values(one)[0].as_ref().map(String::len), Some(1 << 18));
        }
    }

    /// DataFusion folds a call on literals into a literal while planning,
    /// evaluated for one row, and the projection repeats that literal into
    /// each batch with a panic on overflow. A call that references no
    /// column is volatile, so it is evaluated per batch; one that does stays
    /// immutable, as DataFusion's own.
    #[test]
    fn a_constant_call_is_not_folded() {
        let simplify = |expr: Expr| {
            let schema = DFSchema::empty();
            let context = SimplifyContext::builder()
                .with_schema(Arc::new(schema))
                .build();
            ExprSimplifier::new(context).simplify(expr).unwrap()
        };
        for pad in [lpad, rpad] {
            let call = pad(vec![lit("a"), lit(1i64 << 18), lit(" ")]);
            let simplified = simplify(call);
            assert!(
                matches!(simplified, Expr::ScalarFunction(_)),
                "{simplified}"
            );
            let on_column = pad(vec![datafusion::prelude::col("s"), lit(3i64), lit(" ")]);
            let Expr::ScalarFunction(f) = on_column else {
                panic!("{on_column}")
            };
            assert_eq!(f.func.signature().volatility, Volatility::Immutable);
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

    /// Small results whose operands DataFusion's kernels would have expanded
    /// or reserved for far beyond the result: a 1 MiB literal fill (or
    /// string) repeated for each of 4096 rows is 4 GiB, past Utf8's offsets,
    /// though every width is 0; and a million NULL strings padded to 16384
    /// reserved 16 GiB. The wrapper reserves what the result holds.
    #[test]
    fn small_results_reserve_no_more_than_they_hold() {
        let mib = "x".repeat(1 << 20);
        let rows = 4096;
        let zeros = widths(&vec![Some(0); rows]);
        for udf in [&*LPAD, &*RPAD] {
            let wide_fill = vec![strings(&vec![Some("ab"); rows]), zeros.clone(), fill(&mib)];
            let out = values_of(invoke(udf, wide_fill, rows).unwrap(), rows).1;
            assert!(out.iter().all(|v| v.as_deref() == Some("")));

            let wide_string = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(mib.clone()))),
                zeros.clone(),
                fill(" "),
            ];
            let out = values_of(invoke(udf, wide_string, rows).unwrap(), rows).1;
            assert!(out.iter().all(|v| v.as_deref() == Some("")));

            let nulls = 1 << 20;
            let all_null = ColumnarValue::Array(Arc::new(StringArray::new_null(nulls)));
            let out = invoke(udf, vec![all_null, width(16384), fill("é")], nulls)
                .unwrap()
                .into_array(nulls)
                .unwrap();
            assert_eq!(out.null_count(), nulls);
            assert!(out.as_string::<i32>().values().capacity() < 64 * 1024);

            let short = invoke(
                udf,
                vec![strings(&vec![Some("ab"); 1000]), width(5), fill("é")],
                1000,
            )
            .unwrap()
            .into_array(1000)
            .unwrap();
            let held = short.as_string::<i32>().values().len();
            assert_eq!(held, 1000 * 8);
            assert!(short.as_string::<i32>().values().capacity() < held + 64);
        }
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

    /// A literal width beyond `i32::MAX` is an error before any row is read,
    /// as in DataFusion, even when every string is NULL; in a width column it
    /// is checked on the rows whose result is not NULL.
    #[test]
    fn literal_width_beyond_i32_is_an_error_on_null_strings() {
        for udf in [&*LPAD, &*RPAD] {
            for string in [
                strings(&[None, None]),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ] {
                let err = invoke(udf, vec![string, width(1 << 40), fill(" ")], 2)
                    .unwrap_err()
                    .to_string();
                assert!(err.contains("too large"), "{err}");
            }
            let out = invoke(
                udf,
                vec![
                    strings(&[None, Some("a")]),
                    widths(&[Some(1 << 40), Some(2)]),
                    fill("-"),
                ],
                2,
            )
            .unwrap();
            let expected = if udf.name() == "lpad" { "-a" } else { "a-" };
            assert_eq!(values(out), [None, Some(expected.into())]);
        }
    }
}
