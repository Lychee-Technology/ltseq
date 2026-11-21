//! Formatting functions for displaying RecordBatches as ASCII tables

use std::borrow::Cow;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::temporal_conversions;
use datafusion::arrow::util::display::{ArrayFormatter, DurationFormat, FormatOptions};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

const MAX_COL_WIDTH: usize = 50;
const ELLIPSIS: &str = "...";
const ELLIPSIS_WIDTH: usize = 3;

/// How Arrow's own formatter prints the types this module has no arm for
/// (decimals, durations, times, lists, structs, binary, ...). A NULL nested
/// in a list or struct prints as `None`, as a NULL cell does, and a duration
/// prints as `1 days 2 hours 3 mins 4.500000 secs` rather than ISO 8601.
const FALLBACK_FORMAT: FormatOptions<'static> = FormatOptions::new()
    .with_null("None")
    .with_duration_format(DurationFormat::Pretty);

/// A typed column reference that avoids repeated `downcast_ref` per cell.
///
/// By matching `DataType` once per column, we get a typed reference that
/// formats individual rows without any runtime type dispatch overhead.
enum TypedColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    TimestampSec(&'a TimestampSecondArray),
    TimestampMs(&'a TimestampMillisecondArray),
    TimestampUs(&'a TimestampMicrosecondArray),
    TimestampNs(&'a TimestampNanosecondArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    /// Every other type Arrow can display: decimals, durations, lists, ...
    /// Without it they printed as `[unsupported type]` (#312).
    Other(ArrayFormatter<'a>),
    /// A type Arrow's formatter refuses
    Unsupported,
}

impl<'a> TypedColumn<'a> {
    /// Create a TypedColumn by matching on the column's DataType once.
    fn new(column: &'a dyn Array) -> Self {
        match column.data_type() {
            DataType::Utf8 => {
                TypedColumn::Utf8(column.as_any().downcast_ref().expect("type matched Utf8"))
            }
            DataType::LargeUtf8 => TypedColumn::LargeUtf8(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched LargeUtf8"),
            ),
            DataType::Utf8View => TypedColumn::Utf8View(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Utf8View"),
            ),
            DataType::Date32 => {
                TypedColumn::Date32(column.as_any().downcast_ref().expect("type matched Date32"))
            }
            DataType::Date64 => {
                TypedColumn::Date64(column.as_any().downcast_ref().expect("type matched Date64"))
            }
            DataType::Timestamp(TimeUnit::Second, _) => TypedColumn::TimestampSec(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampSec"),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => TypedColumn::TimestampMs(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampMs"),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => TypedColumn::TimestampUs(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampUs"),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => TypedColumn::TimestampNs(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched TimestampNs"),
            ),
            DataType::Int8 => {
                TypedColumn::Int8(column.as_any().downcast_ref().expect("type matched Int8"))
            }
            DataType::Int16 => {
                TypedColumn::Int16(column.as_any().downcast_ref().expect("type matched Int16"))
            }
            DataType::Int32 => {
                TypedColumn::Int32(column.as_any().downcast_ref().expect("type matched Int32"))
            }
            DataType::Int64 => {
                TypedColumn::Int64(column.as_any().downcast_ref().expect("type matched Int64"))
            }
            DataType::UInt8 => {
                TypedColumn::UInt8(column.as_any().downcast_ref().expect("type matched UInt8"))
            }
            DataType::UInt16 => {
                TypedColumn::UInt16(column.as_any().downcast_ref().expect("type matched UInt16"))
            }
            DataType::UInt32 => {
                TypedColumn::UInt32(column.as_any().downcast_ref().expect("type matched UInt32"))
            }
            DataType::UInt64 => {
                TypedColumn::UInt64(column.as_any().downcast_ref().expect("type matched UInt64"))
            }
            DataType::Float32 => TypedColumn::Float32(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Float32"),
            ),
            DataType::Float64 => TypedColumn::Float64(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Float64"),
            ),
            DataType::Boolean => TypedColumn::Boolean(
                column
                    .as_any()
                    .downcast_ref()
                    .expect("type matched Boolean"),
            ),
            _ => match ArrayFormatter::try_new(column, &FALLBACK_FORMAT) {
                Ok(formatter) => TypedColumn::Other(formatter),
                Err(_) => TypedColumn::Unsupported,
            },
        }
    }

    /// Format a single cell value. The type dispatch is already resolved.
    fn format(&self, row_idx: usize, column: &dyn Array) -> String {
        if !column.is_valid(row_idx) {
            return "None".to_string();
        }
        match self {
            TypedColumn::Utf8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::LargeUtf8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Utf8View(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Date32(arr) => {
                let date = arr.value(row_idx);
                match temporal_conversions::date32_to_datetime(date) {
                    Some(dt) => dt.format("%Y-%m-%d").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::Date64(arr) => {
                let ms = arr.value(row_idx);
                match temporal_conversions::date64_to_datetime(ms) {
                    Some(dt) => dt.format("%Y-%m-%d").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampSec(arr) => {
                let secs = arr.value(row_idx);
                match temporal_conversions::timestamp_s_to_datetime(secs) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampMs(arr) => {
                let ms = arr.value(row_idx);
                match temporal_conversions::timestamp_ms_to_datetime(ms) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampUs(arr) => {
                let us = arr.value(row_idx);
                match temporal_conversions::timestamp_us_to_datetime(us) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::TimestampNs(arr) => {
                let ns = arr.value(row_idx);
                match temporal_conversions::timestamp_ns_to_datetime(ns) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    None => "".to_string(),
                }
            }
            TypedColumn::Int8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Int16(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Int32(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Int64(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt8(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt16(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt32(arr) => arr.value(row_idx).to_string(),
            TypedColumn::UInt64(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Float32(arr) => format!("{}", arr.value(row_idx)),
            TypedColumn::Float64(arr) => format!("{}", arr.value(row_idx)),
            TypedColumn::Boolean(arr) => arr.value(row_idx).to_string(),
            TypedColumn::Other(formatter) => formatter.value(row_idx).to_string(),
            TypedColumn::Unsupported => "[unsupported type]".to_string(),
        }
    }
}

fn display_width(s: &str) -> usize {
    UnicodeWidthStr::width(s)
}

/// Append `text` left-aligned in a field of `width` terminal display columns.
fn push_padded(out: &mut String, text: &str, width: usize) {
    out.push_str(text);
    let pad = width.saturating_sub(display_width(text));
    out.extend(std::iter::repeat_n(' ', pad));
}

/// Calculate optimal column widths by scanning through all rows (up to limit)
fn calculate_column_widths(
    batches: &[RecordBatch],
    schema: &ArrowSchema,
    limit: usize,
) -> Vec<usize> {
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut col_widths: Vec<usize> = col_names.iter().map(|name| display_width(name)).collect();

    let mut row_count = 0;
    for batch in batches {
        // Resolve typed columns once per batch (type dispatch happens here)
        let typed_cols: Vec<TypedColumn> = (0..batch.num_columns())
            .map(|i| TypedColumn::new(batch.column(i)))
            .collect();

        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            for (col_idx, typed_col) in typed_cols.iter().enumerate() {
                let col = batch.column(col_idx);
                let value_str = typed_col.format(row_idx, col);
                col_widths[col_idx] = col_widths[col_idx].max(display_width(&value_str));
            }
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    // Limit width to 50 display columns per column for readability
    for width in &mut col_widths {
        *width = (*width).min(MAX_COL_WIDTH);
    }

    col_widths
}

/// Draw a border line with given column widths
fn draw_border(col_widths: &[usize]) -> String {
    let mut border = String::new();
    border.push('+');
    for width in col_widths {
        border.push_str(&"-".repeat(width + 2));
        border.push('+');
    }
    border.push('\n');
    border
}

/// Draw the header row with column names
fn draw_header(schema: &ArrowSchema, col_widths: &[usize]) -> String {
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut header = String::new();
    header.push('|');
    for (i, col_name) in col_names.iter().enumerate() {
        header.push(' ');
        let truncated = truncate_value(col_name);
        push_padded(&mut header, &truncated, col_widths[i]);
        header.push_str(" |");
    }
    header.push('\n');
    header
}

/// Truncate long cell values or headers to at most `MAX_COL_WIDTH` display
/// columns, appending `"..."` when truncated. Cuts on extended grapheme
/// cluster boundaries so multi-byte characters, combining marks, variation
/// selectors, and ZWJ emoji stay intact (issues #125, #316).
fn truncate_value(value_str: &str) -> Cow<'_, str> {
    if display_width(value_str) <= MAX_COL_WIDTH {
        return Cow::Borrowed(value_str);
    }
    let target = MAX_COL_WIDTH - ELLIPSIS_WIDTH;
    let mut end = 0;
    let mut acc = 0;
    for g in UnicodeSegmentation::graphemes(value_str, true) {
        let gw = display_width(g);
        if acc + gw > target {
            break;
        }
        acc += gw;
        end += g.len();
    }
    // Defensive guard in case string-level display width ever exceeds the sum
    // of its grapheme widths, keeping `display_width(result) <= MAX_COL_WIDTH`.
    while end > 0 && display_width(&value_str[..end]) > target {
        let last_len = UnicodeSegmentation::graphemes(&value_str[..end], true)
            .next_back()
            .map_or(0, str::len);
        if last_len == 0 {
            break;
        }
        end -= last_len;
    }
    let mut out = String::with_capacity(end + ELLIPSIS.len());
    out.push_str(&value_str[..end]);
    out.push_str(ELLIPSIS);
    Cow::Owned(out)
}

/// Draw data rows from batches (up to limit)
fn draw_rows(batches: &[RecordBatch], col_widths: &[usize], limit: usize) -> String {
    let mut rows = String::new();
    let mut row_count = 0;

    for batch in batches {
        // Resolve typed columns once per batch
        let typed_cols: Vec<TypedColumn> = (0..batch.num_columns())
            .map(|i| TypedColumn::new(batch.column(i)))
            .collect();

        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            rows.push('|');
            for (col_idx, typed_col) in typed_cols.iter().enumerate() {
                rows.push(' ');
                let col = batch.column(col_idx);
                let value_str = typed_col.format(row_idx, col);
                let truncated = truncate_value(&value_str);
                push_padded(&mut rows, &truncated, col_widths[col_idx]);
                rows.push_str(" |");
            }
            rows.push('\n');
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    rows
}

/// Format RecordBatches as a pretty-printed ASCII table
///
/// Shows up to `limit` rows, streaming through batches
pub fn format_table(
    batches: &[RecordBatch],
    schema: &ArrowSchema,
    limit: usize,
) -> String {
    // Calculate column widths
    let col_widths = calculate_column_widths(batches, schema, limit);

    let mut output = String::new();

    // Draw top border
    output.push_str(&draw_border(&col_widths));

    // Draw header
    output.push_str(&draw_header(schema, &col_widths));

    // Draw header border
    output.push_str(&draw_border(&col_widths));

    // Draw rows
    output.push_str(&draw_rows(batches, &col_widths, limit));

    // Draw bottom border
    output.push_str(&draw_border(&col_widths));

    output
}

#[cfg(test)]
mod tests {
    use super::{display_width, format_table, truncate_value, MAX_COL_WIDTH};
    use datafusion::arrow::array::{
        ArrayRef, Decimal128Array, DurationMicrosecondArray, Int64Array, ListArray, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// The cells of `column` as `show()` prints them, one per row.
    fn cells(column: ArrayRef) -> Vec<String> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "c",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        format_table(&[batch], &schema, usize::MAX)
            .lines()
            .skip(3)
            .filter(|line| line.starts_with('|'))
            .map(|line| line.trim_matches('|').trim().to_string())
            .collect()
    }

    /// Assert every line of `table` has the same display width and every column
    /// divider (`|`) aligns vertically with the border corners (`+`).
    fn assert_aligned(table: &str) {
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines.len() >= 4, "expected at least 4 lines:\n{table}");

        let divider_positions = |line: &str, marker: char| -> Vec<usize> {
            let mut positions = Vec::new();
            let mut prefix_end = 0;
            for (idx, ch) in line.char_indices() {
                if ch == marker {
                    positions.push(display_width(&line[..idx]));
                }
                prefix_end = idx + ch.len_utf8();
            }
            assert_eq!(prefix_end, line.len());
            positions
        };

        let expected_width = display_width(lines[0]);
        let expected_cols = divider_positions(lines[0], '+');
        assert!(expected_cols.len() >= 2, "expected column borders:\n{table}");

        for (i, line) in lines.iter().enumerate() {
            assert_eq!(
                display_width(line),
                expected_width,
                "line {i} width mismatch in:\n{table}"
            );
            let marker = if line.starts_with('+') { '+' } else { '|' };
            assert_eq!(
                divider_positions(line, marker),
                expected_cols,
                "line {i} divider positions mismatch in:\n{table}"
            );
        }
    }

    #[test]
    fn decimals_print_their_value() {
        let decimals = Decimal128Array::from(vec![Some(150), None, Some(-5)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(cells(Arc::new(decimals)), ["1.50", "None", "-0.05"]);
        // `5 - ntile(4)` is decimal128(21, 0) (#312)
        let integral = Decimal128Array::from(vec![4])
            .with_precision_and_scale(21, 0)
            .unwrap();
        assert_eq!(cells(Arc::new(integral)), ["4"]);
    }

    #[test]
    fn durations_print_their_parts() {
        let durations = DurationMicrosecondArray::from(vec![Some(1_800_000_000), None]);
        assert_eq!(
            cells(Arc::new(durations)),
            ["0 days 0 hours 30 mins 0.000000 secs", "None"]
        );
    }

    #[test]
    fn lists_print_their_items_with_none_for_null() {
        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
        ]);
        assert_eq!(cells(Arc::new(lists)), ["[1, None]", "None", "[]"]);
    }

    #[test]
    fn short_values_pass_through() {
        assert_eq!(truncate_value("hello"), "hello");
    }

    #[test]
    fn long_ascii_truncates_at_47() {
        let input = "x".repeat(80);
        let out = truncate_value(&input);
        assert_eq!(out, format!("{}...", "x".repeat(47)));
    }

    #[test]
    fn long_cjk_does_not_panic_and_truncates_by_display_width() {
        // 25 CJK chars = 50 display columns (75 UTF-8 bytes) fits without truncation.
        let fits = "中".repeat(25);
        assert_eq!(truncate_value(&fits), fits);

        // 84 CJK chars = 168 display columns truncates to 23 chars (46 cols) + "..." (49 cols).
        let input = "数据序列处理库".repeat(12);
        let out = truncate_value(&input);
        assert!(out.ends_with("..."));
        assert_eq!(display_width(&out), 49);
        assert!(display_width(&out) <= MAX_COL_WIDTH);
    }

    #[test]
    fn long_emoji_does_not_panic_and_keeps_clusters_intact() {
        let input = "🚀🎉🔥💡🌟".repeat(15);
        let out = truncate_value(&input);
        assert!(out.ends_with("..."));
        assert!(display_width(&out) <= MAX_COL_WIDTH);

        // VS16 heart (U+2764 + U+FE0F) must not lose its variation selector.
        let hearts = format!("{}❤️xyz", "a".repeat(46));
        let out_hearts = truncate_value(&hearts);
        assert_eq!(out_hearts, format!("{}...", "a".repeat(46)));
        assert!(display_width(&out_hearts) <= MAX_COL_WIDTH);

        // ZWJ family emoji must not be split leaving a dangling ZWJ.
        let families = "👨\u{200d}👩\u{200d}👧\u{200d}👦".repeat(30);
        let out_families = truncate_value(&families);
        assert!(out_families.ends_with("..."));
        assert!(!out_families.trim_end_matches("...").ends_with('\u{200d}'));
        assert!(display_width(&out_families) <= MAX_COL_WIDTH);
    }

    #[test]
    fn combining_chars_do_not_panic_or_split_marks() {
        // "éàô" as e/a/o + U+0301/U+0300/U+0302 — display width 3 per unit.
        let e = "e\u{301}a\u{300}o\u{302}";
        // 16 units = 48 cols (144 bytes) <= 50: must not truncate.
        let fits = e.repeat(16);
        assert_eq!(truncate_value(&fits), fits);

        // 20 units = 60 cols > 50: truncates at 47 cols without dropping a combining mark.
        let input = e.repeat(20);
        let out = truncate_value(&input);
        assert!(out.ends_with("..."));
        assert_eq!(display_width(&out), 50);
        assert!(out.trim_end_matches("...").ends_with('\u{300}'));
    }

    #[test]
    fn cjk_and_ascii_rows_align_issue_316() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pair", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["啤酒 + 尿布", "beer + chips"])),
                Arc::new(Int64Array::from(vec![2, 2])),
            ],
        )
        .unwrap();

        let table = format_table(&[batch], &schema, 10);
        assert_aligned(&table);
        let expected = concat!(
            "+--------------+---+\n",
            "| pair         | n |\n",
            "+--------------+---+\n",
            "| 啤酒 + 尿布  | 2 |\n",
            "| beer + chips | 2 |\n",
            "+--------------+---+\n",
        );
        assert_eq!(table, expected);
    }

    #[test]
    fn cjk_column_names_and_empty_table_align() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("商品", DataType::Utf8, false),
            Field::new("数量", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "重要价值客户"])),
                Arc::new(Int64Array::from(vec![1, 42])),
            ],
        )
        .unwrap();

        let table = format_table(&[batch], &schema, 10);
        assert_aligned(&table);
        let expected = concat!(
            "+--------------+------+\n",
            "| 商品         | 数量 |\n",
            "+--------------+------+\n",
            "| a            | 1    |\n",
            "| 重要价值客户 | 42   |\n",
            "+--------------+------+\n",
        );
        assert_eq!(table, expected);

        // Empty table with CJK column names still aligns.
        let empty = format_table(&[], &schema, 10);
        assert_aligned(&empty);
    }

    #[test]
    fn accented_combining_and_long_headers_align() {
        let long_cjk_header = "列".repeat(30); // 60 display cols > 50
        let schema = Arc::new(Schema::new(vec![
            Field::new(long_cjk_header, DataType::Utf8, false),
            Field::new("café", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "数据序列处理库".repeat(12),
                    "x".to_string(),
                ])),
                Arc::new(StringArray::from(vec!["cafe\u{301}", "🚀"])),
            ],
        )
        .unwrap();

        let table = format_table(&[batch], &schema, 10);
        assert_aligned(&table);
    }
}
