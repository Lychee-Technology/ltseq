//! #411 / R1-F1: asks DataFusion itself, through SQL, the compound membership
//! questions that LTSeq's literal-only `is_in` cannot write: IN lists holding
//! a column (an item known only when the query runs) and IN lists over a
//! volatile value. The same source builds against 55.0.0 (`df55/`) and the
//! 56.0.0-rc1 candidate (`df56/`), each with its own Cargo.lock.
//!
//! Input (argument 1): a case file written by `probes.py --emit`, one
//! statement per line, `setup<TAB>sql` or `query<TAB>id<TAB>sql`.
//! Output (stdout): one JSON object per query: the optimized logical plan,
//! the physical plan, and the result, as rows when there are at most 64 of
//! them and as a count of each distinct value of the first column otherwise.

use std::collections::BTreeMap;

use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::Result;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;

fn quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn cell(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(array, &options).unwrap();
    quote(&formatter.value(row).to_string())
}

async fn run(ctx: &SessionContext, id: &str, sql: &str) -> Result<String> {
    let df = ctx.sql(sql).await?;
    let logical = df.clone().into_optimized_plan()?.display_indent().to_string();
    let physical_plan = df.clone().create_physical_plan().await?;
    let physical = displayable(physical_plan.as_ref()).indent(true).to_string();
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut result = String::new();
    if total <= 64 {
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let cells: Vec<String> = (0..batch.num_columns())
                    .map(|c| cell(batch.column(c).as_ref(), row))
                    .collect();
                rows.push(format!("[{}]", cells.join(",")));
            }
        }
        result.push_str(&format!("\"rows\":[{}]", rows.join(",")));
    } else {
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                *counts.entry(cell(batch.column(0).as_ref(), row)).or_default() += 1;
            }
        }
        let counts: Vec<String> = counts.iter().map(|(k, v)| format!("[{k},{v}]")).collect();
        result.push_str(&format!("\"counts\":[{}]", counts.join(",")));
    }
    Ok(format!(
        "{{\"id\":{},\"sql\":{},\"n_rows\":{},{},\"logical\":{},\"physical\":{}}}",
        quote(id),
        quote(sql),
        total,
        result,
        quote(&logical),
        quote(&physical)
    ))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).expect("usage: probe CASES");
    let text = std::fs::read_to_string(path).expect("read the case file");
    let ctx = SessionContext::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let parts: Vec<&str> = line.splitn(3, '\t').collect();
        match parts[0] {
            "setup" => {
                ctx.sql(parts[1]).await?.collect().await?;
            }
            "query" => match run(&ctx, parts[1], parts[2]).await {
                Ok(json) => println!("{json}"),
                Err(e) => println!(
                    "{{\"id\":{},\"sql\":{},\"error\":{}}}",
                    quote(parts[1]),
                    quote(parts[2]),
                    quote(&e.to_string())
                ),
            },
            other => panic!("unknown statement kind {other}"),
        }
    }
    Ok(())
}
