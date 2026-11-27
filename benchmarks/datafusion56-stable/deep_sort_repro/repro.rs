//! #296 / R1-F4 of PR #423's review: DataFusion 56 plans a sort or a
//! grouping over a column computed by a deep expression in time that grows
//! about as the cube of the expression's depth; 55 plans it in about linear
//! time. This is plain DataFusion, with no LTSeq code: a three-row MemTable,
//! one projection `s = a + 1 + 1 + ... + 1` (`depth` levels: the column and
//! `depth - 1` additions), then `ORDER BY s` (`sort`) or `GROUP BY a, s`
//! (`group`, which is how a `SELECT DISTINCT` over both columns plans).
//!
//! The same source builds against DataFusion 55.0.0 (`df55/`) and 56.0.0
//! (`df56/`), each with its own Cargo.lock:
//!
//!     cargo run --release --locked -q -- 25 50 100 200
//!
//! Output: one tab-separated line per (shape, depth): the times to build the
//! logical plan, optimize it, create the physical plan, and collect the
//! result, in seconds, and the number of rows. The work runs on a thread with
//! a 1 GiB stack, because DataFusion recurses through the expression.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;

fn deep(depth: usize) -> Expr {
    let mut e = col("a");
    for _ in 1..depth {
        e = e + lit(1i64);
    }
    e
}

async fn plan(shape: &str, depth: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
    )?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;

    let start = Instant::now();
    let projected = ctx
        .table("t")
        .await?
        .select(vec![col("a"), deep(depth).alias("s")])?;
    let df = match shape {
        "sort" => projected.sort(vec![col("s").sort(true, false)])?,
        "group" => projected.aggregate(vec![col("a"), col("s")], vec![])?,
        other => panic!("unknown shape {other}"),
    };
    let built = start.elapsed().as_secs_f64();

    let start = Instant::now();
    let optimized = df.clone().into_optimized_plan()?;
    let optimize = start.elapsed().as_secs_f64();

    let start = Instant::now();
    let physical = ctx.state().create_physical_plan(&optimized).await?;
    let physical_time = start.elapsed().as_secs_f64();

    let start = Instant::now();
    let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
    let collect = start.elapsed().as_secs_f64();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    println!("{shape}\t{depth}\t{built:.4}\t{optimize:.4}\t{physical_time:.4}\t{collect:.4}\t{rows}");
    Ok(())
}

fn main() {
    let depths: Vec<usize> = std::env::args()
        .skip(1)
        .map(|d| d.parse().expect("a depth"))
        .collect();
    let depths = if depths.is_empty() { vec![25, 50, 100, 200] } else { depths };
    std::thread::Builder::new()
        .stack_size(1 << 30)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(1 << 30)
                .enable_all()
                .build()
                .unwrap();
            println!("shape\tdepth\tbuild_s\toptimize_s\tphysical_s\tcollect_s\trows");
            for shape in ["sort", "group"] {
                for &depth in &depths {
                    rt.block_on(plan(shape, depth)).unwrap();
                }
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
