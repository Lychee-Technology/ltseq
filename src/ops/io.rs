//! I/O operations for LTSeqTable
//!
//! This module contains helper functions for file I/O operations including:
//! - CSV writing (via Arrow's CSV writer)
//! - Parquet writing (via Arrow's Parquet writer)
//! - Arrow interop over the C Data Interface (`from_arrow`, `to_arrow`,
//!   `__arrow_c_stream__`), see `crate::arrow_ffi`
//!
//! Every entry point here runs with the GIL released (`lib.rs` wraps them in
//! `gil::detached`): they take only plain Rust values and report failures as
//! `LtseqError`.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;

use crate::arrow_ffi::{
    collect_imported, collected_reader, BoxedBatchReader, DataFrameBatchReader,
};
use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;

/// Write table data to a CSV file using Arrow's native CSV writer.
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output CSV file
pub fn write_csv_impl(table: &LTSeqTable, path: String) -> Result<(), LtseqError> {
    let df = table.require_df()?;

    RUNTIME.block_on(async {
        use datafusion::arrow::csv::WriterBuilder;
        use std::fs::File;

        let df_clone = (**df).clone();
        let batches = df_clone.collect().await.map_err(LtseqError::collect)?;

        let file = File::create(&path).map_err(|e| {
            LtseqError::io(&format!("Failed to create file '{}'", path), e)
        })?;

        let builder = WriterBuilder::new().with_header(true);
        let mut writer = builder.build(file);

        for batch in &batches {
            writer.write(batch).map_err(|e| {
                LtseqError::Io(format!("Failed to write CSV batch: {}", e))
            })?;
        }

        Ok(())
    })
}

/// Write table data to a Parquet file using Arrow's native Parquet writer.
///
/// Batches are streamed from DataFusion and written incrementally, avoiding
/// full in-memory materialization before the first write.
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output Parquet file
///     compression: Compression algorithm name (e.g. "snappy", "zstd", "gzip", "lz4", "none")
pub fn write_parquet_impl(
    table: &LTSeqTable,
    path: String,
    compression: Option<String>,
) -> Result<(), LtseqError> {
    let df = table.require_df()?;

    RUNTIME.block_on(async {
        use datafusion::arrow::datatypes::Schema as ArrowSchema;
        use futures_util::StreamExt;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;
        use std::sync::Arc;

        let df_clone = (**df).clone();

        // Extract Arrow schema before consuming the DataFrame
        let df_schema = df_clone.schema();
        let arrow_fields: Vec<_> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let schema = Arc::new(ArrowSchema::new(arrow_fields));

        let comp = match compression.as_deref() {
            Some("snappy") => Compression::SNAPPY,
            Some("zstd") | Some("zstandard") => Compression::ZSTD(Default::default()),
            Some("gzip") | Some("gz") => Compression::GZIP(Default::default()),
            Some("lz4") => Compression::LZ4,
            Some("none") | None => Compression::UNCOMPRESSED,
            Some(other) => {
                return Err(LtseqError::Validation(format!(
                    "Unknown compression '{}'. Use 'snappy', 'zstd', 'gzip', 'lz4', or 'none'.",
                    other
                )));
            }
        };

        let props = WriterProperties::builder()
            .set_compression(comp)
            .build();

        // Stream batches from DataFusion directly into the Parquet writer
        let mut stream = df_clone
            .execute_stream()
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to create stream: {}", e)))?;

        let file = File::create(&path).map_err(|e| {
            LtseqError::io(&format!("Failed to create file '{}'", path), e)
        })?;

        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| {
            LtseqError::Io(format!("Failed to create Parquet writer: {}", e))
        })?;

        let mut wrote_any = false;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| {
                LtseqError::Io(format!("Failed to read batch: {}", e))
            })?;
            if batch.num_rows() > 0 {
                writer.write(&batch).map_err(|e| {
                    LtseqError::Io(format!("Failed to write Parquet batch: {}", e))
                })?;
                wrote_any = true;
            }
        }

        if !wrote_any {
            drop(writer);
            let _ = std::fs::remove_file(&path);
            return Err(LtseqError::Validation("No data to write".into()));
        }

        writer.close().map_err(|e| {
            LtseqError::Io(format!("Failed to close Parquet writer: {}", e))
        })?;

        Ok(())
    })
}

/// Build a table from a stream imported over the Arrow C Data Interface.
///
/// The reader comes from any Python object implementing `__arrow_c_stream__`
/// (`pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`, ...).
/// Its buffers are shared, not copied. A stream with a schema but no batches
/// yields a table backed by one empty batch, so the result has a real plan
/// and the schema survives (matching a `pyarrow.Table` with zero rows).
pub fn from_arrow_impl(reader: ArrowArrayStreamReader) -> Result<LTSeqTable, LtseqError> {
    use crate::engine::create_session_context;

    let (schema, mut batches) = collect_imported(reader)?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    LTSeqTable::from_batches(create_session_context(), batches, Vec::new(), None)
}

/// Schema of a table that may have no plan (an unloaded `LTSeq()` has neither).
fn schema_or_empty(table: &LTSeqTable) -> SchemaRef {
    table
        .schema
        .clone()
        .unwrap_or_else(|| Arc::new(Schema::empty()))
}

/// Execute the plan and return a reader over the collected batches.
///
/// This is the `to_arrow()` path: it materializes (ADR 0004 terminal
/// boundary) and hands the batches to pyarrow without re-encoding them. A
/// table with no plan yields an empty reader carrying its schema, if any.
pub fn collect_arrow_reader_impl(table: &LTSeqTable) -> Result<BoxedBatchReader, LtseqError> {
    let Some(df) = table.dataframe.as_ref() else {
        return Ok(collected_reader(schema_or_empty(table), Vec::new()));
    };
    let batches = RUNTIME
        .block_on((**df).clone().collect())
        .map_err(LtseqError::collect)?;
    // Batches from one execution share a schema; prefer it over the logical
    // schema so nullability/metadata match what pyarrow will see per batch.
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| schema_or_empty(table));
    Ok(collected_reader(schema, batches))
}

/// Start executing the plan as a stream and return a lazy reader over it.
///
/// This is the `__arrow_c_stream__` path. Planning happens here (so planning
/// errors surface to the caller); batches are pulled by the consumer through
/// `DataFrameBatchReader`. Each call executes the plan anew and leaves the
/// table untouched.
pub fn arrow_stream_impl(table: &LTSeqTable) -> Result<BoxedBatchReader, LtseqError> {
    let Some(df) = table.dataframe.as_ref() else {
        return Ok(collected_reader(schema_or_empty(table), Vec::new()));
    };
    let stream = RUNTIME
        .block_on((**df).clone().execute_stream())
        .map_err(|e| LtseqError::with_context("Failed to execute plan", e))?;
    Ok(Box::new(DataFrameBatchReader::new(stream)))
}
