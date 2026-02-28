//! I/O operations for LTSeqTable
//!
//! This module contains helper functions for file I/O operations including:
//! - CSV writing (via Arrow's CSV writer)
//! - Parquet writing (via Arrow's Parquet writer)
//! - Arrow IPC loading (for from_arrow / from_pandas interop)

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;
use pyo3::prelude::*;

/// Write table data to a CSV file using Arrow's native CSV writer.
///
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output CSV file
pub fn write_csv_impl(table: &LTSeqTable, path: String) -> PyResult<()> {
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
/// Args:
///     table: Reference to LTSeqTable
///     path: Path to the output Parquet file
///     compression: Compression algorithm name (e.g. "snappy", "zstd", "gzip", "lz4", "none")
pub fn write_parquet_impl(
    table: &LTSeqTable,
    path: String,
    compression: Option<String>,
) -> PyResult<()> {
    let df = table.require_df()?;

    RUNTIME.block_on(async {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;

        let df_clone = (**df).clone();
        let batches = df_clone.collect().await.map_err(LtseqError::collect)?;

        if batches.is_empty() {
            return Err(LtseqError::Validation("No data to write".into()).into());
        }

        let schema = batches[0].schema();

        let file = File::create(&path).map_err(|e| {
            LtseqError::io(&format!("Failed to create file '{}'", path), e)
        })?;

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
                )).into());
            }
        };

        let props = WriterProperties::builder()
            .set_compression(comp)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| {
            LtseqError::Io(format!("Failed to create Parquet writer: {}", e))
        })?;

        for batch in &batches {
            writer.write(batch).map_err(|e| {
                LtseqError::Io(format!("Failed to write Parquet batch: {}", e))
            })?;
        }

        writer.close().map_err(|e| {
            LtseqError::Io(format!("Failed to close Parquet writer: {}", e))
        })?;

        Ok(())
    })
}

/// Load Arrow IPC bytes into a new LTSeqTable.
///
/// This is the reverse of `to_arrow_ipc()` — it takes IPC-serialized RecordBatch
/// bytes (from Python's pyarrow) and creates a DataFusion-backed LTSeqTable.
///
/// Args:
///     ipc_buffers: List of bytes objects, each containing one Arrow IPC-serialized RecordBatch
pub fn load_arrow_ipc_impl(ipc_buffers: Vec<Vec<u8>>) -> PyResult<LTSeqTable> {
    use crate::engine::create_session_context;
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    if ipc_buffers.is_empty() {
        let session = create_session_context();
        return Ok(LTSeqTable::empty(session, None, Vec::new(), None));
    }

    let mut all_batches = Vec::new();
    for buf in &ipc_buffers {
        let cursor = Cursor::new(buf);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            LtseqError::Io(format!("Failed to read Arrow IPC data: {}", e))
        })?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                LtseqError::Io(format!("Failed to read Arrow IPC batch: {}", e))
            })?;
            all_batches.push(batch);
        }
    }

    let session = create_session_context();
    LTSeqTable::from_batches(session, all_batches, Vec::new(), None)
}
