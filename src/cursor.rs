//! LTSeqCursor: Streaming cursor for lazy batch iteration
//!
//! This module provides a streaming cursor that wraps DataFusion's
//! SendableRecordBatchStream, allowing Python to iterate over data
//! in batches without loading the entire dataset into memory.

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::{Arc, Mutex};

use crate::engine::RUNTIME;

/// LTSeqCursor: A streaming cursor for batch-by-batch iteration
///
/// Unlike LTSeqTable which materializes all data, LTSeqCursor holds a
/// SendableRecordBatchStream and yields batches one at a time.
///
/// The stream is wrapped in Mutex to satisfy PyO3's Sync requirement.
#[pyclass]
pub struct LTSeqCursor {
    /// The underlying stream wrapped in Mutex for thread safety
    stream: Mutex<Option<SendableRecordBatchStream>>,
    /// Schema for the data
    schema: Arc<ArrowSchema>,
    /// Path to the source file (for debugging/info)
    source_path: String,
}

impl LTSeqCursor {
    /// Create a new LTSeqCursor from a stream
    pub fn new(
        stream: SendableRecordBatchStream,
        schema: Arc<ArrowSchema>,
        source_path: String,
    ) -> Self {
        LTSeqCursor {
            stream: Mutex::new(Some(stream)),
            schema,
            source_path,
        }
    }
}

#[pymethods]
impl LTSeqCursor {
    /// Fetch the next batch of rows.
    ///
    /// Returns:
    ///     bytes: IPC-serialized RecordBatch, or None if stream is exhausted
    ///
    /// The returned bytes can be deserialized using PyArrow:
    /// ```python
    /// import pyarrow as pa
    /// batch = pa.ipc.read_record_batch(bytes_data, schema)
    /// ```
    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let mut guard = self.stream.lock().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Mutex poisoned: {}", e))
        })?;

        let stream = match guard.as_mut() {
            Some(s) => s,
            None => return Ok(None), // Stream already exhausted
        };

        // Block on async stream.next()
        let result = RUNTIME.block_on(async { stream.next().await });

        match result {
            Some(Ok(batch)) => {
                // Serialize RecordBatch to IPC format for Python consumption
                let ipc_bytes = serialize_batch_to_ipc(&batch).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to serialize batch: {}",
                        e
                    ))
                })?;

                // Return as Python bytes
                Ok(Some(PyBytes::new(py, &ipc_bytes).into()))
            }
            Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Stream error: {}",
                e
            ))),
            None => {
                // Stream exhausted, mark as None
                *guard = None;
                Ok(None)
            }
        }
    }

    /// Get the schema as a list of (name, type) tuples
    fn get_schema(&self) -> Vec<(String, String)> {
        self.schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), format!("{:?}", f.data_type())))
            .collect()
    }

    /// Get column names
    fn get_column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Get the source path
    fn get_source(&self) -> String {
        self.source_path.clone()
    }

    /// Check if cursor is exhausted
    fn is_exhausted(&self) -> bool {
        match self.stream.lock() {
            Ok(guard) => guard.is_none(),
            Err(_) => true, // If mutex is poisoned, consider exhausted
        }
    }
}

/// Serialize a RecordBatch to IPC format (Arrow streaming format)
fn serialize_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    use datafusion::arrow::ipc::writer::StreamWriter;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| format!("Failed to create IPC writer: {}", e))?;

        writer
            .write(batch)
            .map_err(|e| format!("Failed to write batch: {}", e))?;

        writer
            .finish()
            .map_err(|e| format!("Failed to finish IPC stream: {}", e))?;
    }

    Ok(buffer)
}

/// Create a LTSeqCursor from a CSV file path (used by LTSeqTable::scan_csv)
pub fn create_cursor_from_csv(
    session: Arc<SessionContext>,
    path: &str,
    has_header: bool,
) -> Result<LTSeqCursor, String> {
    RUNTIME.block_on(async {
        // Read CSV into DataFrame with has_header option
        let options = CsvReadOptions::new().has_header(has_header);
        let df = session
            .read_csv(path, options)
            .await
            .map_err(|e| format!("Failed to read CSV: {}", e))?;

        // Get schema
        let df_schema = df.schema();
        let arrow_fields: Vec<datafusion::arrow::datatypes::Field> =
            df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        // Execute as stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| format!("Failed to create stream: {}", e))?;

        Ok(LTSeqCursor::new(stream, arrow_schema, path.to_string()))
    })
}

/// Create a LTSeqCursor from a Parquet file path
pub fn create_cursor_from_parquet(
    session: Arc<SessionContext>,
    path: &str,
) -> Result<LTSeqCursor, String> {
    RUNTIME.block_on(async {
        // Read Parquet into DataFrame
        let df = session
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("Failed to read Parquet: {}", e))?;

        // Get schema
        let df_schema = df.schema();
        let arrow_fields: Vec<datafusion::arrow::datatypes::Field> =
            df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        // Execute as stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| format!("Failed to create stream: {}", e))?;

        Ok(LTSeqCursor::new(stream, arrow_schema, path.to_string()))
    })
}
