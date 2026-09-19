//! LTSeqCursor: Streaming cursor for lazy batch iteration
//!
//! This module provides a streaming cursor that wraps DataFusion's
//! SendableRecordBatchStream, allowing Python to iterate over data
//! in batches without loading the entire dataset into memory.

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures_util::StreamExt;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::gil::detached;

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

    /// Pull the next batch off the stream.
    ///
    /// Runs with the GIL released (see `next_batch`). The mutex is taken
    /// **inside** the detached section on purpose: if a thread blocked on
    /// `lock()` while holding the GIL, the thread mid-`stream.next()` could
    /// never re-attach to hand its batch back — a lock-order deadlock between
    /// the GIL and this mutex. Contending threads instead wait here without
    /// the GIL and simply take the following batch.
    fn pull_batch(&self) -> Result<Option<RecordBatch>, LtseqError> {
        let mut guard = self
            .stream
            .lock()
            .map_err(|e| LtseqError::Runtime(format!("Mutex poisoned: {}", e)))?;

        let Some(stream) = guard.as_mut() else {
            return Ok(None); // Stream already exhausted
        };

        match RUNTIME.block_on(stream.next()) {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(LtseqError::Runtime(format!("Stream error: {}", e))),
            None => {
                // Stream exhausted, mark as None
                *guard = None;
                Ok(None)
            }
        }
    }
}

#[pymethods]
impl LTSeqCursor {
    /// Fetch the next batch of rows as a `pyarrow.RecordBatch`.
    ///
    /// Returns:
    ///     pyarrow.RecordBatch, or None if the stream is exhausted
    ///
    /// The batch crosses the boundary over the Arrow C Data Interface: its
    /// buffers are shared with pyarrow and stay valid after the cursor is
    /// dropped.
    fn next_batch<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        // Per-batch streaming I/O runs with the GIL released.
        let batch = detached(py, || self.pull_batch())?;
        batch.map(|b| PyArrowType(b).into_pyobject(py)).transpose()
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
    fn is_exhausted(&self, py: Python<'_>) -> bool {
        // Same lock-order rule as `pull_batch`: never wait on the stream
        // mutex while holding the GIL.
        py.detach(|| match self.stream.lock() {
            Ok(guard) => guard.is_none(),
            Err(_) => true, // If mutex is poisoned, consider exhausted
        })
    }
}

/// Create a LTSeqCursor from a CSV file path (used by LTSeqTable::scan_csv)
pub fn create_cursor_from_csv(
    session: Arc<SessionContext>,
    path: &str,
    has_header: bool,
) -> Result<LTSeqCursor, LtseqError> {
    RUNTIME.block_on(async {
        // Read CSV into DataFrame with has_header option
        let options = CsvReadOptions::new().has_header(has_header);
        let df = session
            .read_csv(path, options)
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to read CSV: {}", e)))?;

        // Get schema
        let arrow_schema = crate::LTSeqTable::schema_from_df(df.schema());

        // Execute as stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to create stream: {}", e)))?;

        Ok(LTSeqCursor::new(stream, arrow_schema, path.to_string()))
    })
}

/// Create a LTSeqCursor from a Parquet file path
pub fn create_cursor_from_parquet(
    session: Arc<SessionContext>,
    path: &str,
) -> Result<LTSeqCursor, LtseqError> {
    RUNTIME.block_on(async {
        // Read Parquet into DataFrame
        let df = session
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to read Parquet: {}", e)))?;

        // Get schema
        let arrow_schema = crate::LTSeqTable::schema_from_df(df.schema());

        // Execute as stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| LtseqError::Runtime(format!("Failed to create stream: {}", e)))?;

        Ok(LTSeqCursor::new(stream, arrow_schema, path.to_string()))
    })
}
