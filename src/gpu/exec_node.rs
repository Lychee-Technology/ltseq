//! `GpuExecNode` — DataFusion leaf `ExecutionPlan` for GPU execution.
//!
//! # Current state (Phase 2/3)
//! Wraps a CPU fallback plan and optionally stores Substrait bytes.
//! `execute()` delegates to the CPU fallback.
//!
//! # Phase 4 upgrade path
//! Replace the `cpu_fallback.execute()` call with an FFI call to the C++
//! libcudf engine:
//! ```rust,ignore
//! cudf_bridge::execute_substrait(&self.substrait_bytes.unwrap(), &self.table_handles)
//! ```
//!
//! # Leaf-node design
//! `children()` returns an empty vec.  Physical optimizer rules run after
//! `QueryPlanner::create_physical_plan()` returns and cannot reach into this
//! node.  Therefore `GpuQueryPlanner` applies `HostToGpuRule` to the CPU plan
//! *before* constructing `GpuExecNode`.
//!
//! # Output partitioning
//! `GpuExecNode` always advertises **1 output partition**.  GPU execution
//! processes the whole table in one shot; the CPU fallback path coalesces
//! the `cpu_fallback` partitions via `CoalescePartitionsExec`.

use std::any::Any;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::arrow::datatypes::SchemaRef;

/// Leaf `ExecutionPlan` node representing a GPU-accelerated query subtree.
///
/// In Phase 2/3 `execute()` delegates to the stored CPU fallback plan.
/// `substrait_bytes` is populated by `GpuQueryPlanner` (Phase 3) and will be
/// consumed by the C++ libcudf engine in Phase 4.
///
/// Output partitioning is always 1: GPU processes the full table atomically,
/// and the CPU fallback path uses `CoalescePartitionsExec` to merge the
/// `cpu_fallback`'s N partitions into a single stream.
#[derive(Debug)]
pub struct GpuExecNode {
    /// Output schema.
    schema: SchemaRef,
    /// CPU execution plan used as fallback.
    cpu_fallback: Arc<dyn ExecutionPlan>,
    /// Serialised Substrait plan bytes, populated in Phase 3.
    pub substrait_bytes: Option<Vec<u8>>,
    /// Parquet file paths: (substrait_table_name, file_path_on_disk).
    /// Used to load tables into GPU HBM when the Substrait ReadRel uses
    /// a named_table reference (like "?table?") rather than local_files.
    pub parquet_paths: Vec<(String, String)>,
    /// Cached plan properties (always 1 output partition).
    cache: PlanProperties,
}

impl GpuExecNode {
    /// Create a new `GpuExecNode` wrapping `cpu_fallback`.
    pub fn new(
        cpu_fallback: Arc<dyn ExecutionPlan>,
        substrait_bytes: Option<Vec<u8>>,
        parquet_paths: Vec<(String, String)>,
    ) -> Self {
        let schema = cpu_fallback.schema();
        let cache = Self::compute_properties(&cpu_fallback);
        Self { schema, cpu_fallback, substrait_bytes, parquet_paths, cache }
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        use datafusion::physical_plan::Partitioning;
        PlanProperties::new(
            input.equivalence_properties().clone(),
            // Always 1 output partition: GPU processes the whole table at once.
            // CPU fallback merges cpu_fallback's N partitions via CoalescePartitionsExec.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            input.boundedness(),
        )
    }
}

impl DisplayAs for GpuExecNode {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let substrait_info = match &self.substrait_bytes {
            Some(b) => format!("substrait={} bytes", b.len()),
            None => "substrait=none".to_string(),
        };
        write!(f, "GpuExecNode [{substrait_info}]")
    }
}

impl ExecutionPlan for GpuExecNode {
    fn name(&self) -> &'static str {
        "GpuExecNode"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    /// Leaf node — the full operator subtree is hidden inside `cpu_fallback`.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // GpuExecNode always advertises 1 output partition.
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GpuExecNode has 1 output partition; partition {partition} requested"
            )));
        }

        // Phase 4: route through the real C++ libcudf engine when available.
        if let Some(ref bytes) = self.substrait_bytes {
            if crate::gpu::ffi::cudf_engine_available() {
                use crate::gpu::ffi::GpuTableHandle;

                if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                    eprintln!("[GPU] parquet_paths: {:?}", self.parquet_paths);
                }

                // Load Parquet files into GPU HBM.  For anonymous tables
                // ("?table?") we get the file path from `parquet_paths`.
                let handles: Vec<_> = self
                    .parquet_paths
                    .iter()
                    .filter_map(|(name, path)| {
                        let h = GpuTableHandle::load_parquet(path, &[]);
                        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                            eprintln!(
                                "[GPU] load_parquet({path:?}) -> {}",
                                if h.is_some() { "ok" } else { "FAILED" }
                            );
                        }
                        h.map(|h| (name.as_str(), h))
                    })
                    .collect();

                // Build the &[(&str, &GpuTableHandle)] slice expected by execute_substrait.
                let table_refs: Vec<(&str, &GpuTableHandle)> = handles
                    .iter()
                    .map(|(name, h)| (*name, h))
                    .collect();

                match crate::gpu::ffi::execute_substrait(bytes, &table_refs) {
                    Ok(raw_batch) => {
                        // The GPU returns all columns from the Parquet file.
                        // Project down to the expected output schema (handles
                        // SELECT col1, col2 style queries).
                        let batch = project_batch(&raw_batch, &self.schema)
                            .unwrap_or(raw_batch);

                        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                            eprintln!(
                                "[GPU] execute_substrait OK: {} rows, {} cols",
                                batch.num_rows(),
                                batch.num_columns(),
                            );
                        }
                        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
                        use futures_util::stream;
                        let schema = batch.schema();
                        let s = stream::once(async move { Ok(batch) });
                        return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)));
                    }
                    Err(e) => {
                        if std::env::var("LTSEQ_GPU_DEBUG").is_ok() {
                            eprintln!("[GPU] execute_substrait error: {e} — falling back to CPU");
                        }
                    }
                }
            }
        }

        // Phase 2/3 fallback (and Phase 4 error recovery): CPU plan.
        // GpuExecNode advertises 1 partition but cpu_fallback may have N.
        // Use CoalescePartitionsExec to merge them.
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
        let n = self.cpu_fallback.properties().output_partitioning().partition_count();
        if n <= 1 {
            return self.cpu_fallback.execute(0, context);
        }
        let coalesced = Arc::new(CoalescePartitionsExec::new(Arc::clone(&self.cpu_fallback)));
        coalesced.execute(0, context)
    }
}

/// Project `batch` down to the columns listed in `target_schema`, by name.
/// Returns `None` if the batch already has the right schema or if any
/// column cannot be found (caller falls back to the original batch).
fn project_batch(
    batch: &arrow::record_batch::RecordBatch,
    target_schema: &SchemaRef,
) -> Option<arrow::record_batch::RecordBatch> {
    if batch.num_columns() == target_schema.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(target_schema.fields())
            .all(|(a, b)| a.name() == b.name())
    {
        return None; // already matches
    }

    let indices: Vec<usize> = target_schema
        .fields()
        .iter()
        .filter_map(|f| batch.schema().index_of(f.name()).ok())
        .collect();

    if indices.len() != target_schema.fields().len() {
        return None; // can't project (schema mismatch)
    }

    batch.project(&indices).ok()
}
