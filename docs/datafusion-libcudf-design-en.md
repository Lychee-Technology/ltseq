# DataFusion × libcudf GPU Execution Engine — Detailed Design

> A GPU-native execution backend for Apache DataFusion, using
> `datafusion-substrait` as the glue layer, `libcudf` (RAPIDS) for
> GPU operator execution, and Sirius as the architectural reference.
>
> This document consolidates insights from three sources:
> - The original layered architecture design (Sirius-inspired)
> - Partition-aware GPU optimization strategies for macro-ordered datasets
> - Expert review feedback on execution model, memory safety, and production hardening

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Layered Architecture](#2-layered-architecture)
3. [Crate Layout](#3-crate-layout)
4. [Layer 1 — DataFusion Frontend & Substrait Export](#4-layer-1--datafusion-frontend--substrait-export)
5. [Layer 2 — FFI Bridge (Rust ↔ C++)](#5-layer-2--ffi-bridge-rust--c)
6. [Layer 3 — C++ GPU Execution Engine (Sirius-inspired)](#6-layer-3--c-gpu-execution-engine-sirius-inspired)
7. [Layer 4 — libcudf Operator Implementations](#7-layer-4--libcudf-operator-implementations)
8. [Layer 5 — RMM Memory Management & CUDA Streams](#8-layer-5--rmm-memory-management--cuda-streams)
9. [Partition-Aware Optimization for Macro-Ordered Data](#9-partition-aware-optimization-for-macro-ordered-data)
10. [End-to-End Data Flow for a Single SQL Query](#10-end-to-end-data-flow-for-a-single-sql-query)
11. [Core Interface Design (Code Level)](#11-core-interface-design-code-level)
12. [Build System](#12-build-system)
13. [Performance Design Decisions](#13-performance-design-decisions)
14. [Known Limitations & Mitigations](#14-known-limitations--mitigations)
15. [Development Roadmap](#15-development-roadmap)

---

## 1. System Overview

### Design Goals

| Goal | Description |
|------|-------------|
| **GPU-Native** | Data lives in GPU HBM from load time through result output — never leaves the GPU during query execution |
| **Zero User-Side Changes** | Users interact with the standard DataFusion API; the GPU backend is transparent |
| **Maximum Reuse** | DataFusion owns SQL parsing and optimization; libcudf owns GPU operators; nothing is reimplemented from scratch |
| **Graceful Degradation** | Unsupported operators automatically fall back to DataFusion's CPU execution path via hybrid CPU/GPU plans |
| **Partition-Aware** | Exploit macro-ordered (partitioned) data layouts to eliminate shuffle, enable sequential-scan operators, and maximize GPU throughput |

### Why Not GPU-as-Coprocessor?

Shanbhag et al. (SIGMOD '20) prove with a formal model that the coprocessor pattern is fundamentally flawed:

```
Given:  CPU memory bandwidth Bc  >  PCIe bandwidth Bp

Coprocessor query time:  Rg = data_size / Bp   ← PCIe is the bottleneck
Pure-CPU query time:     Rc = data_size / Bc

Since Bc > Bp  →  Rc < Rg
→ A well-optimized CPU is faster than GPU-as-coprocessor
  (empirically 1.4× slower than Hyper on SSB)
```

The correct approach is: **load data into GPU HBM once, execute the entire query plan on the GPU, and transfer only the final (small) result back to the CPU**.

> **Important nuance:** While we reject per-operator coprocessor transfers, the CPU still plays a critical system-level role as the **scheduler, flow controller, and fallback executor**. The CPU orchestrates which plan fragments run on the GPU, handles data loading, manages memory budgets, and executes operators that the GPU does not yet support.

---

## 2. Layered Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     User Code (Rust / Python)                    │
│            ctx.sql("SELECT ...").collect().await                 │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Standard DataFusion API
┌──────────────────────────▼──────────────────────────────────────┐
│                Layer 1 — DataFusion Frontend                    │
│   SQL Parser → Logical Plan → Optimizer → Physical Plan       │
│   ┌──────────────────────────────────────────────────────────┐   │
│   │  GpuPhysicalOptimizerRule                                │   │
│   │  Bottom-up subtree replacement:                          │   │
│   │    FilterExec → GpuFilterExec                           │   │
│   │    JoinExec   → GpuHashJoinExec                         │   │
│   │  Injects CpuToGpuExec / GpuToCpuExec at boundaries       │   │
│   └──────────────────────────────────────────────────────────┘   │
│   datafusion-substrait:  GPU subtree  →  Substrait bytes        │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Substrait protobuf (~KB, plan only — no data)
┌──────────────────────────▼──────────────────────────────────────┐
│                Layer 2 — FFI Bridge  (Rust ↔ C++)              │
│   cudf-bridge crate                                              │
│   ┌──────────────────────────────────────────────────────────┐   │
│   │  extern "C" {                                            │   │
│   │      fn cudf_compile_pipeline(plan) -> PipelineHandle    │   │
│   │      fn cudf_execute_pipeline(h, handles) -> ArrowFFI    │   │
│   │      fn cudf_load_parquet(path) -> GpuTableHandle        │   │
│   │      fn cudf_buffer_init(cache, proc) -> bool            │   │
│   │  }                                                       │   │
│   └──────────────────────────────────────────────────────────┘   │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Stable C ABI
┌──────────────────────────▼──────────────────────────────────────┐
│                Layer 3 — C++ GPU Execution Engine               │
│                (libcudf_engine.so, Sirius-inspired)              │
│   ┌────────────────────┐    ┌─────────────────────┐              │
│   │  SubstraitPlanner  │ → │    GpuPipeline      │              │
│   │  (Substrait→ops)  │    │  (chunk-based bulk) │              │
│   └────────────────────┘    └──────────┬──────────┘              │
│                                         │                        │
│   ┌────────┬────────┬────────┬──────────▼───────┬────────┐      │
│   │  Scan  │ Filter │  Join  │     GroupBy       │  Sort  │      │
│   └────────┴────────┴────────┴───────────────────┴────────┘      │
└──────────────────────────┬───────────────────────────────────────┘
                           │  libcudf C++ API
┌──────────────────────────▼──────────────────────────────────────┐
│                Layer 4 — libcudf (RAPIDS)                       │
│   cudf::inner_join / left_join / merge (sort-merge join)         │
│   cudf::groupby::groupby / segmented_reduce                      │
│   cudf::apply_boolean_mask  (filter)                             │
│   cudf::sort / cudf::sort_by_key                                 │
│   cudf::rolling_window                                           │
│   All operations act on cudf::table_view in GPU HBM              │
└──────────────────────────┬───────────────────────────────────────┘
                           │  CUDA / HBM
┌──────────────────────────▼──────────────────────────────────────┐
│                Layer 5 — RMM Memory Management                  │
│   rmm::mr::pool_memory_resource                                  │
│   Three pools:  Cache Region + Processing Region + Pinned Memory │
│   Multi-stream concurrency for PCIe / compute overlap            │
│   (mirrors Sirius gpu_buffer_manager design)                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Crate Layout

```
datafusion-gpu/
├── Cargo.toml                        # workspace
│
├── crates/
│   ├── datafusion-gpu-core/          # GpuPhysicalOptimizerRule, GpuExecNode
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── optimizer_rule.rs     # GpuPhysicalOptimizerRule (sub-plan pushdown)
│   │   │   ├── exec_nodes.rs         # GpuFilterExec, GpuJoinExec, etc.
│   │   │   ├── transitions.rs        # CpuToGpuExec / GpuToCpuExec boundary nodes
│   │   │   ├── gpu_session.rs        # GpuSessionContext wrapper
│   │   │   └── fallback.rs           # CPU fallback logic
│   │   └── Cargo.toml
│   │
│   ├── cudf-bridge/                  # FFI bridge layer
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── ffi.rs                # extern "C" declarations
│   │   │   ├── table_handle.rs       # GpuTableHandle (GPU data token)
│   │   │   ├── pipeline_handle.rs    # Compiled pipeline handle (prepared execution)
│   │   │   └── arrow_transfer.rs     # Arrow C Data Interface with release safety
│   │   ├── build.rs                  # bindgen + cc to compile the C++ lib
│   │   └── Cargo.toml
│   │
│   └── datafusion-gpu-python/        # Optional PyO3 Python bindings
│       └── ...
│
├── cpp/                              # C++ GPU execution engine
│   ├── CMakeLists.txt
│   ├── include/
│   │   ├── cudf_engine.h             # Public C ABI header
│   │   ├── substrait_planner.h
│   │   ├── gpu_pipeline.h
│   │   ├── gpu_buffer_manager.h
│   │   └── operators/
│   │       ├── gpu_scan.h
│   │       ├── gpu_filter.h
│   │       ├── gpu_join.h            # Hash join + sort-merge join
│   │       ├── gpu_aggregate.h       # Hash-based + partition-wise (segmented)
│   │       ├── gpu_sort.h
│   │       └── gpu_window.h          # Rolling window functions
│   └── src/
│       ├── cudf_engine.cpp           # C ABI implementation entry point
│       ├── substrait_planner.cpp     # Substrait → physical operator tree
│       ├── gpu_pipeline.cpp          # Chunk-based bulk execution
│       ├── gpu_buffer_manager.cpp    # RMM memory pool + CUDA stream management
│       └── operators/
│           ├── gpu_scan.cpp
│           ├── gpu_filter.cpp        # cudf::apply_boolean_mask
│           ├── gpu_join.cpp          # cudf::inner_join + cudf::merge
│           ├── gpu_aggregate.cpp     # cudf::groupby + segmented_reduce
│           ├── gpu_sort.cpp          # cudf::sort_by_key
│           └── gpu_window.cpp        # cudf::rolling_window
│
└── examples/
    ├── tpch_benchmark.rs
    └── simple_query.rs
```

---

## 4. Layer 1 — DataFusion Frontend & Substrait Export

### 4.1 GpuSessionContext — User Entry Point

Wraps a standard `SessionContext` and registers the GPU physical optimizer rule:

```rust
// crates/datafusion-gpu-core/src/gpu_session.rs

pub struct GpuSessionContext {
    inner: SessionContext,
}

impl GpuSessionContext {
    pub fn new(cache_gb: f64, proc_gb: f64) -> Self {
        // Initialize GPU memory pools via FFI
        cudf_bridge::init_buffer(cache_gb, proc_gb)
            .expect("GPU buffer initialization failed");

        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);

        // Register the GPU physical optimizer rule
        let state = ctx.state()
            .add_physical_optimizer_rule(Arc::new(GpuPhysicalOptimizerRule::new()));
        let ctx = SessionContext::new_with_state(state);

        Self { inner: ctx }
    }

    pub async fn sql(&self, sql: &str)
        -> datafusion::error::Result<DataFrame>
    {
        self.inner.sql(sql).await
    }

    /// Register data that is already resident in GPU HBM,
    /// avoiding any CPU→GPU transfer.
    pub fn register_gpu_table(&self, name: &str, handle: GpuTableHandle) {
        let provider = Arc::new(GpuTableProvider::new(handle));
        self.inner.register_table(name, provider).unwrap();
    }
}
```

### 4.2 GpuPhysicalOptimizerRule — Sub-Plan Pushdown

> **Design change from original:** The original design used an all-or-nothing
> `GpuQueryPlanner` that required the *entire* logical plan to be GPU-compatible.
> In practice this is extremely fragile — a single unsupported UDF or window
> function forces the entire query back to CPU. Following the approach of
> Apache Spark RAPIDS and DataFusion Comet, we instead implement a
> `PhysicalOptimizerRule` that performs **bottom-up operator replacement**.

The rule walks the physical plan tree bottom-up. For each node it checks whether
the operator is GPU-supported and whether the input data types are compatible.
When it encounters a boundary between GPU and CPU operators, it automatically
injects `CpuToGpuExec` (PCIe upload) or `GpuToCpuExec` (PCIe download)
transition nodes.

```rust
// crates/datafusion-gpu-core/src/optimizer_rule.rs

pub struct GpuPhysicalOptimizerRule;

impl PhysicalOptimizerRule for GpuPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Bottom-up traversal: replace supported operators with GPU variants
        self.transform_bottom_up(plan)
    }

    fn name(&self) -> &str { "gpu_operator_pushdown" }

    fn schema_check(&self) -> bool { true }
}

impl GpuPhysicalOptimizerRule {
    fn transform_bottom_up(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Recursively transform children first
        let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
            .children()
            .into_iter()
            .map(|child| self.transform_bottom_up(child.clone()))
            .collect::<Result<Vec<_>>>()?;

        let plan = plan.with_new_children(new_children)?;

        // 2. Try to replace this node with a GPU variant
        if let Some(gpu_node) = self.try_gpu_replace(&plan) {
            return Ok(gpu_node);
        }

        // 3. If this node stays on CPU but a child is GPU,
        //    inject a GpuToCpuExec transition
        let plan = self.inject_transitions(plan)?;
        Ok(plan)
    }

    fn try_gpu_replace(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // Pattern-match against known operator types:
        //   FilterExec      → GpuFilterExec
        //   AggregateExec   → GpuAggregateExec
        //   SortExec        → GpuSortExec
        //   HashJoinExec    → GpuHashJoinExec
        //   ProjectionExec  → GpuProjectionExec
        //
        // Check data type compatibility (e.g. reject nested types
        // not supported by libcudf) and available GPU memory.
        //
        // If the input child is a CPU node, wrap it in CpuToGpuExec.
        // ...
        None // placeholder
    }
}
```

### 4.3 Transition Nodes — CpuToGpuExec / GpuToCpuExec

These are `ExecutionPlan` implementations that handle the PCIe data transfer at
GPU/CPU operator boundaries:

```rust
// crates/datafusion-gpu-core/src/transitions.rs

/// Transfers a RecordBatch stream from CPU memory to GPU HBM.
/// Inserted automatically when a GPU operator reads from a CPU operator.
#[derive(Debug)]
pub struct CpuToGpuExec {
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

/// Transfers GPU results back to CPU memory via Arrow C Data Interface.
/// Inserted automatically when a CPU operator reads from a GPU operator.
#[derive(Debug)]
pub struct GpuToCpuExec {
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

// The optimizer minimizes the number of these transition nodes
// to reduce PCIe transfers. Adjacent GPU operators form contiguous
// GPU sub-plans with no intermediate CPU materialization.
```

### 4.4 GpuExecNode — Substrait-Based Full Subtree Execution

When the optimizer determines that a large contiguous subtree is fully
GPU-compatible, it can collapse it into a single `GpuExecNode` that serializes
the subtree to Substrait and sends it to the C++ engine in one shot:

```rust
// crates/datafusion-gpu-core/src/exec_nodes.rs

#[derive(Debug)]
pub struct GpuExecNode {
    /// Serialized Substrait plan (protobuf bytes)
    substrait_bytes: Vec<u8>,
    /// Output schema required by DataFusion
    schema: SchemaRef,
    /// Map of table name → GPU memory handle
    table_handles: HashMap<String, Arc<GpuTableHandle>>,
    /// Optional: compiled pipeline handle for repeated execution
    compiled_pipeline: Option<Arc<PipelineHandle>>,
}

impl ExecutionPlan for GpuExecNode {
    fn schema(&self) -> SchemaRef { self.schema.clone() }

    fn output_partitioning(&self) -> Partitioning {
        // GPU engine produces a single consolidated output partition
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node — the GPU engine owns the full subtree
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {

        let substrait = self.substrait_bytes.clone();
        let handles   = self.table_handles.clone();
        let compiled  = self.compiled_pipeline.clone();

        // GPU execution runs in a blocking thread so it does not
        // starve the async Tokio runtime.
        let batch = tokio::task::spawn_blocking(move || {
            match compiled {
                Some(pipeline) => {
                    // Fast path: reuse a pre-compiled pipeline
                    cudf_bridge::execute_pipeline(&pipeline, &handles)
                }
                None => {
                    // Compile + execute in one shot
                    cudf_bridge::execute_substrait(&substrait, &handles)
                }
            }
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

        Ok(Box::pin(stream::once(async move { Ok(batch) })))
    }
}
```

---

## 5. Layer 2 — FFI Bridge (Rust ↔ C++)

### 5.1 C ABI Header — The Stable Interface Contract

```c
// cpp/include/cudf_engine.h
// All symbols use plain C linkage to guarantee ABI stability.

#ifdef __cplusplus
extern "C" {
#endif

// ── Lifecycle ────────────────────────────────────────────────────
/// Initialize RMM memory pools.
/// cache_bytes : GPU data cache region size
/// proc_bytes  : GPU intermediate results region size
///               (hash tables, sort buffers, etc.)
bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes);

/// Release all GPU resources.
void cudf_engine_destroy();

// ── Data Loading (one-time cost; data stays in GPU HBM) ──────────
/// Load a Parquet file directly into GPU HBM.
/// Returns an opaque handle; caller must free it with cudf_table_free.
GpuTableHandle* cudf_load_parquet(
    const char*  path,
    const char** column_names,
    int          num_columns
);

/// Import an Arrow table from CPU memory into GPU HBM.
/// This path involves a PCIe transfer — avoid on the hot path.
GpuTableHandle* cudf_import_arrow(
    ArrowArray*  array,
    ArrowSchema* schema
);

/// Free a GPU table handle and release its HBM allocation.
void cudf_table_free(GpuTableHandle* handle);

// ── Core Execution (two-phase: compile + execute) ─────────────────
/// Phase 1 — Compile a Substrait plan into a reusable pipeline.
/// Parses the protobuf, allocates operator state, and returns an
/// opaque pipeline handle.  This avoids repeated parsing for
/// high-frequency short queries.
///
/// Returns NULL on failure (check error_msg).
GpuPipelineHandle* cudf_compile_pipeline(
    const uint8_t*    plan_bytes,
    size_t            plan_len,
    char*             error_msg,
    size_t            error_msg_len
);

/// Phase 2 — Execute a pre-compiled pipeline with the given table handles.
/// Results are returned via Arrow C Data Interface.
///
/// Returns 0 on success, negative error code on failure.
int cudf_execute_pipeline(
    GpuPipelineHandle*  pipeline,
    const char**        table_names,
    GpuTableHandle**    table_handles,
    int                 num_tables,
    ArrowArray*         out_array,
    ArrowSchema*        out_schema,
    char*               error_msg,
    size_t              error_msg_len
);

/// Free a compiled pipeline handle.
void cudf_pipeline_free(GpuPipelineHandle* pipeline);

/// Convenience: compile + execute in one call (for ad-hoc queries).
int cudf_execute_substrait(
    const uint8_t*    plan_bytes,
    size_t            plan_len,
    const char**      table_names,
    GpuTableHandle**  table_handles,
    int               num_tables,
    ArrowArray*       out_array,
    ArrowSchema*      out_schema,
    char*             error_msg,
    size_t            error_msg_len
);

// ── Memory Diagnostics ────────────────────────────────────────────
size_t cudf_cache_used_bytes();
size_t cudf_cache_total_bytes();
size_t cudf_proc_used_bytes();
size_t cudf_proc_total_bytes();

#ifdef __cplusplus
}
#endif
```

### 5.2 Rust FFI Declarations

```rust
// crates/cudf-bridge/src/ffi.rs

/// Opaque C++ object — never dereferenced from Rust.
#[repr(C)]
pub struct GpuTableHandleOpaque { _private: [u8; 0] }

/// Opaque compiled pipeline — never dereferenced from Rust.
#[repr(C)]
pub struct GpuPipelineHandleOpaque { _private: [u8; 0] }

pub use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

#[link(name = "cudf_engine")]
extern "C" {
    pub fn cudf_engine_init(cache_bytes: usize, proc_bytes: usize) -> bool;
    pub fn cudf_engine_destroy();

    pub fn cudf_load_parquet(
        path: *const c_char,
        column_names: *const *const c_char,
        num_columns: i32,
    ) -> *mut GpuTableHandleOpaque;

    pub fn cudf_import_arrow(
        array:  *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> *mut GpuTableHandleOpaque;

    pub fn cudf_table_free(handle: *mut GpuTableHandleOpaque);

    pub fn cudf_compile_pipeline(
        plan_bytes:    *const u8,
        plan_len:      usize,
        error_msg:     *mut c_char,
        error_msg_len: usize,
    ) -> *mut GpuPipelineHandleOpaque;

    pub fn cudf_execute_pipeline(
        pipeline:      *mut GpuPipelineHandleOpaque,
        table_names:   *const *const c_char,
        table_handles: *const *mut GpuTableHandleOpaque,
        num_tables:    i32,
        out_array:     *mut FFI_ArrowArray,
        out_schema:    *mut FFI_ArrowSchema,
        error_msg:     *mut c_char,
        error_msg_len: usize,
    ) -> i32;

    pub fn cudf_pipeline_free(pipeline: *mut GpuPipelineHandleOpaque);

    pub fn cudf_execute_substrait(
        plan_bytes:    *const u8,
        plan_len:      usize,
        table_names:   *const *const c_char,
        table_handles: *const *mut GpuTableHandleOpaque,
        num_tables:    i32,
        out_array:     *mut FFI_ArrowArray,
        out_schema:    *mut FFI_ArrowSchema,
        error_msg:     *mut c_char,
        error_msg_len: usize,
    ) -> i32;
}
```

### 5.3 Safe Rust Wrappers

```rust
// crates/cudf-bridge/src/lib.rs

/// Owned handle to a table resident in GPU HBM.
/// Dropping this value frees the underlying GPU memory.
pub struct GpuTableHandle {
    ptr: NonNull<ffi::GpuTableHandleOpaque>,
}

impl Drop for GpuTableHandle {
    fn drop(&mut self) {
        unsafe { ffi::cudf_table_free(self.ptr.as_ptr()) }
    }
}

unsafe impl Send for GpuTableHandle {}
unsafe impl Sync for GpuTableHandle {}

/// Owned handle to a compiled GPU pipeline.
/// Reusable across multiple executions with different data.
pub struct PipelineHandle {
    ptr: NonNull<ffi::GpuPipelineHandleOpaque>,
}

impl Drop for PipelineHandle {
    fn drop(&mut self) {
        unsafe { ffi::cudf_pipeline_free(self.ptr.as_ptr()) }
    }
}

unsafe impl Send for PipelineHandle {}
unsafe impl Sync for PipelineHandle {}

impl GpuTableHandle {
    /// Load a Parquet file into GPU HBM.
    pub fn load_parquet(path: &str, columns: &[&str])
        -> Result<Self, CudfError>
    {
        let c_path = CString::new(path)?;
        let c_cols: Vec<CString> = columns.iter()
            .map(|s| CString::new(*s).unwrap())
            .collect();
        let ptrs: Vec<*const c_char> =
            c_cols.iter().map(|s| s.as_ptr()).collect();

        let ptr = unsafe {
            ffi::cudf_load_parquet(
                c_path.as_ptr(), ptrs.as_ptr(), columns.len() as i32,
            )
        };

        NonNull::new(ptr)
            .map(|ptr| Self { ptr })
            .ok_or_else(|| CudfError::LoadFailed(path.to_string()))
    }
}

/// Compile a Substrait plan into a reusable pipeline handle.
pub fn compile_pipeline(plan_bytes: &[u8])
    -> Result<PipelineHandle, CudfError>
{
    let mut error_buf = vec![0i8; 1024];

    let ptr = unsafe {
        ffi::cudf_compile_pipeline(
            plan_bytes.as_ptr(),
            plan_bytes.len(),
            error_buf.as_mut_ptr(),
            error_buf.len(),
        )
    };

    NonNull::new(ptr)
        .map(|ptr| PipelineHandle { ptr })
        .ok_or_else(|| {
            let msg = unsafe { CStr::from_ptr(error_buf.as_ptr()) }
                .to_string_lossy().to_string();
            CudfError::CompileFailed(msg)
        })
}

/// Execute a Substrait plan on the GPU (compile + execute in one call).
/// Returns a CPU-side Arrow RecordBatch containing the final result.
/// This is the only point at which data crosses the PCIe bus.
pub fn execute_substrait(
    plan_bytes: &[u8],
    tables: &HashMap<String, Arc<GpuTableHandle>>,
) -> Result<RecordBatch, CudfError> {

    let names: Vec<CString> =
        tables.keys().map(|k| CString::new(k.as_str()).unwrap()).collect();
    let name_ptrs: Vec<*const c_char> =
        names.iter().map(|s| s.as_ptr()).collect();
    let handle_ptrs: Vec<*mut ffi::GpuTableHandleOpaque> =
        tables.values().map(|h| h.ptr.as_ptr()).collect();

    let mut out_array  = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    let mut error_buf  = vec![0i8; 1024];

    let ret = unsafe {
        ffi::cudf_execute_substrait(
            plan_bytes.as_ptr(), plan_bytes.len(),
            name_ptrs.as_ptr(),
            handle_ptrs.as_ptr(),
            tables.len() as i32,
            &mut out_array,
            &mut out_schema,
            error_buf.as_mut_ptr(),
            error_buf.len(),
        )
    };

    if ret != 0 {
        let msg = unsafe { CStr::from_ptr(error_buf.as_ptr()) }
            .to_string_lossy().to_string();
        return Err(CudfError::ExecutionFailed(msg));
    }

    // Arrow C Data Interface → Rust RecordBatch
    //
    // CRITICAL: The exported FFI_ArrowArray's `release` callback MUST
    // be bound to the correct deallocator.  When the result originates
    // from GPU memory, the release callback calls cudaFree() or
    // rmm::mr::device_memory_resource::deallocate().  When the C++
    // engine has already copied the result to host memory, the release
    // callback calls the standard host deallocator.
    //
    // Rust takes ownership of the FFI_ArrowArray.  Once the RecordBatch
    // is dropped, the release callback fires, freeing the underlying
    // memory on the correct side (host or device).
    let array_data = unsafe { from_ffi(out_array, &out_schema) }?;
    let struct_array = StructArray::from(array_data);
    Ok(RecordBatch::from(&struct_array))
}
```

---

## 6. Layer 3 — C++ GPU Execution Engine (Sirius-inspired)

### 6.1 Engine Entry Point

```cpp
// cpp/src/cudf_engine.cpp

#include "cudf_engine.h"
#include "substrait_planner.h"
#include "gpu_pipeline.h"
#include "gpu_buffer_manager.h"
#include <substrait/plan.pb.h>

// Global singletons — analogous to Sirius's SiriusContext
static std::unique_ptr<GpuBufferManager> g_buffer_manager;
static std::unique_ptr<SubstraitPlanner> g_planner;

extern "C" {

bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes) {
    try {
        g_buffer_manager = std::make_unique<GpuBufferManager>(
            cache_bytes, proc_bytes);
        g_planner = std::make_unique<SubstraitPlanner>();
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

GpuPipelineHandle* cudf_compile_pipeline(
    const uint8_t* plan_bytes, size_t plan_len,
    char* error_msg, size_t error_msg_len)
{
    try {
        substrait::Plan plan;
        plan.ParseFromArray(plan_bytes, static_cast<int>(plan_len));

        // Build but do not execute the pipeline
        auto pipeline = g_planner->build_pipeline(plan);
        return pipeline.release();  // transfer ownership to caller

    } catch (const std::exception& e) {
        snprintf(error_msg, error_msg_len, "%s", e.what());
        return nullptr;
    }
}

int cudf_execute_pipeline(
    GpuPipelineHandle* pipeline,
    const char** table_names,
    GpuTableHandle** table_handles, int num_tables,
    ArrowArray* out_array, ArrowSchema* out_schema,
    char* error_msg, size_t error_msg_len)
{
    try {
        TableRegistry registry;
        for (int i = 0; i < num_tables; ++i)
            registry.register_table(table_names[i], table_handles[i]);

        // Bind data handles and execute
        auto result = pipeline->execute(registry);
        export_to_arrow(*result, out_array, out_schema);
        return 0;

    } catch (const std::exception& e) {
        snprintf(error_msg, error_msg_len, "%s", e.what());
        return -1;
    }
}

int cudf_execute_substrait(
    const uint8_t* plan_bytes, size_t plan_len,
    const char** table_names,
    GpuTableHandle** table_handles, int num_tables,
    ArrowArray* out_array, ArrowSchema* out_schema,
    char* error_msg, size_t error_msg_len)
{
    try {
        // 1. Deserialize the Substrait plan
        substrait::Plan plan;
        plan.ParseFromArray(plan_bytes, static_cast<int>(plan_len));

        // 2. Build a name → GPU data registry
        TableRegistry registry;
        for (int i = 0; i < num_tables; ++i)
            registry.register_table(table_names[i], table_handles[i]);

        // 3. Translate Substrait → GPU physical operator pipeline
        //    Mirrors Sirius's GPUPhysicalPlanGenerator
        auto pipeline = g_planner->build_pipeline(plan);

        // 4. Execute — data stays entirely in GPU HBM
        auto result = pipeline->execute(registry);

        // 5. Export only the final (small) result to the CPU
        export_to_arrow(*result, out_array, out_schema);
        return 0;

    } catch (const std::exception& e) {
        snprintf(error_msg, error_msg_len, "%s", e.what());
        return -1;
    }
}

} // extern "C"
```

### 6.2 SubstraitPlanner — Plan Translation

```cpp
// cpp/src/substrait_planner.cpp
// Mirrors Sirius's gpu_physical_plan_generator.cpp

class SubstraitPlanner {
public:
    std::unique_ptr<GpuPipeline> build_pipeline(
        const substrait::Plan& plan)
    {
        auto& root_rel = plan.relations(0).root().input();
        auto pipeline  = std::make_unique<GpuPipeline>();
        auto root_op   = build_operator(root_rel, *pipeline);
        pipeline->set_root(std::move(root_op));
        return pipeline;
    }

private:
    std::unique_ptr<GpuOperator> build_operator(
        const substrait::Rel& rel,
        GpuPipeline& pipeline)
    {
        switch (rel.rel_type_case()) {
            case substrait::Rel::kRead:
                return build_scan(rel.read());
            case substrait::Rel::kFilter:
                return build_filter(rel.filter(), pipeline);
            case substrait::Rel::kProject:
                return build_project(rel.project(), pipeline);
            case substrait::Rel::kJoin:
                return build_join(rel.join(), pipeline);
            case substrait::Rel::kAggregate:
                return build_aggregate(rel.aggregate(), pipeline);
            case substrait::Rel::kSort:
                return build_sort(rel.sort(), pipeline);
            case substrait::Rel::kFetch:   // LIMIT / TOP-N
                return build_limit(rel.fetch(), pipeline);
            default:
                throw std::runtime_error(
                    "Unsupported Substrait operator type: " +
                    std::to_string(rel.rel_type_case()));
        }
    }

    std::unique_ptr<GpuOperator> build_join(
        const substrait::JoinRel& join,
        GpuPipeline& pipeline)
    {
        auto left_op  = build_operator(join.left(),  pipeline);
        auto right_op = build_operator(join.right(), pipeline);

        auto [left_keys, right_keys] = extract_join_keys(join.expression());
        auto join_type = convert_join_type(join.type());

        // Choose join strategy based on data ordering hints:
        // - If both inputs are sorted on join keys → sort-merge join
        // - Otherwise → hash join
        if (has_sort_hint(join, left_keys, right_keys)) {
            return std::make_unique<GpuSortMergeJoinOp>(
                std::move(left_op), std::move(right_op),
                left_keys, right_keys, join_type);
        }

        return std::make_unique<GpuHashJoinOp>(
            std::move(left_op), std::move(right_op),
            left_keys, right_keys, join_type);
    }
};
```

### 6.3 GpuPipeline — Chunk-Based Bulk Execution

> **Design note (corrected from original):** The original design described a
> "push-based" pipeline in the DuckDB sense, where small vectors (2048 rows)
> flow between operators. This model is dangerous for GPUs — launching a CUDA
> kernel for only a few thousand rows means kernel launch overhead dominates
> computation time. Instead, the pipeline follows a **chunk-based bulk
> execution** model where each operator processes **millions of rows** (target:
> ~10M rows per chunk, occupying hundreds of MB to a few GB of HBM). This
> saturates the GPU's thousands of SM cores and HBM bandwidth.

```cpp
// cpp/src/gpu_pipeline.cpp
// Mirrors Sirius's gpu_pipeline.cpp + gpu_meta_pipeline.cpp

class GpuPipeline {
public:
    /// Execute the pipeline.
    ///
    /// For tables that fit in GPU HBM, all data is processed in one
    /// bulk pass.  For out-of-core tables, the scan operator streams
    /// chunks (each ~10M rows) through the pipeline, and a final
    /// merge step combines partial results.
    std::unique_ptr<cudf::table> execute(const TableRegistry& registry) {
        bind_tables(registry);

        if (requires_chunked_execution()) {
            return execute_chunked();
        }
        return root_op_->execute();
    }

private:
    std::unique_ptr<GpuOperator> root_op_;

    /// Chunked execution for out-of-core datasets.
    /// Each chunk is processed independently; partial results (e.g.
    /// partial aggregates) are merged at the end.
    std::unique_ptr<cudf::table> execute_chunked() {
        std::vector<std::unique_ptr<cudf::table>> partial_results;

        while (auto chunk = scan_next_chunk()) {
            auto partial = root_op_->execute_on_chunk(std::move(chunk));
            partial_results.push_back(std::move(partial));
        }

        return merge_partial_results(std::move(partial_results));
    }

    /// Split a long pipeline at breaker operators into sub-pipelines
    /// executed sequentially — the MetaPipeline concept from Sirius.
    ///
    /// Example split points:
    ///   MetaPipeline 1:  Scan → Filter → [Build Hash Table]   ← breaker
    ///   MetaPipeline 2:  Scan → Filter → Probe → [GroupBy]    ← breaker
    ///   MetaPipeline 3:  [Sort] → Limit → output to CPU
    void split_at_breakers() { /* ... */ }
};
```

---

## 7. Layer 4 — libcudf Operator Implementations

### 7.1 Hash Join (the most important operator)

According to Shanbhag et al., join is the operator that benefits most from the
GPU's ability to hide irregular memory-access latency. With a large hash table
that does not fit in the CPU's L3 cache, GPUs achieve ~10-14x speedup; for a
complete multi-join query the speedup exceeds 25x.

```cpp
// cpp/src/operators/gpu_join.cpp

class GpuHashJoinOp : public GpuOperator {
    std::vector<int> left_key_cols_;
    std::vector<int> right_key_cols_;
    cudf::join_kind  join_type_;

public:
    std::unique_ptr<cudf::table> execute() override {
        // Both children produce results that remain in GPU HBM
        auto left_table  = left_child_->execute();
        auto right_table = right_child_->execute();

        auto left_keys  = left_table->select(left_key_cols_);
        auto right_keys = right_table->select(right_key_cols_);

        // cudf::inner_join returns a pair of integer index columns —
        // still in GPU HBM, very compact.
        auto [left_indices, right_indices] =
            cudf::inner_join(left_keys, right_keys);

        // Late materialization: gather only after the index pair is ready.
        // This avoids producing wide intermediate tables.
        auto left_result  = cudf::gather(
            left_table->view(),  left_indices->view());
        auto right_result = cudf::gather(
            right_table->view(), right_indices->view());

        return merge_tables(std::move(left_result),
                            std::move(right_result));
    }
};
```

### 7.2 Sort-Merge Join (for pre-sorted / macro-ordered data)

When both inputs are already sorted on the join key (e.g. partition-aligned
tables), sort-merge join avoids building a hash table entirely. GPU sort is
extremely fast (~10ms for 100M int64 values), so even if micro-level reordering
is needed, the cost is negligible. The resulting merge pass achieves perfect
**coalesced memory access** — the GPU's ideal access pattern.

```cpp
// cpp/src/operators/gpu_join.cpp

class GpuSortMergeJoinOp : public GpuOperator {
    std::vector<int> left_key_cols_;
    std::vector<int> right_key_cols_;
    cudf::join_kind  join_type_;

public:
    std::unique_ptr<cudf::table> execute() override {
        auto left_table  = left_child_->execute();
        auto right_table = right_child_->execute();

        // Micro-sort each side if not already perfectly sorted.
        // GPU sort of 100M int64 values takes ~10ms — negligible.
        auto left_sorted  = ensure_sorted(left_table,  left_key_cols_);
        auto right_sorted = ensure_sorted(right_table, right_key_cols_);

        // cudf::merge produces a sorted merge of pre-sorted tables.
        // The merge pass is a pure sequential scan — ideal for GPU
        // coalesced memory access patterns.
        return cudf::merge(
            {left_sorted->view(), right_sorted->view()},
            left_key_cols_,
            std::vector<cudf::order>(left_key_cols_.size(),
                                     cudf::order::ASCENDING),
            std::vector<cudf::null_order>(left_key_cols_.size(),
                                          cudf::null_order::AFTER)
        );
    }
};
```

### 7.3 Filter (Predicate Pushdown)

```cpp
// cpp/src/operators/gpu_filter.cpp

class GpuFilterOp : public GpuOperator {
    SubstraitExprCompiler expr_compiler_;
    substrait::Expression filter_expr_;

public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        // Compile the Substrait expression into a boolean cudf::column.
        // Complex expressions use cudf::compute::transform internally.
        auto mask = expr_compiler_.eval(filter_expr_, input->view());

        // Vectorized filter entirely on the GPU
        return cudf::apply_boolean_mask(input->view(), mask->view());
    }
};
```

### 7.4 GroupBy Aggregation

Two strategies are available, selected by the optimizer based on data ordering:

```cpp
// cpp/src/operators/gpu_aggregate.cpp

class GpuAggregateOp : public GpuOperator {
    bool use_segmented_; // true when data is pre-sorted on group keys

public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        if (use_segmented_) {
            // Partition-wise aggregation: data is already sorted on group
            // keys (e.g. query GROUP BY date on date-partitioned data).
            // No hash table needed — use segmented scan / reduce-by-key.
            // GPU processes consecutive rows with identical keys via
            // shared-memory vectorized accumulation, nearly saturating
            // HBM bandwidth.
            return execute_segmented(input);
        }

        // Standard hash-based aggregation
        return execute_hash_based(input);
    }

private:
    std::unique_ptr<cudf::table> execute_hash_based(
        std::unique_ptr<cudf::table>& input)
    {
        cudf::groupby::groupby gb_obj(
            input->select(group_key_cols_));

        std::vector<cudf::groupby::aggregation_request> requests;
        for (auto& [col_idx, agg_kind] : agg_specs_) {
            cudf::groupby::aggregation_request req;
            req.values = input->get_column(col_idx).view();
            req.aggregations.push_back(
                make_gpu_aggregation(agg_kind));
            requests.push_back(std::move(req));
        }

        auto [result_keys, result_aggs] = gb_obj.aggregate(requests);
        return merge_tables(std::move(result_keys),
                            extract_results(std::move(result_aggs)));
    }

    std::unique_ptr<cudf::table> execute_segmented(
        std::unique_ptr<cudf::table>& input)
    {
        // When data is sorted on group keys, use cudf::groupby with a
        // pre-sorted hint (hash-free path).  This avoids hash table
        // construction entirely.
        //
        // The GPU scans contiguous rows with identical keys and
        // accumulates in shared memory — nearly full HBM bandwidth.
        cudf::groupby::groupby gb_obj(
            input->select(group_key_cols_),
            cudf::null_policy::EXCLUDE,
            cudf::sorted::YES  // hint: keys are already sorted
        );

        std::vector<cudf::groupby::aggregation_request> requests;
        for (auto& [col_idx, agg_kind] : agg_specs_) {
            cudf::groupby::aggregation_request req;
            req.values = input->get_column(col_idx).view();
            req.aggregations.push_back(
                make_gpu_aggregation(agg_kind));
            requests.push_back(std::move(req));
        }

        auto [result_keys, result_aggs] = gb_obj.aggregate(requests);
        return merge_tables(std::move(result_keys),
                            extract_results(std::move(result_aggs)));
    }
};
```

### 7.5 Sort

```cpp
// cpp/src/operators/gpu_sort.cpp

class GpuSortOp : public GpuOperator {
    std::vector<int>              sort_cols_;
    std::vector<cudf::order>      orders_;
    std::vector<cudf::null_order> null_orders_;

public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();
        auto keys  = input->select(sort_cols_);

        return cudf::sort_by_key(
            input->view(), keys, orders_, null_orders_);
    }
};
```

### 7.6 Rolling Window Functions

For time-series analytics (e.g. 7-day moving average), window functions depend
heavily on data's physical ordering. When data is partitioned by time, cross-batch
boundary data ("ghost zone" or "halo data") is minimal — only a small overlap
between adjacent batches is needed for correct sliding window computation.

```cpp
// cpp/src/operators/gpu_window.cpp

class GpuRollingWindowOp : public GpuOperator {
    int window_size_;
    cudf::rolling_aggregation agg_type_;

public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        // cudf::rolling_window computes the window function on the GPU.
        // Because the data is pre-sorted by the partition key (time),
        // the window slides over physically contiguous rows — no
        // random memory access, just sequential scans.
        auto result_col = cudf::rolling_window(
            input->get_column(value_col_).view(),
            window_size_,   // preceding
            0,              // following (for trailing window)
            1,              // min_periods
            agg_type_
        );

        // Replace the value column with the windowed result
        return replace_column(std::move(input), value_col_,
                              std::move(result_col));
    }
};
```

### 7.7 Dictionary Encoding for String Columns

String data (VARCHAR) is expensive on GPUs — variable-length data causes
irregular memory access and wastes HBM. The engine uses **dictionary encoding**
to map strings to compact `int32` indices at load time.

```cpp
// During Parquet loading, string columns are read as dictionary columns:
//   cudf::dictionary_column_view
//
// Benefits:
//   - Join and GroupBy operate on int32 indices instead of variable-length
//     strings → 5–10× faster for string-heavy workloads
//   - Dramatic HBM savings for low-cardinality columns
//   - Dictionary decode is deferred until the final result export
//
// The Parquet reader in libcudf supports this natively via:
//   cudf::io::parquet_reader_options::set_use_dictionary(true)
```

---

## 8. Layer 5 — RMM Memory Management & CUDA Streams

Mirrors the two-region design in Sirius's `gpu_buffer_manager.cpp`, extended
with CUDA stream concurrency and out-of-core spill support.

### 8.1 Memory Pools

```cpp
// cpp/src/gpu_buffer_manager.cpp

class GpuBufferManager {
    // ── Cache Region ──────────────────────────────────────────────
    // Stores raw table data loaded from disk or PCIe.
    // Pre-allocated at a fixed size to eliminate runtime fragmentation.
    using CachePool = rmm::mr::pool_memory_resource<
        rmm::mr::cuda_memory_resource>;
    std::shared_ptr<CachePool> cache_pool_;

    // ── Processing Region ─────────────────────────────────────────
    // Stores intermediate results: hash tables, sort buffers, etc.
    // Allocated from a pool; memory is returned after each query.
    using ProcPool = rmm::mr::pool_memory_resource<
        rmm::mr::cuda_memory_resource>;
    std::shared_ptr<ProcPool> proc_pool_;

    // ── Pinned Memory ─────────────────────────────────────────────
    // CPU↔GPU transfer staging area.
    // Used during cold-start data loading and out-of-core spill.
    std::shared_ptr<rmm::mr::pinned_memory_resource> pinned_pool_;

    // ── CUDA Streams ──────────────────────────────────────────────
    // Multiple streams for overlapping PCIe transfers with computation.
    rmm::cuda_stream compute_stream_;
    rmm::cuda_stream transfer_stream_;

public:
    GpuBufferManager(size_t cache_bytes, size_t proc_bytes) {
        auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();

        // Fixed-size pools: initial == maximum to prevent growth
        cache_pool_ = std::make_shared<CachePool>(
            cuda_mr.get(), cache_bytes, cache_bytes);

        proc_pool_ = std::make_shared<ProcPool>(
            cuda_mr.get(), proc_bytes, proc_bytes);

        pinned_pool_ = std::make_shared<
            rmm::mr::pinned_memory_resource>();

        // All libcudf operator allocations use the processing region
        rmm::mr::set_current_device_resource(proc_pool_.get());
    }

    rmm::device_buffer allocate_cache(size_t bytes) {
        return rmm::device_buffer(
            bytes, rmm::cuda_stream_default, cache_pool_.get());
    }

    // ── CUDA Stream Accessors ─────────────────────────────────────
    rmm::cuda_stream& compute_stream() { return compute_stream_; }
    rmm::cuda_stream& transfer_stream() { return transfer_stream_; }
};
```

### 8.2 CUDA Stream Concurrency

High-performance GPU databases must use multiple CUDA streams to overlap
PCIe transfers with GPU computation. This is essential for:

- **Chunked execution:** While Stream A computes a hash join on the current
  chunk, Stream B prefetches the next chunk from host memory or NVMe into
  GPU HBM via PCIe.
- **Result export:** While Stream A processes the next query, Stream B
  copies the previous query's result from GPU HBM back to host memory.

```
Timeline:
  Stream A (compute):  [── hash join chunk 1 ──][── hash join chunk 2 ──]
  Stream B (transfer):              [── load chunk 2 ──][── load chunk 3 ──]
                       ↑ overlapped — total time ≈ max(compute, transfer)
```

### 8.3 Out-of-Core Spill Strategy

When a dataset exceeds GPU HBM capacity, the system employs a streaming
chunk execution model rather than failing with OOM:

1. **Never load entire tables at once.** The scan operator streams fixed-size
   chunks (e.g. 10M rows / ~2 GB) through the pipeline.
2. **Spill to pinned host memory.** When the processing region runs out of
   HBM (e.g. during hash table construction for a large join), intermediate
   data spills to pinned host memory. This uses the `pinned_pool_` and CUDA
   unified memory (managed memory) as a fallback.
3. **Chunked hash join build.** For joins where the build side exceeds HBM,
   the hash table is built in chunks: each chunk's hash table partition is
   built on GPU, probed, and the partial results are accumulated before the
   next chunk is loaded. See Sirius issue #19 for the reference design.

---

## 9. Partition-Aware Optimization for Macro-Ordered Data

Data lakes typically organize data with directory-level partitioning (e.g.
`year=2024/month=01/`) and file-level sorting (Z-Order or coarse sort within
Parquet files). This section describes how the GPU engine exploits these
properties for dramatic speedups.

### 9.1 Zone Skipping via Partition Metadata

DataFusion's Parquet reader uses partition keys and file-level Min/Max statistics
to prune irrelevant files before any GPU execution begins. This is especially
effective for time-partitioned data:

```
Query: WHERE date BETWEEN '2024-01-01' AND '2024-03-31'
Data:  year=2024/month=01/  year=2024/month=02/  ... year=2024/month=12/

→ Only 3 out of 12 partitions are loaded into GPU HBM.
  The remaining 9 partitions are never touched.
```

For retained files, libcudf's native Parquet reader combined with GPUDirect
Storage (GDS) can bypass CPU memory entirely, reading column data directly
from NVMe into GPU HBM.

### 9.2 Shuffle Elimination via Partition-Key Alignment

When the query's GROUP BY or JOIN key matches the data's partition key,
the engine eliminates the expensive global shuffle (repartition) step:

```
Data partitioned by: date
Query: SELECT date, SUM(sales) FROM t GROUP BY date

→ DataFusion scheduler detects key alignment.
→ Removes the RepartitionExec node from the physical plan.
→ Each partition is aggregated independently on the GPU.
→ No cross-partition data movement needed.
```

### 9.3 Sort-Then-Compute Strategy

Data lake files are "macro-ordered" (sorted at the directory level) but may
be "micro-unordered" (unsorted within individual Parquet RowGroups due to
concurrent writes). The engine inserts a lightweight **local GPU sort** at the
pipeline entry point:

```
Cost:   GPU sort of 100M int64 values ≈ ~10ms
Benefit: All downstream operators can use sequential-scan algorithms:
  - GROUP BY → segmented reduce (no hash table)
  - DISTINCT → adjacent-duplicate removal (no hash set)
  - TOP-N    → partial sort + sequential merge (no heap)
```

This "sacrifice a trivial sort cost, gain sequential access for everything
downstream" trade-off is a golden rule of GPU database design.

### 9.4 Rolling Window with Ghost Zones

For sliding window functions on time-series data (e.g. 7-day moving average),
the engine only needs to maintain a small overlap ("ghost zone" or "halo data")
between adjacent chunks. Since data is partitioned by time, cross-chunk boundary
data is minimal:

```
Chunk N:    [..... data for days 1-30 .....]
Chunk N+1:  [days 24-30 (ghost)][... data for days 31-60 .....]
                 ↑ small overlap — just enough for the 7-day window
```

This enables GPU shared-memory-based sliding computation with near-zero
cross-chunk overhead.

---

## 10. End-to-End Data Flow for a Single SQL Query

The following trace walks through TPC-H Q5 (a five-table join with aggregation):

```
User:
  ctx.sql("SELECT n_name, SUM(revenue) AS revenue
           FROM customer JOIN orders JOIN lineitem
                JOIN supplier JOIN nation JOIN region
           WHERE r_name = 'ASIA' AND ...
           GROUP BY n_name ORDER BY revenue DESC")

Step 1 — DataFusion parses and optimizes the query
  Logical plan:  Sort → Aggregate → Filter → Join×5 → Scan×6

Step 2 — GpuPhysicalOptimizerRule inspects the physical plan
  Bottom-up replacement:
    ScanExec      → GpuScanExec        (reads from GPU HBM cache)
    FilterExec    → GpuFilterExec       (cudf::apply_boolean_mask)
    HashJoinExec  → GpuHashJoinExec     (cudf::inner_join)
    AggregateExec → GpuAggregateExec    (cudf::groupby)
    SortExec      → GpuSortExec         (cudf::sort_by_key)
  All operators are GPU variants → no CpuToGpu/GpuToCpu transitions needed.
  The optimizer collapses the entire tree into a single GpuExecNode.
  to_substrait_plan() → ~3 KB protobuf bytes

                                  ↓
                          [ FFI boundary ]
                    Only ~3 KB of plan bytes cross here.
                    Zero data moves across PCIe at this point.

Step 3 — C++ SubstraitPlanner builds the operator tree
  cudf_execute_substrait() receives the 3 KB plan.
  SubstraitPlanner traverses it and constructs:
    GpuSortOp
      └─ GpuAggregateOp
           └─ GpuHashJoinOp (×5, each referencing GPU HBM handles)
                └─ GpuFilterOp  (r_name = 'ASIA')
                     └─ GpuScanOp  (reads from cache region)

Step 4 — GpuPipeline executes (everything in GPU HBM)
  Pipeline split at breakers:
    MetaPipeline 1: Scan region → Filter → [Build 5 hash tables]
    MetaPipeline 2: Probe hash tables → [GroupBy n_name]
    MetaPipeline 3: [Sort by revenue DESC]
  Result: ~25 rows × 2 columns ≈ a few hundred bytes

Step 5 — Result transfer (the only PCIe transfer in the entire query)
  export_to_arrow(result) via Arrow C Data Interface
  ~hundreds of bytes cross PCIe back to the CPU

Step 6 — Rust receives the result
  from_ffi(out_array, out_schema) → RecordBatch
  Wrapped in a DataFusion stream → returned to the user

Total PCIe transfers during query execution: 1
(Compare to the coprocessor model: one transfer per operator boundary)
```

---

## 11. Core Interface Design (Code Level)

### Complete Usage Example

```rust
use datafusion_gpu::{GpuSessionContext, GpuTableHandle};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    // Initialize: 6 GB cache region + 4 GB processing region
    // Mirrors Sirius's  call gpu_buffer_init("6 GB", "4 GB")
    let ctx = GpuSessionContext::new(6.0, 4.0);

    // ── Cold start: load data into GPU HBM (PCIe transfer happens here) ──
    // NOTE: For datasets larger than GPU HBM, the engine will
    // automatically switch to chunked streaming execution.
    let orders   = GpuTableHandle::load_parquet("data/orders.parquet",   &[])?;
    let lineitem = GpuTableHandle::load_parquet("data/lineitem.parquet", &[])?;
    let part     = GpuTableHandle::load_parquet("data/part.parquet",     &[])?;
    let supplier = GpuTableHandle::load_parquet("data/supplier.parquet", &[])?;
    let nation   = GpuTableHandle::load_parquet("data/nation.parquet",   &[])?;
    let region   = GpuTableHandle::load_parquet("data/region.parquet",   &[])?;

    ctx.register_gpu_table("orders",   orders);
    ctx.register_gpu_table("lineitem", lineitem);
    ctx.register_gpu_table("part",     part);
    ctx.register_gpu_table("supplier", supplier);
    ctx.register_gpu_table("nation",   nation);
    ctx.register_gpu_table("region",   region);

    // ── Hot queries: data is in GPU cache, no further PCIe transfers ──

    // TPC-H Q1
    let df = ctx.sql("
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity)       AS sum_qty,
               SUM(l_extendedprice)  AS sum_base_price,
               AVG(l_quantity)       AS avg_qty,
               COUNT(*)              AS count_order
        FROM lineitem
        WHERE l_shipdate <= DATE '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    ").await?;
    df.show().await?;

    // TPC-H Q5  (five-table join — data still in GPU cache)
    let df = ctx.sql("
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey  AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey  AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= DATE '1994-01-01'
          AND o_orderdate < DATE '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    ").await?;
    df.show().await?;

    Ok(())
}
```

### Registering Data Already in GPU Memory

If data arrives as a GPU buffer (e.g. from a CUDA ML pipeline), you can skip
the PCIe round-trip entirely:

```rust
// Suppose `raw_ptr` and `size` describe an existing GPU allocation
let handle = GpuTableHandle::from_raw_device_ptr(
    raw_ptr, size, schema.clone()
)?;
ctx.register_gpu_table("ml_features", handle);

// Now query it directly — zero additional data movement
let df = ctx.sql(
    "SELECT feature_id, AVG(score) FROM ml_features GROUP BY feature_id"
).await?;
```

### Hybrid CPU/GPU Execution Example

When a query contains an unsupported UDF, the optimizer automatically creates
a hybrid plan:

```rust
// Register a CPU-only UDF
ctx.register_udf(create_udf(
    "my_custom_fn",
    vec![DataType::Float64],
    Arc::new(DataType::Float64),
    Volatility::Immutable,
    make_scalar_function(my_custom_fn),
));

// The optimizer produces a hybrid plan:
//   GpuScanExec → GpuFilterExec → GpuToCpuExec → ProjectionExec(my_custom_fn)
// Only the UDF projection runs on CPU; everything else runs on GPU.
let df = ctx.sql("
    SELECT my_custom_fn(price) AS adjusted_price
    FROM orders
    WHERE o_orderdate > '2024-01-01'
").await?;
```

---

## 12. Build System

### build.rs — Auto-compile the C++ Engine

```rust
// crates/cudf-bridge/build.rs

fn main() {
    let cudf_prefix = std::env::var("LIBCUDF_ENV_PREFIX")
        .expect("Set LIBCUDF_ENV_PREFIX to the conda environment path");

    // Compile the C++ + CUDA GPU engine into a static library
    cc::Build::new()
        .cpp(true)
        .cuda(true)
        .flag("-std=c++17")
        .flag("-O3")
        .include(format!("{}/include", cudf_prefix))
        .include("../../cpp/include")
        .files(
            glob::glob("../../cpp/src/**/*.cpp")
                .unwrap()
                .map(|p| p.unwrap()),
        )
        .compile("cudf_engine");

    // Link against libcudf and its runtime dependencies
    println!("cargo:rustc-link-search={}/lib", cudf_prefix);
    println!("cargo:rustc-link-lib=dylib=cudf");
    println!("cargo:rustc-link-lib=dylib=rmm");
    println!("cargo:rustc-link-lib=dylib=cudart");
    println!("cargo:rustc-link-lib=dylib=nccl");

    // Generate Rust FFI bindings from the C header
    bindgen::Builder::default()
        .header("../../cpp/include/cudf_engine.h")
        .generate()
        .unwrap()
        .write_to_file("src/bindings.rs")
        .unwrap();
}
```

### Environment Requirements

```
Software requirements (identical to Sirius):
  Ubuntu >= 20.04
  NVIDIA GPU with compute capability >= 7.0  (Volta or later)
  CUDA >= 11.2
  libcudf  — install via:  conda install -c rapidsai libcudf
  CMake >= 3.30  (C++ components)
  Rust >= 1.75

Setup:
  conda activate libcudf-env
  export LIBCUDF_ENV_PREFIX=$(conda info --base)/envs/libcudf-env
  cargo build --release

What build.rs does automatically:
  1. Compiles  cpp/**/*.cpp  →  libcudf_engine.a  (via cc-rs)
  2. Generates  src/bindings.rs  from cudf_engine.h  (via bindgen)
  3. Links: libcudf_engine.a + libcudf + librmm + libcudart
```

---

## 13. Performance Design Decisions

### Decision 1: Chunk-Based Bulk Execution (not fine-grained push-based)

> **Corrected from original design.** The original proposal referenced DuckDB's
> push-based pipeline with small vectors (2048 rows). This model is unsuitable
> for GPUs — CUDA kernel launch overhead (~5-10 us) would dominate computation
> time for small batches.

The engine uses a **chunk-based bulk execution** model:

- Each operator processes **~10M rows per chunk** (hundreds of MB to a few GB)
- This fully saturates the GPU's SM cores and HBM bandwidth
- Kernel launches are amortized over millions of rows
- For out-of-core data, chunks stream through the pipeline with partial result
  merging at the end

### Decision 2: Late Materialization for Joins

Following the analysis in Shanbhag et al., joins first produce compact integer
index pairs, then gather the actual column data in a single pass. This avoids
materializing wide intermediate tables that would waste GPU HBM:

```
Probe phase   →   (left_row_indices[], right_row_indices[])
                    integers only — very small
                         ↓
                  cudf::gather()   ← single-pass column collection
                         ↓
                  Final joined table
```

### Decision 3: Two-Region Memory Separation (from Sirius)

| Region | Contents | Allocation Strategy |
|--------|----------|---------------------|
| **Cache Region** | Raw table data loaded from disk | Pre-allocated at a fixed size; never grows; no fragmentation |
| **Processing Region** | Hash tables, sort buffers, intermediate results | Pool-allocated; returned to the pool after each query |
| **Pinned Memory** | CPU↔GPU transfer staging + out-of-core spill | Used during cold-start data loading and spill-to-host |

The processing region is set as libcudf's default memory resource, so all
operator-internal allocations automatically draw from it.

### Decision 4: MetaPipeline Decomposition at Breaker Operators

A linear chain of operators is split at "pipeline breakers" — nodes that must
fully materialize their input before producing output:

```
MetaPipeline 1:  Scan → Filter → [ Build Join Hash Table ]  ← breaker
MetaPipeline 2:  Scan → Filter → Probe → [ GroupBy ]        ← breaker
MetaPipeline 3:  [ Sort ] → Limit → output to CPU
```

Each sub-pipeline runs to completion before the next starts, keeping peak GPU
memory usage bounded.

### Decision 5: Sort-Then-Compute for Macro-Ordered Data

For datasets with directory-level partitioning but potential micro-level
disorder within files, a lightweight local GPU sort is inserted at the
pipeline entry point. The cost (~10ms for 100M rows) is negligible; the
benefit is that all downstream operators (GROUP BY, DISTINCT, TOP-N) can
use pure sequential-scan algorithms instead of hash-based algorithms.

### Decision 6: Dictionary Encoding for String Columns

String columns are loaded as dictionary-encoded columns at Parquet read time.
All join and groupby operations operate on the compact `int32` dictionary
indices. Dictionary decoding is deferred until result export. This provides
5-10x speedup for string-heavy workloads and significant HBM savings.

### Decision 7: CUDA Stream Concurrency

Two CUDA streams overlap computation with data transfer:
- **Compute stream:** Runs GPU operators (joins, aggregations, sorts)
- **Transfer stream:** Prefetches the next data chunk or exports the current
  result while computation proceeds on the other stream

This hides PCIe latency during chunked execution and cold-start loading.

### Decision 8: Sub-Plan Pushdown (not All-or-Nothing)

Following Apache Spark RAPIDS and DataFusion Comet, the optimizer performs
bottom-up operator replacement instead of requiring the entire plan to be
GPU-compatible. This ensures that a single unsupported function does not
force the entire query back to CPU — only the unsupported node runs on CPU,
with automatic `CpuToGpuExec` / `GpuToCpuExec` transitions at boundaries.

---

## 14. Known Limitations & Mitigations

| Limitation | Root Cause | Mitigation |
|------------|-----------|------------|
| Row count < ~2 billion per chunk | libcudf uses `int32_t` row IDs | Chunked pipeline execution — splits tables into sub-2B-row chunks and merges results. DataFusion's multi-partition concurrency can also map partitions to parallel CUDA streams, bypassing the single-table limit. See Sirius issue #12. |
| Dataset must fit in GPU HBM (without chunking) | No automatic spill yet | Chunked streaming execution (Section 6.3) + spill to pinned host memory or NVMe. CUDA Unified Memory as a fallback for very large hash tables. See Sirius issue #19. |
| No WINDOW functions or complex UDFs on GPU | Not yet fully wrapped in the operator layer | Graceful fallback: the `GpuPhysicalOptimizerRule` leaves these nodes as CPU operators and injects `GpuToCpuExec` transition nodes. The query still benefits from GPU acceleration on all other operators. |
| Cold-start latency | PCIe data transfer on first load | GPUDirect Storage: read Parquet directly from NVMe into GPU HBM, bypassing CPU memory entirely. CUDA stream overlap hides remaining transfer latency. |
| Single GPU only | Current design scope | Multi-GPU extension using NCCL for data exchange. DataFusion partitions map naturally to multiple GPUs. See Sirius issue #18. |
| Variable-length string performance | GPU irregular memory access for VARCHAR | Dictionary encoding at load time; all operations on `int32` indices. Decode only at result export. |
| Data skew may cause OOM | Skewed partitions may overwhelm GPU HBM | The `GpuExecNode` detects OOM conditions and falls back to CPU execution for the affected partition. DataFusion Statistics metadata informs the optimizer about partition sizes. |

---

## 15. Development Roadmap

```
Phase 1 — Core skeleton  (~3 months)
  ✓ FFI bridge layer + stable C ABI definition
  ✓ Five basic operators: Scan, Filter, Project, Join, Aggregate
  ✓ Substrait planner (ported from Sirius's substrait planner)
  ✓ RMM two-region memory management
  ✓ TPC-H Q1 and Q6 running end-to-end

Phase 2 — SQL coverage + robustness  (~2 months)
  ○ Sort, TopN, Limit, CTE
  ○ Chunked streaming execution (breaks 2B-row limit; enables out-of-core)
  ○ Sub-plan pushdown with CpuToGpuExec / GpuToCpuExec transitions
  ○ Dictionary encoding for string columns
  ○ All 22 TPC-H queries passing correctness checks
  ○ Performance parity with Sirius (target: 8× over DuckDB on TPC-H SF=100)

Phase 3 — Partition-aware optimization  (~2 months)
  ○ Partition metadata integration with DataFusion's Statistics
  ○ Partition-wise aggregation (segmented reduce, hash-free GROUP BY)
  ○ Sort-merge join for pre-partitioned data
  ○ Sort-then-compute pipeline for macro-ordered datasets
  ○ Rolling window functions with ghost zone handling

Phase 4 — Production hardening  (~3 months)
  ○ GPUDirect Storage for zero-copy Parquet ingestion
  ○ Multi-GPU support with NCCL exchange operators
  ○ CUDA stream concurrency (compute/transfer overlap)
  ○ Compiled pipeline caching (cudf_compile_pipeline / cudf_execute_pipeline)
  ○ Full CPU fallback for every unsupported operator
  ○ Python bindings via PyO3
  ○ ClickBench end-to-end benchmark
```

---

## Appendix A: Changelog — Sources Consolidated

This document was produced by merging three design artifacts:

| Source | Key Contributions Incorporated |
|--------|-------------------------------|
| **Original architecture design** (Chinese) | 5-layer architecture, crate layout, Substrait-based plan passing, FFI bridge design, C++ engine structure, libcudf operator implementations (hash join, filter, groupby, sort), RMM two-region memory management, build system, end-to-end data flow, usage examples, development roadmap |
| **Partition-aware GPU optimization** (Chinese) | Zone skipping via partition metadata, shuffle elimination for partition-key-aligned queries, sort-merge join for pre-sorted data, partition-wise aggregation (segmented reduce), rolling window functions with ghost zones, sort-then-compute strategy, micro-batch pipeline with fixed GPU chunk size, GPUDirect Storage integration |
| **Expert review comments** (Chinese) | Sub-plan pushdown replacing all-or-nothing GPU planning, `CpuToGpuExec`/`GpuToCpuExec` boundary injection, chunk-based bulk execution model (correcting push-based description), out-of-core streaming execution for OOM prevention, Arrow FFI release callback safety requirements, dictionary encoding for string columns, compiled pipeline caching (two-phase compile/execute), CUDA stream concurrency for PCIe/compute overlap, multi-partition parallelism as 2B-row mitigation |

---

## References

- **Sirius** (CIDR '26) — Yogatama et al., *Rethinking Analytical Processing in the GPU Era*. [github.com/sirius-db/sirius](https://github.com/sirius-db/sirius)
- **Shanbhag et al.** (SIGMOD '20) — *A Study of the Fundamental Performance Characteristics of GPUs and CPUs for Database Analytics.* Crystal library, tile-based execution model.
- **datafusion-substrait** — [github.com/apache/datafusion](https://github.com/apache/datafusion)
- **DataFusion Comet** — Apache DataFusion-based Spark accelerator plugin. [github.com/apache/datafusion-comet](https://github.com/apache/datafusion-comet)
- **Apache Spark RAPIDS** — GPU acceleration plugin for Spark. [nvidia.github.io/spark-rapids](https://nvidia.github.io/spark-rapids/)
- **libcudf API reference** — [docs.rapids.ai/api/libcudf/stable](https://docs.rapids.ai/api/libcudf/stable/)
- **RMM** — [github.com/rapidsai/rmm](https://github.com/rapidsai/rmm)
- **Arrow C Data Interface** — [arrow.apache.org/docs/format/CDataInterface.html](https://arrow.apache.org/docs/format/CDataInterface.html)
- **Substrait** — [substrait.io](https://substrait.io)
