# LTSeq GPU Execution Engine — Detailed Design

> A sort-order-aware GPU execution backend for LTSeq, built on
> Apache DataFusion and libcudf (RAPIDS). The engine exploits
> sort-order metadata to select optimal algorithms — sequential
> operators for ordered data, hash-based operators for arbitrary data —
> and accelerates both paths on the GPU.
>
> This document consolidates insights from three sources:
> - The original layered architecture design (Sirius-inspired)
> - Partition-aware GPU optimization strategies for macro-ordered datasets
> - Expert review feedback on execution model, memory safety, and production hardening

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Integration Architecture](#2-integration-architecture)
3. [Sort-Order-Aware Optimizer](#3-sort-order-aware-optimizer)
4. [Layered Architecture](#4-layered-architecture)
5. [Crate Layout](#5-crate-layout)
6. [Layer 1 — DataFusion Frontend & Sort-Order Propagation](#6-layer-1--datafusion-frontend--sort-order-propagation)
7. [Layer 2 — FFI Bridge (Rust to C++)](#7-layer-2--ffi-bridge-rust-to-c)
8. [Layer 3 — C++ GPU Execution Engine](#8-layer-3--c-gpu-execution-engine)
9. [Layer 4 — LTSeq-Specific GPU Operators](#9-layer-4--ltseq-specific-gpu-operators)
10. [Layer 5 — RMM Memory Management & CUDA Streams](#10-layer-5--rmm-memory-management--cuda-streams)
11. [Partition-Aware Optimization for Macro-Ordered Data](#11-partition-aware-optimization-for-macro-ordered-data)
12. [End-to-End Data Flow](#12-end-to-end-data-flow)
13. [Build System](#13-build-system)
14. [Performance Design Decisions](#14-performance-design-decisions)
15. [Known Limitations & Mitigations](#15-known-limitations--mitigations)
16. [Development Roadmap](#16-development-roadmap)

---

## 1. System Overview

### Design Goals

| Goal | Description |
|------|-------------|
| **Ordered-First** | Exploit LTSeq's sort-order metadata (`sort_keys`) to select optimal algorithms: sequential operators when data is sorted, hash-based operators otherwise |
| **5x on Ordered Data** | Achieve 5x+ speedup over general-purpose engines on ordered-sequence workloads (consecutive grouping, sorted joins, windowing on pre-sorted data) |
| **0.95x on Arbitrary Data** | Match general-purpose engine performance on arbitrary (unsorted) data via DataFusion's standard hash-based operators |
| **GPU-Native** | Data lives in GPU HBM from load time through result output — never leaves the GPU during query execution |
| **Transparent Acceleration** | Users interact with the standard LTSeq Python API; GPU acceleration is automatic when hardware is available |
| **Graceful Degradation** | Unsupported operators automatically fall back to DataFusion's CPU execution path via hybrid CPU/GPU plans |

### Why Sort-Order Awareness Matters

General-purpose query engines treat data as unordered sets and default to hash-based algorithms for grouping, joining, and deduplication. When data is known to be sorted (a property LTSeq tracks via `sort_keys` / `sort_exprs`), fundamentally different — and faster — algorithms become available:

| Operation | Unordered Algorithm | Ordered Algorithm | Speedup Factor |
|-----------|-------------------|-------------------|----------------|
| GROUP BY | Hash table construction + probing | Segmented scan (sequential) | 2-5x |
| JOIN | Hash build + probe | Merge join (sequential scan) | 3-10x (L3-exceeding data) |
| DISTINCT | Hash set construction | Adjacent-duplicate removal | 2-4x |
| search_first | Linear scan O(n) | Binary search O(log n) | 100-1000x (large datasets) |
| group_ordered | Impossible without sort | Single-pass consecutive grouping | unique to LTSeq |
| Rolling window | Random access | Sequential scan | 2-5x |

GPU amplifies these advantages because:
- GPU sort is nearly free (~10ms for 100M int64) — the cost of establishing order is negligible
- Sequential/segmented operations achieve **coalesced memory access**, the GPU's ideal pattern, saturating HBM bandwidth
- Hash-based operations on GPU are also fast (GPU hides cache-miss latency), so the unordered path maintains parity

### Why Not GPU-as-Coprocessor?

Shanbhag et al. (SIGMOD '20) prove that the coprocessor pattern (transfer data per operator) is fundamentally flawed:

```
Given:  CPU memory bandwidth Bc  >  PCIe bandwidth Bp

Coprocessor query time:  Rg = data_size / Bp   <- PCIe is the bottleneck
Pure-CPU query time:     Rc = data_size / Bc

Since Bc > Bp  ->  Rc < Rg
-> A well-optimized CPU is faster than GPU-as-coprocessor
```

The correct approach: **load data into GPU HBM once, execute the entire query plan on the GPU, and transfer only the final (small) result back to the CPU**.

> **Important nuance:** The CPU still plays a critical system-level role as the **scheduler, flow controller, and fallback executor**. The CPU orchestrates which plan fragments run on the GPU, handles data loading, manages memory budgets, and executes operators that the GPU does not yet support.

---

## 2. Integration Architecture

GPU acceleration is **transparent** through the existing LTSeq pipeline. No separate GPU session or API is needed:

```
                      User Code (Python)
    t = LTSeq.read_csv("data.csv").sort("date")
    result = t.group_ordered(lambda r: r.is_up).filter(...)
                            |
                   Python Expression DSL
    lambda r: r.price > 100  ->  SchemaProxy captures  ->  PyExpr dict
                            |
                   Rust (LTSeqTable)
    dict_to_py_expr()  ->  pyexpr_to_datafusion()  ->  DataFusion Expr
                            |
                   DataFusion Execution
    Logical Plan  ->  Physical Plan  ->  Optimizer Pipeline
                            |
                            v
              +----------------------------------+
              | GpuPhysicalOptimizerRule          |
              | (extends existing HostToGpuRule)  |
              |                                  |
              | 1. Read sort_exprs metadata      |
              | 2. For each operator node:       |
              |    - sort_exprs match key?       |
              |      -> Ordered GPU variant      |
              |    - no sort match?              |
              |      -> Hash-based GPU variant   |
              |    - not GPU-eligible?           |
              |      -> Keep CPU operator        |
              | 3. Inject CpuToGpu/GpuToCpu      |
              |    transitions at boundaries     |
              +----------------------------------+
                            |
                   Execute (mixed CPU/GPU plan)
                            |
                   Result (Arrow RecordBatch)
```

This preserves the Python expression DSL as the primary interface. SQL support is available through DataFusion but is secondary. The user writes the same Python code regardless of whether GPU is present.

### Sort-Order Metadata Propagation

LTSeq's `sort_keys` (Python) maps to `sort_exprs` (Rust `LTSeqTable` field). This metadata propagates through operations:

```python
t = LTSeq.read_csv("data.csv")     # sort_keys = None
t = t.sort("date", "id")           # sort_keys = [("date", False), ("id", False)]
t = t.filter(lambda r: r.x > 0)    # sort_keys preserved (filter preserves order)
t = t.derive(y=lambda r: r.x + 1)  # sort_keys preserved (derive preserves order)
t = t.join(other, ...)              # sort_keys = None (join may reorder)
```

The optimizer reads this metadata when selecting GPU operator variants. For example, `t.sort("id").join(other.sort("id"), on=lambda a, b: a.id == b.id, strategy="merge")` triggers the merge join path because `sort_exprs` confirms both inputs are sorted on the join key.

---

## 3. Sort-Order-Aware Optimizer

The core innovation: the optimizer uses LTSeq's `sort_exprs` metadata to select between ordered and unordered GPU operator variants.

### Operator Selection Matrix

| LTSeq API Operation | sort_exprs Match? | GPU Operator | Algorithm |
|---------------------|-------------------|--------------|-----------|
| `.agg(by=...)` | Yes: sorted on group key | `GpuSegmentedAggregateExec` | `cudf::groupby` with `sorted::YES` hint |
| `.agg(by=...)` | No | `GpuHashAggregateExec` | `cudf::groupby` hash-based |
| `.join(on=...)` | N/A (hash join) | `GpuHashJoinExec` | `cudf::inner_join` |
| `.join(on=..., strategy="merge")` | Yes: both sides sorted on key | `GpuMergeJoinExec` | `cudf::merge` (sequential scan) |
| `.distinct(...)` | Yes: sorted on key | `GpuAdjacentDistinctExec` | Adjacent-duplicate removal |
| `.distinct(...)` | No | `GpuHashDistinctExec` | Hash set |
| `.search_first(...)` | Yes: sorted on predicate col | `GpuBinarySearchExec` | `cudf::lower_bound` O(log n) |
| `.search_first(...)` | No | `GpuLinearScanExec` | `cudf::apply_boolean_mask` + limit 1 |
| `.group_ordered(...)` | Yes (required) | `GpuConsecutiveGroupExec` | Parallel adjacent-key comparison |
| `.group_sorted(...)` | Yes (required) | `GpuSegmentedGroupExec` | Segmented reduce-by-key |
| `.shift()` / `.diff()` | Yes (required) | `GpuWindowExec` | `cudf::rolling_window` |
| `.rolling(n).agg()` | Yes (required) | `GpuRollingWindowExec` | `cudf::rolling_window` with ghost zones |
| `.cum_sum()` | Yes (required) | `GpuPrefixSumExec` | GPU prefix sum (parallel scan) |
| `.asof_join(...)` | Yes: both sorted on time | `GpuAsofJoinExec` | Parallel binary search |
| `.filter(...)` | N/A | `GpuFilterExec` | `cudf::apply_boolean_mask` |
| `.sort(...)` | N/A | `GpuSortExec` | `cudf::sort_by_key` |
| `.scan(...)` | N/A | **CPU only** | Sequential stateful scan (not parallelizable) |

### Extended HostToGpuRule

The existing `HostToGpuRule` (`src/gpu/optimizer.rs`) replaces `FilterExec` with `GpuFilterExec`. This is extended to a broader `GpuPhysicalOptimizerRule` that handles all operator types:

```rust
// crates/datafusion-gpu-core/src/optimizer_rule.rs  (extended from src/gpu/optimizer.rs)

pub struct GpuPhysicalOptimizerRule;

impl PhysicalOptimizerRule for GpuPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Bottom-up traversal: replace supported operators with GPU variants
        plan.transform_up(|node| self.try_replace(node))
            .map(|t| t.data)
    }

    fn name(&self) -> &str { "gpu_sort_aware_pushdown" }
    fn schema_check(&self) -> bool { true }
}

impl GpuPhysicalOptimizerRule {
    fn try_replace(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // 1. Check if this node is a GPU-eligible operator type
        // 2. Read sort_exprs from the LTSeqTable context
        // 3. Select ordered or unordered GPU variant based on sort match
        // 4. Check row count statistics (skip GPU for small datasets)
        // 5. If input is CPU but this node is GPU, inject CpuToGpuExec
        // ...
    }
}
```

### Transition Nodes

When GPU and CPU operators are mixed in the same plan, transition nodes handle data transfer at boundaries:

```rust
/// Transfers a RecordBatch stream from CPU memory to GPU HBM.
/// Inserted automatically when a GPU operator reads from a CPU operator.
pub struct CpuToGpuExec { ... }

/// Transfers GPU results back to CPU memory via Arrow C Data Interface.
/// Inserted automatically when a CPU operator reads from a GPU operator.
pub struct GpuToCpuExec { ... }
```

The optimizer minimizes transitions: adjacent GPU operators form contiguous GPU sub-plans with no intermediate CPU materialization.

---

## 4. Layered Architecture

```
+------------------------------------------------------------------+
|                     User Code (Python)                            |
|   t.sort("date").group_ordered(lambda r: r.is_up).filter(...)    |
+----------------------------+-------------------------------------+
                             |  LTSeq Python DSL (lambda -> PyExpr)
+----------------------------v-------------------------------------+
|                Layer 1 -- LTSeq Rust Frontend                    |
|   PyExpr -> DataFusion Expr -> Logical Plan -> Physical Plan     |
|   +----------------------------------------------------------+   |
|   |  GpuPhysicalOptimizerRule                                |   |
|   |  Bottom-up, sort-order-aware operator replacement:       |   |
|   |    FilterExec      -> GpuFilterExec                      |   |
|   |    AggregateExec   -> GpuSegmented / GpuHash AggExec     |   |
|   |    JoinExec        -> GpuMerge / GpuHash JoinExec        |   |
|   |  Injects CpuToGpuExec / GpuToCpuExec at boundaries      |   |
|   +----------------------------------------------------------+   |
|   For contiguous GPU subtrees: serialize to Substrait bytes      |
+----------------------------+-------------------------------------+
                             |  Substrait protobuf (~KB, plan only)
+----------------------------v-------------------------------------+
|                Layer 2 -- FFI Bridge  (Rust <-> C++)             |
|   cudf-bridge crate                                              |
|   +----------------------------------------------------------+   |
|   |  extern "C" {                                            |   |
|   |      fn cudf_compile_pipeline(plan) -> PipelineHandle    |   |
|   |      fn cudf_execute_pipeline(h, handles) -> ArrowFFI    |   |
|   |      fn cudf_load_parquet(path) -> GpuTableHandle        |   |
|   |  }                                                       |   |
|   +----------------------------------------------------------+   |
+----------------------------+-------------------------------------+
                             |  Stable C ABI
+----------------------------v-------------------------------------+
|                Layer 3 -- C++ GPU Execution Engine               |
|                (libcudf_engine.so)                                |
|   +--------------------+    +---------------------+              |
|   |  SubstraitPlanner  | -> |    GpuPipeline      |              |
|   |  (Substrait->ops)  |    |  (chunk-based bulk) |              |
|   +--------------------+    +----------+----------+              |
|                                        |                         |
|   +--------+--------+--------+--------+v-------+--------+       |
|   |  Scan  | Filter | MergeJ | HashJ  | SegAgg | Sort   |       |
|   +--------+--------+--------+--------+--------+--------+       |
|   | BinSrch| ConsGrp| AsoFJ  | Window | PfxSum | Dedup  |       |
|   +--------+--------+--------+--------+--------+--------+       |
+----------------------------+-------------------------------------+
                             |  libcudf C++ API
+----------------------------v-------------------------------------+
|                Layer 4 -- libcudf (RAPIDS)                       |
|   cudf::inner_join / merge / lower_bound / groupby               |
|   cudf::apply_boolean_mask / sort_by_key / rolling_window        |
|   All operations act on cudf::table_view in GPU HBM              |
+----------------------------+-------------------------------------+
                             |  CUDA / HBM
+----------------------------v-------------------------------------+
|                Layer 5 -- RMM Memory Management                  |
|   rmm::mr::pool_memory_resource                                  |
|   Three pools: Cache Region + Processing Region + Pinned Memory  |
|   Multi-stream concurrency for PCIe / compute overlap            |
+------------------------------------------------------------------+
```

---

## 5. Crate Layout

```
ltseq/
+-- Cargo.toml                          # workspace, gpu feature flag
|
+-- src/                                # Existing LTSeq Rust core
|   +-- lib.rs                          # LTSeqTable struct + #[pymethods]
|   +-- ops/                            # Operation implementations
|   +-- transpiler/                     # PyExpr -> DataFusion Expr
|   +-- engine.rs                       # Session config, GPU rule registration
|   +-- gpu/                            # GPU acceleration (feature-gated)
|       +-- mod.rs                      # CUDA kernel source (filter kernels)
|       +-- optimizer.rs                # HostToGpuRule -> GpuPhysicalOptimizerRule
|       +-- planner.rs                  # GpuQueryPlanner
|       +-- filter_exec.rs             # GpuFilterExec
|       +-- exec_node.rs               # GpuExecNode (Substrait leaf node)
|       +-- ffi.rs                      # FFI bindings to C++ engine
|       +-- substrait.rs               # Logical plan -> Substrait bytes
|       +-- raw_exec.rs                # Zero-copy GPU filter on device pointers
|       +-- ordered_ops.rs             # NEW: GPU operators for ordered data
|       |   +-- consecutive_group.rs   # GpuConsecutiveGroupExec
|       |   +-- binary_search.rs       # GpuBinarySearchExec
|       |   +-- merge_join.rs          # GpuMergeJoinExec
|       |   +-- asof_join.rs           # GpuAsofJoinExec
|       |   +-- segmented_agg.rs       # GpuSegmentedAggregateExec
|       |   +-- prefix_sum.rs          # GpuPrefixSumExec
|       |   +-- rolling_window.rs      # GpuRollingWindowExec
|       +-- hash_ops.rs                # NEW: GPU operators for unordered data
|           +-- hash_join.rs           # GpuHashJoinExec
|           +-- hash_agg.rs            # GpuHashAggregateExec
|           +-- hash_distinct.rs       # GpuHashDistinctExec
|
+-- cpp/                                # C++ GPU execution engine
|   +-- CMakeLists.txt
|   +-- include/
|   |   +-- cudf_engine.h               # Public C ABI header
|   |   +-- substrait_planner.h
|   |   +-- gpu_pipeline.h
|   |   +-- gpu_buffer_manager.h
|   |   +-- operators/
|   |       +-- gpu_scan.h
|   |       +-- gpu_filter.h
|   |       +-- gpu_hash_join.h
|   |       +-- gpu_merge_join.h         # Sort-merge join for ordered data
|   |       +-- gpu_asof_join.h          # As-of join for time series
|   |       +-- gpu_hash_aggregate.h
|   |       +-- gpu_segmented_aggregate.h # Segmented agg for sorted groups
|   |       +-- gpu_consecutive_group.h  # Consecutive grouping
|   |       +-- gpu_binary_search.h      # Binary search on sorted columns
|   |       +-- gpu_sort.h
|   |       +-- gpu_window.h
|   |       +-- gpu_prefix_sum.h         # Cumulative sum
|   +-- src/
|       +-- cudf_engine.cpp
|       +-- substrait_planner.cpp
|       +-- gpu_pipeline.cpp
|       +-- gpu_buffer_manager.cpp
|       +-- operators/
|           +-- (matching .cpp files)
|
+-- py-ltseq/                            # Existing Python package (unchanged)
    +-- ltseq/
        +-- core.py                      # LTSeq class
        +-- expr/                        # Expression DSL
        +-- grouping/                    # NestedTable
        +-- [mixins].py                  # IOMixin, TransformMixin, etc.
```

---

## 6. Layer 1 — DataFusion Frontend & Sort-Order Propagation

### 6.1 Existing Integration Point

GPU acceleration registers through the existing `build_session_state()` function in `src/engine.rs`. When the `gpu` feature is enabled and a CUDA device is available, the optimizer rule is appended to DataFusion's pipeline:

```rust
// src/engine.rs (existing code)

fn build_session_state(config: SessionConfig) -> SessionState {
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();

    #[cfg(feature = "gpu")]
    {
        let gpu_disabled = std::env::var("LTSEQ_DISABLE_GPU").is_ok();
        if !gpu_disabled && crate::gpu::is_gpu_available() {
            builder = builder
                .with_query_planner(Arc::new(GpuQueryPlanner))
                .with_physical_optimizer_rule(Arc::new(GpuPhysicalOptimizerRule));
        }
    }

    builder.build()
}
```

No separate `GpuSessionContext` is needed. The same `LTSeqTable` struct handles both CPU and GPU execution paths transparently.

### 6.2 Sort-Order Propagation Through the Pipeline

`LTSeqTable` carries `sort_exprs: Vec<String>` that tracks the current sort order. This field is populated by `sort()` and `assume_sorted()`, preserved by order-preserving operations (`filter`, `derive`, `select`, `slice`), and cleared by order-destroying operations (`join`, `agg`, `distinct`).

The optimizer reads `sort_exprs` to decide which GPU operator variant to use:

```rust
// Pseudocode for sort-order-aware operator selection

fn select_gpu_aggregate(
    input_sort_exprs: &[String],
    group_keys: &[String],
) -> GpuAggregateVariant {
    // Check if group keys are a prefix of the sort order
    if group_keys.iter().zip(input_sort_exprs).all(|(k, s)| k == s) {
        GpuAggregateVariant::Segmented  // no hash table needed
    } else {
        GpuAggregateVariant::HashBased  // standard hash aggregation
    }
}
```

### 6.3 GpuPhysicalOptimizerRule — Sort-Aware Sub-Plan Pushdown

> **Design rationale:** Following Apache Spark RAPIDS and DataFusion Comet,
> the optimizer performs **bottom-up operator replacement** instead of requiring
> the entire plan to be GPU-compatible. A single unsupported function does not
> force the entire query back to CPU.

The rule walks the physical plan tree bottom-up. For each node it:

1. Checks whether the operator type is GPU-supported
2. Reads sort-order metadata to select the optimal GPU variant
3. Checks data type compatibility (rejects nested types not supported by libcudf)
4. Checks row count statistics (skips GPU for datasets below threshold)
5. Injects `CpuToGpuExec` / `GpuToCpuExec` transitions at CPU/GPU boundaries

```rust
// src/gpu/optimizer.rs (extended)

impl GpuPhysicalOptimizerRule {
    fn try_replace(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // Pattern-match against known operator types and sort metadata:
        //
        //   FilterExec                            -> GpuFilterExec
        //   AggregateExec + sorted on group key   -> GpuSegmentedAggregateExec
        //   AggregateExec + unsorted              -> GpuHashAggregateExec
        //   SortMergeJoinExec + both sides sorted -> GpuMergeJoinExec
        //   HashJoinExec                          -> GpuHashJoinExec
        //   SortExec                              -> GpuSortExec
        //   ProjectionExec                        -> GpuProjectionExec
        //
        // LTSeq-specific operators (custom ExecutionPlan impls):
        //   ConsecutiveGroupExec                  -> GpuConsecutiveGroupExec
        //   BinarySearchExec                      -> GpuBinarySearchExec
        //   AsofJoinExec                          -> GpuAsofJoinExec
        //   RollingWindowExec                     -> GpuRollingWindowExec
        //   PrefixSumExec                         -> GpuPrefixSumExec
        //
        // If the input child is a CPU node, wrap it in CpuToGpuExec.
        // If this node stays on CPU but a child is GPU, inject GpuToCpuExec.
    }
}
```

### 6.4 GpuExecNode — Substrait-Based Full Subtree Execution

When the optimizer determines that a large contiguous subtree is fully GPU-compatible, it can collapse it into a single `GpuExecNode`. This node serializes the subtree to Substrait and sends it to the C++ engine in one shot (already implemented in `src/gpu/exec_node.rs`):

```rust
// src/gpu/exec_node.rs (existing code)

pub struct GpuExecNode {
    schema: SchemaRef,
    cpu_fallback: Arc<dyn ExecutionPlan>,
    pub substrait_bytes: Option<Vec<u8>>,
    pub parquet_paths: Vec<(String, String)>,
    cache: PlanProperties,
}

impl ExecutionPlan for GpuExecNode {
    fn execute(&self, partition: usize, context: Arc<TaskContext>)
        -> Result<SendableRecordBatchStream>
    {
        // Phase 4: route through C++ libcudf engine when available
        if let Some(ref bytes) = self.substrait_bytes {
            if crate::gpu::ffi::cudf_engine_available() {
                // Load Parquet into GPU HBM, execute Substrait plan
                // Return result via Arrow C Data Interface
                // ...
            }
        }

        // Fallback: CPU execution
        self.cpu_fallback.execute(0, context)
    }
}
```

---

## 7. Layer 2 — FFI Bridge (Rust to C++)

### 7.1 C ABI Header

The C++ engine exposes a stable C ABI. All symbols use plain C linkage for ABI stability:

```c
// cpp/include/cudf_engine.h

#ifdef __cplusplus
extern "C" {
#endif

// -- Lifecycle --
bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes);
void cudf_engine_destroy();

// -- Data Loading (one-time; data stays in GPU HBM) --
GpuTableHandle* cudf_load_parquet(
    const char* path, const char** column_names, int num_columns);
GpuTableHandle* cudf_import_arrow(ArrowArray* array, ArrowSchema* schema);
void cudf_table_free(GpuTableHandle* handle);

// -- Execution (two-phase: compile + execute) --
GpuPipelineHandle* cudf_compile_pipeline(
    const uint8_t* plan_bytes, size_t plan_len,
    char* error_msg, size_t error_msg_len);
int cudf_execute_pipeline(
    GpuPipelineHandle* pipeline,
    const char** table_names, GpuTableHandle** table_handles, int num_tables,
    ArrowArray* out_array, ArrowSchema* out_schema,
    char* error_msg, size_t error_msg_len);
void cudf_pipeline_free(GpuPipelineHandle* pipeline);

// -- Convenience: compile + execute in one call --
int cudf_execute_substrait(
    const uint8_t* plan_bytes, size_t plan_len,
    const char** table_names, GpuTableHandle** table_handles, int num_tables,
    ArrowArray* out_array, ArrowSchema* out_schema,
    char* error_msg, size_t error_msg_len);

// -- Memory Diagnostics --
size_t cudf_cache_used_bytes();
size_t cudf_cache_total_bytes();
size_t cudf_proc_used_bytes();
size_t cudf_proc_total_bytes();

#ifdef __cplusplus
}
#endif
```

### 7.2 Rust FFI Bindings

Already implemented in `src/gpu/ffi.rs`. Key types:

```rust
// src/gpu/ffi.rs (existing code)

/// Owned handle to a table resident in GPU HBM.
/// Dropping this value calls cudf_table_free() and returns GPU memory.
pub struct GpuTableHandle {
    ptr: NonNull<GpuTableHandleOpaque>,
}

impl Drop for GpuTableHandle {
    fn drop(&mut self) {
        unsafe { cudf_table_free(self.ptr.as_ptr()) }
    }
}

/// Execute a Substrait plan on the GPU and return the result as a
/// CPU-side Arrow RecordBatch.  This is the only point where data
/// crosses the PCIe bus.
pub fn execute_substrait(
    plan_bytes: &[u8],
    tables: &[(&str, &GpuTableHandle)],
) -> Result<RecordBatch> { ... }
```

Currently links against `cudf_engine_stub.c` which returns `false`/`-ENOSYS`. When the real C++ engine is built, `cudf_engine_available()` returns `true` and `GpuExecNode` routes to the GPU path.

---

## 8. Layer 3 — C++ GPU Execution Engine

### 8.1 SubstraitPlanner — Sort-Order-Aware Plan Translation

The SubstraitPlanner translates Substrait plans into GPU operator trees. It uses sort-order hints embedded in the Substrait plan (via DataFusion's `Ordering` properties) to select operator variants:

```cpp
// cpp/src/substrait_planner.cpp

class SubstraitPlanner {
public:
    std::unique_ptr<GpuPipeline> build_pipeline(const substrait::Plan& plan) {
        auto& root_rel = plan.relations(0).root().input();
        auto pipeline  = std::make_unique<GpuPipeline>();
        auto root_op   = build_operator(root_rel, *pipeline);
        pipeline->set_root(std::move(root_op));
        return pipeline;
    }

private:
    std::unique_ptr<GpuOperator> build_join(
        const substrait::JoinRel& join, GpuPipeline& pipeline)
    {
        auto left_op  = build_operator(join.left(),  pipeline);
        auto right_op = build_operator(join.right(), pipeline);
        auto [left_keys, right_keys] = extract_join_keys(join.expression());

        // Sort-order-aware join strategy selection:
        // If both inputs carry sort hints on join keys -> merge join
        // Otherwise -> hash join
        if (has_sort_hint(join, left_keys, right_keys)) {
            return std::make_unique<GpuSortMergeJoinOp>(
                std::move(left_op), std::move(right_op),
                left_keys, right_keys, convert_join_type(join.type()));
        }
        return std::make_unique<GpuHashJoinOp>(
            std::move(left_op), std::move(right_op),
            left_keys, right_keys, convert_join_type(join.type()));
    }

    std::unique_ptr<GpuOperator> build_aggregate(
        const substrait::AggregateRel& agg, GpuPipeline& pipeline)
    {
        auto child_op = build_operator(agg.input(), pipeline);
        auto group_keys = extract_group_keys(agg);

        // Sort-order-aware aggregation strategy:
        // If input is sorted on group keys -> segmented (no hash table)
        // Otherwise -> hash-based
        if (has_sort_hint_on_keys(agg, group_keys)) {
            return std::make_unique<GpuSegmentedAggregateOp>(
                std::move(child_op), group_keys, extract_agg_specs(agg));
        }
        return std::make_unique<GpuHashAggregateOp>(
            std::move(child_op), group_keys, extract_agg_specs(agg));
    }
};
```

### 8.2 GpuPipeline — Chunk-Based Bulk Execution

> **Design note:** Each operator processes **millions of rows** (target: ~10M rows
> per chunk). This saturates the GPU's SM cores and HBM bandwidth. Small-batch
> push-based models (2048 rows) are unsuitable for GPU — kernel launch overhead
> would dominate.

```cpp
// cpp/src/gpu_pipeline.cpp

class GpuPipeline {
public:
    std::unique_ptr<cudf::table> execute(const TableRegistry& registry) {
        bind_tables(registry);
        if (requires_chunked_execution()) {
            return execute_chunked();
        }
        return root_op_->execute();
    }

private:
    // Chunked execution for out-of-core datasets
    std::unique_ptr<cudf::table> execute_chunked() {
        std::vector<std::unique_ptr<cudf::table>> partial_results;
        while (auto chunk = scan_next_chunk()) {
            auto partial = root_op_->execute_on_chunk(std::move(chunk));
            partial_results.push_back(std::move(partial));
        }
        return merge_partial_results(std::move(partial_results));
    }
};
```

---

## 9. Layer 4 — LTSeq-Specific GPU Operators

This section describes GPU implementations for operators that distinguish LTSeq from general-purpose engines. These are the operators that deliver the 5x+ ordered-data advantage.

### 9.1 Consecutive Grouping — `GpuConsecutiveGroupExec`

**Maps to:** `LTSeq.group_ordered(lambda r: r.key)`

Groups only consecutive equal values without reordering — a uniquely LTSeq operation with no SQL equivalent. On GPU, this becomes embarrassingly parallel:

```cpp
// cpp/src/operators/gpu_consecutive_group.cpp

class GpuConsecutiveGroupOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();
        auto key_col = input->get_column(key_col_idx_).view();

        // Step 1: Parallel adjacent-key comparison
        //   Each GPU thread i computes: boundary[i] = (key[i] != key[i-1])
        //   boundary[0] = 1 (first row is always a group start)
        auto boundary_mask = compute_adjacent_boundaries(key_col);

        // Step 2: Prefix sum on boundary mask -> group IDs
        //   group_id[i] = sum(boundary[0..i])
        //   This is a standard GPU parallel prefix sum (O(n) work, O(log n) span)
        auto group_ids = cudf::inclusive_scan(
            boundary_mask->view(), cudf::scan_aggregation::SUM);

        // Step 3: Return input table with group_id column appended
        //   Downstream NestedTable operations (first, last, filter, derive)
        //   use group_id for segmented operations
        return append_column(std::move(input), "__group_id__",
                             std::move(group_ids));
    }
};
```

**Performance:** The entire operation is O(n) with O(log n) span — fully parallelizable. No hash table construction. On GPU, processes 100M rows in ~5ms (dominated by prefix sum).

### 9.2 Binary Search — `GpuBinarySearchExec`

**Maps to:** `LTSeq.search_first(lambda r: r.price > 100)` on sorted data

When data is sorted on the predicate column, binary search is O(log n) instead of O(n):

```cpp
// cpp/src/operators/gpu_binary_search.cpp

class GpuBinarySearchOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();
        auto sorted_col = input->get_column(search_col_idx_).view();

        // cudf::lower_bound finds the first row where the value >= threshold
        // For a 100M-row table, this is ~27 comparisons instead of ~100M
        auto search_value = cudf::make_column_from_scalar(
            cudf::numeric_scalar<double>(threshold_), 1);

        auto position = cudf::lower_bound(
            cudf::table_view{{sorted_col}},
            cudf::table_view{{search_value->view()}},
            {cudf::order::ASCENDING},
            {cudf::null_order::AFTER});

        // Gather the single row at the found position
        return cudf::gather(input->view(), position->view());
    }
};
```

**Performance:** O(log n) per search. For batch searches (multiple values), GPU launches parallel binary searches — O(k log n) for k searches, all in parallel.

### 9.3 Sort-Merge Join — `GpuMergeJoinExec`

**Maps to:** `t1.sort("id").join(t2.sort("id"), on=lambda a, b: a.id == b.id, strategy="merge")`

When both inputs are already sorted on the join key, sort-merge join avoids hash table construction entirely. The merge pass achieves perfect coalesced memory access:

```cpp
// cpp/src/operators/gpu_merge_join.cpp

class GpuSortMergeJoinOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto left_table  = left_child_->execute();
        auto right_table = right_child_->execute();

        // Micro-sort if not perfectly sorted within chunks
        // GPU sort of 100M int64 values takes ~10ms -- negligible
        auto left_sorted  = ensure_sorted(left_table,  left_key_cols_);
        auto right_sorted = ensure_sorted(right_table, right_key_cols_);

        // cudf::merge produces a sorted merge of pre-sorted tables
        // The merge pass is a pure sequential scan -- ideal for GPU
        // coalesced memory access patterns
        return cudf::merge(
            {left_sorted->view(), right_sorted->view()},
            left_key_cols_,
            std::vector<cudf::order>(left_key_cols_.size(),
                                     cudf::order::ASCENDING),
            std::vector<cudf::null_order>(left_key_cols_.size(),
                                          cudf::null_order::AFTER));
    }
};
```

**Performance:** On data that exceeds L3 cache (>30MB), merge join avoids the random-access pattern of hash probing. Speedup: 3-10x over hash join for large datasets.

### 9.4 As-of Join — `GpuAsofJoinExec`

**Maps to:** `trades.asof_join(quotes, on=lambda t, q: t.time >= q.time, direction="backward")`

The quintessential time-series operation. When both sides are sorted by time, each left row finds its nearest match in the right table via binary search:

```cpp
// cpp/src/operators/gpu_asof_join.cpp

class GpuAsofJoinOp : public GpuOperator {
    AsofDirection direction_;  // BACKWARD, FORWARD, NEAREST

public:
    std::unique_ptr<cudf::table> execute() override {
        auto left_table  = left_child_->execute();
        auto right_table = right_child_->execute();

        auto left_time  = left_table->get_column(time_col_left_).view();
        auto right_time = right_table->get_column(time_col_right_).view();

        // For each left row, find the nearest right row:
        //   BACKWARD: largest right.time <= left.time  (upper_bound - 1)
        //   FORWARD:  smallest right.time >= left.time (lower_bound)
        //   NEAREST:  min(|left.time - right.time|)
        auto match_indices = compute_asof_indices(
            left_time, right_time, direction_);

        // Gather matched right rows
        auto right_matched = cudf::gather(
            right_table->view(), match_indices->view());

        return merge_tables(std::move(left_table),
                            std::move(right_matched));
    }
};
```

**Performance:** Each left row's binary search is independent — GPU launches all searches in parallel. For N left rows and M right rows: O(N log M) total work, O(log M) span.

### 9.5 Segmented Aggregation — `GpuSegmentedAggregateExec`

**Maps to:** `t.sort("region").agg(by=lambda r: r.region, total=lambda g: g.sales.sum())`

When data is sorted on the group key, aggregation uses segmented scan instead of hash table construction:

```cpp
// cpp/src/operators/gpu_segmented_aggregate.cpp

class GpuSegmentedAggregateOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        // cudf::groupby with sorted::YES hint tells libcudf to skip
        // hash table construction.  The GPU scans contiguous rows with
        // identical keys and accumulates in shared memory.
        cudf::groupby::groupby gb_obj(
            input->select(group_key_cols_),
            cudf::null_policy::EXCLUDE,
            cudf::sorted::YES  // key hint: data is already sorted
        );

        std::vector<cudf::groupby::aggregation_request> requests;
        for (auto& [col_idx, agg_kind] : agg_specs_) {
            cudf::groupby::aggregation_request req;
            req.values = input->get_column(col_idx).view();
            req.aggregations.push_back(make_gpu_aggregation(agg_kind));
            requests.push_back(std::move(req));
        }

        auto [result_keys, result_aggs] = gb_obj.aggregate(requests);
        return merge_tables(std::move(result_keys),
                            extract_results(std::move(result_aggs)));
    }
};
```

**Performance:** No hash table construction or probing. The GPU processes consecutive same-key rows via shared-memory accumulation, nearly saturating HBM bandwidth. Speedup: 2-5x for high-cardinality groups.

### 9.6 Rolling Window — `GpuRollingWindowExec`

**Maps to:** `t.sort("date").derive(ma_5=lambda r: r.close.rolling(5).mean())`

Sliding window on sorted data is a sequential scan — GPU's ideal access pattern:

```cpp
// cpp/src/operators/gpu_rolling_window.cpp

class GpuRollingWindowOp : public GpuOperator {
    int window_size_;
    cudf::rolling_aggregation agg_type_;

public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        // cudf::rolling_window computes the window function on GPU.
        // Because data is pre-sorted, the window slides over physically
        // contiguous rows -- no random memory access.
        auto result_col = cudf::rolling_window(
            input->get_column(value_col_).view(),
            window_size_,   // preceding
            0,              // following (trailing window)
            1,              // min_periods
            agg_type_);

        return replace_column(std::move(input), value_col_,
                              std::move(result_col));
    }
};
```

For chunked execution, ghost zones (halo data) handle cross-chunk boundaries:

```
Chunk N:    [..... data for days 1-30 .....]
Chunk N+1:  [days 24-30 (ghost)][... data for days 31-60 .....]
                 ^ small overlap -- just enough for the window
```

### 9.7 Hash Join — `GpuHashJoinExec` (Unordered Path)

**Maps to:** `a.join(b, on=lambda a, b: a.id == b.id)` (no sort required)

The standard hash join for arbitrary data. According to Shanbhag et al., joins benefit most from GPU — 10-14x speedup for hash tables exceeding L3 cache:

```cpp
// cpp/src/operators/gpu_hash_join.cpp

class GpuHashJoinOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto left_table  = left_child_->execute();
        auto right_table = right_child_->execute();

        // cudf::inner_join returns compact integer index pairs (in GPU HBM)
        auto [left_indices, right_indices] = cudf::inner_join(
            left_table->select(left_key_cols_),
            right_table->select(right_key_cols_));

        // Late materialization: gather only after index pairs are ready
        // Avoids producing wide intermediate tables
        auto left_result  = cudf::gather(left_table->view(),  left_indices->view());
        auto right_result = cudf::gather(right_table->view(), right_indices->view());

        return merge_tables(std::move(left_result), std::move(right_result));
    }
};
```

### 9.8 Filter — `GpuFilterExec` (Existing)

Already implemented in `src/gpu/filter_exec.rs`. For simple `Column CMP Literal` predicates on numeric types (i32, i64, f32, f64), custom CUDA kernels execute the filter. For complex predicates, delegates to `cudf::apply_boolean_mask`.

### 9.9 Sort — `GpuSortExec`

```cpp
class GpuSortOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();
        return cudf::sort_by_key(input->view(),
            input->select(sort_cols_), orders_, null_orders_);
    }
};
```

GPU sort of 100M int64 values takes ~10ms. This near-zero cost enables the "sort-then-compute" strategy where a lightweight local sort at the pipeline entry enables all downstream operators to use sequential algorithms.

### 9.10 Prefix Sum — `GpuPrefixSumExec`

**Maps to:** `t.sort("date").cum_sum("volume")`

GPU parallel prefix sum (scan) computes cumulative sums in O(n) work with O(log n) span:

```cpp
class GpuPrefixSumOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        for (int col_idx : cum_cols_) {
            auto result_col = cudf::inclusive_scan(
                input->get_column(col_idx).view(),
                cudf::scan_aggregation::SUM);
            input = replace_column(std::move(input), col_idx,
                                   std::move(result_col));
        }
        return input;
    }
};
```

### 9.11 `scan()` — CPU-Only Limitation

**Maps to:** `t.sort("date").scan(lambda s, r: s * (1 + r.rate), init=1.0)`

LTSeq's `scan()` applies an arbitrary state-transition function sequentially. This is fundamentally not parallelizable when the function is opaque (each state depends on the previous).

**Design decision:** `scan()` always executes on CPU. The optimizer does not attempt GPU replacement. If users need GPU-accelerated cumulative operations, they should use `cum_sum()` (associative, parallelizable via prefix sum) or express the operation as a window function.

For specific associative operations (sum, product, min, max), the existing `parallel_scan.rs` implementation in the Rust layer uses Rayon for CPU parallelism via a prefix-sum decomposition.

### 9.12 Dictionary Encoding for String Columns

String data is expensive on GPUs due to variable-length irregular memory access. The engine uses dictionary encoding at load time:

```cpp
// During Parquet loading, string columns are read as dictionary columns:
//   cudf::dictionary_column_view
//
// Benefits:
//   - Join and GroupBy operate on int32 indices instead of variable-length
//     strings -> 5-10x faster for string-heavy workloads
//   - Dramatic HBM savings for low-cardinality columns
//   - Dictionary decode is deferred until result export
```

---

## 10. Layer 5 — RMM Memory Management & CUDA Streams

### 10.1 Two-Region Memory Design

Mirrors the Sirius `gpu_buffer_manager` design:

```cpp
// cpp/src/gpu_buffer_manager.cpp

class GpuBufferManager {
    // Cache Region: raw table data loaded from disk
    // Pre-allocated, fixed-size, no fragmentation
    std::shared_ptr<rmm::mr::pool_memory_resource<
        rmm::mr::cuda_memory_resource>> cache_pool_;

    // Processing Region: hash tables, sort buffers, intermediates
    // Pool-allocated; memory returned after each query
    std::shared_ptr<rmm::mr::pool_memory_resource<
        rmm::mr::cuda_memory_resource>> proc_pool_;

    // Pinned Memory: CPU<->GPU transfer staging
    std::shared_ptr<rmm::mr::pinned_memory_resource> pinned_pool_;

    // CUDA Streams for overlapping PCIe and computation
    rmm::cuda_stream compute_stream_;
    rmm::cuda_stream transfer_stream_;

public:
    GpuBufferManager(size_t cache_bytes, size_t proc_bytes) {
        auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
        cache_pool_ = std::make_shared<CachePool>(
            cuda_mr.get(), cache_bytes, cache_bytes);
        proc_pool_ = std::make_shared<ProcPool>(
            cuda_mr.get(), proc_bytes, proc_bytes);
        // All libcudf allocations use the processing region
        rmm::mr::set_current_device_resource(proc_pool_.get());
    }
};
```

| Region | Contents | Strategy |
|--------|----------|----------|
| **Cache** | Raw table data from disk | Fixed-size; no fragmentation |
| **Processing** | Hash tables, sort buffers, intermediates | Pool; returned after each query |
| **Pinned** | CPU<->GPU staging + out-of-core spill | Used during cold-start and spill |

### 10.2 CUDA Stream Concurrency

Two streams overlap computation with data transfer:

```
Timeline:
  Stream A (compute):  [-- hash join chunk 1 --][-- hash join chunk 2 --]
  Stream B (transfer):              [-- load chunk 2 --][-- load chunk 3 --]
                       ^ overlapped -- total time ~ max(compute, transfer)
```

### 10.3 Out-of-Core Spill Strategy

When datasets exceed GPU HBM:
1. Scan operator streams fixed-size chunks (~10M rows / ~2 GB) through the pipeline
2. When processing region exhausts HBM (e.g. hash table too large), spill to pinned host memory
3. For large join build sides, build hash table in chunks: each chunk's partition is built on GPU, probed, and partial results accumulated

---

## 11. Partition-Aware Optimization for Macro-Ordered Data

Data lakes organize data with directory-level partitioning (e.g. `year=2024/month=01/`) and file-level sorting. LTSeq exploits these properties.

### 11.1 Zone Skipping via Partition Metadata

DataFusion's Parquet reader prunes irrelevant files using partition keys and Min/Max statistics before GPU execution:

```
Query: t.filter(lambda r: r.date.between("2024-01-01", "2024-03-31"))
Data:  year=2024/month=01/  year=2024/month=02/  ... year=2024/month=12/

-> Only 3 out of 12 partitions loaded into GPU HBM
```

With GPUDirect Storage (GDS), libcudf reads Parquet directly from NVMe into GPU HBM, bypassing CPU memory entirely.

### 11.2 Shuffle Elimination via Partition-Key Alignment

When the grouping/join key matches the partition key, the engine eliminates global shuffle:

```
Data partitioned by: date
Query: t.sort("date").agg(by=lambda r: r.date, total=lambda g: g.sales.sum())

-> Each partition aggregated independently on GPU
-> No cross-partition data movement
```

### 11.3 Sort-Then-Compute Strategy

Data lake files are "macro-ordered" (sorted at directory level) but may be "micro-unordered" (unsorted within Parquet RowGroups). A lightweight local GPU sort at the pipeline entry enables sequential operators downstream:

```
Cost:   GPU sort of 100M int64 ~ 10ms
Benefit: All downstream operators use sequential-scan algorithms:
  - GROUP BY  -> segmented reduce (no hash table)
  - DISTINCT  -> adjacent-duplicate removal (no hash set)
  - TOP-N     -> partial sort + sequential merge (no heap)
```

This is triggered when LTSeq's `assume_sorted()` declares macro-order but the engine detects potential micro-disorder within file chunks.

### 11.4 Rolling Window with Ghost Zones

For sliding windows on time-partitioned data, cross-chunk boundary data is minimal:

```
Chunk N:    [..... data for days 1-30 .....]
Chunk N+1:  [days 24-30 (ghost)][... data for days 31-60 .....]
                 ^ small overlap -- just enough for the window
```

This maps to LTSeq's `.rolling()` and `.diff()` operations on pre-sorted data.

---

## 12. End-to-End Data Flow

### Example: Finding Consecutive Uptrend Intervals (LTSeq-Specific)

This is a workload that general-purpose engines cannot optimize well — it requires consecutive grouping (not hash-based GROUP BY) on ordered data:

```python
# User code (Python)
result = (
    LTSeq.read_csv("stock.csv")
    .sort("date")
    .derive(is_up=lambda r: r.price > r.price.shift(1))
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: (g.first().is_up == True) & (g.count() > 3))
    .derive(lambda g: {
        "start": g.first().date,
        "end": g.last().date,
        "gain": (g.last().price - g.first().price) / g.first().price,
    })
)
```

```
Step 1 -- Python captures expressions
  lambda r: r.price > r.price.shift(1)
  -> PyExpr dict: {type: "BinOp", op: ">", left: {type: "Column", name: "price"},
                   right: {type: "Call", func: "shift", args: [1],
                           on: {type: "Column", name: "price"}}}

Step 2 -- Rust transpiles to DataFusion
  pyexpr_to_datafusion() -> WindowFunction(LAG(col("price"), 1))
  sort_exprs = ["date"]  (from .sort("date"))

Step 3 -- GpuPhysicalOptimizerRule inspects the plan
  .sort("date")         -> GpuSortExec (or skip if already sorted)
  .derive(is_up=...)    -> GpuWindowExec (LAG on sorted data)
  .group_ordered(...)   -> GpuConsecutiveGroupExec (adjacent-key comparison)
  .filter(g.count()>3)  -> GpuSegmentedFilterExec (filter by group size)
  .derive(start/end)    -> GpuSegmentedFirstLastExec (first/last per group)

Step 4 -- GPU execution (everything in HBM)
  MetaPipeline 1: Scan -> GpuSortExec (if needed)
  MetaPipeline 2: GpuWindowExec (LAG) -> GpuConsecutiveGroupExec
  MetaPipeline 3: GpuSegmentedFilterExec -> GpuSegmentedFirstLastExec
  Result: ~50 intervals x 3 columns ~ hundreds of bytes

Step 5 -- Result transfer (only PCIe transfer in entire query)
  Arrow C Data Interface -> CPU RecordBatch -> Python list of dicts

Total PCIe transfers during query: 1 (result only)
```

### Example: Standard TPC-H Query (Arbitrary Data)

For standard SQL workloads on unordered data, LTSeq uses DataFusion's hash-based operators, achieving parity with general-purpose engines:

```python
# TPC-H Q1 equivalent
result = (
    LTSeq.read_parquet("lineitem.parquet")
    .filter(lambda r: r.l_shipdate <= "1998-09-02")
    .agg(
        by=lambda r: [r.l_returnflag, r.l_linestatus],
        sum_qty=lambda g: g.l_quantity.sum(),
        sum_base_price=lambda g: g.l_extendedprice.sum(),
        avg_qty=lambda g: g.l_quantity.avg(),
        count_order=lambda g: g.count(),
    )
    .sort("l_returnflag", "l_linestatus")
)
```

This follows the hash-based aggregation path (no sort-order advantage), but GPU acceleration still provides hardware-level speedup over CPU engines.

---

## 13. Build System

### build.rs

```rust
// build.rs (existing, extended for full C++ engine)

fn main() {
    #[cfg(feature = "gpu")]
    {
        // Currently compiles cudf_engine_stub.c
        // Phase 4: compile full cpp/**/*.cpp via cc-rs + CUDA
        cc::Build::new()
            .file("cpp/src/cudf_engine_stub.c")
            .compile("cudf_engine");

        // Phase 4 upgrade:
        // cc::Build::new()
        //     .cpp(true).cuda(true).flag("-std=c++17").flag("-O3")
        //     .include(format!("{}/include", cudf_prefix))
        //     .include("cpp/include")
        //     .files(glob::glob("cpp/src/**/*.cpp").unwrap().map(|p| p.unwrap()))
        //     .compile("cudf_engine");
        //
        // println!("cargo:rustc-link-lib=dylib=cudf");
        // println!("cargo:rustc-link-lib=dylib=rmm");
        // println!("cargo:rustc-link-lib=dylib=cudart");
    }
}
```

### Environment Requirements

```
Software requirements:
  Ubuntu >= 20.04
  NVIDIA GPU with compute capability >= 7.0  (Volta or later)
  CUDA >= 11.2
  libcudf  -- install via:  conda install -c rapidsai libcudf
  CMake >= 3.30  (C++ components)
  Rust >= 1.75

Current state (Phase 2/3 -- CUDA filter only):
  cargo build --release --features gpu
  # Links against cudf_engine_stub.c; GPU filter kernels compiled via cudarc

Phase 4 (full libcudf engine):
  conda activate libcudf-env
  export LIBCUDF_ENV_PREFIX=$(conda info --base)/envs/libcudf-env
  cargo build --release --features gpu
```

---

## 14. Performance Design Decisions

### Decision 1: Sort-Order-Aware Algorithm Selection

The defining architectural decision. Unlike general-purpose engines that always default to hash-based algorithms, LTSeq's optimizer reads `sort_exprs` metadata and selects:
- **Ordered variant** when data is known to be sorted on the relevant key
- **Hash-based variant** otherwise

This is a compile-time decision (at physical plan construction), not a runtime check. Zero overhead when not applicable.

### Decision 2: Chunk-Based Bulk Execution

Each operator processes ~10M rows per chunk, fully saturating GPU SM cores and HBM bandwidth. Kernel launches are amortized over millions of rows. For out-of-core data, chunks stream through the pipeline with partial result merging.

### Decision 3: Late Materialization for Joins

Joins produce compact integer index pairs first, then gather column data in a single pass. This avoids materializing wide intermediate tables that waste GPU HBM.

### Decision 4: Two-Region Memory Separation

Cache region (table data) and processing region (intermediates) are pre-allocated separately, preventing fragmentation and enabling predictable memory budgets.

### Decision 5: Sort-Then-Compute for Macro-Ordered Data

GPU sort is ~10ms for 100M rows. This negligible cost enables all downstream operators to use sequential algorithms, a net positive for any workload with 2+ ordered operations in the pipeline.

### Decision 6: Dictionary Encoding for Strings

String columns use dictionary encoding at Parquet load time. Join and GroupBy operate on `int32` indices. Decode deferred until result export. 5-10x speedup for string-heavy workloads.

### Decision 7: CUDA Stream Concurrency

Two CUDA streams overlap computation with data transfer, hiding PCIe latency during chunked execution and cold-start loading.

### Decision 8: Sub-Plan Pushdown (not All-or-Nothing)

Bottom-up operator replacement ensures a single unsupported function does not force the entire query to CPU. Only the unsupported node runs on CPU, with automatic transitions at boundaries.

### Decision 9: CPU Fallback for Non-Parallelizable Operations

`scan()` with arbitrary state functions is CPU-only. The optimizer does not attempt GPU replacement. This is an honest architectural boundary: LTSeq does not pretend everything runs on GPU.

---

## 15. Known Limitations & Mitigations

| Limitation | Root Cause | Mitigation |
|------------|-----------|------------|
| Row count < ~2 billion per chunk | libcudf uses `int32_t` row IDs | Chunked pipeline execution splits tables into sub-2B-row chunks |
| Dataset must fit in GPU HBM (without chunking) | No automatic spill yet | Chunked streaming + spill to pinned host memory or NVMe |
| `scan()` is CPU-only | Arbitrary state functions are sequential | Document limitation; users should use `cum_sum()` for associative operations |
| WINDOW functions on GPU not yet implemented | Scheduled for Phase 3 | Graceful fallback to CPU via `GpuToCpuExec` transition; see roadmap |
| Cold-start latency | PCIe data transfer on first load | GPUDirect Storage + CUDA stream overlap |
| Single GPU only | Current design scope | Multi-GPU via NCCL; DataFusion partitions map to GPUs |
| Variable-length string performance | GPU irregular memory access | Dictionary encoding at load time |
| Data skew may cause OOM | Skewed partitions overwhelm HBM | `GpuExecNode` detects OOM and falls back to CPU |

---

## 16. Development Roadmap

The roadmap is ordered to deliver LTSeq's ordered-data advantages first, then broaden GPU coverage.

```
Phase 1 -- Ordered-Data GPU Operators  (~3 months)
  Target: 5x on ordered workloads
  + GpuConsecutiveGroupExec (group_ordered)
  + GpuSegmentedAggregateExec (sorted GROUP BY)
  + GpuMergeJoinExec (join with strategy="merge" / merge join)
  + GpuBinarySearchExec (search_first on sorted data)
  + Sort-order metadata propagation through the optimizer
  + Benchmark: LTSeq consecutive-grouping benchmark vs DuckDB/Polars

Phase 2 -- Standard SQL GPU Operators  (~2 months)
  Target: 0.95x on arbitrary workloads
  + GpuHashJoinExec (standard hash join)
  + GpuHashAggregateExec (standard hash aggregation)
  + GpuSortExec (full sort)
  + GpuHashDistinctExec / GpuAdjacentDistinctExec
  + GpuFilterExec extensions (complex predicates via cudf::apply_boolean_mask)
  + TPC-H Q1, Q6 end-to-end
  + Dictionary encoding for string columns

Phase 3 -- Time-Series GPU Operators  (~2 months)
  Target: Complete ordered-data operator coverage
  + GpuAsofJoinExec (time-series as-of join)
  + GpuRollingWindowExec (rolling window with ghost zones)
  + GpuPrefixSumExec (cumulative sum / prefix scan)
  + GpuWindowExec (shift/diff/lag/lead)
  + Partition-aware optimization (zone skipping, shuffle elimination)
  + All 22 TPC-H queries passing

Phase 4 -- Production Hardening  (~3 months)
  + Full C++ libcudf engine replacing cudf_engine_stub
  + Chunked streaming execution (out-of-core)
  + GPUDirect Storage for zero-copy Parquet ingestion
  + Compiled pipeline caching (cudf_compile_pipeline / cudf_execute_pipeline)
  + CUDA stream concurrency (compute/transfer overlap)
  + Multi-GPU support with NCCL
  + ClickBench end-to-end benchmark
```

---

## References

- **Sirius** (CIDR '26) — Yogatama et al., *Rethinking Analytical Processing in the GPU Era*. [github.com/sirius-db/sirius](https://github.com/sirius-db/sirius)
- **Shanbhag et al.** (SIGMOD '20) — *A Study of the Fundamental Performance Characteristics of GPUs and CPUs for Database Analytics.*
- **datafusion-substrait** — [github.com/apache/datafusion](https://github.com/apache/datafusion)
- **DataFusion Comet** — Apache DataFusion-based Spark accelerator plugin. [github.com/apache/datafusion-comet](https://github.com/apache/datafusion-comet)
- **Apache Spark RAPIDS** — GPU acceleration plugin for Spark. [nvidia.github.io/spark-rapids](https://nvidia.github.io/spark-rapids/)
- **libcudf API reference** — [docs.rapids.ai/api/libcudf/stable](https://docs.rapids.ai/api/libcudf/stable/)
- **RMM** — [github.com/rapidsai/rmm](https://github.com/rapidsai/rmm)
- **Arrow C Data Interface** — [arrow.apache.org/docs/format/CDataInterface.html](https://arrow.apache.org/docs/format/CDataInterface.html)
- **Substrait** — [substrait.io](https://substrait.io)
