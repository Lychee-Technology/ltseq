# DataFusion × libcudf GPU 执行引擎详细设计方案

> 基于 Sirius 架构经验，以 `datafusion-substrait` 为胶水层，
> 在 DataFusion 中实现 GPU-Native 执行路径。

---

## 目录

1. [系统总览](#1-系统总览)
2. [分层架构](#2-分层架构)
3. [Crate 组织结构](#3-crate-组织结构)
4. [Layer 1：DataFusion 前端与 Substrait 导出](#4-layer-1datafusion-前端与-substrait-导出)
5. [Layer 2：FFI 桥接层（Rust ↔ C++）](#5-layer-2ffi-桥接层rust--c)
6. [Layer 3：C++ GPU 执行引擎（参考 Sirius）](#6-layer-3c-gpu-执行引擎参考-sirius)
7. [Layer 4：libcudf 算子实现](#7-layer-4libcudf-算子实现)
8. [Layer 5：RMM 内存管理](#8-layer-5rmm-内存管理)
9. [关键数据流：一条 SQL 的完整生命周期](#9-关键数据流一条-sql-的完整生命周期)
10. [核心接口设计（代码级）](#10-核心接口设计代码级)
11. [构建系统设计](#11-构建系统设计)
12. [性能设计决策](#12-性能设计决策)
13. [当前限制与缓解策略](#13-当前限制与缓解策略)
14. [开发路线图](#14-开发路线图)

---

## 1. 系统总览

### 设计目标

| 目标 | 说明 |
|------|------|
| **GPU-Native** | 数据从载入 GPU HBM 到输出结果，全程不离开 GPU |
| **零侵入** | 用户使用标准 DataFusion API，无需感知 GPU |
| **复用最大化** | DataFusion 负责 SQL 解析/优化，libcudf 负责 GPU 算子，不重复造轮子 |
| **渐进式降级** | 不支持的算子自动回退 DataFusion CPU 执行 |

### 为什么不用 GPU-as-Coprocessor？

Shanbhag et al. (SIGMOD'20) 的模型证明：

```
CPU 内存带宽 Bc > PCIe 带宽 Bp

协处理器模式延迟: Rg = data_size / Bp   ← PCIe 是瓶颈
纯 CPU 执行延迟:  Rc = data_size / Bc

因为 Bc > Bp，所以 Rc < Rg
→ 协处理器模式比优化后的纯 CPU 还慢（实测慢 1.4×）
```

正确的方式是：**数据一次性载入 GPU HBM，整个查询计划在 GPU 上执行完毕，再将最终（小）结果返回 CPU**。

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     用户代码 (Rust/Python)                       │
│           ctx.sql("SELECT ...").collect().await                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │  标准 DataFusion API
┌─────────────────────────▼───────────────────────────────────────┐
│               Layer 1: DataFusion 前端                           │
│   SQL Parser → Logical Plan → Optimizer → Physical Plan         │
│   ┌─────────────────────────────────────────────┐              │
│   │  GpuQueryPlanner (实现 QueryPlanner trait)  │              │
│   │  检测可 GPU 化的子树 → 包装为 GpuExecNode    │              │
│   └─────────────────────────────────────────────┘              │
│           datafusion-substrait: plan → Substrait bytes          │
└─────────────────────────┬───────────────────────────────────────┘
                          │  Substrait protobuf (仅计划，无数据)
┌─────────────────────────▼───────────────────────────────────────┐
│               Layer 2: FFI 桥接层 (Rust ↔ C++)                  │
│   cudf_bridge crate                                             │
│   ┌──────────────────────────────────────────────────────────┐ │
│   │  extern "C" {                                            │ │
│   │      fn cudf_execute(plan, data_handles) -> ArrowFFI     │ │
│   │      fn cudf_load_parquet(path) -> GpuTableHandle        │ │
│   │      fn cudf_buffer_init(cache_gb, proc_gb) -> bool      │ │
│   │  }                                                       │ │
│   └──────────────────────────────────────────────────────────┘ │
└─────────────────────────┬───────────────────────────────────────┘
                          │  C ABI (稳定接口)
┌─────────────────────────▼───────────────────────────────────────┐
│               Layer 3: C++ GPU 执行引擎 (libcudf_engine.so)      │
│   参考 Sirius: gpu_physical_plan_generator + gpu_executor        │
│   ┌──────────────────┐  ┌──────────────────┐                   │
│   │ SubstraitPlanner │→ │   GpuPipeline    │                   │
│   │ (Substrait→算子) │  │  (push-based)    │                   │
│   └──────────────────┘  └────────┬─────────┘                   │
│                                  │                              │
│   ┌──────┬──────┬──────┬─────────▼──────┬──────┐              │
│   │ Scan │Filter│ Join │    GroupBy     │ Agg  │  GPU Operators│
│   └──────┴──────┴──────┴────────────────┴──────┘              │
└─────────────────────────┬───────────────────────────────────────┘
                          │  libcudf C++ API
┌─────────────────────────▼───────────────────────────────────────┐
│               Layer 4: libcudf (RAPIDS)                          │
│   cudf::inner_join / left_join                                  │
│   cudf::groupby::groupby                                        │
│   cudf::apply_boolean_mask (filter)                             │
│   cudf::sort / cudf::sort_by_key                                │
│   全程操作 GPU HBM 上的 cudf::table_view                         │
└─────────────────────────┬───────────────────────────────────────┘
                          │  CUDA / HBM
┌─────────────────────────▼───────────────────────────────────────┐
│               Layer 5: RMM 内存管理                               │
│   rmm::mr::pool_memory_resource                                 │
│   两个内存池：Cache Region + Processing Region                   │
│   与 Sirius gpu_buffer_manager 设计相同                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Crate 组织结构

```
datafusion-gpu/
├── Cargo.toml                    # workspace
│
├── crates/
│   ├── datafusion-gpu-core/      # 核心：GpuQueryPlanner, GpuExecNode
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── planner.rs        # GpuQueryPlanner
│   │   │   ├── exec_node.rs      # GpuExecNode (ExecutionPlan impl)
│   │   │   ├── gpu_session.rs    # GpuSessionContext (封装 SessionContext)
│   │   │   └── fallback.rs       # CPU 降级逻辑
│   │   └── Cargo.toml
│   │
│   ├── cudf-bridge/              # FFI 桥接层
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── ffi.rs            # extern "C" 声明
│   │   │   ├── table_handle.rs   # GpuTableHandle (GPU 数据句柄)
│   │   │   └── arrow_transfer.rs # Arrow C Data Interface 结果接收
│   │   ├── build.rs              # bindgen + cc 编译 C++ 库
│   │   └── Cargo.toml
│   │
│   └── datafusion-gpu-python/    # 可选：PyO3 Python 绑定
│       └── ...
│
├── cpp/                          # C++ GPU 执行引擎
│   ├── CMakeLists.txt
│   ├── include/
│   │   ├── cudf_engine.h         # C ABI 公开头文件
│   │   ├── substrait_planner.h
│   │   ├── gpu_pipeline.h
│   │   ├── gpu_buffer_manager.h
│   │   └── operators/
│   │       ├── gpu_scan.h
│   │       ├── gpu_filter.h
│   │       ├── gpu_join.h
│   │       ├── gpu_aggregate.h
│   │       └── gpu_sort.h
│   └── src/
│       ├── cudf_engine.cpp       # C ABI 实现入口
│       ├── substrait_planner.cpp # Substrait → 物理算子树
│       ├── gpu_pipeline.cpp      # push-based 流水线
│       ├── gpu_buffer_manager.cpp# RMM 内存池管理
│       └── operators/
│           ├── gpu_scan.cpp
│           ├── gpu_filter.cpp    # cudf::apply_boolean_mask
│           ├── gpu_join.cpp      # cudf::inner_join / hash join
│           ├── gpu_aggregate.cpp # cudf::groupby
│           └── gpu_sort.cpp      # cudf::sort_by_key
│
└── examples/
    ├── tpch_benchmark.rs
    └── simple_query.rs
```

---

## 4. Layer 1：DataFusion 前端与 Substrait 导出

### 4.1 GpuSessionContext

用户入口，封装标准 `SessionContext` 并注入 GPU 查询规划器：

```rust
// crates/datafusion-gpu-core/src/gpu_session.rs
use datafusion::prelude::*;
use datafusion::execution::context::QueryPlanner;
use std::sync::Arc;

pub struct GpuSessionContext {
    inner: SessionContext,
}

impl GpuSessionContext {
    pub fn new(cache_gb: f64, proc_gb: f64) -> Self {
        // 初始化 GPU 内存池（调用 FFI）
        cudf_bridge::init_buffer(cache_gb, proc_gb)
            .expect("GPU buffer init failed");

        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);

        // 注入 GPU 查询规划器
        let state = ctx.state();
        let state = state.with_query_planner(Arc::new(GpuQueryPlanner::new()));
        let ctx = SessionContext::new_with_state(state);

        Self { inner: ctx }
    }

    // 代理所有 DataFusion API
    pub async fn sql(&self, sql: &str)
        -> datafusion::error::Result<DataFrame>
    {
        self.inner.sql(sql).await
    }

    /// 直接注册 GPU 上已有数据（避免 CPU→GPU 传输）
    pub fn register_gpu_table(&self, name: &str, handle: GpuTableHandle) {
        let provider = GpuTableProvider::new(handle);
        self.inner.register_table(name, Arc::new(provider)).unwrap();
    }
}
```

### 4.2 GpuQueryPlanner：识别可 GPU 化子树

```rust
// crates/datafusion-gpu-core/src/planner.rs
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use async_trait::async_trait;

pub struct GpuQueryPlanner;

#[async_trait]
impl QueryPlanner for GpuQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {

        // 1. 检查整棵计划是否可以全部在 GPU 上执行
        if is_fully_gpu_compatible(logical_plan) {
            // 2. 整体序列化为 Substrait，交给 GPU 引擎
            let substrait_bytes = to_substrait_plan(
                logical_plan,
                session_state,
            ).await?;

            return Ok(Arc::new(GpuExecNode::new(
                substrait_bytes,
                logical_plan.schema().clone(),
                GpuExecutionMode::FullGpu,
            )));
        }

        // 3. 部分 GPU 化：找出最大可 GPU 化子树
        // 对不支持的部分降级为 DataFusion CPU 执行
        let default_planner = DefaultPhysicalPlanner::default();
        default_planner.create_physical_plan(logical_plan, session_state).await
    }
}

/// 检查 LogicalPlan 中所有节点是否都被 GPU 引擎支持
fn is_fully_gpu_compatible(plan: &LogicalPlan) -> bool {
    plan.exists(|node| {
        matches!(node,
            LogicalPlan::Projection(_) |
            LogicalPlan::Filter(_)     |
            LogicalPlan::Join(_)       |
            LogicalPlan::Aggregate(_)  |
            LogicalPlan::Sort(_)       |
            LogicalPlan::Limit(_)      |
            LogicalPlan::TableScan(_)
        )
    })
    // TODO: 排除 Window, UDF 等暂不支持的算子
}
```

### 4.3 GpuExecNode：DataFusion ExecutionPlan 实现

这是 Rust 侧最核心的结构，实现 `ExecutionPlan` trait，在 `execute()` 时通过 FFI 触发 GPU 执行：

```rust
// crates/datafusion-gpu-core/src/exec_node.rs
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream,
    DisplayAs, DisplayFormatType,
};
use datafusion::execution::TaskContext;
use futures::stream;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct GpuExecNode {
    /// 序列化后的 Substrait 计划
    substrait_bytes: Vec<u8>,
    /// 输出 schema（DataFusion 需要）
    schema: SchemaRef,
    /// GPU 表句柄映射（表名 → GPU 内存地址）
    table_handles: HashMap<String, GpuTableHandle>,
}

impl DisplayAs for GpuExecNode {
    fn fmt_as(
        &self, _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "GpuExecNode(libcudf)")
    }
}

impl ExecutionPlan for GpuExecNode {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    // GPU 执行是单分区输出（结果汇聚在一处）
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    // GpuExecNode 是叶子节点或完整子树，children 视情况
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {

        let substrait = self.substrait_bytes.clone();
        let handles = self.table_handles.clone();
        let schema = self.schema.clone();

        // 将 GPU 执行包装为 async stream
        // GPU 执行在独立线程中进行，避免阻塞 tokio 运行时
        let batch = tokio::task::spawn_blocking(move || {
            // ← 这里触发 FFI 调用 C++ GPU 引擎
            cudf_bridge::execute_substrait(&substrait, &handles)
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

        // 将单个 RecordBatch 包装成 Stream
        Ok(Box::pin(stream::once(async move { Ok(batch) })))
    }
}
```

---

## 5. Layer 2：FFI 桥接层（Rust ↔ C++）

### 5.1 C ABI 头文件设计（稳定接口）

```c
// cpp/include/cudf_engine.h
// 这是 Rust-C++ 边界，使用 C ABI 保证稳定性

#ifdef __cplusplus
extern "C" {
#endif

// ─── 生命周期管理 ────────────────────────────────────────
/// 初始化 RMM 内存池
/// cache_bytes: GPU 数据缓存区大小
/// proc_bytes:  GPU 中间计算区大小（hash table、中间结果等）
bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes);

/// 销毁所有 GPU 资源
void cudf_engine_destroy();

// ─── 数据加载（仅发生一次，之后数据驻留 GPU）──────────────
/// 从 Parquet 文件加载到 GPU HBM，返回不透明句柄
/// 句柄所有权交给调用方，需用 cudf_table_free 释放
GpuTableHandle* cudf_load_parquet(
    const char* path,
    const char** column_names,
    int num_columns
);

/// 从 Arrow C Data Interface 导入（CPU 侧 Arrow → GPU）
/// 注意：这条路径存在 PCIe 传输，尽量避免在热路径使用
GpuTableHandle* cudf_import_arrow(
    ArrowArray* array,
    ArrowSchema* schema
);

/// 释放 GPU 表句柄
void cudf_table_free(GpuTableHandle* handle);

// ─── 核心：执行 Substrait 计划 ────────────────────────────
/// 执行 Substrait 计划，返回结果通过 Arrow C Data Interface 传出
/// plan_bytes: Substrait Plan 的 protobuf 序列化字节
/// plan_len:   字节长度
/// table_names/table_handles: 计划中引用的表名与其 GPU 句柄
/// out_array/out_schema: Arrow C Data Interface 输出（结果在 CPU）
/// 返回 0 表示成功，负数表示错误码
int cudf_execute_substrait(
    const uint8_t* plan_bytes,
    size_t plan_len,
    const char** table_names,
    GpuTableHandle** table_handles,
    int num_tables,
    ArrowArray* out_array,
    ArrowSchema* out_schema,
    char* error_msg,
    size_t error_msg_len
);

// ─── 内存信息 ─────────────────────────────────────────────
size_t cudf_cache_used_bytes();
size_t cudf_cache_total_bytes();

#ifdef __cplusplus
}
#endif
```

### 5.2 Rust FFI 声明与安全封装

```rust
// crates/cudf-bridge/src/ffi.rs
use std::ffi::c_char;

// 不透明 C++ 对象
#[repr(C)]
pub struct GpuTableHandleOpaque {
    _private: [u8; 0],
}

// Arrow C Data Interface (与 arrow-rs 兼容)
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
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> *mut GpuTableHandleOpaque;

    pub fn cudf_table_free(handle: *mut GpuTableHandleOpaque);

    pub fn cudf_execute_substrait(
        plan_bytes: *const u8,
        plan_len: usize,
        table_names: *const *const c_char,
        table_handles: *const *mut GpuTableHandleOpaque,
        num_tables: i32,
        out_array: *mut FFI_ArrowArray,
        out_schema: *mut FFI_ArrowSchema,
        error_msg: *mut c_char,
        error_msg_len: usize,
    ) -> i32;
}
```

### 5.3 安全的 Rust 封装

```rust
// crates/cudf-bridge/src/lib.rs

/// GPU 表句柄 —— 代表驻留在 GPU HBM 中的一张表
pub struct GpuTableHandle {
    ptr: NonNull<ffi::GpuTableHandleOpaque>,
}

// 确保析构时释放 GPU 内存
impl Drop for GpuTableHandle {
    fn drop(&mut self) {
        unsafe { ffi::cudf_table_free(self.ptr.as_ptr()) }
    }
}

unsafe impl Send for GpuTableHandle {}
unsafe impl Sync for GpuTableHandle {}

impl GpuTableHandle {
    pub fn load_parquet(path: &str, columns: &[&str])
        -> Result<Self, CudfError>
    {
        let c_path = CString::new(path)?;
        let c_cols: Vec<CString> = columns.iter()
            .map(|s| CString::new(*s).unwrap())
            .collect();
        let c_col_ptrs: Vec<*const c_char> = c_cols.iter()
            .map(|s| s.as_ptr())
            .collect();

        let ptr = unsafe {
            ffi::cudf_load_parquet(
                c_path.as_ptr(),
                c_col_ptrs.as_ptr(),
                columns.len() as i32,
            )
        };

        NonNull::new(ptr)
            .map(|ptr| Self { ptr })
            .ok_or(CudfError::LoadFailed(path.to_string()))
    }
}

/// 执行 Substrait 计划，返回 Arrow RecordBatch（CPU 侧结果）
pub fn execute_substrait(
    plan_bytes: &[u8],
    tables: &HashMap<String, Arc<GpuTableHandle>>,
) -> Result<RecordBatch, CudfError> {

    let names: Vec<CString> = tables.keys()
        .map(|k| CString::new(k.as_str()).unwrap())
        .collect();
    let name_ptrs: Vec<*const c_char> = names.iter()
        .map(|s| s.as_ptr())
        .collect();
    let handle_ptrs: Vec<*mut ffi::GpuTableHandleOpaque> = tables.values()
        .map(|h| h.ptr.as_ptr())
        .collect();

    let mut out_array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    let mut error_buf = vec![0i8; 1024];

    let ret = unsafe {
        ffi::cudf_execute_substrait(
            plan_bytes.as_ptr(),
            plan_bytes.len(),
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
    // 注意：这里发生的是 GPU→CPU 的最终结果传输（只有一次）
    let array_data = unsafe { from_ffi(out_array, &out_schema) }?;
    let struct_array = StructArray::from(array_data);
    Ok(RecordBatch::from(&struct_array))
}
```

---

## 6. Layer 3：C++ GPU 执行引擎（参考 Sirius）

### 6.1 核心入口：cudf_engine.cpp

```cpp
// cpp/src/cudf_engine.cpp

#include "cudf_engine.h"
#include "substrait_planner.h"
#include "gpu_pipeline.h"
#include "gpu_buffer_manager.h"
#include <substrait/plan.pb.h>

// 全局单例（与 Sirius SiriusContext 对应）
static std::unique_ptr<GpuBufferManager> g_buffer_manager;
static std::unique_ptr<SubstraitPlanner> g_planner;

extern "C" {

bool cudf_engine_init(size_t cache_bytes, size_t proc_bytes) {
    try {
        // 初始化 RMM 内存池（与 Sirius gpu_buffer_manager 对应）
        g_buffer_manager = std::make_unique<GpuBufferManager>(
            cache_bytes, proc_bytes
        );
        g_planner = std::make_unique<SubstraitPlanner>();
        return true;
    } catch (const std::exception& e) {
        return false;
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
        // 1. 反序列化 Substrait Plan
        substrait::Plan plan;
        plan.ParseFromArray(plan_bytes, plan_len);

        // 2. 构建表名 → GPU 数据的映射
        TableRegistry registry;
        for (int i = 0; i < num_tables; ++i) {
            registry.register_table(table_names[i], table_handles[i]);
        }

        // 3. Substrait → GPU 物理算子树
        //    （与 Sirius GPUPhysicalPlanGenerator 对应）
        auto pipeline = g_planner->build_pipeline(plan, registry);

        // 4. 执行流水线（数据全程在 GPU HBM）
        auto result_table = pipeline->execute();

        // 5. 仅将最终结果通过 Arrow C Data Interface 传回 CPU
        //    result_table 通常很小（聚合后的结果）
        export_to_arrow(result_table, out_array, out_schema);
        return 0;

    } catch (const std::exception& e) {
        snprintf(error_msg, error_msg_len, "%s", e.what());
        return -1;
    }
}

} // extern "C"
```

### 6.2 SubstraitPlanner：计划转换

```cpp
// cpp/src/substrait_planner.cpp
// 对应 Sirius 的 gpu_physical_plan_generator.cpp

class SubstraitPlanner {
public:
    std::unique_ptr<GpuPipeline> build_pipeline(
        const substrait::Plan& plan,
        const TableRegistry& registry)
    {
        // Substrait plan 的根节点
        auto& root_rel = plan.relations(0).root().input();
        auto pipeline = std::make_unique<GpuPipeline>();

        // 递归构建算子链（push-based，与 Sirius/DuckDB 相同）
        auto root_op = build_operator(root_rel, registry, *pipeline);
        pipeline->set_root(std::move(root_op));
        return pipeline;
    }

private:
    std::unique_ptr<GpuOperator> build_operator(
        const substrait::Rel& rel,
        const TableRegistry& registry,
        GpuPipeline& pipeline)
    {
        switch (rel.rel_type_case()) {
            case substrait::Rel::kRead:
                return build_scan(rel.read(), registry);
            case substrait::Rel::kFilter:
                return build_filter(rel.filter(), registry, pipeline);
            case substrait::Rel::kProject:
                return build_project(rel.project(), registry, pipeline);
            case substrait::Rel::kJoin:
                return build_join(rel.join(), registry, pipeline);
            case substrait::Rel::kAggregate:
                return build_aggregate(rel.aggregate(), registry, pipeline);
            case substrait::Rel::kSort:
                return build_sort(rel.sort(), registry, pipeline);
            case substrait::Rel::kFetch:  // LIMIT
                return build_limit(rel.fetch(), registry, pipeline);
            default:
                throw std::runtime_error(
                    "Unsupported Substrait operator: " +
                    std::to_string(rel.rel_type_case())
                );
        }
    }

    std::unique_ptr<GpuOperator> build_join(
        const substrait::JoinRel& join,
        const TableRegistry& registry,
        GpuPipeline& pipeline)
    {
        auto left_op = build_operator(join.left(), registry, pipeline);
        auto right_op = build_operator(join.right(), registry, pipeline);

        // 解析 Join 条件（从 Substrait expression 提取 key 列索引）
        auto [left_keys, right_keys] = extract_join_keys(join.expression());

        auto join_type = convert_join_type(join.type());

        return std::make_unique<GpuHashJoinOp>(
            std::move(left_op), std::move(right_op),
            left_keys, right_keys, join_type
        );
    }
};
```

### 6.3 GpuPipeline：Push-based 流水线

```cpp
// cpp/src/gpu_pipeline.cpp
// 对应 Sirius 的 gpu_pipeline.cpp + gpu_meta_pipeline.cpp

class GpuPipeline {
public:
    // push-based 执行：executor 推送数据，保持算子无状态
    // 与 Sirius 的设计一致（区别于 Velox 的 pull-based）
    std::unique_ptr<cudf::table> execute() {
        // 根节点开始，递归拉取数据
        // pipeline breaker（join build phase, sort）处理批量数据
        return root_op_->execute();
    }

    // 将长流水线分割为多段（pipeline breakers: join, sort, groupby）
    // 对应 Sirius 的 MetaPipeline 概念
    void split_at_breakers() {
        // 识别需要物化的节点（join build side, sort, aggregation）
        // 分成多个子 pipeline 顺序执行
    }
};
```

---

## 7. Layer 4：libcudf 算子实现

### 7.1 Hash Join（最重要的算子）

参考 Shanbhag et al. 的分析，Join 是最能体现 GPU 优势的算子（大 hash table 时 GPU 延迟隐藏效果显著）：

```cpp
// cpp/src/operators/gpu_join.cpp

class GpuHashJoinOp : public GpuOperator {
    std::vector<int> left_key_cols_;
    std::vector<int> right_key_cols_;
    cudf::join_kind join_type_;

public:
    std::unique_ptr<cudf::table> execute() override {
        // 执行左右子树，数据全程在 GPU HBM
        auto left_table = left_child_->execute();
        auto right_table = right_child_->execute();

        // 提取 key 列视图
        auto left_keys = left_table->select(left_key_cols_);
        auto right_keys = right_table->select(right_key_cols_);

        // libcudf hash join
        // 关键：cudf::inner_join 返回的是行索引对，仍在 GPU 上
        auto [left_indices, right_indices] = cudf::inner_join(
            left_keys, right_keys
        );

        // 根据索引收集结果行（late materialization）
        auto left_result = cudf::gather(
            left_table->view(), left_indices->view()
        );
        auto right_result = cudf::gather(
            right_table->view(), right_indices->view()
        );

        // 合并左右结果列
        return merge_tables(std::move(left_result),
                           std::move(right_result));
    }
};
```

### 7.2 Filter（Predicate Pushdown）

```cpp
// cpp/src/operators/gpu_filter.cpp

class GpuFilterOp : public GpuOperator {
    SubstraitExprCompiler expr_compiler_;
    substrait::Expression filter_expr_;

public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        // 将 Substrait 表达式编译为 cudf::column（boolean mask）
        // 复杂表达式走 cudf::compute::transform
        auto mask = expr_compiler_.eval(filter_expr_, input->view());

        // cudf::apply_boolean_mask：GPU 上的向量化过滤
        return cudf::apply_boolean_mask(input->view(), mask->view());
    }
};
```

### 7.3 GroupBy Aggregation

```cpp
// cpp/src/operators/gpu_aggregate.cpp

class GpuAggregateOp : public GpuOperator {
public:
    std::unique_ptr<cudf::table> execute() override {
        auto input = child_->execute();

        // 构建 cudf groupby 请求
        cudf::groupby::groupby gb_obj(
            input->select(group_key_cols_)
        );

        std::vector<cudf::groupby::aggregation_request> requests;
        for (auto& [col_idx, agg_kind] : agg_specs_) {
            cudf::groupby::aggregation_request req;
            req.values = input->get_column(col_idx).view();
            req.aggregations.push_back(
                cudf::make_sum_aggregation<cudf::groupby_aggregation>()
            );
            requests.push_back(std::move(req));
        }

        auto [result_keys, result_aggs] = gb_obj.aggregate(requests);

        // 合并 keys 和聚合结果
        return merge_tables(std::move(result_keys),
                           extract_results(std::move(result_aggs)));
    }
};
```

---

## 8. Layer 5：RMM 内存管理

参考 Sirius 的双区内存管理设计：

```cpp
// cpp/src/gpu_buffer_manager.cpp

class GpuBufferManager {
    // 数据缓存区：存储从磁盘/PCIe 加载的原始表数据
    // 预分配，避免查询执行期间动态分配的开销
    std::shared_ptr<rmm::mr::pool_memory_resource<
        rmm::mr::cuda_memory_resource>> cache_pool_;

    // 计算区：存储中间结果（hash table, sort buffer, etc.）
    std::shared_ptr<rmm::mr::pool_memory_resource<
        rmm::mr::cuda_memory_resource>> proc_pool_;

    // Pinned Memory：CPU-GPU 传输通道（用于冷启动数据加载）
    std::shared_ptr<rmm::mr::pinned_memory_resource> pinned_pool_;

public:
    GpuBufferManager(size_t cache_bytes, size_t proc_bytes) {
        auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();

        cache_pool_ = std::make_shared<
            rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>
        >(cuda_mr.get(), cache_bytes, cache_bytes);  // 固定大小

        proc_pool_ = std::make_shared<
            rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>
        >(cuda_mr.get(), proc_bytes, proc_bytes);

        // libcudf 默认 MR 设为 proc_pool_（算子中间结果使用计算区）
        rmm::mr::set_current_device_resource(proc_pool_.get());
    }

    // 数据加载到缓存区（使用 cache_pool_）
    rmm::device_buffer allocate_cache(size_t bytes) {
        return rmm::device_buffer(bytes, rmm::cuda_stream_default,
                                  cache_pool_.get());
    }
};
```

---

## 9. 关键数据流：一条 SQL 的完整生命周期

```
用户: ctx.sql("SELECT o_year, SUM(revenue) FROM ... GROUP BY o_year")
                              │
              ┌───────────────▼───────────────┐
              │   DataFusion: SQL → LogicalPlan│
              │   (Projection, Aggregate,      │
              │    Join×3, Filter, Scan×4)     │
              └───────────────┬───────────────┘
                              │
              ┌───────────────▼───────────────┐
              │   GpuQueryPlanner:            │
              │   is_fully_gpu_compatible()?  │
              │   → YES                       │
              │   to_substrait_plan() → bytes │  ← 只传计划，不传数据
              └───────────────┬───────────────┘
                              │ ~2KB protobuf
              ┌───────────────▼───────────────┐  ← FFI 边界
              │   cudf_execute_substrait()    │
              │   C++ GPU 执行引擎            │
              │                               │
              │  SubstraitPlanner → Pipeline  │
              │                               │
              │  [Scan]  读 GPU HBM 缓存区    │  ← 数据已在 GPU 上
              │    ↓                          │
              │  [Filter] cudf boolean_mask   │  ← 全在 GPU HBM
              │    ↓                          │
              │  [Join×3] cudf inner_join     │  ← hash table 在计算区
              │    ↓                          │
              │  [GroupBy] cudf groupby       │  ← 同上
              │    ↓                          │
              │  result: ~365行小表           │
              └───────────────┬───────────────┘
                              │ Arrow C Data Interface
                              │ (~365行 × 2列 ≈ 数KB)  ← 唯一一次 PCIe 传输
              ┌───────────────▼───────────────┐
              │   Rust: from_ffi → RecordBatch│
              │   → DataFusion Stream         │
              └───────────────────────────────┘
```

**PCIe 传输次数：1次（仅最终结果），而非 Coprocessor 模式的每算子传输**

---

## 10. 核心接口设计（代码级）

### 10.1 完整使用示例

```rust
use datafusion_gpu::GpuSessionContext;
use datafusion_gpu::GpuTableHandle;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化：6GB 缓存 + 4GB 计算（类似 Sirius 的 gpu_buffer_init）
    let ctx = GpuSessionContext::new(6.0, 4.0);

    // 一次性将数据加载到 GPU HBM（冷启动，发生 PCIe 传输）
    let orders = GpuTableHandle::load_parquet("data/orders.parquet", &[])?;
    let lineitem = GpuTableHandle::load_parquet("data/lineitem.parquet", &[])?;
    let part = GpuTableHandle::load_parquet("data/part.parquet", &[])?;

    ctx.register_gpu_table("orders", orders);
    ctx.register_gpu_table("lineitem", lineitem);
    ctx.register_gpu_table("part", part);

    // 后续所有查询：数据已在 GPU 上，全程 GPU 执行
    // 第1次查询
    let df = ctx.sql("
        SELECT o_year, SUM(revenue) as revenue
        FROM orders JOIN lineitem ON o_orderkey = l_orderkey
        WHERE o_orderdate BETWEEN '1994-01-01' AND '1995-01-01'
        GROUP BY o_year
        ORDER BY o_year
    ").await?;
    df.show().await?;

    // 第2次查询（数据仍在 GPU 缓存，无 PCIe 传输）
    let df2 = ctx.sql("SELECT COUNT(*) FROM lineitem WHERE l_discount > 0.05").await?;
    df2.show().await?;

    Ok(())
}
```

### 10.2 build.rs：自动编译 C++ 引擎

```rust
// crates/cudf-bridge/build.rs

fn main() {
    let libcudf_prefix = std::env::var("LIBCUDF_ENV_PREFIX")
        .expect("Set LIBCUDF_ENV_PREFIX to conda env path");

    // 编译 C++ GPU 引擎
    cc::Build::new()
        .cpp(true)
        .cuda(true)
        .flag("-std=c++17")
        .flag("-O3")
        .include(format!("{}/include", libcudf_prefix))
        .include("../../cpp/include")
        .files(glob::glob("../../cpp/src/**/*.cpp").unwrap()
               .map(|p| p.unwrap()))
        .compile("cudf_engine");

    // 链接 libcudf 及其依赖
    println!("cargo:rustc-link-search={}/lib", libcudf_prefix);
    println!("cargo:rustc-link-lib=dylib=cudf");
    println!("cargo:rustc-link-lib=dylib=rmm");
    println!("cargo:rustc-link-lib=dylib=cudart");
    println!("cargo:rustc-link-lib=dylib=nccl");  // 分布式时需要

    // 生成 FFI bindings
    bindgen::Builder::default()
        .header("../../cpp/include/cudf_engine.h")
        .generate()
        .unwrap()
        .write_to_file("src/bindings.rs")
        .unwrap();
}
```

---

## 11. 构建系统设计

```
环境要求（与 Sirius 相同）:
  - Ubuntu >= 20.04
  - CUDA >= 11.2，compute capability >= 7.0 (Volta+)
  - libcudf（通过 conda: rapidsai::libcudf）
  - CMake >= 3.30（C++ 部分）
  - Rust >= 1.75

构建流程:
  1. conda activate libcudf-env
  2. export LIBCUDF_ENV_PREFIX=$(conda info --base)/envs/libcudf-env
  3. cargo build --release
     └── build.rs 自动:
         a. 调用 cc-rs 编译 cpp/ 下的 C++/CUDA 代码
         b. 生成 libcudf_engine.a
         c. 调用 bindgen 生成 Rust FFI bindings
         d. 链接 libcudf, librmm, libcudart

最终产物:
  target/release/libdatafusion_gpu.rlib  (Rust library)
  target/release/libcudf_engine.a        (C++ static lib, 内嵌)
```

---

## 12. 性能设计决策

### 决策 1：Push-based vs Pull-based

选择与 Sirius/DuckDB 相同的 **push-based** 模型：
- 算子无状态，executor 推送数据
- 减少函数调用栈深度
- 更适合 GPU 的批量处理模式

### 决策 2：Late Materialization

参考 Shanbhag et al. 的分析，Join 时先产生行索引对，
最后统一 gather，避免中间宽表膨胀占用过多 GPU HBM：

```
Join probe → (left_idx[], right_idx[])  // 只有整数索引，很小
              ↓
         cudf::gather()                  // 一次性收集需要的列
```

### 决策 3：内存区分离（来自 Sirius）

- **Cache Region（预分配）**：存原始数据，大小固定，避免碎片化
- **Proc Region（池化）**：算子中间结果，查询结束后归还
- **Pinned Memory**：CPU-GPU 数据搬运通道（只在冷启动时使用）

### 决策 4：两阶段执行（Pipeline Breaker 处理）

```
MetaPipeline 1: Scan → Filter → [Build Join Hash Table]  ← pipeline breaker
MetaPipeline 2: Scan → Filter → Probe → [GroupBy]        ← pipeline breaker
MetaPipeline 3: [Sort] → Limit → 输出
```

---

## 13. 当前限制与缓解策略

| 限制 | 原因 | 缓解方案 |
|------|------|----------|
| 行数 < 2B | libcudf 用 int32_t 作行 ID | 分批执行（chunked pipeline，Sirius issue #12 的解法）|
| 数据必须装入 GPU | 暂不支持 spill | 分批 + Pinned Memory 溢写，参考 Sirius issue #19 |
| 不支持 WINDOW/UDF | libcudf 暂未封装 | 降级到 DataFusion CPU 执行（graceful fallback）|
| 冷启动慢 | PCIe 数据搬运 | 利用 GPUDirect Storage 直接从 NVMe 加载到 GPU |
| 单 GPU | 当前设计 | 多 GPU 时用 NCCL 做 exchange，参考 Sirius issue #18 |

---

## 14. 开发路线图

```
Phase 1（核心骨架，~3个月）
  ✓ FFI 桥接层 + C ABI 接口定义
  ✓ 单节点 5 个基本算子：Scan, Filter, Project, Join, Aggregate
  ✓ Substrait 解析器（基于 Sirius substrait planner 移植）
  ✓ RMM 双区内存管理
  ✓ TPC-H Q1/Q6 可运行

Phase 2（SQL 覆盖完整化，~2个月）
  ○ Sort, TopN, Limit, CTE
  ○ 分批执行（突破 2B 行限制 & 超内存支持）
  ○ 所有 22 条 TPC-H 查询通过
  ○ 与 Sirius 性能对齐（目标 8x+ vs DuckDB）

Phase 3（生产化，~3个月）
  ○ GPUDirect Storage（Parquet 直接读到 GPU）
  ○ 多 GPU + NCCL exchange
  ○ 完整 CPU fallback 机制
  ○ Python 绑定（PyO3）
  ○ ClickBench 性能测试
```

---

## 参考资料

- **Sirius** (CIDR'26): GPU-Native SQL Engine, [github.com/sirius-db/sirius](https://github.com/sirius-db/sirius)
- **Shanbhag et al.** (SIGMOD'20): GPU/CPU 性能特性基础研究，Crystal 库
- **datafusion-substrait**: [github.com/apache/datafusion](https://github.com/apache/datafusion)
- **libcudf API**: [docs.rapids.ai/api/libcudf](https://docs.rapids.ai/api/libcudf/stable/)
- **RMM**: [github.com/rapidsai/rmm](https://github.com/rapidsai/rmm)
- **Arrow C Data Interface**: [arrow.apache.org/docs/format/CDataInterface.html](https://arrow.apache.org/docs/format/CDataInterface.html)
