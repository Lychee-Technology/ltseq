# ADR 0017：Python 边界采用 Arrow C Data Interface（取消 IPC 隧道）

- 状态：Accepted
- 决策日期：2026-09-19（issue #143）· 记录日期：2026-09-19

[English version](0017-arrow-c-data-interface-boundary.md)

## 背景

#143 之前，Rust 内核与 Python 之间的每一次数据跨越都经过一条 Arrow IPC 字节隧道。`to_arrow_ipc` 用 `StreamWriter` 把每个 collect 出来的 `RecordBatch` 编码进 `Vec<u8>`，再拷进 `PyBytes`，Python 侧用 `pa.ipc.open_stream(...).read_all()` 和 `pa.concat_tables` 解回来。`from_arrow` 是镜像：`pa.ipc.new_stream` → `bytes` → `Vec<Vec<u8>>` → `StreamReader`。cursor 对每个批次重复导出这一段，每条 IPC 流还各带一份 schema。数据模型并不需要这些：两侧本来都持有 Arrow buffer，而 pyarrow 多年前就实现了 [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) 及其 [PyCapsule 协议](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)（`__arrow_c_stream__`）。DataFusion 55 解析到的 Arrow 59.2 自带 `arrow-pyarrow` crate，对应的 pyo3 0.29 正是项目在用的版本。

issue 的评审意见为决策划定了两条边界："零拷贝"指 Rust/Python 之间的 buffer 交接，不承诺 `to_arrow()` 不再执行惰性计划；`Cursor` 的整流导出有自己的所有权语义，不在本次范围内。

## 决策

**数据以共享的 Arrow buffer 经 C Data Interface 跨越边界，永不以 IPC 字节形式传输。** 内核中不再有 IPC reader/writer；`py-ltseq/tests/test_arrow_boundary.py` 守卫源码，防止它们回来。

三种形态跨越边界（`src/arrow_ffi.rs`、`src/ops/io.rs`、`src/cursor.rs`）：

1. **导出·已收集（`to_arrow()`）。** `LTSeqTable.to_arrow_reader` 在 detach 下执行计划（ADR 0016），把批次包成 `RecordBatchReader`，以 `pyarrow.RecordBatchReader` 交给 pyarrow；Python 调 `read_all()`。reader 的 schema 取第一个批次的 schema（没有批次时才用逻辑 schema），使 nullability 与 metadata 与 pyarrow 逐批看到的一致。错误仍映射为 `RuntimeError`。未加载的表导出带自身 schema 的空 reader，`LTSeq().to_arrow()` 不再依赖 Python 侧产生无类型列的兜底逻辑。
2. **导出·惰性（`__arrow_c_stream__`）。** `LTSeqTable.__arrow_c_stream__` 在 detach 下运行 `execute_stream`，用 `DataFrameBatchReader` 包住 DataFusion 流，返回 `arrow_array_stream` capsule。语义：计划在调用时准备，规划错误立即抛出；批次由消费者在其 `get_next` 回调中拉取；每次调用重新执行计划，表本身不变；capsule（或导入它的消费者）持有执行流，丢弃即取消执行；`requested_schema` 接受但忽略（协议允许生产者按自己的 schema 返回）；执行错误经 C 接口报告，以消费者的 Arrow 错误类型（`pyarrow.ArrowInvalid`）抛出；DataFusion 内部的 panic 被捕获并作为错误报告，不会跨 C ABI 展开。导出方既不获取也不释放 GIL，执行期间解释器是否空闲取决于消费者（pyarrow 的 `read_all` / `read_next_batch` 会释放）。
3. **导入（`from_arrow`）。** `LTSeqTable.from_arrow` 接收 `PyArrowType<ArrowArrayStreamReader>`：参数提取阶段持 GIL 调用源对象的 `__arrow_c_stream__`；拉取流与构建 `MemTable` 在 detach 下运行（reader 是 `Send`；pyarrow 导出的流不需要 GIL，Python 实现的 reader 会自行重新获取）。任何实现该协议的对象都可接受（`pyarrow.Table`、`RecordBatch`、`RecordBatchReader`、polars、duckdb 等），其余由 Python 抛 `TypeError`。有 schema 但零批次的流变成一个空批次，使表保有真实计划与带类型的 schema，与 IPC 路径在 Python 侧合成空批次的效果一致。

cursor 的 `next_batch` 把拉到的 `RecordBatch` 以 `pyarrow.RecordBatch`（`PyArrowType<RecordBatch>`）返回，仍在 detach 下拉取、在 detach 段内取锁（ADR 0016）。导入与导出的 buffer 由接口的 release 回调跨边界引用计数，两个方向的结果都比其来源对象活得更久。

`Cargo.toml` 在 `parquet` 旁边固定 `arrow = "59.2.0"` 并开启 `pyarrow` feature，两者都必须与 DataFusion 的 Arrow 版本一致。

## 备选方案

- *保留 IPC，只借用 `PyBytes`（`Vec<Bound<PyBytes>>`）。* 少一次拷贝，别无改善；仍要编码、解析，并把内核耦合在 pyarrow 的 IPC reader 上。作为权宜之计被否决。
- *`to_arrow()` 用 `PyArrowType<Table>`。* `Table::try_new` 要求各批次 schema（含 metadata）完全相等，比它要取代的 `pa.concat_tables` 更严格；reader 交接保留了原有容忍度，且只需一次 FFI 调用而非每批一次。
- *让 `to_arrow()` 走惰性的 `__arrow_c_stream__`。* 只剩一个原语，但执行错误的类型会变（`RuntimeError` → `ArrowInvalid`），执行期间 GIL 是否释放也将取决于消费者。主导出 API 保留 collect 后导出的路径，遵守内核自己的 GIL 契约。
- *`__arrow_c_stream__` 先 collect 再导出。* 所有权更简单，但 duckdb、polars 等消费者失去流式读取，且与 `to_arrow()` 重复。

## 影响

- 任何 Python↔Rust 数据路径都不再序列化或解析，边界成本是每批一次指针交接。同一台机器上的实测（2680 万行、约 1 GB、107 个 chunk；`benchmarks/bench_arrow_boundary.py`，改动前后均为 release 构建）：

  | 路径 | 改动前 (s) | 改动后 (s) | 加速 | 改动前峰值 RSS 增量 (MB) | 改动后峰值 RSS 增量 (MB) |
  |---|---|---|---|---|---|
  | `from_arrow`（883 MB `pyarrow.Table`，107 个 chunk） | 1.435 | 0.032 | 45x | 2487 | 0 |
  | 已 collect 的表 `to_arrow()` | 0.719 | 0.011 | 65x | 1396 | 0 |
  | 惰性 Parquet 扫描 `to_arrow()` | 0.984 | 0.261 | 3.8x | 2218 | 1134 |
  | 已 collect 的表 `pa.table(t)`（`__arrow_c_stream__`） | 无 | 0.181 | 无 | 无 | 24 |
  | `scan_parquet` cursor 遍历全部批次 | 0.284 | 0.188 | 1.5x | 164 | 111 |

  已 collect 的路径现在只剩指针交接的成本。惰性 Parquet 导出剩余的增量是解码结果本身（返回表持有的约 883 MB Arrow buffer）加上解码工作区；两侧的 IPC 拷贝都已消失。

- `LTSeq` 作为生产者加入 Arrow 生态：`pa.table(t)`、`pa.RecordBatchReader.from_stream(t)`、`pl.from_arrow(t)`、`duckdb.sql("... FROM t")` 无需 `to_arrow()` 即可工作。作为消费者，`from_arrow` 接受任何带 `__arrow_c_stream__` 的对象，消除了对 pyarrow IPC 格式与版本的隐性耦合。
- 两个方向上的空结果都保留带类型的 schema。
- `Cursor` 仍是每次拉一个批次。cursor 上的整流 `__arrow_c_stream__`（把剩余的 DataFusion 流交给消费者，这会消耗掉 cursor）是独立的设计，见 #143 关联的后续 issue。
- 新增边界路径时，detach 的那一半返回 `RecordBatchReader` 或 `RecordBatch`，持 GIL 时用 `arrow_pyarrow` 转换；`src/` 中出现 `ipc::`、`StreamWriter` 会让守卫测试失败。

## 来源

- Issue #143 及其评审意见（"零拷贝"的范围、cursor 分离、验收矩阵）
- `src/arrow_ffi.rs`、`src/ops/io.rs`、`src/cursor.rs`、`src/lib.rs`
- `py-ltseq/tests/test_arrow_boundary.py`、`benchmarks/bench_arrow_boundary.py`
- [ADR 0016](0016-gil-release-execution-boundary.cn.md)（边界路径遵循的 GIL 契约）、[ADR 0004](0004-lazy-execution-immutable-tables.cn.md)（终端边界）
