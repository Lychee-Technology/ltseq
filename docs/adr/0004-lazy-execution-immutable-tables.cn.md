# ADR 0004: 惰性求值、不可变表与显式终端边界

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0004-lazy-execution-immutable-tables.md)

## 背景

链式变换（`filter → derive → sort → join → …`）应当组合成本低，并让 DataFusion 优化*整条*计划而非孤立的每一步。用户也需要一个可预期的答案："计算到底什么时候发生？"

## 决策

1. **默认惰性。** 绝大多数操作（`filter`、`select`、`derive`、`sort`、`slice`、`join`、`group_ordered` 等）返回新的惰性查询对象。只有在显式终端边界才真正执行：`show()`、`count()`/`len()`、`collect()`、`to_arrow()`/`to_arrow_ipc()`、`to_pandas()`、`to_dicts()` 以及文件写出。
2. **表不可变。** 每个操作返回新的 `LTSeq`，原表不变。*看起来*像修改的 API（`insert`、`delete`、`update`、`modify`）在专门的 `mutation_mixin.py` 中以写时复制（copy-on-write）实现。
3. **流式是独立对象。** 内存装不下的数据集用 `LTSeq.scan()` / `scan_parquet()` 返回流式 `Cursor`（迭代 Arrow `RecordBatch`，实现于 `src/cursor.rs`），而非 `LTSeq`；`cursor.count()` 无需加载全量数据即可计数。

返回新表对象意味着：复用同一 session、包装新计划、保持/更新 schema 元数据、保持或作废排序元数据（[ADR 0008](0008-explicit-sort-metadata.cn.md)）。

## 影响与取舍

- DataFusion 可以优化整条流水线；链式工作流在终端调用之前保持廉价。
- 用户必须理解 `to_pandas()`/`collect()` 会"改变成本模型"——物化是最主要的架构成本中心（设计教训 §7.4），这也是 [ADR 0005](0005-no-materialization-rule.cn.md) 硬性规则的动机。
- 不可变风格让语义简单，但"修改"类 API 是构建新计划而非原地改数据。

## 来源

- `docs/ARCHITECTURE.cn.md` — 设计目标 #3、懒执行模型
- `docs/DESIGN_SUMMARY.cn.md` — §1.2、§7.4
- `docs/USER_MODEL.cn.md` — 核心心智模型 #1–2、物化模型
- `docs/api.cn.md` — §0 约定、§9（mutation）、§1/§11（Cursor）
- `docs/MODULE_GUIDE.cn.md` — `mutation_mixin.py`、`src/cursor.rs`
