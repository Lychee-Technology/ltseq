# LTSeq Benchmarks

本目录存放 LTSeq 的性能基准：核心算子微基准、与 DuckDB 的 ClickBench 对比，以及一套基准门控的自动化性能研究试点（autoresearch）。

This directory holds LTSeq's performance benchmarks: core-operation micro-benchmarks, a ClickBench comparison against DuckDB, and a benchmark-gated automated performance-research pilot (autoresearch).

## 文件速览 / Files at a glance

| 文件 / File | 用途 / Purpose | 需要数据 / Needs data |
|---|---|---|
| `run_all.py` | 一键编排：按序运行 build → 核心基准 → ClickBench 对比，并打印 PASS/SKIP/FAIL 摘要 | 否（自带门控）/ No (gated) |
| `bench_core.py` | LTSeq 单引擎核心基准，10K/100K/1M 三档，分三组：算子（输入先解析进内存，计时区只含算子的构建与执行）、I/O、端到端（`e2e_csv_*`，含 CSV 解析） / Core suite in three groups: operators on in-memory input, I/O, and end-to-end CSV pipelines (`e2e_csv_*`) | 否（内部生成临时数据）/ No |
| `bench_arrow_boundary.py` | Python↔Rust Arrow 边界：`from_arrow` / `to_arrow` / `pa.table(t)` / cursor 的耗时与峰值 RSS 增量，每项在独立子进程中测量（#143） / Arrow boundary transfer time and peak-RSS growth, one subprocess per op | 否（内部生成）/ No |
| `bench_writer_fsync.py` | 写出器在重命名前刷盘（`fsync`，macOS 为 `F_FULLFSYNC`）的开销，三档输出大小，中位数与 p95；Linux 下 `--shim` 在写出器内部计时并给出不刷盘的对照（#259） / Cost of the writers' flush before the rename at three output sizes, median and p95; on Linux `--shim` times it inside the writer and gives a no-flush baseline | 否（内部生成）/ No |
| `bench_vs.py` | LTSeq vs DuckDB 的 ClickBench 三轮对比（Top URLs / Sessionization / Funnel）；R2/R3 的 LTSeq 耗时来自只对特定查询形状生效的快路径 / R2 and R3 LTSeq timings come from shape-specific fast paths | 是 / Yes |
| `prepare_data.py` | 下载 ClickBench `hits.parquet`（约 14GB）并生成预排序数据集与 1M 采样 | — |
| `verify_parquet_order.py` | 校验 parquet 文件是否按指定列物理有序 | 传入的 parquet |
| `CLICKBENCH_MAPPING.md` | 三轮设计与标准 ClickBench（43 条查询）的对应关系、实测结论与 R2/R3 快路径的适用条件 | — |
| `autoresearch/` | 基准门控的自动化性能研究 + 受控 pilot | 是 / Yes |
| `m0/` | v0.5 契约六项高成本承诺的可行性测量（#251）：`m0` cargo feature 下的原型、逐格子进程的测量框架、结果与报告 `m0/REPORT.md` / Feasibility measurements of six costly v0.5 contract commitments (#251): prototypes behind the `m0` cargo feature, a one-subprocess-per-cell harness, results and `m0/REPORT.md` | 部分 / Partly (ClickBench for m3–m6) |

## 快速开始 / Quick start

先构建 Rust 扩展：

```bash
maturin develop --release
```

> ClickBench 对比（`bench_vs.py`）和数据准备（`prepare_data.py`）依赖
> `duckdb` + `psutil`，它们在可选的 `bench` 依赖组里。用 `uv run --group bench`
> 激活该组,duckdb/psutil 才会被同步进 venv 供子进程步骤使用。
> **不要**用 `uv sync --group bench` 一次性安装后再跑普通 `uv run`——后者会重新
> 同步并把这两个包卸载掉。只跑核心微基准（`--only core`）时不需要这个组。

跑一遍采样冒烟（构建 + 核心基准 + 1M 采样的 ClickBench 对比）：

```bash
uv run --group bench python benchmarks/run_all.py --sample
```

`run_all.py` 常用开关：

```bash
uv run --group bench python benchmarks/run_all.py --full        # ClickBench 用完整 sorted 数据集
uv run --group bench python benchmarks/run_all.py --skip-build  # 扩展已最新，跳过重新构建
uv run python benchmarks/run_all.py --only core                 # 只跑核心微基准（无需 bench 组）
uv run --group bench python benchmarks/run_all.py --only vs     # 只跑 ClickBench 对比
uv run --group bench python benchmarks/run_all.py --prepare     # 运行前显式准备数据（可能下载 ~14GB）
```

`bench_core.py` 默认按 CPU 核数分区。`--target-partitions N` 和 `--batch-size N` 通过 #274 的测试专用开关固定整次运行的分区数与批大小，结果 JSON 的 `engine` 字段记录实际取值；按分区数比较扩展性（如 #251）时用它们。

`bench_core.py` runs at the CPU count's partitions by default. `--target-partitions N` and `--batch-size N` pin the partition count and batch size for the whole run through #274's test-only override, and the results JSON records them under `engine`. Use them to compare scaling across partition counts (#251).

```bash
uv run python benchmarks/bench_core.py --target-partitions 2
```

`run_all.py` **不会**自动下载约 14GB 的 ClickBench 数据集。若对比所需的 parquet 不存在，`bench_vs` 步骤会被标记为 SKIP 并给出准备数据的指引；只有显式传 `--prepare` 或先手动运行 `prepare_data.py` 才会准备数据。

## 准备 ClickBench 数据 / Preparing ClickBench data

```bash
# 完整数据集 + 1M 采样（约 14GB 下载）
uv run --group bench python benchmarks/prepare_data.py

# 仅采样（用于冒烟）
uv run --group bench python benchmarks/prepare_data.py --sample-only

# 覆盖内存上限（容器内存较小、排序被 OOM-kill 时）；默认取 cgroup/宿主内存的 60%
uv run --group bench python benchmarks/prepare_data.py --mem-limit 8GB

# 校验物理有序
uv run --group bench python benchmarks/verify_parquet_order.py \
  benchmarks/data/hits_sorted.parquet userid eventtime watchid
```

数据文件落在 `benchmarks/data/`（已被 `.gitignore` 忽略）。

## 生成产物 / Generated outputs

运行基准会写出结果 JSON，这些是**运行产物**，已被 `.gitignore` 忽略、不纳入版本控制：

- `results.json` — 来自 `bench_core.py`
- `clickbench_results.json` — 来自 `bench_vs.py`

## 延伸阅读 / See also

- [`docs/BENCHMARK.md`](../docs/BENCHMARK.md) — 完整运行说明、可复现性约定、各轮次细节、核心基准的分组与计时口径、R2/R3 快路径的适用条件
- [`docs/BENCHMARK_AUTORESEARCH.md`](../docs/BENCHMARK_AUTORESEARCH.md) — autoresearch 受控自动循环、产物保留与评审规则
- [`CLICKBENCH_MAPPING.md`](CLICKBENCH_MAPPING.md) — 三轮设计原理与和标准 ClickBench 的对应
