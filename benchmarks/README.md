# LTSeq Benchmarks

本目录存放 LTSeq 的性能基准：核心算子微基准、与 DuckDB 的 ClickBench 对比，以及一套基准门控的自动化性能研究试点（autoresearch）。

This directory holds LTSeq's performance benchmarks: core-operation micro-benchmarks, a ClickBench comparison against DuckDB, and a benchmark-gated automated performance-research pilot (autoresearch).

## 文件速览 / Files at a glance

| 文件 / File | 用途 / Purpose | 需要数据 / Needs data |
|---|---|---|
| `run_all.py` | 一键编排：按序运行 build → 核心基准 → ClickBench 对比，并打印 PASS/SKIP/FAIL 摘要 | 否（自带门控）/ No (gated) |
| `bench_core.py` | LTSeq 单引擎核心算子微基准（filter / derive / join / window / group / sort / mutation / I/O），10K/100K/1M 三档 | 否（内部生成临时数据）/ No |
| `bench_vs.py` | LTSeq vs DuckDB 的 ClickBench 三轮对比（Top URLs / Sessionization / Funnel） | 是 / Yes |
| `prepare_data.py` | 下载 ClickBench `hits.parquet`（约 14GB）并生成预排序数据集与 1M 采样 | — |
| `verify_parquet_order.py` | 校验 parquet 文件是否按指定列物理有序 | 传入的 parquet |
| `CLICKBENCH_MAPPING.md` | 三轮设计与标准 ClickBench（43 条查询）的对应关系与实测结论 | — |
| `autoresearch/` | 基准门控的自动化性能研究 + 受控 pilot | 是 / Yes |

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

- [`docs/BENCHMARK.md`](../docs/BENCHMARK.md) — 完整运行说明、可复现性约定、各轮次细节
- [`docs/BENCHMARK_AUTORESEARCH.md`](../docs/BENCHMARK_AUTORESEARCH.md) — autoresearch 受控自动循环、产物保留与评审规则
- [`CLICKBENCH_MAPPING.md`](CLICKBENCH_MAPPING.md) — 三轮设计原理与和标准 ClickBench 的对应
