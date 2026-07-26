# ADR 0015: 测试与基准是架构层的一部分

- 状态：已采纳（Accepted）
- 日期：2026-07-26（补记；决策早于本 ADR）

[English](0015-tests-benchmarks-as-architecture.md)

## 背景

这个代码库里最难的反复出现的问题都是实证性的：DataFusion 在哪儿够用、物化在哪儿占主导、专用 Rust 何时值得、一次架构改动是否真的改善了真实工作负载（[ADR 0006](0006-multi-path-execution-strategy.cn.md)）。回答这些问题要求把验证与性能度量当作系统的一部分，"而不只是工程卫生"。

## 决策

把系统建模为四层——在 API、编排、执行之外，加上第四层**验证与性能层**（`py-ltseq/tests/`、`benchmarks/`、autoresearch 工作流）。

### 测试组织

- 测试**按产品能力/行为组织，而非按源码文件**——"这是一个架构选择"。测试套件兼作产品能力地图和重构护栏；`MODULE_GUIDE.md` 把它推为首要导航工具（"先找最具体的相关测试，再追进实现"）。
- 架构不变量有专属测试，如 `test_no_materialization_rule.py`（[ADR 0005](0005-no-materialization-rule.cn.md)）。
- 五条贡献者启发式被固化为约束：除非 API 明确是终端的，否则保持惰性执行；不要随意破坏排序元数据传播；返回表的 API 内部不做 Python 侧物化；保持 Python `_schema` 与 Rust schema 一致；优先小而局部的改动而非横切式重构。

### 基准协议

- 默认 1 次预热 + 3 轮计时，`time.perf_counter()`，报告**中位数**，记录 RSS 内存增量。LTSeq 的数据加载与 `assume_sorted` 声明发生在计时轮**之前**并单独报告。每个 ClickBench 轮次都**用 DuckDB 校验**正确性。结果输出为机器可读 JSON（样本、中位数、内存、校验状态、主机信息）。
- 可复现规则：同机低负载、重建 `maturin develop --release`、记录 git commit 与工具链版本，并且"把抽样结果当作冒烟测试证据，而非全量数据集上的性能决策依据"。
- 工作负载检验序列论题：Top-URL 聚合、用户会话切分、顺序 URL 漏斗匹配，外加 10K/100K/1M 行的核心套件。

### 基准门控的 autoresearch（受监督）

一个 LLM agent 实验闭环（baseline → candidate → gate），配有防止其投机或破坏仓库稳定的护栏：以机器可读 JSON 做门控（`benchmark-diff.json`、`evaluation.json`、`keep`/`discard`；基础设施故障持久化为 `infra_failure`，而非从 stdout 解析）；每轮一个隔离 git worktree、一个候选，结束即丢弃；产出任何工件前先做 preflight 校验；**受限的可编辑范围**（只能改目标允许的源文件——测试、基准脚本与基础设施一律禁改）；阈值（目标改进 −3.0%，受保护工作负载回归容忍 +5.0%）；明确的工件保留策略；以及明确的**监督优先、先审后并**立场——在任何自动 commit/merge 之前须满足成文的毕业标准（包括 `keep` 推荐的误放率必须为零），范围扩大也按增量规则门控。

## 影响与取舍

- 性能研究变成"可重复的工程实践"，而非临时调优。
- 按能力组织的测试能在源码文件重组的重构中存活。
- autoresearch 的护栏用自动化速度换信任；放宽护栏须满足显式标准。

## 来源

- `docs/ARCHITECTURE.md` — 分层架构 §4、Testing Strategy、Benchmarks and Performance Research
- `docs/DESIGN_SUMMARY.cn.md` — §6.1、§6.2
- `docs/MODULE_GUIDE.md` — Test Suite as a Navigation Tool、Contributor Heuristics
- `docs/BENCHMARK.md`、`docs/BENCHMARK_AUTORESEARCH.md`
