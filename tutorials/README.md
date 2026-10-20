# LTSeq Tutorials · LTSeq 实战案例集

**中英双语合并排版 · Bilingual (Chinese + English in one page)**

一组可以直接在 Jupyter 里跑的 LTSeq Cookbook：12 个来自真实业务场景的有序计算案例，覆盖**金融时序、电商用户行为、日志与时序监控**三大领域。

A hands-on LTSeq cookbook you can run in Jupyter: 12 ordered-computing cases drawn from real business scenarios, covering **financial time series, e-commerce user behavior, and log/monitoring streams**.

---

## 为什么是"序列思维" · Why "sequence thinking"

LTSeq 把表当作**有顺序的序列**而不是无序集合：`shift` 看邻居、`rolling` 滑窗、`group_ordered` 抓连续段、`asof_join` 对齐两条时间线、`fold` 传状态。凡是"顺序有业务含义"的数据——行情、埋点、日志、流水——用这套算子表达，比绕 SQL 窗口函数或手写循环直接得多。这 12 个案例就是这条主张的证据链。

顺序必须显式声明：窗口函数之前没有 `sort()` 或 `assume_sorted()`，LTSeq 直接抛 `SortRequiredError`，而不是按不确定的顺序算出一个结果，所以结果总能复现。

LTSeq treats tables as **ordered sequences** rather than unordered sets: `shift` sees neighbors, `rolling` slides, `group_ordered` captures runs, `asof_join` aligns two timelines, `fold` threads state. For data whose order carries business meaning — quotes, events, logs, transaction streams — these operators express the intent far more directly than SQL window gymnastics or hand-written loops. These 12 cases are the evidence.

Order has to be declared: a window function without a prior `sort()` or `assume_sorted()` raises `SortRequiredError` instead of computing over an arbitrary order, so results are always reproducible.

## 目录结构 · Layout

```
tutorials/
├── README.md            ← 本页 / this page
├── cn/                  ← 12 个中文笔记本
└── en/                  ← 12 English notebooks (same code)
```

## 安装 · Install

> LTSeq 要求 Python 3.13+，目前从源码安装。
>
> LTSeq requires Python 3.13+ and is installed from source for now.

```bash
git clone https://github.com/Lychee-Technology/ltseq
cd ltseq
uv sync                 # 创建环境并构建 Rust 内核 / create the env and build the Rust kernel
uv run python -c "import ltseq; print('ok')"
```

Jupyter 不在项目依赖里，用 `--with` 临时带上 · Jupyter is not a project dependency; `--with` adds it for the session:

```bash
uv run --with jupyterlab jupyter lab tutorials/cn/01_hello_sequence.ipynb
```

30 秒感受一下序列思维 · A 30-second taste:

```python
from ltseq import LTSeq

t = LTSeq.from_dict({"date": ["D1", "D2", "D3", "D4"], "close": [10.0, 10.8, 10.5, 11.2]})
(
    t.sort("date")
     .derive(chg=lambda r: r.close - r.close.shift(1))     # 上一行 / previous row
     .cum_sum("chg")                                        # 累计 / running total
).show()
```

## 案例目录 · Case catalog

| # | 中文标题 | English title | 领域 / Domain | 核心 API | 难度 |
|---|---------|---------------|---------------|----------|------|
| 01 | [序列思维入门](cn/01_hello_sequence.ipynb) | [Hello LTSeq: Think in Sequences](en/01_hello_sequence.ipynb) | 通用 | `from_dict` `filter` `derive` `sort` `shift` `cum_sum` | ★ |
| 02 | [股价技术指标](cn/02_stock_indicators.ipynb) | [Stock Indicators: MA & Returns](en/02_stock_indicators.ipynb) | 金融时序 | `rolling` `pct_change` `when` `fill_null` | ★★ |
| 03 | [金叉死叉与状态段](cn/03_golden_cross.ipynb) | [Golden Cross: State Runs](en/03_golden_cross.ipynb) | 金融时序 | `group_ordered` `agg` `cum_sum` `search_first` | ★★★ |
| 04 | [逐笔成交聚合 K 线](cn/04_kline_ohlc.ipynb) | [Ticks to Candles: Ordered Grouping](en/04_kline_ohlc.ipynb) | 金融时序 | `dt.*` `partition` `group_sorted` `pivot` | ★★★ |
| 05 | [成交对齐行情](cn/05_asof_join_quotes.ipynb) | [Trades Meet Quotes: asof_join](en/05_asof_join_quotes.ipynb) | 金融时序 | `asof_join(on, by, strategy)` | ★★ |
| 06 | [最大回撤与复利](cn/06_max_drawdown.ipynb) | [Max Drawdown & Compounding](en/06_max_drawdown.ipynb) | 金融时序 | `cum_max` `fold` 全表 `agg` | ★★★ |
| 07 | [转化漏斗](cn/07_funnel_analysis.ipynb) | [Conversion Funnel: search_pattern](en/07_funnel_analysis.ipynb) | 电商行为 | `search_pattern(_count)` `distinct` | ★★★ |
| 08 | [用户留存](cn/08_user_retention.ipynb) | [User Retention: Set Operations](en/08_user_retention.ipynb) | 电商行为 | `intersect` `semi_join` `anti_join` | ★★ |
| 09 | [RFM 用户分层](cn/09_rfm_segments.ipynb) | [RFM Segmentation](en/09_rfm_segments.ipynb) | 电商行为 | `group_by().agg` `ntile().over` `when` | ★★★ |
| 10 | [会话切分](cn/10_sessionization.ipynb) | [Sessionization: Gap Detection](en/10_sessionization.ipynb) | 日志监控 | `shift(partition_by)` `group_ordered` `flatten` | ★★★ |
| 11 | [错误风暴检测](cn/11_error_burst.ipynb) | [Error Burst Detection](en/11_error_burst.ipynb) | 日志监控 | `rolling(partition_by)` `g.all/any` | ★★★ |
| 12 | [大文件流式处理](cn/12_streaming_cursor.ipynb) | [Streaming Big Files: Cursor](en/12_streaming_cursor.ipynb) | 通用/日志 | `scan` `Cursor` `scan_parquet` | ★★ |

> 01–04 适合按顺序入门；05–12 可按业务领域跳读，每个笔记本开头注明了前置案例。
> Cases 01–04 read best in order; 05–12 can be read by domain, and each notebook's header names its prerequisites.

## 每个笔记本的结构 · Notebook anatomy

每个笔记本按同一顺序展开 · Every notebook follows the same order:

1. **问题描述** — 业务口径与目标，先说清"要什么" / the business question first
2. **示例数据与思路**（按需）— 手造数据或固定种子模拟，无需下载；较难的案例先讲思路 / deterministic data, zero download; harder cases lay out the approach first
3. **分步代码** — 代码块与预期输出交替呈现 / code and expected output, interleaved
4. **关键点解析** — 易错语义、行为差异、当前限制 / gotchas, differences, limits
5. **延伸练习** — 换口径、换数据、换算子的进阶路径 / where to go next

> ⚠️ **关于输出块**：代码单元不附执行输出，"预期输出"是手写的 Markdown 块，已对照 `main` 的实际运行核对。随机数据都用固定种子（`random.Random(n)`）生成，重复运行结果一致；唯一的例外是案例 12 的游标批大小，它取决于机器的 CPU 核数。
>
> ⚠️ **About outputs**: code cells ship without execution outputs. The expected outputs are hand-written Markdown blocks, checked against real runs on `main`. Random data comes from fixed seeds (`random.Random(n)`), so reruns give the same numbers. The one exception is the cursor batch sizes in case 12, which depend on the machine's CPU count.

> **API 版本**：笔记本使用 `main` 上当前的 API，并随 v0.5 的实现同步迁移；CI 每次都会执行全部笔记本，保证它们在 `main` 上能跑。
>
> **API version**: the notebooks use the API currently on `main` and migrate along with the v0.5 implementation. CI executes every notebook, so they always run on `main`.

## 学习路径建议 · Suggested paths

- **从 pandas 来 · From pandas**：01 → 02 → 06 → 10（重点体会"顺序是元数据"与部分帧语义）
- **从 SQL 来 · From SQL**：01 → 05 → 08 → 09（窗口函数与集合运算的对照最强烈）
- **可观测性场景 · Observability**：10 → 11 → 12（会话化 → 事件段 → 大文件流式）

## 相关文档 · See also

- 仓库首页 · Repo: <https://github.com/Lychee-Technology/ltseq>
- 用户心智模型 · User model: [`docs/USER_MODEL.md`](../docs/USER_MODEL.md)（[中文](../docs/USER_MODEL.cn.md)）
- API 参考 · API reference: [`docs/api.md`](../docs/api.md)（[中文](../docs/api.cn.md)），记录当前实现状态 / tracks implementation status；v0.5 API 合同 · the v0.5 API contract: [`docs/proposals/v0.5/`](../docs/proposals/v0.5/README.md)
- Linking 指南 · Linking guide: [`docs/LINKING_GUIDE.md`](../docs/LINKING_GUIDE.md)
- 性能基准 · Benchmarks: [`benchmarks/README.md`](../benchmarks/README.md)（含 ClickBench 对比 / includes the ClickBench harness）
