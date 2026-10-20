# LTSeq Tutorials · LTSeq 实战案例集

**中英双语合并排版 · Bilingual (Chinese + English in one page)**

一组可以直接在 Jupyter 里跑的 LTSeq Cookbook：28 个来自真实业务场景的有序计算案例，覆盖**金融时序、电商用户行为、日志与时序监控、IoT 传感器**四个领域。

A hands-on LTSeq cookbook you can run in Jupyter: 28 ordered-computing cases drawn from real business scenarios, covering **financial time series, e-commerce user behavior, log/monitoring streams, and IoT sensors**.

---

## 为什么是"序列思维" · Why "sequence thinking"

LTSeq 把表当作**有顺序的序列**而不是无序集合：`shift` 看邻居、`rolling` 滑窗、`group_ordered` 抓连续段、`asof_join` 对齐两条时间线、`fold` 传状态。凡是"顺序有业务含义"的数据——行情、埋点、日志、流水——用这套算子表达，比绕 SQL 窗口函数或手写循环直接得多。这 28 个案例就是这条主张的证据链。

顺序必须显式声明：`shift`、`rolling`、`diff`、`cum_sum` 这类序列窗口，以及 `group_ordered`、`fold`，都沿表的声明顺序计算；之前没有 `sort()` 或 `assume_sorted()`，LTSeq 直接抛 `SortRequiredError`，而不是按行碰巧到达的顺序算出一个结果。显式写了 `.over(order_by=...)` 的窗口（`rank()`、`ntile()` 通常这样用）自带顺序，不需要先给全表排序。

**已知限制（[#311](https://github.com/Lychee-Technology/ltseq/issues/311)）**：声明的顺序目前还不保证行按这个顺序返回。输入较大时，引擎会把它分到多个分区上（默认每个 CPU 核一个），这时窗口之后再接 `filter` 或 `derive`，或者单独一个 `partition_by` 窗口，返回的行都可能不按声明顺序排列，而 `sort_keys` 仍然报告这个顺序。之后的窗口和 `group_ordered` 照样算对，因为它们会按声明的键给输入排序；按返回顺序取行的调用则会出错：`show()`、`to_dicts()`、`to_pandas()` 可能列出乱序的行，`head()`、`tail()`、`slice()` 可能取到错误的行，`fold` 可能按错误的顺序传递状态。#311 修复之前，请在这些调用之前用同样的键再 `sort()` 一次。教程的示例数据很小，不会乱序，所以代码里没有这一步；换成你自己的数据时可能需要。

LTSeq treats tables as **ordered sequences** rather than unordered sets: `shift` sees neighbors, `rolling` slides, `group_ordered` captures runs, `asof_join` aligns two timelines, `fold` threads state. For data whose order carries business meaning — quotes, events, logs, transaction streams — these operators express the intent far more directly than SQL window gymnastics or hand-written loops. These 28 cases are the evidence.

Order has to be declared. Sequence windows such as `shift`, `rolling`, `diff` and `cum_sum`, as well as `group_ordered` and `fold`, follow the table's declared order: without a prior `sort()` or `assume_sorted()` they raise `SortRequiredError` instead of computing over whatever order the rows arrived in. A window with an explicit `.over(order_by=...)`, the usual way to call `rank()` or `ntile()`, carries its own order and needs no table sort.

**Known limitation ([#311](https://github.com/Lychee-Technology/ltseq/issues/311)).** The declared order does not yet guarantee the order rows come back in. On a large input, which the engine spreads over several partitions (by default one per CPU core), a window followed by `filter` or `derive`, or a `partition_by` window on its own, can return rows out of the declared order while `sort_keys` still reports it. Later windows and `group_ordered` still compute correctly, because they order their input by the declared keys. Calls that take the rows as they come back do not: `show()`, `to_dicts()` and `to_pandas()` can list them out of order, `head()`, `tail()` and `slice()` can keep the wrong rows, and `fold` can thread its state in the wrong order. Until #311 is fixed, repeat the `sort()` with the same keys right before those calls. The tutorials' sample data is small enough to stay in order, so their code has no such re-sort; your own data may need it.

## 目录结构 · Layout

```
tutorials/
├── README.md            ← 本页 / this page
├── cn/                  ← 28 个中文笔记本
├── en/                  ← 28 English notebooks (same code)
├── markdown/            ← 每个笔记本的 Markdown 版（生成）/ each notebook as a Markdown page (generated)
└── render_markdown.py   ← 重新生成 markdown/ / regenerates markdown/
```

## 安装 · Install

> LTSeq 要求 Python 3.13+，目前从源码安装：`uv sync` 会编译 Rust 内核，所以需要 [uv](https://docs.astral.sh/uv/) 和 stable 版 Rust 工具链（可用 [rustup](https://rustup.rs) 安装）。
>
> LTSeq requires Python 3.13+ and is installed from source for now: `uv sync` compiles the Rust kernel, so you need [uv](https://docs.astral.sh/uv/) and a stable Rust toolchain (for example from [rustup](https://rustup.rs)).

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
).show();
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
| 13 | [组内 TopN](cn/13_topn_within_group.ipynb) | [Top-N Within Groups](en/13_topn_within_group.ipynb) | 金融 | `sort` + `slice` `rank().over` `top_k` | ★★ |
| 14 | [同比与环比](cn/14_yoy_mom.ipynb) | [YoY & MoM: shift(12)](en/14_yoy_mom.ipynb) | 金融 | `shift(n, partition_by)` `when` | ★★ |
| 15 | [账期收口分组](cn/15_payment_cycle.ipynb) | [Payment-Cycle Grouping](en/15_payment_cycle.ipynb) | 金融 | `cum_sum(partition_by)` `group_ordered` | ★★★ |
| 16 | [最长连涨区间](cn/16_longest_rising_span.ipynb) | [Longest Rising Span](en/16_longest_rising_span.ipynb) | 金融 | `group_ordered` `rank().over` | ★★ |
| 17 | [榜单排名迁移](cn/17_rank_migration.ipynb) | [Rank Migration](en/17_rank_migration.ipynb) | 金融 | `rank().over` `join` `rename` | ★★★ |
| 18 | [复合会话规则](cn/18_session_composite.ipynb) | [Composite Session Rules](en/18_session_composite.ipynb) | 电商行为 | `shift(partition_by)` `&` `\|` `cum_sum` | ★★★ |
| 19 | [汇总分摊到明细](cn/19_alloc_summary.ipynb) | [Allocating Group Totals](en/19_alloc_summary.ipynb) | 电商行为 | `join` `group_sorted` `NestedTable.derive` | ★★ |
| 20 | [稀疏报表置空](cn/20_sparse_report.ipynb) | [Sparse Reports](en/20_sparse_report.ipynb) | 电商行为 | `shift` `if_else` `fold` | ★ |
| 21 | [购物篮同现对](cn/21_basket_pairs.ipynb) | [Basket Co-occurrence Pairs](en/21_basket_pairs.ipynb) | 电商行为 | 自连接 self `join` `group_by().agg` | ★★★ |
| 22 | [余额快照](cn/22_balance_snapshot.ipynb) | [Balance Snapshot](en/22_balance_snapshot.ipynb) | 电商行为 | `cum_sum(partition_by)` `NestedTable.last` `g.any` | ★★ |
| 23 | [重叠区间合并](cn/23_interval_merge.ipynb) | [Merging Overlapping Intervals](en/23_interval_merge.ipynb) | 日志监控 | `cum_max(partition_by)` `cum_sum` `group_by().agg` | ★★★ |
| 24 | [进出事件配对](cn/24_pair_events.ipynb) | [Pairing In/Out Events](en/24_pair_events.ipynb) | 日志监控 | `shift(-1)` `fold(partition_by)` | ★★★ |
| 25 | [告警冷却抑制](cn/25_alert_suppress.ipynb) | [Alert Cooldown](en/25_alert_suppress.ipynb) | 日志监控 | `fold(init, into, partition_by)` `count_if` | ★★★ |
| 26 | [波动带异常检测](cn/26_sensor_anomaly.ipynb) | [Volatility-Band Anomalies](en/26_sensor_anomaly.ipynb) | IoT | `rolling(partition_by).mean/std` `count_if` | ★★ |
| 27 | [重采样与日历对齐](cn/27_resample_align.ipynb) | [Resampling & Calendar Alignment](en/27_resample_align.ipynb) | IoT | `dt.diff` `align` `join(how="left")` `fold` | ★★★ |
| 28 | [心跳离线段检测](cn/28_offline_segments.ipynb) | [Offline Segment Detection](en/28_offline_segments.ipynb) | IoT | `shift(partition_by)` `rank().over` `max_if` | ★★ |

> 01–04 适合按顺序入门；05–28 可按业务领域跳读，每个笔记本开头注明了前置案例。
> Cases 01–04 read best in order; 05–28 can be read by domain, and each notebook's header names its prerequisites.

## 每个笔记本的结构 · Notebook anatomy

每个笔记本按同一顺序展开 · Every notebook follows the same order:

1. **问题描述** — 业务口径与目标，先说清"要什么" / the business question first
2. **示例数据与思路**（按需）— 手造数据或固定种子模拟，无需下载；较难的案例先讲思路 / deterministic data, zero download; harder cases lay out the approach first
3. **分步代码** — 代码块与预期输出交替呈现 / code and expected output, interleaved
4. **关键点解析** — 易错语义、行为差异、当前限制 / gotchas, differences, limits
5. **延伸练习** — 换口径、换数据、换算子的进阶路径 / where to go next

> ⚠️ **关于输出块**：代码单元不附执行输出，"预期输出"是手写的 Markdown 块，已对照 `main` 的实际运行核对。随机数据都用固定种子（`random.Random(n)`）生成，重复运行结果一致；唯一的例外是案例 12 的游标批大小：它取决于机器的 CPU 核数，同一台机器上也可能每次运行不同。
>
> ⚠️ **About outputs**: code cells ship without execution outputs. The expected outputs are hand-written Markdown blocks, checked against real runs on `main`. Random data comes from fixed seeds (`random.Random(n)`), so reruns give the same numbers. The one exception is the cursor batch sizes in case 12, which depend on the machine's CPU count and can differ between runs on the same machine.

> **API 版本**：笔记本使用 `main` 上当前的 API，并随 v0.5 的实现同步迁移；CI 每次都会执行全部笔记本，保证它们在 `main` 上能跑。
>
> **API version**: the notebooks use the API currently on `main` and migrate along with the v0.5 implementation. CI executes every notebook, so they always run on `main`.

## 学习路径建议 · Suggested paths

- **从 pandas 来 · From pandas**：01 → 02 → 06 → 10（重点体会"顺序是元数据"与部分帧语义）
- **从 SQL 来 · From SQL**：01 → 05 → 08 → 09（窗口函数与集合运算的对照最强烈）
- **可观测性场景 · Observability**：10 → 11 → 12（会话化 → 事件段 → 大文件流式）
- **状态机与配对 · State machines and pairing**：06 → 20 → 24 → 25（`fold` 传状态：前向填充 → 进出配对 → 告警冷却）
- **IoT 传感器 · IoT sensors**：02 → 26 → 27 → 28（滚动波动带 → 重采样补点 → 离线段）

## 相关文档 · See also

- 仓库首页 · Repo: <https://github.com/Lychee-Technology/ltseq>
- 用户心智模型 · User model: [`docs/USER_MODEL.md`](../docs/USER_MODEL.md)（[中文](../docs/USER_MODEL.cn.md)）
- API 参考 · API reference: [`docs/api.md`](../docs/api.md)（[中文](../docs/api.cn.md)），记录当前实现状态 / tracks implementation status；v0.5 API 合同 · the v0.5 API contract: [`docs/proposals/v0.5/`](../docs/proposals/v0.5/README.md)
- Linking 指南 · Linking guide: [`docs/LINKING_GUIDE.md`](../docs/LINKING_GUIDE.md)
- 性能基准 · Benchmarks: [`benchmarks/README.md`](../benchmarks/README.md)（含 ClickBench 对比 / includes the ClickBench harness）
