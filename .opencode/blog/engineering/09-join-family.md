# Join 家族：hash / merge / asof 的权衡

Join 是数据工程里最常见、也最容易踩坑的操作。你把两张表合并成一张，但“合并”的方式并不只有一种：hash join、merge join、asof join，每一种都在解决不同的“秩序问题”。LTSeq 的 join 家族并不是在堆功能，而是在表达一种事实：**不同的 join 需要不同的排序与语义承诺**。

这一篇聚焦 LTSeq 的 join 设计：它如何从表达式中提取 join key、如何区分普通 join 与 asof join，以及这些设计背后的工程权衡。

## 1. 痛点：Join 是语义敏感操作

在很多工具里，join 被当作统一操作：给出 key 就能合并。但在序列分析里，join 的语义可能非常不同：

- 传统等值 join：要求匹配的 key 完全相等
- 时间近似 join（asof）：要求时间上“最近”的匹配
- 有序 join：要求数据按某种顺序排列，才能高效执行

如果系统不能区分这些语义，就会导致结果不可靠或性能不可控。

## 2. 心智模型：Join 是“秩序的承诺”

可以这样理解：

- **hash join** 适合“无序世界”，不要求排序，但需要构建哈希索引
- **merge join** 适合“有序世界”，需要排序，但可以线性合并
- **asof join** 适合“时间世界”，匹配最近时间而不是完全相等

所以 join 的选择不是技术细节，而是对数据秩序的一种承诺。

## 3. LTSeq 的 join 入口：表达式解析

LTSeq 在 Python 层的 join API 允许你用 lambda 表达 join 条件，例如：

- `lambda o, p: o.product_id == p.id`
- `lambda t, q: t.time >= q.time`（asof）

这些 lambda 会被 `_extract_join_keys()` 或 `_extract_asof_keys()` 解析（见 `py-ltseq/ltseq/helpers.py`），并转换成可序列化的表达式结构交给 Rust 内核。

### 3.1 等值 join 的解析

`_extract_join_keys()` 会检查表达式是否是“列对列的相等”或由多个相等条件用 `&` 组合。它会：

- 验证左右列分别来自 source/target
- 生成 left/right key 的序列化表达式

如果表达式不是简单等值，LTSeq 会直接拒绝，避免语义不清或执行不可控。

### 3.2 asof join 的解析

`_extract_asof_keys()` 专门处理不等式条件（`>=`, `<=`, `>` , `<`）。它会：

- 确定左表与右表的时间列
- 提取比较方向（Ge, Le, Gt, Lt）

这一步非常关键，因为 asof join 的含义不是“等值匹配”，而是“时间最近匹配”。

## 4. 为什么 LTSeq 强调表达式约束

你可能会问：为什么 join 条件必须是简单的列对列比较？

原因是工程稳定性：

- 复杂表达式难以保证正确性
- join 的执行策略高度依赖 key 的清晰性
- Rust 内核需要明确的 join key 才能优化执行路径

这种约束听起来严格，但它换来的是可靠性和可优化性。

## 5. Join 类型与执行策略

LTSeq 支持常见的 join 类型（inner、left、right、full），并把实际执行交给 Rust 内核。

虽然 Python 层不直接暴露 hash/merge join 的选择，但底层会根据数据特性与排序状态选择合适策略。这也是为什么 LTSeq 对排序状态如此敏感——它在为更高效的执行路线铺路。

## 6. 工程权衡：灵活性 vs 可控性

LTSeq 的 join 设计有明显权衡：

- **灵活性降低**：不支持任意复杂条件
- **可控性提升**：执行路径更稳定，错误更可预期
- **语义清晰**：join 行为更容易解释与调试

这体现了 LTSeq 的一贯风格：宁可约束表达式，也不牺牲执行与语义的确定性。

## 7. 为什么 join 在序列分析中更敏感

在序列场景里，join 往往不是简单的“合并表”，而是“对齐时间”。比如：

- 订单与行情的最近价格匹配
- 事件流与用户画像的时间点对齐

这些场景对排序和时间语义高度敏感，传统 join 往往无法准确表达。LTSeq 的 asof join 提供了一种更贴近真实语义的方式。

## 8. 下一篇预告

下一篇会进入 partition 与 grouping 的对比：`partition()` 与 `group_ordered()` 的语义完全不同，一个是“分区容器”，一个是“连续段”。理解这个区别，才能正确选择你的切片策略。
