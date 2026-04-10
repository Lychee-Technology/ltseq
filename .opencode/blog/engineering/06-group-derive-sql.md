# Group Derive：把组操作翻译成窗口 SQL

当你在 LTSeq 里写下 `g.first().price` 或 `g.count()`，你可能以为系统会“先分组再计算”。但真正发生的事情更有工程味：LTSeq 会把这些组级操作翻译成窗口 SQL 表达式，并把它交给执行引擎。这是一个很关键的设计，因为它让“组级计算”既可读又可优化。

这一篇聚焦 `group_ordered` / `group_sorted` 之后的核心动作：**group derive 如何被编译成窗口 SQL**。

## 1. 痛点：组级计算容易变成低效的行级循环

在很多 Python 工具里，组级计算常常意味着：

- 先构造分组
- 再逐组遍历
- 最后把结果回写

这条路径虽然直观，但性能差，而且很难与引擎层优化融合。LTSeq 要解决的问题是：**能不能把组级计算变成可优化的执行计划？**

## 2. 心智模型：组级计算其实是“窗口视角”

如果你把每个组想象成一条轨道，组级操作就是在轨道上做窗口计算：

- `g.count()` 相当于 “COUNT(*) OVER (PARTITION BY group_id)”
- `g.first().price` 相当于 “FIRST_VALUE(price) OVER (PARTITION BY group_id ORDER BY rn)”
- `g.last().price` 相当于 “LAST_VALUE(price) OVER ...”

这意味着：**组级计算可以被转换成窗口函数，而不是在 Python 层循环**。

## 3. 源码机制：从 GroupExpr 到 SQL

LTSeq 在这件事上走了两条路径：

### 3.1 Proxy 表达式捕获（首选路径）

当你写 `grouped.derive(lambda g: {"span": g.count()})`，LTSeq 会使用代理对象捕获表达式（`DeriveGroupProxy`），生成 `GroupExpr`（见 `py-ltseq/ltseq/grouping/expr.py`）。

`GroupExpr` 会被序列化为 dict 结构，例如：

- `g.count()` → `{ "type": "GroupCount" }`
- `g.first().price` → `{ "type": "GroupRowColumn", "row": "first", "column": "price" }`

接着 `group_expr_to_sql()`（见 `py-ltseq/ltseq/grouping/sql_parsing.py`）会把这些结构转换成窗口 SQL 表达式。

### 3.2 AST 解析路径（兼容路径）

为了兼容一些旧用法或 REPL 场景，LTSeq 还提供 AST 解析路径（`DeriveSQLParser`）。它通过解析 lambda 的源码字符串，提取 `g.count()`、`g.first().col` 等模式，并转为 SQL。

这条路径不是最稳定的，但在无法使用 proxy 捕获时，是一个务实的备选方案。

## 4. SQL 是“桥梁”，不是“目的”

这里的 SQL 不是让你写 SQL，而是系统用 SQL 描述你已经写出的组级逻辑。它扮演的是“中间表示”的角色。

具体来说：

- 组级表达式 → 窗口 SQL
- 窗口 SQL → 交给 Rust 内核

因此你不会在 API 层看到 SQL，但引擎层会得到一个可优化的计划。

## 5. 这个设计带来的工程价值

### 5.1 性能更稳定
窗口函数可以在引擎层矢量化执行，不需要 Python 逐组循环。

### 5.2 语义统一
组级操作被翻译成窗口函数后，语义与窗口体系一致，避免出现“组逻辑与窗口逻辑不一致”的问题。

### 5.3 扩展更容易
新增一种组级表达式，只需要扩展 `GroupExpr` 与 `group_expr_to_sql()` 的映射，不必修改用户 API。

## 6. 权衡：可表达性与可解析性的博弈

把组级操作翻译成 SQL，有明显的收益，但也带来边界：

- **支持的表达式有限**：只有系统识别的模式能被翻译
- **复杂表达式可能 fallback**：遇到超出范围的 lambda，会退回到 pandas 路径
- **AST 解析不稳定**：在 REPL 或链式调用中，源码可能无法可靠获取

这就是工程现实：**可优化性来自约束**。LTSeq 选择把“可表达性”控制在一个可优化的范围内。

## 7. 为什么这一步很关键

连续分组只是“划分片段”，而真正让这些片段产生业务价值的是“组级计算”。

LTSeq 选择用窗口 SQL 作为桥梁，意味着它把“组级计算”提升到了与窗口函数同等的执行路径中。这让 LTSeq 的序列分析能力不仅正确，而且高效。

## 8. 下一篇预告

下一篇会进入 `LinkedTable`，看看 LTSeq 如何在不急着 join 的情况下建立表间关系，实现“先连线，后取值”的惰性关联。
