# ADR 0014: 为动态表面手写 `.pyi` Stubs + 移除废弃方法

- 状态：已采纳（Accepted）
- 决策日期：2026-04-05（设计规格） · 记录日期：2026-07-26
- Issue：[#8](https://github.com/Lychee-Technology/ltseq/issues/8)

[English](0014-pyi-stubs-typed-surface.md)

## 背景

表达式 DSL 按设计就是动态的（[ADR 0007](0007-lambda-dsl-schemaproxy.cn.md)）：`SchemaProxy`、`ColumnExpr`、`CallExpr` 在运行时都依赖 `__getattr__`，IDE 和类型检查器因此什么都看不到。同时，API 里还残留着三个废弃方法。

## 决策

**1. 手写 `.pyi` stub 文件**，与 `.py` 模块并排分发——纯类型声明，零运行时改动。关键类型决策：

- `SchemaProxy.__getattr__` 对任意属性静态承诺一个表达式类型（运行时仍会校验列存在性）。规格写的是 `-> ColumnExpr`；当前 stub 返回 `_SchemaAttr(ColumnExpr)` 子类，同时类型化嵌套访问（`expr/proxy.pyi`）。
- **`ColumnExpr`/`CallExpr` 上"显式声明 + 兜底"**：为可发现性显式声明最常用方法（窗口操作 `shift`/`rolling`/`diff`；聚合 `sum`/`mean`/`min`/`max`/`count`/`std`/`var`/`first`/`last`/`median`；rolling 链目标），再加 `__getattr__` 兜底。这是本规格的核心取舍：显式清单换来 IDE 可发现性，兜底保住动态运行时——代价是清单之外的任何名字都能通过类型检查。规格把兜底类型定为 `-> Callable[..., CallExpr]`；当前 stubs 用的是 `-> Any`（`expr/types.pyi`），进一步放宽。
- `.s` / `.dt` 访问器声明为返回 `StringAccessor`/`TemporalAccessor` 的 `@property`；`TemporalAccessor.diff(other: Expr, unit)` 接受 `Expr` 而非 `int`。
- **Stub 把 mixin 摊平**：运行时 `LTSeq` 由 mixin 组合（[ADR 0012](0012-rust-thin-shell-python-mixins.cn.md)），但 `__init__.pyi` 声明的是一个包含全部方法的扁平 `LTSeq` 类——声明结构与运行时结构的有意分叉，为 IDE 人体工学服务。lambda 参数类型为 `Callable[[SchemaProxy], Expr]`；别名（`with_columns`、`group_consecutive`）包含在内；工厂函数给出完整签名。
- 规格把私有方法与内部 helper 排除在 stub 之外；当前 stubs 实际声明了少数跨模块使用的私有成员（`__init__.pyi` 中的 `_inner`、`_schema`、`_sort_keys`、`_from_rows`、`_capture_expr`）。

**2. 同一变更中移除三个废弃方法**（当前开发阶段接受 breaking change）：`join_merge()` 与 `join_sorted()`（由 `join(..., strategy="merge")` 取代）、集合差集别名 `diff()`（由 `except_()` 取代）。移除以 TDD 方式落地：`py-ltseq/tests/test_deprecated_removed.py`。

### 实施与规格的偏差（按实际落地记录）

- 范围从规格的 7 个 stub 文件在实施计划中扩至 14 个；当前包内实际有 **15 个**（新增 `cursor.pyi`、`linking.pyi`、`partitioning.pyi`、`aggregation.pyi`、`grouping/nested_table.pyi`、`expr/lookup_expr.pyi`、`exceptions.pyi`、`ltseq_core.pyi` 等）。
- `Cursor` 最终**被** stub 了，尽管规格的"不做 stub"清单曾把它排除在外。
- Stubs 用 `TYPE_CHECKING` 守卫打破循环导入（仅在实施计划中说明）。
- Stubs 自身没有单元测试，但 CI 现已把 **pyright 作为门禁**——分别检查库本身（`py-ltseq/ltseq`）与专门的类型检查测试（`py-ltseq/typecheck_tests`），见 `.github/workflows/ci.yml`（规格时期尚无类型检查门禁）。

### 相关的人体工学决策

- **兼容别名**降低从 pandas/Polars 迁移的成本：`with_columns`（= `derive`）、`descending=`（= `desc=`）、`group_consecutive`（= `group_ordered`）、`except_`/`subtract`、`link` 上的 `alias`/`as_`；另有 `api.cn.md` 的"从 Pandas 迁移"附录。
- **异常层级双继承**：所有错误派生自 `LTSeqError`，且每个具体错误同时继承它历史上抛出的内建异常——`SortRequiredError(LTSeqError, ValueError)`、`SchemaMismatchError(LTSeqError, ValueError)`、`ColumnNotFoundError(LTSeqError, ValueError, AttributeError)`（加 `AttributeError` 专为保住 `hasattr()` 探测）。错误信息附修复提示；`api.cn.md` 维护"错误→原因→解决"对照表。

## 影响与取舍

- 在完全动态的运行时之上获得完整的 IDE 自动补全与 mypy/pyright 支持。
- Stubs 是手工维护的：每个新公开方法至少要同步 mixin、stub 与文档三处——一项没有自动化检查的持续同步成本。

## 来源

- `docs/superpowers/specs/2026-04-05-pyi-stubs-design.md` — 设计规格
- `docs/superpowers/plans/2026-04-05-pyi-stubs.md` — 实施计划（偏差、完整 stub 源码）
- `docs/api.cn.md` — 附录 A（pandas 迁移）、常见错误与解决、异常层级
- `README.md` — Developer Experience
- `py-ltseq/ltseq/*.pyi`、`py-ltseq/tests/test_deprecated_removed.py`
