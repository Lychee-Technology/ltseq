<!-- v0.5-modular:header -->

# v0.5 contract: Exports and object model (§2–§3)

[Index](../README.md) › Contract · Previous: [Overview, principles and rule ownership (§1)](overview.md) · Next: [Loading and lazy evaluation (§4–§5)](loading-and-laziness.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** The names `ltseq` and `ltseq.typing` export, the type aliases used in signatures, and the object model: the public table types, their invariants, and the proxies lambdas receive.

**Most cited from here.** [§7] Basic table operations · [§8] Expression DSL · [§4] Loading · [§15] Streaming

<!-- /v0.5-modular:header -->

## 2. Exports

### 2.1 The `ltseq` namespace

`ltseq.__all__` MUST be exactly the following names, and `from ltseq import *` MUST import nothing else.

| Kind | Names |
|---|---|
| Table types | `LTSeq`, `NestedTable`, `GroupBy` |
| Expression type | `Expr` |
| Order metadata | `SortKey` |
| Expression functions | `lit`, `when`, `if_else`, `coalesce` |
| Ranking functions | `row_number`, `rank`, `dense_rank`, `ntile` |
| Math functions | `sqrt`, `exp`, `log`, `sign`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2` |
| Two-column aggregates | `corr`, `cov` |
| Clock functions | `now`, `today` |
| Exceptions | `LTSeqError`, `LTSeqTypeError`, `LTSeqValueError`, `ColumnNotFoundError`, `SchemaMismatchError`, `SortRequiredError`, `OrderViolationError`, `DuplicateKeyError`, `CastError`, `LTSeqIndexError`, `ArithmeticOverflowError`, `DivisionByZeroError`, `LTSeqIOError`, `SourceNotFoundError`, `ExecutionError` |

`ltseq.__version__` is a `str` (PEP 440). It is not in `__all__`.

### 2.2 Other modules

- `ltseq.typing` is a runtime module that holds typing names only: `Row`, `Group` (protocols for the lambda argument, [§8.1]), the builder and namespace classes `When` ([§8.3]), `Rolling` ([§10.1]), `StrNamespace` ([§8.6]) and `DtNamespace` ([§19.3]), and the aliases `DTypeLike`, `IntoExpr`, `ExprFn`, `AggFn`, `RowExpr`, `AggExpr`, `PathLike`, `Literal`. The classes are for annotations: constructing one directly raises `TypeError`. Importing the module has no side effects.
- The compiled extension is `ltseq._ltseq_core`. It is private: its names, signatures and behavior are not part of this contract, and it MUST NOT be re-exported.
- No other module or attribute of the package is public. Expression node classes (`BinOpExpr`, `CallExpr`, `ColumnExpr`, `UnaryOpExpr`, `LiteralExpr`, `LookupExpr`, `WindowExpr`), `SchemaProxy`, `NestedSchemaProxy`, `Cursor`, `LinkedTable`, `PartitionedTable` and `SQLPartitionedTable` are removed from the public surface.

### 2.3 Type aliases used in signatures

```python
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING
import datetime, decimal, os
import pyarrow as pa
if TYPE_CHECKING:
    import numpy                              # not a dependency; names the scalars §17.2 accepts

PathLike  = str | os.PathLike[str]
DTypeLike = str | pa.DataType                 # str: a pyarrow alias, see §6.3
type Literal = (None | bool | int | float | str | bytes | decimal.Decimal
                | datetime.date | datetime.datetime | datetime.time | datetime.timedelta
                | numpy.bool_ | numpy.integer | numpy.floating | numpy.datetime64 | numpy.timedelta64)
                # lazy, so importing ltseq.typing never imports NumPy; pandas Timestamp
                # and Timedelta are datetime and timedelta subclasses (§17.2)
IntoExpr  = Expr | Literal
ExprFn    = Callable[[Row], IntoExpr]         # a lambda in row context, §8.1
AggFn     = Callable[[Group], IntoExpr]       # a lambda in aggregate context, §8.1
RowExpr   = IntoExpr | ExprFn                 # a value: select, derive, update
AggExpr   = IntoExpr | AggFn                  # a value: agg, NestedTable.derive
```

A *value* argument is `RowExpr` or `AggExpr`: an `Expr`, a literal, or a lambda returning either (`derive(flag=False)`, `derive(x=None)` and `derive(c=lambda r: r.a + 1)` all type-check). A *condition* argument (`filter`, `delete`, `update`'s `where`, `search_first`, `search_pattern`, `starts_when`, `NestedTable.filter`) is `Expr | ExprFn` or `Expr | AggFn`, because no literal is a condition: a Python `bool` raises ([§3.4]), and any other literal is not of type `bool` ([§7.1]). `delete` and `update` also take a position, `int | numpy.integer` ([§7.10]). A condition inside an expression (the condition of `when` and `if_else`, an aggregate's `where=`, either operand of `&` and `|`) stays `IntoExpr`: after the rewrite of [§3.4], `when(r.x is None, 0)` passes what a type checker sees as a `bool`, so the annotation admits it, and [§3.4] refuses a Python `bool` there at plan time. A lambda's return type stays `IntoExpr`, `bool` included, for the same reason (`derive(m=lambda r: r.x is None)`); that a lambda returning a Python `bool` raises is checked when the lambda runs, not by the annotation.

---

## 3. Object model

### 3.1 Types

| Type | What it is | Created by | Lazy |
|---|---|---|---|
| `LTSeq` | An immutable table: a schema, an order state ([§9.1]) and a plan that produces its rows | Readers and constructors ([§4]), every table-returning method | Yes |
| `NestedTable` | A table partitioned into consecutive groups, in table order | `LTSeq.group_ordered` | Yes |
| `GroupBy` | A builder holding a table and grouping keys; its only method is `agg` | `LTSeq.group_by` | Yes |
| `Expr` | An opaque expression node built inside lambdas or by the functions of [§2.1] | Operators and methods on `Row`/`Group` proxies, functions | Not executable on its own |
| `SortKey` | `NamedTuple(column: str, descending: bool, nulls_last: bool)` | `LTSeq.sort_keys` | n/a |

v0.5 has no other user-visible table type. `LinkedTable` is replaced by `join` with `alias=` ([§12.1]), `PartitionedTable` by `partition()` returning a `dict` ([§14.5]), and `Cursor` by `pyarrow.RecordBatchReader` ([§15]).

### 3.2 `LTSeq` invariants

- **Immutable.** No method changes an existing `LTSeq`. Methods that read like mutation (`insert`, `delete`, `update`) return a new table ([§7.10]).
- **Not directly constructible.** `LTSeq()` MUST raise `TypeError`; tables come from the constructors of [§4].
- **Schema known at plan time.** `t.schema` and `t.columns` never execute the plan. Schema discovery of sources happens when the source is opened ([§4]).
- **Plans are re-executed.** Every terminal executes the plan again and re-reads its sources. `collect()` ([§5.2]) takes an in-memory snapshot when re-reading is not wanted.
- **Thread safety.** An `LTSeq` MAY be shared between threads; concurrent terminals on the same table are independent executions. Execution releases the GIL (ADR 0016).
- **Python protocols.**

| Protocol | Behavior |
|---|---|
| `repr(t)`, `str(t)` | Column names, types and order state. MUST NOT execute. |
| Notebook display | No `_repr_html_` or other display hook is defined, so notebooks show `repr` and displaying a table never executes. `t.show()` prints rows. |
| `len(t)` | Same as `t.count()`: executes. |
| `bool(t)` | Raises `TypeError` ("the truth value of a table is ambiguous; use `t.count() > 0`"). |
| `iter(t)` | Streams rows as `dict[str, Any]` ([§15.2]). |
| `t == u`, `hash(t)` | Python identity; content equality is not overloaded. |
| `t[...]` | Not supported: `TypeError`. |
| `copy.copy(t)`, `copy.deepcopy(t)` | Return an `LTSeq` with the same plan; never execute. |
| `pickle.dumps(t)` | Executes and stores a materialized snapshot ([§16.6]). |
| `t.__arrow_c_stream__(requested_schema=None)` | Arrow PyCapsule stream export ([§16.4]). |

### 3.3 `NestedTable` and `GroupBy` invariants

- Both are immutable, lazy and not directly constructible (`TypeError`).
- Neither supports `len`, `bool`, `iter`, `pickle` or the Arrow protocols; each of these raises `TypeError` naming the method that produces a table (`flatten()` or `agg()`).
- `NestedTable.count()` executes and returns the number of groups ([§11.2]).

### 3.4 Lambdas and proxies

Expressions are written as lambdas: `t.filter(lambda r: r.price > 10)`.

- LTSeq calls each lambda **exactly once, when the plan-building method is called**, with a proxy object. The lambda MUST return an `Expr` or a literal other than a Python `bool` (below). The return value is captured; the lambda is never called per row.
- An expression callable (`ExprFn`, `AggFn`) that cannot be called with exactly one positional argument raises `LTSeqTypeError` at plan time naming the method (`t.filter(lambda a, b: ...)`). A lambda that raises propagates its exception unchanged.
- Python values the lambda closes over are read at that call and become literals ([§17.2]). Changing the variable afterwards does not change the plan.
- Python control flow on an `Expr` raises `TypeError` at plan time: `Expr.__bool__` raises, so `and`, `or`, `not`, `if`, chained comparisons and `in` cannot be used. Use `&`, `|`, `~`, `when`, `is_in`.
- An attribute or item that names no column raises `ColumnNotFoundError` at plan time, with close matches in the message.
- **Identity tests.** Python cannot overload `is`, so LTSeq rewrites the lambda's own code before calling it. There, `x is None` and `x is not None` with an `Expr` operand mean `x.is_null()` and `x.is_not_null()`, as `== None` does ([§8.2]), and keep their Python meaning on Python values (`th is None` on a closure variable). Any other identity test with an `Expr` operand (`r.a is r.b`) raises `TypeError` at plan time, and so does a lambda whose identity tests cannot be rewritten (its source is unavailable).
- The rewrite does not reach functions the lambda calls, where `r.x is None` is the Python value `False`. To keep that from passing silently, a Python `bool` raises `LTSeqTypeError` at plan time in two places. One is where a condition is required: predicates (`filter`, `delete`, `update`, `search_first`, `search_pattern` steps, `starts_when`, `NestedTable.filter`), the condition of `when` and `if_else`, an aggregate's `where=`, and either operand of `&` and `|`. The other is the whole return value of any expression lambda, so `t.derive(missing=lambda r: is_missing(r.x))` raises instead of deriving a constant `False`. A constant is written `lit(False)` inside a lambda, or as a non-callable value (`derive(flag=False)`, [§7.3]).
- Forms that Python resolves before LTSeq sees a value stay undetectable: `helper(r) or expr` is `expr` and `helper(r) and expr` is `False` (then caught as above when it is the whole value); `a if helper(r) else b` is `b`; `~helper(r)` is the integer `-1` or `-2`, since Python's `~` on a `bool` is integer inversion, so it raises only where a `bool` is required; and a `bool` that a helper returns inside an expression (`if_else(r.c, is_missing(r.x), r.b)`) is the literal it looks like. Helper functions therefore write null tests as `.is_null()`.

<!-- v0.5-modular:footer -->

---

Previous: [Overview, principles and rule ownership (§1)](overview.md) · [Index](../README.md) · Next: [Loading and lazy evaluation (§4–§5)](loading-and-laziness.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§2.1]: #21-the-ltseq-namespace
[§3.4]: #34-lambdas-and-proxies
[§4]: loading-and-laziness.md#4-loading
[§5.2]: loading-and-laziness.md#52-ltseqcollect
[§7]: schema-and-table-operations.md#7-basic-table-operations
[§7.1]: schema-and-table-operations.md#71-filter
[§7.3]: schema-and-table-operations.md#73-derive
[§7.10]: schema-and-table-operations.md#710-value-level-edits-insert-delete-update
[§8]: expressions.md#8-expression-dsl
[§8.1]: expressions.md#81-contexts-and-proxies
[§8.2]: expressions.md#82-operators
[§8.3]: expressions.md#83-conditional-and-null-functions
[§8.6]: expressions.md#86-string-methods-str
[§9.1]: ordering.md#91-order-state
[§10.1]: windows-and-grouping.md#101-window-methods
[§11.2]: windows-and-grouping.md#112-nestedtable
[§12.1]: joins-and-sets.md#121-join
[§14.5]: aggregation.md#145-partition
[§15]: streaming-and-output.md#15-streaming
[§15.2]: streaming-and-output.md#152-iteration
[§16.4]: streaming-and-output.md#164-arrow-pycapsule-stream
[§16.6]: streaming-and-output.md#166-pickle
[§17.2]: numeric-null-temporal.md#172-literals
[§19.3]: numeric-null-temporal.md#193-dt-fields

<!-- /v0.5-modular:links -->
