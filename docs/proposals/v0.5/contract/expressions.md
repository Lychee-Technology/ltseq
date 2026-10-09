<!-- v0.5-modular:header -->

# v0.5 contract: Expression DSL (§8)

[Index](../README.md) › Contract · Previous: [Schema, types and basic table operations (§6–§7)](schema-and-table-operations.md) · Next: [Ordering contract (§9)](ordering.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** The expression DSL: contexts and proxies, operators, conditional, NULL and math functions, the general `Expr` methods, the `.str` namespace, and the closed method set.

**Most cited from here.** [§17] Numeric semantics and literals · [§10] Windows and ordered computation · [§14] Aggregation, partitioning and pivot · [§3] Object model

<!-- /v0.5-modular:header -->

## 8. Expression DSL

### 8.1 Contexts and proxies

A lambda receives one of two proxies. Both are typed in `ltseq.typing`.

| Proxy | Passed by | A bare column reference means |
|---|---|---|
| `Row` | `filter`, `select`, `derive`, `update`, `delete`, `search_first`, `search_pattern`, `group_ordered(starts_when=)` | The column's value in the current row |
| `Group` | `GroupBy.agg`, `LTSeq.agg`, `NestedTable.agg`, `NestedTable.filter`, `NestedTable.derive` | In `NestedTable.derive`, the value in the current row; everywhere else, MUST appear inside an aggregate ([§14.2]) |

- `p.name` and `p["name"]` are column references. `Row` has no other attributes. `Group` has exactly one more, `count(*, where=None)`, the number of rows in the group ([§14.2]); `g["count"]` still reaches a column named `count`. The v0.4 group helpers are therefore gone: `g.sum("x")`, `g.first()`, `g.all(pred)` and `g.none(pred)` read `g.sum`, `g.first`, `g.all` and `g.none` as columns, so they raise `ColumnNotFoundError` when no such column exists. Their forms are `g.x.sum()`, `g.x.first()`, `(g.x > 0).all()` and `~(g.x > 0).any()` ([§14.2]).
- An aggregate method ([§14.2]) on an expression in a `Row` context without `.over()` ([§10.4]) raises `LTSeqTypeError`.
- Window methods ([§10]) are allowed in `Row` contexts and in `NestedTable.derive`, where they run within each group.
- `fold` ([§10.7]) is not an expression context: its callback receives plain Python values.

### 8.2 Operators

Operands may be expressions or literals on either side (`1 - r.x` works). All operators are vectorized and NULL-propagating unless the table says otherwise. Result types are in [§17].

| Operator | Meaning |
|---|---|
| `a + b`, `a - b`, `a * b` | Numeric arithmetic, checked: an integer or decimal result that does not fit its type raises `ArithmeticOverflowError`. Temporal operands follow [§19]. `str + str` concatenates. |
| `a / b` | True division ([§17.3]). |
| `a // b`, `a % b` | Floored division and its remainder, as in Python: `-7 // 2 == -4`, `-7 % 2 == 1`, `7 % -2 == -1` ([§17.3]). |
| `a ** b` | Power ([§17.3]). |
| `-a`, `abs(a)` | Negation and absolute value, checked: `-(-2**63)` raises `ArithmeticOverflowError`. |
| `round(a, n)`, `math.floor(a)`, `math.ceil(a)` | Same as `a.round(n)`, `a.floor()`, `a.ceil()` ([§8.5]). |
| `==`, `!=`, `<`, `<=`, `>`, `>=` | Comparison. NULL operands give NULL. Numeric comparisons compare exact values ([§17.4]); NaN follows [§18]. |
| `a == None`, `a != None` | Same as `a.is_null()` and `a.is_not_null()`. This applies when an operand is the literal `None` or `lit(None)`; ordering comparisons with them raise `LTSeqTypeError`. |
| `a & b`, `a \| b`, `~a` | Kleene AND, OR, NOT on `bool` expressions. `FALSE & NULL` is FALSE, `TRUE \| NULL` is TRUE. Either operand of `&` guards the other on rows where it is FALSE, and of `\|` where it is TRUE, in either order ([§20.2]). Non-`bool` operands, and a Python `bool` operand of `&` or `\|` ([§3.4]; write `lit(True)`), raise `LTSeqTypeError`. Python applies `~` to a `bool` itself ([§3.4]). |

Not supported, raising `LTSeqTypeError` at plan time: `^`, `<<`, `>>`, `@`, unary `+`, and the bitwise reading of `&`, `|`, `~` on integers. As [§3.4] states, `and`, `or`, `not`, `in`, `if` and chained comparisons raise `TypeError` through `Expr.__bool__`.

Operands MUST be type-compatible: a number or `bool` next to a string, or a `bool` next to a number, raises `LTSeqTypeError` ([§17.4]).

### 8.3 Conditional and NULL functions

```python
def lit(value: Literal, /) -> Expr
def when(condition: IntoExpr, value: IntoExpr, /) -> When      # an Expr with .when() and .otherwise()
def if_else(condition: IntoExpr, then: IntoExpr, otherwise: IntoExpr, /) -> Expr
def coalesce(*values: IntoExpr) -> Expr
```

- **`lit`** makes a literal expression with the type of [§17.2]. It is needed only where a method has to be called on a constant (`lit(None).cast("int64")`).
- **`when(c1, v1).when(c2, v2).otherwise(v)`** returns the value of the first branch whose condition is TRUE. A NULL condition is not taken. Without `.otherwise()` the default is NULL. `.when()` and `.otherwise()` exist only on the expression `when` returns; `.otherwise()` ends the chain. The one-argument `when(c).then(v)` form is removed.
- **`if_else(c, a, b)`** is `when(c, a).otherwise(b)`. It is kept as the two-branch form because it is the most common conditional and needs no chain.
- **`coalesce(v1, v2, ...)`** returns the first non-NULL value. NaN is not NULL. Fewer than two arguments raise `LTSeqValueError`.
- **Types.** Conditions MUST be `bool`. The values of `when`, `if_else` and `coalesce` are *shared values*: the result type is their common type and every literal among them MUST be exact in it ([§17.5]).
- **Evaluation.** A branch or argument is evaluated only for the rows that take it ([§20.2]): `if_else(r.d == 0, None, r.n // r.d)` never divides by zero.

### 8.4 Math functions

```python
def sqrt(x: IntoExpr, /) -> Expr
def exp(x: IntoExpr, /) -> Expr
def log(x: IntoExpr, /, base: float | None = None) -> Expr
def sign(x: IntoExpr, /) -> Expr
def sin(x, /), cos(x, /), tan(x, /), asin(x, /), acos(x, /), atan(x, /) -> Expr
def atan2(y: IntoExpr, x: IntoExpr, /) -> Expr
```

- Inputs MUST be numeric (`LTSeqTypeError`). Except for `sign`, the result is `float32` for `float32` input and `float64` otherwise; integer and decimal inputs convert to `float64` first.
- These are IEEE 754 functions and never raise for domain errors: `sqrt(-1)` and `log(-1)` are NaN, `log(0)` is `-inf`, `exp(1000)` is `inf`.
- `log(x)` is the natural logarithm; `log(x, base)` is `ln(x) / ln(base)`.
- `sign(x)` returns `-1`, `0` or `1` in the input type; for floats it returns NaN for NaN and `0.0` for both zeros.
- `power`, `ln`, `rand`, `gcd`, `lcm`, `factorial`, `char`, `concat_ws`, `ifa`, `nvl`, `skew` and `covar` are not part of v0.5 (review document, [Deliverable B]). The table constructor `seq` is `LTSeq.range` ([§4.7]).

### 8.5 General `Expr` methods

```python
def is_null(self) -> Expr
def is_not_null(self) -> Expr
def is_nan(self) -> Expr
def fill_null(self, value: IntoExpr, /) -> Expr
def fill_nan(self, value: IntoExpr, /) -> Expr
def is_in(self, values: Iterable[Literal], /) -> Expr
def between(self, low: IntoExpr, high: IntoExpr, /) -> Expr
def cast(self, dtype: DTypeLike, /) -> Expr
def try_cast(self, dtype: DTypeLike, /) -> Expr
def abs(self) -> Expr
def round(self, decimals: int = 0) -> Expr
def floor(self) -> Expr
def ceil(self) -> Expr
```

- **`is_null`, `is_not_null`** never return NULL. NaN is not NULL.
- **`is_nan`** is TRUE for NaN, FALSE otherwise, NULL for NULL. Floating-point input only (`LTSeqTypeError`).
- **`fill_null(v)`** is `coalesce(self, v)`.
- **`fill_nan(v)`** replaces NaN with `v` and leaves NULL alone. Floating-point input only; `v` is a shared value ([§17.5]).
- **`is_in(values)`** is a membership test and never returns NULL: a NULL input is TRUE if `None` is among `values` and FALSE otherwise; NaN is in `values` if NaN is. Values are compared exactly ([§17.4]), so `r.int_col.is_in([1.5])` is FALSE for every row. A value of an incompatible type (a string against a number) raises `LTSeqTypeError`. An empty `values` gives FALSE for every row. `values` MUST be literals; membership in another table is `semi_join` ([§12.2]).
- **`between(low, high)`** is `(self >= low) & (self <= high)`, so NULL bounds follow Kleene logic.
- **`cast`, `try_cast`** convert to `dtype` with the rules of [§17.6]. `cast` raises `CastError` during execution for a value the [§17.6] table says fails; `try_cast` gives NULL instead. Where a rule rounds or truncates (for example float to integer), both return that value. A conversion with no defined rule raises `LTSeqTypeError` at plan time.
- **`abs`** keeps the type, checked. **`round(decimals)`** rounds half to even, as Python's `round`, at `decimals` digits after the point (before it when negative), and keeps the type; for floats it rounds the exact binary64 value, so `round(2.675, 2) == 2.67`. For integers, a non-negative `decimals` returns the value unchanged; a result that does not fit raises `ArithmeticOverflowError`. **`floor`, `ceil`** keep the type.

Window methods (`shift`, `diff`, `pct_change`, `cum_sum`, `cum_min`, `cum_max`, `rolling`) are in [§10]. Aggregate methods (`sum`, `mean`, … `all`, `any`) and `over` are in [§10.4] and [§14.2]. `.dt` is in [§19].

### 8.6 String methods: `.str`

`expr.str` is defined for `string` and `large_string` expressions; on any other type it raises `LTSeqTypeError`. Every method returns NULL for a NULL input. Positions and lengths count Unicode code points, as Python's `str` does. String results are `string`, or `large_string` when the input is. The accessor `.s` is removed; `.str` is the only spelling.

| Method | Result | Semantics |
|---|---|---|
| `len()` | `int64` | `len(s)` |
| `lower()`, `upper()` | string | `s.lower()`, `s.upper()` (full Unicode case mapping: `"ß".upper() == "SS"`) |
| `strip(chars=None)`, `lstrip(chars=None)`, `rstrip(chars=None)` | string | `s.strip(chars)` and siblings: Unicode whitespace when `chars` is `None` |
| `startswith(prefix)`, `endswith(suffix)` | `bool` | `s.startswith(prefix)`, `s.endswith(suffix)`; the argument may be an expression |
| `contains_literal(substring)` | `bool` | `substring in s`. There is no bare `contains`, because pandas and Polars read that name as a regular expression and Python's `in` reads it literally |
| `contains_regex(pattern)` | `bool` | A match anywhere in `s` |
| `replace(old, new)` | string | `s.replace(old, new)`: every occurrence, literal |
| `replace_regex(pattern, replacement)` | string | Replaces every match; `$1` or `${name}` insert groups, `$$` a dollar sign |
| `slice(offset, length=None)` | string | `s[offset:][:length]`; a negative `offset` counts from the end |
| `find(substring)` | `int64` | `s.find(substring)`: 0-based, `-1` when absent |
| `split(delimiter, index)` | string | `s.split(delimiter)[index]`, NULL when `index` is out of range; negative `index` counts from the end |
| `rjust(width, fillchar=" ")`, `ljust(width, fillchar=" ")` | string | `s.rjust(width, fillchar)`, `s.ljust(width, fillchar)`; never truncates |
| `isalpha()`, `isdigit()`, `islower()`, `isupper()` | `bool` | Python's `str` methods, Unicode-aware; FALSE for `""` |

- Regular expressions use the syntax of the Rust `regex` crate. A `pattern` MUST be a literal `str`; an invalid one raises `LTSeqValueError` at plan time.
- `split` with an empty `delimiter`, `rjust` or `ljust` with a `fillchar` that is not exactly one character, a negative `length` in `slice`, or a negative `width` raise `LTSeqValueError`.
- Renamed to Python's spellings: `starts_with`/`ends_with` are `startswith`/`endswith`; `pad_left`/`pad_right` are `rjust`/`ljust`, which never truncate where the old methods cut a longer string to `width`; `regex_match` is `contains_regex`; `contains` is `contains_literal` or `contains_regex`.
- Removed with no replacement method, because each is a composition of the above: `left(n)` is `slice(0, n)`, `right(n)` is `slice(-n)` for `n > 0` and `""` for `n = 0`, the 1-based `pos(s)` is `find(s) + 1` (0 when absent), the 1-based `split_part(d, n)` is `split(d, n - 1)` (NULL where it gave `""`), `like` is `startswith`/`endswith`/`contains_literal`/`contains_regex`, `concat` is `+`. `ord` and `asc` are removed.

### 8.7 The closed method set

The methods listed in [§8] through [§19] are the complete set of `Expr` methods. An unknown attribute on an `Expr` or an accessor raises `AttributeError`, naming the closest valid method when one is close (`starts_with` → `startswith`; `contains` → `contains_literal` and `contains_regex`). An unknown attribute on a `Row` or `Group` proxy raises `ColumnNotFoundError`, which is also an `AttributeError`.

<!-- v0.5-modular:footer -->

---

Previous: [Schema, types and basic table operations (§6–§7)](schema-and-table-operations.md) · [Index](../README.md) · Next: [Ordering contract (§9)](ordering.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§3]: public-surface.md#3-object-model
[§3.4]: public-surface.md#34-lambdas-and-proxies
[§4.7]: loading-and-laziness.md#47-ltseqrange
[§8]: #8-expression-dsl
[§8.5]: #85-general-expr-methods
[§10]: windows-and-grouping.md#10-windows-and-ordered-computation
[§10.4]: windows-and-grouping.md#104-over
[§10.7]: windows-and-grouping.md#107-fold
[§12.2]: joins-and-sets.md#122-semi_join-and-anti_join
[§14]: aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: aggregation.md#142-aggregate-expressions
[§17]: numeric-null-temporal.md#17-numeric-semantics-and-literals
[§17.2]: numeric-null-temporal.md#172-literals
[§17.3]: numeric-null-temporal.md#173-arithmetic-operators
[§17.4]: numeric-null-temporal.md#174-types-of-mixed-operands
[§17.5]: numeric-null-temporal.md#175-shared-values
[§17.6]: numeric-null-temporal.md#176-explicit-casts
[§18]: numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§19]: numeric-null-temporal.md#19-temporal-semantics
[§20.2]: errors-and-performance.md#202-stages
[Deliverable B]: ../review/inventory.md#b-api-inventory-and-review

<!-- /v0.5-modular:links -->
