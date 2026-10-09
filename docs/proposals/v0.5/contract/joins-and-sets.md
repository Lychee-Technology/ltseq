<!-- v0.5-modular:header -->

# v0.5 contract: Joins and set operations (§12–§13)

[Index](../README.md) › Contract · Previous: [Windows and ordered grouping (§10–§11)](windows-and-grouping.md) · Next: [Aggregation, partitioning and pivot (§14)](aggregation.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** `join`, `semi_join`, `anti_join` and `asof_join`, then the set and bag operations `concat`, `intersect` and `difference`.

**Most cited from here.** [§18] NULL, NaN and Boolean logic · [§9] Ordering contract · [§20] Errors

<!-- /v0.5-modular:header -->

## 12. Joins

### 12.1 `join`

```python
def join(self, other: LTSeq, on: str | Sequence[str] | None = None, *,
         how: Literal["inner", "left", "right", "full", "cross"] = "inner",
         left_on: str | Sequence[str] | None = None,
         right_on: str | Sequence[str] | None = None,
         suffix: str = "_right", alias: str | None = None,
         validate: Literal["1:1", "1:m", "m:1", "m:m"] = "m:m") -> LTSeq
```

**Keys.** A non-cross join takes either `on` (the same names on both sides) or both `left_on` and `right_on` (equal length). A cross join takes neither. Any other combination raises `LTSeqValueError`. Keys MUST be column names.

**Key types.** Each key pair MUST belong to one class; otherwise `LTSeqTypeError` at plan time.

| Class | Members | Matching |
|---|---|---|
| Exact numeric | All integer types and all decimal types | By exact value: `Int32 7` matches `Decimal("7.00")` |
| Floating | `float32`, `float64` | By value; NaN matches NaN, `-0.0` matches `0.0` |
| String | `string`, `large_string` | By code points |
| Binary | `binary`, `large_binary`, `fixed_size_binary` | By bytes |
| Boolean | `bool` | |
| Date | `date32` | |
| Timestamp | `timestamp` of any unit | Naive with naive, aware with aware (by instant); naive with aware raises `LTSeqTypeError` |
| Duration, time | `duration`, `time32`/`time64` of any unit | By value |

An integer key does not match a float key, and a date key does not match a timestamp key: cast one side explicitly. NULL keys never match anything, including NULL.

**Output columns.**

- With `on=`, each key appears once, under its name, at the left key's position. Its value is the left key for `inner` and `left`, the right key for `right`, and for `full` the left key when it is non-NULL, else the right key (the two can differ in bits, as `-0.0` and `0.0` do). Its type is the common type of the pair.
- With `left_on`/`right_on`, both sides' key columns are kept.
- The left columns come first, in order, followed by the right columns not coalesced above, in order. A right column whose name is already in the output gets `suffix` appended; if that name is also taken, `LTSeqValueError`.
- With `alias=`, every right column, keys included, is renamed `f"{alias}_{name}"` and no key is coalesced. A resulting name that is already taken raises `LTSeqValueError`, and so does passing a non-default `suffix` together with `alias`. This replaces `link` (ADR 0011) without a lazy wrapper type: `trades.join(accounts, left_on="account", right_on="id", how="left", alias="acct")` returns the trade columns followed by `acct_id`, `acct_name`, and so on.
- Right columns are nullable in `left` and `full` joins, and left columns in `right` and `full` joins.

**Row order.** For `inner` and `left`: left rows in left order; the matches of one left row in right order (unspecified ties when the right order is undefined); a `left` row without a match appears once, at its position, with NULL right columns. For `cross`: left order, then right order within each left row. `right` and `full`: undefined order. `sort_keys` follows [§9.2].

**`validate`.** `"1:1"` requires the key to be unique among the non-NULL keys on both sides, `"1:m"` on the left, `"m:1"` on the right. A violation raises `DuplicateKeyError` during execution, naming the side and a duplicated key. `validate` demands the key of every row on each side it constrains, whatever the terminal ([§20.2]), so `head(1)` raises for a duplicate it would never return. A cross join accepts only `"m:m"`.

**Errors.** Unknown key `ColumnNotFoundError`; incompatible key types `LTSeqTypeError`; `other` not an `LTSeq` `LTSeqTypeError`.

Removed: `strategy=` (the engine chooses the algorithm, and no algorithm changes the result), the lambda form of `on`, `link`, `LinkedTable` and `Expr.lookup`. A lookup of one column is a `left` join with `validate="m:1"` followed by `select`.

```python
enriched = trades.join(symbols, on="symbol", how="left", validate="m:1")
```

### 12.2 `semi_join` and `anti_join`

```python
def semi_join(self, other: LTSeq, on: str | Sequence[str] | None = None, *,
              left_on: str | Sequence[str] | None = None,
              right_on: str | Sequence[str] | None = None) -> LTSeq
def anti_join(self, other: LTSeq, on: str | Sequence[str] | None = None, *,
              left_on: str | Sequence[str] | None = None,
              right_on: str | Sequence[str] | None = None) -> LTSeq
```

- `semi_join` keeps the left rows that have at least one match in `other`; `anti_join` keeps those that have none. Each left row appears at most once. Key rules as `join`.
- A left row with a NULL key has no match, so `anti_join` keeps it (the semantics of SQL `NOT EXISTS`, not `NOT IN`).
- The schema is the left schema. Keeps left order and `sort_keys`.

### 12.3 `asof_join`

```python
def asof_join(self, other: LTSeq, on: str | None = None, *,
              left_on: str | None = None, right_on: str | None = None,
              by: str | Sequence[str] | None = None,
              left_by: str | Sequence[str] | None = None,
              right_by: str | Sequence[str] | None = None,
              direction: Literal["backward", "forward", "nearest"] = "backward",
              tolerance: Literal | None = None,
              allow_exact_matches: bool = True,
              suffix: str = "_right") -> LTSeq
```

- **Behavior.** For each left row, considers the right rows whose `by` keys equal the left row's (all right rows when there are no `by` keys) and whose as-of key is not NULL or NaN, and picks:
  - `backward`: the greatest right key `<=` the left key (`<` when `allow_exact_matches=False`);
  - `forward`: the smallest right key `>=` the left key (`>`);
  - `nearest`: the right key with the smallest absolute distance, preferring the `backward` candidate on a tie of distances.

  Among right rows with the chosen key value, the last one in right order is used. With `tolerance`, a candidate further than `tolerance` from the left key is not a match.
- **Result.** Exactly one row per left row, in left order, with the right columns NULL when there is no match. Keeps the left `sort_keys`. Neither input needs to be sorted.
- **Keys.** The as-of key is one column per side, of the same class ([§12.1]) among exact numeric, floating, date, timestamp and duration; units are compared exactly. `by` keys follow `join`. NULL and NaN left keys never match.
- **`tolerance`.** A non-negative literal: a number exact in the key type for numeric keys, a `timedelta` for date, timestamp and duration keys. A negative value raises `LTSeqValueError`, a value of the wrong kind `LTSeqTypeError`.
- **Columns.** As `join` with `how="left"`: `on=` and `by=` coalesce the key, `left_on`/`right_on` and `left_by`/`right_by` keep both.
- **Order requirement.** `other` is in the positional tier (its order resolves ties). The left table needs no order.
- **Errors.** As `join`, plus: `on` with `left_on`/`right_on`, or `by` with `left_by`/`right_by`, raises `LTSeqValueError`.
- Removed: `is_sorted=` and `strategy=`.

```python
t.asof_join(quotes, on="ts", by="symbol", tolerance=timedelta(seconds=5))
```

---

## 13. Set and bag operations

All three methods compare whole rows and require the inputs to have exactly the same schema: the same column names, in the same order, with the same types. Nullability may differ. A mismatch raises `SchemaMismatchError` at plan time listing every difference, also when an input is empty. Row equality is that of [§18](numeric-null-temporal.md#18-null-nan-and-boolean-logic): NULL equals NULL, NaN equals NaN, `-0.0` equals `0.0`.

### 13.1 `concat`

```python
def concat(self, *others: LTSeq) -> LTSeq
```

- **Behavior.** The rows of `self`, then the rows of each of `others` in argument order. Duplicates are kept (bag semantics). `t.concat()` returns an equivalent table.
- **Order.** Defined when every input's order is defined: each input's rows in their own order, inputs in sequence. `sort_keys` is `None`.
- **Errors.** Schema mismatch as above (#222): v0.5 never promotes or casts column types in a concatenation; cast explicitly first.

### 13.2 `intersect` and `difference`

```python
def intersect(self, other: LTSeq, /, *, distinct: bool = True) -> LTSeq
def difference(self, other: LTSeq, /, *, distinct: bool = True) -> LTSeq
```

- With `distinct=True`: `intersect` returns each distinct left row that also occurs in `other`; `difference` returns each distinct left row that does not. Each result row is the first occurrence in left order.
- With `distinct=False` (multiset semantics): a row occurring *l* times on the left and *r* times on the right appears `min(l, r)` times in `intersect` and `max(l - r, 0)` times in `difference`, as its first occurrences in left order.
- Keeps left order and left `sort_keys`.
- When the left order is undefined, "first occurrence" picks no particular row: which of several equal rows ([§18]) represents a value, and which `min(l, r)` or `max(l - r, 0)` occurrences are kept, is unspecified, as for `distinct(keep="any")` (principle 9). Equal rows differ at most in the sign of a zero or the payload of a NaN.

Removed: `union`, an alias of `concat` that kept duplicates (`concat`; SQL's `UNION` is `concat(...).distinct()`); `subtract` and `except_` (`difference`, which deduplicates or counts occurrences; their own behavior, every left row with no equal right row and NULL never equal, is `anti_join(other, on=t.columns)`); `xunion` (`a.difference(b).concat(b.difference(a))`); `is_subset` (`a.difference(b).count() == 0`); `contain(k, *values)`, which returned whether every value occurs (`t.filter(lambda r: r.k.is_in(values)).select("k").distinct(keep="any").count() == len(set(values))`); `align` (a `left` join of a reference table), and the `on=` argument of `intersect`/`except_` (keyed set operations are `semi_join` and `anti_join`).

<!-- v0.5-modular:footer -->

---

Previous: [Windows and ordered grouping (§10–§11)](windows-and-grouping.md) · [Index](../README.md) · Next: [Aggregation, partitioning and pivot (§14)](aggregation.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.6]: overview.md#16-rule-ownership
[§9]: ordering.md#9-ordering-contract
[§9.2]: ordering.md#92-sources-and-propagation
[§12.1]: #121-join
[§18]: numeric-null-temporal.md#18-null-nan-and-boolean-logic
[§20]: errors-and-performance.md#20-errors
[§20.2]: errors-and-performance.md#202-stages

<!-- /v0.5-modular:links -->
