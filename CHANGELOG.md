# Changelog

## Unreleased

### Changed

Literal values in lambdas are typed (#145). They used to be sent to the engine as strings and parsed back, so some queries that ran before now raise, and a few return a different result. Expression types now come from DataFusion's coercion as the expression executes (#225). `docs/api.md` § "Literal values" describes the rules.

- An unsupported value in a lambda raises `TypeError` where it is written, instead of being turned into a string: `r.a == [1, 2]` (use `.is_in([...])`), `bytes`, `timedelta`, `datetime.time`, `Fraction`, `complex`. An integer outside Int64, a non-finite `Decimal` or one with more than 38 digits, and `pandas.NaT`/`pandas.NA` raise `ValueError`.
- A string is not a number for a method argument: `shift("1")`, `diff("1")`, `rolling("3")`, `ntile("4")`, `top_k("3")`, `percentile("0.5")` and `dt.add(days="1")` raise `ValueError` naming the method. Pass the number.
- A `Decimal`, `date` or `datetime` compared with a string column raises `ValueError`; it used to be compared as text.
- An aware `datetime` compared with a naive timestamp column raises `ValueError`; the column used to be read as UTC.
- A `datetime` compared with a date column compares instants: `r.d == datetime(2024, 1, 1, 6)` no longer matches 2024-01-01.
- A `date` or `datetime` compared with a numeric column raises `ValueError` when the query is built; it used to fail at collect.
- `fill_null`, `coalesce`, `if_else` and `when` keep DataFusion's common type of their values when every literal fits it exactly, and otherwise give a literal the other values' type when that type holds it exactly:
  - a `Decimal` fill value for an integer column gives a decimal column; it used to fail at collect;
  - one for a decimal column widens the type to keep its digits (`decimal(5, 2)` with `Decimal("1.236")` is `decimal(6, 3)`); it used to be rounded to the column's scale;
  - a `Decimal` that no 38-digit type holds together with the other values raises `ValueError` naming it; it used to be rounded;
  - dates and timestamps work in these positions, an aware `datetime` of another zone as its instant in the column's zone; they used to fail planning;
  - values with no common type raise `ValueError`: a date with a number, a number or Boolean with a date or timestamp column, a `datetime` with a time of day next to a date column (for an aware `datetime`, a time of day in UTC, as comparisons read dates), a `Decimal`/`date`/`datetime` with a string column, and an aware value with a naive column;
  - a string or Boolean literal, and a number next to a string column, keep DataFusion's reading: `r.p.fill_null("1.5")` on `decimal(5, 2)` is `1.50`, and `r.p.fill_null("1.236")` is the rounded `1.24` where `Decimal("1.236")` keeps its digits.
- `shift(default=)` must be a literal the column holds exactly. `1.5` or `Decimal("1.5")` for an integer column, `-1` for `uint64` and `300` for `int8` raise `ValueError` when the query is built; they used to be truncated, or to fail at collect. A number or Boolean default for a date or timestamp column raises too: `default=5` was 1970-01-06 on a date column and 5 µs past 1970 on a timestamp column. Defaults without an exact rule (a string, a Boolean for a number column) are cast by DataFusion, as before.
- `&` and `|` with `True`/`False` follow the other operand's type (#209). `r.a & True` on an Int64 column raises, as `r.a & r.flag` does; it used to return `r.a`. `r.s & False` on a string column raises; it used to select no rows.
- Expressions without a column are folded by DataFusion, with the types and results it gives the same arithmetic on columns (#193). `LiteralExpr(7) / 2` is the Int64 `3`, as `/` on two Int64 columns is; it used to be `3.5`. `LiteralExpr(2**62) * 4` wraps to `0` like Int64 column arithmetic; it used to be a float.
- `group_ordered(...).first().count()` uses its counting kernel only for predicates the kernel computes as DataFusion does. Predicates with UInt64 order comparisons, Int32/UInt32 or timestamp arithmetic, or a `shift()` with `default=` or `partition_by=` are counted on the general path, which is slower (`docs/BENCHMARK.md`).

### Fixed

- `Decimal` literals compare exactly with decimal and integer columns, also when the comparison would need more than 38 digits: `r.price > Decimal("1.236")` on `decimal(5, 2)` now selects 1.24, and `r.w == 10**18` on `decimal(38, 20)` is false instead of failing at collect (#227). They work in arithmetic with a float column.
- Literals compared with `decimal32`, `decimal64` and `decimal256` columns follow the same rules. A `Decimal` with more digits than a `decimal32` or `decimal64` column holds failed at collect, `decimal256(76, 0) >= Decimal("1.2345")` was true for 1, and an int compared with a negative-scale `decimal64` column panicked.
- `fill_null(1)`, `coalesce` and `if_else` with an integer literal keep a `decimal32` or `decimal64` column's type unless int64 holds every value of it. DataFusion's common type of those decimals and Int64 is int64, which rounded `1.23` to `1`, and failed at collect on `decimal64(18, -1)` values past the int64 range and on `decimal32(9, -1)` values, which Arrow scales up in 32 bits.
- An integer, float or `Decimal` literal compared with a decimal column of negative scale (`decimal128(5, -2)`) no longer panics in DataFusion's simplifier (#226). A fine-scale `Decimal` next to a very coarse column (`Decimal("0E-38")` and `decimal256(76, -14)`, 128 digits together) compares, matches and fills exactly. It used to fail at collect, and `if_else` returned strings.
- A timestamp literal finer than the column's unit was truncated. It now compares exactly in filters, windows, group filters and `is_in` (#200).
- `is_in` compares each item the way `==` does, also in a list of mixed kinds: on an Int64 column, `r.x.is_in([2**53 + 1, 0.5])` matched `2**53` too.
- A naive `datetime` in a DST gap or fold, compared with a timezone-aware column, raises `ValueError` naming the zone instead of failing inside the optimizer.
- A `datetime` before 1677 or after 2262 can be compared with timestamp columns.
- `dt.diff` and timestamp subtraction accept `date` and `datetime` literals, and read them as comparisons do: against an aware column, a `date` is local midnight and a naive `datetime` is wall-clock time. A date against a timestamp is no longer pushed through nanoseconds, so dates outside 1677–2262 work.
- An expression that mixes column types in a CASE (`if_else`, `when`) has the type it executes as. `dt.diff` on an `if_else` of a seconds and a microseconds column reported 10^6 times the elapsed seconds, and an empty result of such a column had the wrong dtype.
- Constant folding keeps integer precision and types: `2**53 + 1` is exact, and `2.0 + 3.0` stays a float (#193).
- `group_ordered(...).first().count()` returns the materialized count for the predicates listed under Changed; the counting kernel used to count some of them differently, for example across `shift(partition_by=)` partitions.
- An unsupported value passed to a method (`r.x.shift(1, default=b"x")`) raises inside the lambda, at the call.
- `shift(default=<expression>)` raises instead of ignoring the default.
- `search_pattern` predicates accept `True`, `False` and `None` literals.
