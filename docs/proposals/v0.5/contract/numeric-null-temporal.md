<!-- v0.5-modular:header -->

# v0.5 contract: Numeric, NULL and temporal semantics (§17–§19)

[Index](../README.md) › Contract · Previous: [Streaming, output and interchange (§15–§16)](streaming-and-output.md) · Next: [Errors and performance (§20–§21)](errors-and-performance.md)

**Normative.** One module of the LTSeq v0.5 public API contract. It uses the BCP 14 key words as the contract's [status block](../README.md#ltseq-v05-public-api-contract) declares, and [§1.6] names the section that owns each cross-cutting rule.

**Scope.** Value semantics: checked integer and decimal results, literals, operator result types, mixed operands, shared values and explicit casts; NULL, NaN and Boolean logic; temporal types, arithmetic, `.dt` fields and methods, and clock functions.

**Most cited from here.** [§16] Output and interchange · [§4] Loading · [§8] Expression DSL · [§14] Aggregation, partitioning and pivot

<!-- /v0.5-modular:header -->

## 17. Numeric semantics and literals

### 17.1 Integer and decimal results are checked

Every arithmetic operation and aggregate whose result type is an integer or decimal type computes its value exactly, except where its own rule rounds, and raises `ArithmeticOverflowError` during execution when that value does not fit the result type (#221). The rules that round are decimal `/` and the decimal `mean`, which truncate toward zero at the result's scale ([§17.3], [§14.2]), and `round`, which rounds half to even ([§8.5]). The operations include `+`, `-`, `*`, `/`, `//`, `%` and `**` where [§17.3] gives them an integer or decimal type, unary `-`, `abs`, `round`, `floor`, `ceil`, `sign`, `sum`, `cum_sum`, `diff`, and the aggregates of [§14.2]. Conversions are not covered here: `cast` and `try_cast` follow [§17.6], where a value that does not fit raises `CastError` (`try_cast` gives NULL) and the rounding rules are those of its table. No operation wraps around, saturates or returns NULL on overflow. This holds on every execution path ([§21.2]), including the paths that today use wrapping kernels (DataFusion's `SUM` uses `add_wrapping`).

The result type decides, not the operand types, with one exception: integer `/` raises `DivisionByZeroError` for a zero divisor although its result is `float64` ([§17.3]). Otherwise an operation with integer or decimal operands and a float result follows the float rules below: integer `/` and `**` with a decimal operand ([§17.3]), `pct_change` ([§10.1]), the math functions other than `sign` ([§8.4]), and the statistics with a `float64` result (`mean` of integers, `median`, `quantile`, `var`, `std`, `cov`, `corr`, [§14.2]). Integer `/`, `pct_change` and these statistics work from the exact operands; `**` and the math functions convert their operands to the nearest `float64` first ([§17.4]). So `r.dec ** 2` is `float64` where `dec` is `Decimal("1.1")`: `1.2100000000000002`, not `1.21`, and no error.

Floating-point arithmetic follows IEEE 754 binary64 (or binary32 for `float32` pairs): overflow gives `±inf`, invalid operations give NaN, and nothing raises.

### 17.2 Literals

A Python value used in an expression, or passed to `lit`, becomes a literal of this type:

| Python value | Literal type |
|---|---|
| `None` | NULL of type `null` |
| `bool`, `numpy.bool_` | `bool` |
| `int`, `numpy` integer | `int64`; a value in [`2**63`, `2**64`) is `uint64`. A value outside both ranges raises `LTSeqValueError` at plan time; write a `Decimal` for larger values. |
| `float`, `numpy` floating | `float64`, denoting the exact binary64 value (ADR 0018 D-j) |
| `str` | `string` |
| `bytes` | `binary` |
| `decimal.Decimal` | `decimal128(p, s)` sized to its digits; more than 38 digits, NaN or infinity raise `LTSeqValueError` |
| `datetime.date` | `date32` |
| `datetime.datetime` (naive) | `timestamp[us]` |
| `datetime.datetime` (aware) | `timestamp[us, tz]`, with `tz` the IANA key of a `zoneinfo`/`pytz` zone, `"UTC"` for `timezone.utc`, else the fixed offset `"±HH:MM"` of a `datetime.timezone`. An offset that is not a whole number of minutes, and any other `tzinfo`, raise `LTSeqValueError` at plan time, because no zone string of [§19.4] names them. The value is the instant in whole microseconds, `(value - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)`, which Python computes in integers from `value.utcoffset()`, so `fold` picks the occurrence of a repeated hour and no digit is rounded. `value.timestamp()` is not this value: it goes through a float and is a microsecond off for some values, such as `datetime(2300, 1, 1, microsecond=1, tzinfo=timezone.utc)`. |
| `datetime.time` | `time64[us]`. A `time` with a `tzinfo` raises `LTSeqValueError` at plan time, because a `time64` holds no zone |
| `datetime.timedelta` | `duration[us]`. A value whose count of microseconds does not fit `int64`, beyond about ±106,751,991 days (`timedelta.max` is 86,399,999,999,999,999,999 µs), raises `LTSeqValueError` at plan time |
| `pandas.Timestamp` | `timestamp` at the value's unit, with the zone of an aware value named as for `datetime.datetime` |
| `pandas.Timedelta` | `duration` at the value's unit |
| `numpy.datetime64` | Naive `timestamp` at the value's unit if Arrow has it (`s`, `ms`, `us`, `ns`), in seconds for a coarser unit (`Y`, `M`, `W`, `D`, `h`, `m`) and in nanoseconds for a finer one (`ps`, `fs`, `as`) |
| `numpy.timedelta64` | `duration` at the value's unit if Arrow has it, in seconds for `W`, `D`, `h` and `m` and in nanoseconds for `ps`, `fs` and `as` |

Arrow's only timestamp and duration units are `s`, `ms`, `us` and `ns`, so a NumPy value in another unit changes unit. A multiplied NumPy unit counts in its base unit: `np.datetime64(5, "10ms")` is 50 ms. Each conversion is exact or raises `LTSeqValueError` at plan time: a value finer than nanoseconds must be a whole number of them (`np.datetime64(1001, "ps")` raises), and the count in the literal type's unit must fit `int64` (`np.datetime64(2**60, "D")` raises, because 2**60 days do not fit in seconds). In both cases NumPy's own unit conversion returns a wrong value without an error. A `numpy.timedelta64` in years or months, or with no unit (`np.timedelta64(5)`), raises `LTSeqValueError`: years and months have no fixed length, and NumPy converts them with an average year and month, while a value with no unit takes whichever unit it meets. Write `dt.add(months=n)` for calendar arithmetic ([§19.4]). A `numpy.datetime64` in years or months is an instant, the start of that year or month, and converts.

`float("nan")` and `float("inf")` are valid `float64` literals. `pandas.NA`, `pandas.NaT`, `numpy.datetime64("NaT")` and `numpy.timedelta64("NaT")` raise `LTSeqValueError`: write `None`. Any other Python object raises `LTSeqTypeError` naming its type.

Literal arithmetic is folded by DataFusion with these types (ADR 0018 D-f): `0.1 + 0.2` is the binary64 sum, and `lit(2**62) * 4` raises `ArithmeticOverflowError`. Folding is an optimization and does not change stages or demand ([§20.2]): an error in a constant expression is raised during execution, and only if the value is demanded, so `if_else(r.d == 0, None, lit(1) // 0)` fails only for rows where `r.d` is not 0.

### 17.3 Arithmetic operators

| Expression | Operand types | Result type | Semantics |
|---|---|---|---|
| `a + b`, `a - b` | Decimal with decimal or integer | `decimal(min(max(p1 − s1, p2 − s2) + s + 1, P), s)` with `s = max(s1, s2)` | Exact, checked ([§17.1]) |
| `a * b` | Decimal with decimal or integer | `decimal(min(p1 + p2 + 1, P), s1 + s2)`; when `s1 + s2` exceeds `P`, `LTSeqTypeError` at plan time (cast an operand to a smaller scale first) | Exact, checked ([§17.1]) |
| `a / b` | Integers | `float64` | The exact quotient rounded once to the nearest `float64`, as Python's `int / int`, so operands above `2**53` are not rounded before dividing: `1 / 2` is `0.5`. A zero divisor raises `DivisionByZeroError` during execution, as Python's `int / int` raises `ZeroDivisionError`: `1 / 0` and `0 / 0` raise, for signed and unsigned operands, columns and literals alike (#218) |
| `a / b` | A float operand | `float64` (`float32` when both are `float32`) | IEEE division, a non-float operand converted to the result type first, as Python does. A zero divisor gives `±inf` or NaN and never raises: `1 / 0.0` and `1.0 / 0` are `inf`, `0.0 / 0.0` is NaN |
| `a / b` | Decimal with decimal or integer | `decimal(min(p1 − s1 + s2 + s, P), s)` with `s = min(s1 + 4, P)` | The quotient truncated toward zero at scale `s`; a zero divisor raises `DivisionByZeroError` |
| `a // b` | Integers | The common type of [§17.4] | Floored; a zero divisor raises `DivisionByZeroError` |
| `a // b` | A float operand | `float64` (`float32` when both are `float32`) | CPython's `float.__floordiv__`, which is not `floor(a / b)`: *m* = `fmod(a, b)` and *q* = (*a* − *m*) / *b*, minus 1 when *m* is nonzero and its sign differs from *b*'s; the result is `floor(q)`, plus 1 when *q* − `floor(q)` > 0.5 (a zero *q* gives a zero with the sign of *a* / *b*). So `1.0 // 0.1` is `9.0` where `floor(1.0 / 0.1)` is `10.0`. A zero divisor gives `±inf` or NaN as IEEE division does |
| `a // b` | Decimals, or decimal with integer | `decimal(P, 0)` | `floor` of the exact quotient; a zero divisor raises `DivisionByZeroError` |
| `a % b` | Integers | The common type of [§17.4] | `a - b * (a // b)`: the sign of the divisor, as Python; a zero divisor raises `DivisionByZeroError` |
| `a % b` | Decimals, or decimal with integer | `decimal(min(p2 − s2 + s, P), s)` with `s = max(s1, s2)` | As for integers: the remainder takes the divisor's sign and is smaller in magnitude than the divisor, so the divisor's integer digits bound it. Where `p2 − s2 + s` exceeds `P` and the type is clamped, a remainder that does not fit raises `ArithmeticOverflowError` ([§17.1]) |
| `a % b` | A float operand | `float64` (`float32` when both are `float32`) | Python's float `%`; a zero divisor gives NaN |
| `a ** b` | Integers | The type of `a` | Exact, checked; a negative exponent raises `LTSeqValueError` during execution |
| `a ** b` | A float operand | `float64` | IEEE `pow` |
| `a ** b` | A decimal operand | `float64` | IEEE `pow` on both operands converted to the nearest `float64`, as for a float operand; not checked ([§17.1]) |

**Integer result types.** One rule covers every integer operation, so whether a query raises depends on the values and the operand types, never on which operator was written:

| Operation on integers | Result type |
|---|---|
| `a + b`, `a - b`, `a * b`, `a // b`, `a % b` | The common type of [§17.4](#174-types-of-mixed-operands): the smallest signed or unsigned type that holds both operand types (`int8` with `int8` is `int8`, `int8` with `uint8` is `int16`, `int32` with `uint32` is `int64`). A signed type with `uint64` makes both operands `decimal128(20, 0)`, and the decimal rule of the operator gives the type: `decimal128(21, 0)` for `+` and `-`, `decimal128(38, 0)` for `*` and `//`, `decimal128(20, 0)` for `%`. An `int` literal is `int64` ([§17.2]), so `r.i8 + 1` is `int64` and `r.u64 - 1` is `decimal128(21, 0)`. |
| `a ** b` | The type of `a`, whatever the exponent's type: the exponent is a count, and `a ** 2` has the type of `a * a`. `r.u64 ** 1` is `uint64`; `r.i8 ** 2` is `int8`, checked. |
| Unary `-`, `abs(a)`, `round(a)` | The operand's type, so `-a` raises for every nonzero unsigned value |
| `a / b` | `float64` (above) |
| `sum`, `cum_sum` | `int64` for signed input, `uint64` for unsigned |
| `diff(n)` | `int64`, or `decimal128(20, 0)` for `uint64`, so a decreasing unsigned series never overflows |
| `mean`, `var`, `std`, `cov`, `corr`, `pct_change` | `float64` |

Every result is checked ([§17.1]): a value its type cannot hold raises `ArithmeticOverflowError`. For integer operands, the rule for `+`, `-`, `*` and `%` is DataFusion's coercion (ADR 0018 D-a), and `//` follows it.

In the decimal rows, `p1`, `s1` and `p2`, `s2` are the operands' precision and scale, after `decimal32` and `decimal64` operands are widened ([§17.4] rule 2). An integer operand counts as `decimal(d, 0)` with `d` = 3, 5, 10 or 20 for 8-, 16-, 32- and 64-bit integers. `P` is 76 when either operand is `decimal256`, else 38, and the result is `decimal256` or `decimal128` accordingly. The `+`, `-`, `*` and `/` formulas are DataFusion 55's (from Arrow's decimal kernels), written out so that an engine upgrade cannot change them silently. `%` is not: DataFusion's precision, `min(p1 − s1, p2 − s2) + s`, bounds a truncated remainder, and a floored one can be as large as the divisor (`decimal(4, 2)` −10.50 `%` `decimal(5, 2)` 360.00 is 349.50). `//` has no DataFusion rule; its integer quotient can need every digit, so it takes `P`.

**Zero divisors.** When neither operand is a float, a zero divisor raises `DivisionByZeroError` during execution in `/`, `//` and `%`. For `//`, `%` and decimal `/` the result type has no infinity or NaN. For integer `/` it has, but its operands are exact numbers with neither: an `inf` or NaN result would be a value no operand implies, and it would pass silently through later sums and comparisons, so the division raises as Python's does. When either operand is a float, the operation follows IEEE and never raises. NULL in either operand gives NULL and never raises, also beside a zero. A zero divisor that is not demanded does not raise ([§20.2]).

For `int` and `float` operands this makes `/`, `//` and `%` agree with Python, an integer zero divisor included, except for a zero divisor with a float operand: there `/`, `//` and `%` follow IEEE where Python raises `ZeroDivisionError`. Decimal `//` and `%` are floored as for integers, unlike Python's `decimal.Decimal`, which truncates toward zero (`Decimal(-7) // 2 == -3`); one operator has one meaning across numeric types ([§1.3]).

### 17.4 Types of mixed operands

Result types come from DataFusion's coercion rules (ADR 0018 D-a), with these overrides:

1. **Float with decimal** (#228): arithmetic and shared values ([§17.5]) between a float and a decimal give `float64`. Comparisons follow rule 3.
2. **`decimal32`/`decimal64`** (#241): they are widened to `decimal128` with the same precision and scale before any binary arithmetic or aggregate and wherever they meet another type, so the formulas of [§17.3] and [§14.2] only ever see `decimal128` and `decimal256`, and an integer next to a small decimal never fails to coerce. Two `decimal32(9, 2)` values 9999999.99 and 0.01 add to `decimal128(10, 2)` 10000000.00, not to an overflow of `decimal32`.
3. **Exact numeric comparison**: `==`, `!=`, `<`, `<=`, `>`, `>=`, `is_in`, `between`, set-operation equality, and join equality between the key classes [§12.1] allows (integers with decimals; join keys never pair an integer with a float) compare the exact mathematical values of their numeric operands, as Python compares `int` and `float`. `r.i64 == r.f64` is FALSE when the `int64` value is `2**53 + 1` and the float is `2.0**53`, where a cast to `float64` would make them equal. Between a float column and an integer or decimal column, NaN is greater than every number ([§18]). A NaN or infinite float *literal* next to an integer or decimal operand still raises `LTSeqValueError` at plan time (ADR 0018 D-j), because it is almost always a mistake. This extends ADR 0018 D-e and D-j from literals to columns and to float contexts.
4. **No implicit kind changes**: a number or `bool` next to a string, a `bool` next to a number, a string next to a number, date, time or timestamp, and a number or `bool` next to a date, time, timestamp or duration raise `LTSeqTypeError` at plan time, except an integer factor or divisor of a duration ([§19.2]). There is no implicit parsing of strings. Write `datetime.date(2024, 1, 1)`, not `"2024-01-01"`, or `cast` explicitly. A declared or requested type is stricter ([§17.6], way 5): it takes no value of another kind, with no exception for durations, so it never parses, formats or turns a `bool` into a number.

Other numeric pairs follow DataFusion: integer with integer gives the smallest common signed or unsigned type (`int64` with `uint64` gives `decimal128(20, 0)`), from which [§17.3] derives each integer operator's result type; integer with decimal gives a decimal that holds both. Integer with any float gives `float64`, and `float32` with `float32` gives `float32`. This overrides DataFusion and pyarrow, which give `float32` for an integer with a `float32` and so round integers above `2**24`; NumPy and pandas give `float64`. Wherever an integer or decimal value meets a float this way (rule 1, this paragraph, and [§17.5] rule 1 for shared values without a fixed target), it is converted to the nearest `float64` (half to even), as Python's `int + float` does, so `int64` values above `2**53` and decimal fractions such as `0.1` lose their exact value. This holds for a literal operand as for a column: `r.f + (2**53 + 1)` adds `2.0**53`, so it is `0.0` where `f` is `-2.0**53`, as in Python. Comparisons never convert (rule 3), and a literal placed into a shared value or a fixed target is exact or raises ([§17.5]).

### 17.5 Shared values

The values of `coalesce`, `fill_null`, `fill_nan`, `when`/`if_else` branches, `update`, `insert` and `shift(fill_value=)` *share* one result column. Their type is decided per expression node, and the same rules hold for nested nodes and every branch of a `when` chain. This revises ADR 0018 D-b and D-i, under which a literal could widen a column whose type the wider type holds, so that `r.i32.fill_null(1.5)` was `float64` while `r.i64.fill_null(1.5)` raised:

1. When at least one value is not a literal, let *C* be the common type of the non-literal values: DataFusion's, with the overrides of [§17.4] (so a float with a decimal gives `float64`). If every literal is exactly representable in *C*, the result type is *C*. Literals never widen it, so the result type does not depend on a column's integer width: `r.i64.fill_null(0.0)` and `r.i32.fill_null(0.0)` keep their types, and `coalesce(r.i32, r.f64)` is `float64`.
2. Otherwise, `CastError` at plan time naming the literal: `r.i32.fill_null(1.5)` and `r.i64.fill_null(1.5)` both raise. Cast the column first: `r.i32.cast("float64").fill_null(1.5)`.
3. When every value is a literal, the result type is their common type *U*, under the same overrides, and every literal MUST be exactly representable in *U*, else `CastError` at plan time (#248): `if_else(c, 2**53 + 1, 1.5)` raises, because `float64` cannot hold `2**53 + 1`, where DataFusion would round it. Each node is judged on its own, so `if_else(r.b, r.i, if_else(r.b, 2**53 + 1, 1.5))` raises at its inner node, and `if_else(c, Decimal("0.1"), 1.5)` raises because `float64` cannot hold `0.1`.

`update`, `insert` and `shift(fill_value=)` have a fixed target, the column's type, and use rule 1 with *C* equal to it (ADR 0018 D-c). A non-literal value placed into a fixed target (an `update` expression) MUST have a type whose every value *C* holds exactly, else `LTSeqTypeError` at plan time: a `float64` expression into an `int64` column, and an `int64` expression into an `int32` or a `float64` column, raise; cast it explicitly.

**Temporal values** follow the same rules, made concrete here:

- Naive and aware values never share a column, and neither do a date and a timestamp: `LTSeqTypeError` at plan time ([§19.2], ADR 0018 D-m). Cast the date first; `if_else(c, date(2300, 1, 1), datetime(2024, 1, 1, 12))` raises.
- Aware values of different zones share the zone of the first non-literal value in argument order, or of the first literal when every value is a literal. Every aware value contributes its instant, never its wall-clock reading (#246). `r.ts_ny.fill_null(datetime(2024, 1, 1, tzinfo=timezone.utc))` is `timestamp[us, tz=America/New_York]` holding 2023-12-31 19:00 New York time.
- Literals never widen a column's unit (rule 1). A literal finer than *C*'s unit MUST be exact at that unit, else `CastError` at plan time (#247, revising ADR 0018 D-m). `r.ts_s.fill_null(pd.Timestamp("2024-01-01 00:00:00.5"))` raises; `pd.Timestamp("2024-01-01")` keeps `timestamp[s]`. To keep sub-unit precision, `cast` the column to the finer unit first, which fails only for rows outside that unit's range.

### 17.6 Explicit casts

`cast` and `try_cast` ([§8.5]) convert under these rules. `cast` raises `CastError` during execution for a value the table says fails; `try_cast` returns NULL for it. Where a rule rounds or truncates, both return the rounded or truncated value: an explicit cast is how a user asks for rounding. Implicit conversions that place a supplied value into a type never round and never change its kind: a literal among shared values or into a fixed target ([§17.5]) and a value placed into a declared or requested type are exact compatible conversions (below), and a type that `read_csv`, `from_dict` or `from_rows` infers holds every integer exactly or the call raises ([§4.2], [§4.6]). Aggregates do not convert their inputs: a float statistic is the exact value rounded once ([§14.2]). Apart from reading float text, which takes the nearest value ([§16.5]), the only implicit conversion that rounds is that of an integer or decimal value to a float type: an operand, column or literal, of arithmetic with a float operand, of `**` with a decimal operand or of a math function other than `sign` ([§8.4], [§17.1], [§17.3], [§17.4]), and a non-literal value among shared values without a fixed target whose common type is a float ([§17.5] rule 1). It takes the nearest value and never fails.

**Which pairs have a rule.** Three steps decide it, and a pair that none of them gives a rule raises `LTSeqTypeError` at plan time:

1. A type to itself is the identity.
2. Within a kind, every pair has a rule except naive ↔ aware timestamps: the value is kept exactly, and a value the target cannot hold exactly fails, except where a row of the table rounds or truncates. The kinds are:
   - integers and decimals (the first row);
   - floats (`float16`, `float32`, `float64`): to a wider float is exact, to a narrower one takes the nearest value as the `float64` → `float32` row does;
   - four temporal kinds, each its own: dates (`date32`, `date64`), timestamps, durations, and times (`time32`, `time64`). Within each, a change of unit is as for timestamps, and for timestamps a change of zone is as its row, applied after the unit change; naive ↔ aware has no rule;
   - text (`string`, `large_string`, `string_view`);
   - bytes (`binary`, `large_binary`, `binary_view`, `fixed_size_binary`), where a value of another length fails into `fixed_size_binary`;
   - in the implicit conversions of a declared constructor type ([§4.6]), a `fold` `dtype` ([§10.7]), `requested_schema` ([§16.4]) and the Parquet writer ([§16.5]) only, nested types, whose children convert by exact compatible conversion (below): lists (`list`, `large_list`, `fixed_size_list`, where a list of another length fails), a `struct` to one with the same field names in the same order, and `map` to `map`. A dictionary type converts as its value type does. Every expression on a nested type raises ([§6.2]), so `cast` has none of these.

   No column of a table is `float16` or `date64`: [§6.2] converts them when read. These types therefore appear here only as the source of that conversion and as `requested_schema` targets. As the `dtype` of `cast` or `try_cast`, `pa.float16()` and `date64` raise `LTSeqTypeError` at plan time (the string `"float16"` raises `LTSeqValueError`, [§6.3]).
3. Between kinds, only the rows of the table apply. An implicit conversion uses only the rows among integers, decimals and floats and the `null` row (exact compatible conversion, below); the other rows serve `cast`, `try_cast` and CSV text alone.

| From → to | Rule |
|---|---|
| Integer, decimal → integer, decimal | Exact value; a value that does not fit fails. Rescaling a decimal to fewer fractional digits rounds half to even. |
| Integer, decimal → float | Nearest representable value (round half to even); never fails |
| Float → integer | Truncates toward zero; NaN, `±inf` and out-of-range values fail |
| Float → decimal | Rounds the binary64 value half to even at the target scale; NaN, `±inf` and out-of-range values fail |
| `float64` → `float32` | Nearest value; finite values beyond the `float32` range fail |
| `null` → any type | A NULL of the target type; never fails |
| `bool` ↔ number | `True` is 1, `False` is 0; a number to `bool` is `x != 0`, NaN fails |
| A type with a text form → string | The canonical text form ([§16.5]); a date or timestamp outside the years 1 to 9999 fails |
| String → a type with a text form | Parses the forms [§16.5] lists for the target type; anything else fails |
| `date32` → `timestamp` | Midnight; to an aware type, midnight in that zone (a nonexistent or ambiguous local midnight fails, [§19.4]). A date whose midnight does not fit the unit fails: in `ns`, one before 1677-09-22 or after 2262-04-11 |
| `timestamp` → `date32` | The (local, for an aware type) calendar date; a date that `date32` cannot hold fails (only `s` and `ms` values reach one) |
| `timestamp` → `timestamp`, other unit | Exact; a value with a nonzero remainder at a coarser unit fails, and so does one whose count at a finer unit does not fit `int64` (`timestamp[s]` 9223372036854776 to `ms`) |
| `timestamp` naive ↔ aware | No rule (`LTSeqTypeError`): use `.dt.replace_time_zone` or `.dt.convert_time_zone` ([§19.4]) |
| `timestamp` aware → aware, other zone | The same instant, relabelled |
| `duration` → `duration`, other unit | As timestamps |

**Five ways a value changes type.** Apart from the operand conversions of arithmetic, comparisons and functions, which [§17.3] and [§17.4] decide, the common type of the non-literal values that share a column ([§17.5]) and of a pair of join keys ([§12.1]), type inference ([§4.2], [§4.6], [§10.7]), the conversions between pandas and Arrow ([§4.5], [§16.2]) and conversion to Python ([§16.3]), every conversion in this contract is one of these. The first four happen; the fifth never does.

1. **Identity.** A value keeps its type (step 1).
2. **Exact compatible conversion.** The only conversion an implicit placement makes: into a declared `from_dict` or `from_rows` type ([§4.6]) or a `fold` `dtype` ([§10.7]), from each Python value by its kind and exact value ([§4.6]); a literal among shared values or into a fixed target, once [§17.5] has fixed the type; into a `requested_schema` type ([§16.4]); the Parquet writer's change of unit ([§16.5]); and the normalizations of [§6.2]. It applies step 2 within a kind and, between kinds, only the rows among integers, decimals and floats, which count as one kind here as they do in [§17.4] rule 4, and the `null` row, so NULL becomes a NULL of the target type. The value is kept exactly: a value that a row would round or truncate, or that the target cannot hold, fails with `CastError`. So `2` and `2.0` both go into `int64` and into `float64`, `int32` into `int64`, and a `timestamp[us]` of a whole second into `timestamp[s]`, while `1.5` into `int64`, `2**31` into `int32` and a `timestamp[us]` with a fraction of a second into `timestamp[s]` fail.
3. **Explicit cast.** `cast` and `try_cast` ([§8.5]) apply all three steps, every between-kind row included, and round or truncate where a row says so. A change of kind is asked for this way.
4. **CSV text.** `read_csv` with a declared type parses each field as `cast` from `string` does ([§4.2]), and `write_csv` writes each value's canonical text form as `cast` to `string` does ([§16.5]). A CSV field is text, so parsing it into the declared type is part of what the reader is asked to do; it grants no implicit parsing anywhere else.
5. **Implicit cross-kind conversion, forbidden.** Any other change of kind in an implicit placement raises `LTSeqTypeError`, even where `cast` would convert the value: at plan time for a literal ([§17.4] rule 4, [§17.5]), at the call for a declared or requested type, and during execution of the `fold` call for a `fold` `dtype` ([§10.7]). That covers a string into a number, `bool`, date, time or timestamp; a number or `bool` into a string; `bool` ↔ number; a date ↔ a timestamp; and a number into a temporal type. A declared or requested type represents values of its kind; it does not parse or format them. Give the value in its own kind, `date(2024, 1, 1)` rather than `"2024-01-01"`, or convert explicitly with `cast` ([§23.18]).

---

## 18. NULL, NaN and Boolean logic

- **NULL** is a missing value. It propagates through arithmetic, comparison and functions unless a function's contract says otherwise. `&`, `|` and `~` use Kleene three-valued logic. `filter` keeps only TRUE.
- **NaN** is a float value, not a missing value. It is never converted to NULL, or NULL to NaN, except by `from_pandas` ([§4.5]) and `to_pandas(dtype_backend="numpy_nullable")` ([§16.2]), where pandas does it.
- **Equality.** `NaN == NaN` is TRUE, and `-0.0 == 0.0` is TRUE, in comparisons, `is_in`, joins, grouping, `distinct`, set operations, `group_ordered` keys, window and `search_pattern` partitions, and `n_unique`. Every NaN bit pattern is the same value. (This departs from IEEE 754 and agrees with Polars and DuckDB; it is what makes grouping and joining by float keys well defined.)
- **Order.** NaN is greater than every other float, including `+inf`; `-0.0` and `0.0` are equal ([§9.4]). `<`, `<=`, `>`, `>=` use the same order.
- **NULL equality in comparisons.** A comparison with a NULL operand is NULL; `a == None` is `a.is_null()` ([§8.2]). Grouping, `distinct`, set operations and `group_ordered` treat NULL as equal to NULL; joins do not match NULL keys.
- **Representatives.** Where an aggregate (`min`, `max`, `mode`, `cum_min`, `cum_max`) or a grouping key (`group_by`, `group_ordered`, `pivot` column values) yields one value for several values that are equal here but differ in representation, it yields `0.0` for the zeros and the quiet NaN `float("nan")` for NaNs. A `partition` key is `0.0` for the zeros and refuses NaN ([§14.5]). Rows that an operation passes through (`filter`, `distinct`, `first`/`last`, joins, set operations) keep their values bit for bit.
- **Booleans** are not numbers: `True + 1` and `r.flag > 0` raise `LTSeqTypeError`. Count TRUE values with `g.count(where=g.flag)`.

---

## 19. Temporal semantics

### 19.1 Types

Dates are `date32`. Timestamps are `timestamp[unit]` (naive, a wall-clock reading) or `timestamp[unit, tz]` (aware, an instant displayed in zone `tz`). Durations are `duration[unit]`. Calendar-unit arithmetic is a method (`dt.add`), not a type.

### 19.2 Arithmetic and comparison

| Expression | Result | Notes |
|---|---|---|
| `timestamp ± duration` | Timestamp, finer of the two units | Checked. On an aware timestamp, the duration is added to the instant. |
| `date ± duration` | `date32` | Checked. A duration that is not a whole number of days raises `CastError`: at plan time for a literal, during execution for a row. |
| `timestamp − timestamp` | `duration`, finer unit | Checked. Both naive, or both aware: aware operands subtract instants even when their zones differ (#246). |
| `date − date` | `duration[s]` | |
| `duration ± duration` | `duration`, finer unit | Checked |
| `duration * integer`, `integer * duration`, `duration // integer` | `duration`, the duration's unit | Exact in that unit and checked: `*` multiplies the count of units, and `//` is the mathematical floor of the count divided by the integer, as integer `//` is, negative values included. `duration[s]` 5 s `* 2` is 10 s, `duration[s]` −5 s `// 2` is −3 s, and `duration[ms]` −5000 ms `// 2` is −2500 ms. The integer is of any signed or unsigned type, a column or a literal. During execution, a result the unit's `int64` count cannot hold raises `ArithmeticOverflowError` and a zero divisor raises `DivisionByZeroError`; a NULL operand gives NULL. |
| Comparisons | `bool` | Exact across units. Aware values compare instants. A date compared with a naive timestamp is midnight of that date. |

Naive with aware raises `LTSeqTypeError` in every operation, as Python does for `datetime`; so does a date with an aware timestamp. An integer factor or divisor of a duration, in the row above, is the only number an operator takes with a temporal value. Any other number or `bool` with a date, time, timestamp or duration raises `LTSeqTypeError` at plan time: `r.ts + 1`, `r.ts > 5`, `r.d + 1`, `r.d > 5`, `r.d * 1.5`, `r.d / 2`, `r.d * Decimal(2)` and `r.d * True`. ADR 0018 D-l states the rule for dates and timestamps. Durations add the integer exception because scaling by a count is exact and is what Python, pandas and Arrow offer; a float factor or a true quotient would round the duration, which Python's `timedelta * 1.5` and `timedelta / 2` do and LTSeq never does implicitly ([§17.6]), so the scaling is written with integers (`r.d * 3 // 2`).

Any other pairing, such as `timestamp + timestamp` or `date + timestamp`, raises `LTSeqTypeError` at plan time.

Integer scaling never produces a unit finer than the duration's, so it is unit-sensitive: equal elapsed times in different units can give different quotients. −5 s `// 2` is −3 s in `duration[s]` and −2.5 s in `duration[ms]`. This is not Python's `timedelta // 2`, which floors in microseconds whatever the value's origin and gives −2.5 s for −5 s. To keep a finer quotient, cast to the finer unit first: `r.d.cast("duration[ms]") // 2` is −2500 ms where `d` is `duration[s]` −5 s, and the cast fails for a value the finer unit cannot hold ([§17.6]). No operation widens the unit implicitly. When two durations of different units meet first (`duration ± duration`), the finer unit is the result's, and a later `//` floors in it.

Aware arithmetic is on instants within one zone too, which departs from Python's `datetime`. In New York, 2024-03-10 03:30 EDT minus 01:30 EST is one hour in LTSeq; Python gives two hours, because when both operands share a `tzinfo` it subtracts wall-clock readings, and it adds a `timedelta` to the wall clock in the same way ([`datetime` docs](https://docs.python.org/3/library/datetime.html), supported operations, note 3). LTSeq follows pandas, where `pd.Timestamp` arithmetic gives one hour (checked with pandas 3.0.5): an aware value is an instant, so a result never depends on whether two values happen to share a zone. Calendar arithmetic in local time is `dt.add` ([§19.4]), which raises at a DST gap or overlap instead of guessing.

### 19.3 `.dt` fields

`expr.dt` is defined for `date32` and `timestamp` expressions, for `time` expressions (the time fields only) and for `duration` expressions (`total_seconds` and `days` only, [§19.4]); any other method or type raises `LTSeqTypeError` at plan time. Fields of an aware timestamp are those of its local time in its zone.

| Method | Result | Value |
|---|---|---|
| `year()`, `month()`, `day()` | `int32` | Calendar fields |
| `hour()`, `minute()`, `second()` | `int32` | Clock fields |
| `millisecond()`, `microsecond()`, `nanosecond()` | `int32` | The sub-second part in that unit (`0`–`999`, `0`–`999_999`, `0`–`999_999_999`) |
| `weekday()` | `int32` | Monday 0 … Sunday 6, as `datetime.weekday()` |

### 19.4 `.dt` methods

```python
def truncate(self, unit: Literal["year", "quarter", "month", "week", "day", "hour", "minute", "second"], /) -> Expr
def add(self, *, years: IntoExpr = 0, months: IntoExpr = 0, weeks: IntoExpr = 0, days: IntoExpr = 0) -> Expr
def total_seconds(self) -> Expr          # on a duration
def days(self) -> Expr                   # on a duration
def replace_time_zone(self, tz: str | None, /) -> Expr
def convert_time_zone(self, tz: str, /) -> Expr
```

- **`truncate(unit)`** rounds down to the start of the unit, in local time for aware values; weeks start on Monday. The type is unchanged.
- **`add(...)`** adds calendar units to a date or timestamp in local time: years and months first, clamping the day to the month's length (`2024-01-31` + 1 month is `2024-02-29`), then weeks and days. The time of day is kept. Each argument is an `int` literal or an integer expression evaluated per row, so `r.invoice_date.dt.add(days=r.terms)` adds each row's terms; a NULL argument gives NULL, and a non-integer argument raises `LTSeqTypeError` at plan time.
- **`total_seconds()`** is the duration in seconds as `float64`; **`days()`** is the whole number of days, floored, as `int64`.
- **`replace_time_zone(tz)`** reads a naive timestamp as wall-clock time in `tz` (giving an aware one), or with `tz=None` drops an aware timestamp's zone keeping its local time.
- **`convert_time_zone(tz)`** keeps the instant and changes the zone of an aware timestamp. A naive input raises `LTSeqTypeError`.
- **DST.** A local time that does not exist (in a spring-forward gap) or is ambiguous (in a fall-back overlap), produced by `add`, `truncate`, `replace_time_zone` or a `cast` from date to an aware timestamp ([§17.6]), raises `LTSeqValueError` (`CastError`, its subclass, for `cast`) during execution. LTSeq never shifts or picks a side silently.
- A time zone string MUST be an IANA name, `"UTC"` or `"±HH:MM"`; anything else raises `LTSeqValueError` at plan time.

`dt.diff` is removed (subtract and use `total_seconds()` or `days()`), and so is `dt.age`. Its `month` and `year` units counted calendar fields, not elapsed time; they are `(r.a.dt.year() - r.b.dt.year()) * 12 + (r.a.dt.month() - r.b.dt.month())` and `r.a.dt.year() - r.b.dt.year()`. The v0.4 `dt.add(hours=, minutes=, seconds=)` is `+ timedelta(...)`.

### 19.5 Clock functions

`now()` is the current instant as `timestamp[us, tz=UTC]`; `today()` is the current UTC date as `date32`. Each is fixed for one execution: every row and every reference sees the same value.

<!-- v0.5-modular:footer -->

---

Previous: [Streaming, output and interchange (§15–§16)](streaming-and-output.md) · [Index](../README.md) · Next: [Errors and performance (§20–§21)](errors-and-performance.md)

<!-- /v0.5-modular:footer -->

<!-- v0.5-modular:links -->

[§1.3]: overview.md#13-design-principles
[§1.6]: overview.md#16-rule-ownership
[§4]: loading-and-laziness.md#4-loading
[§4.2]: loading-and-laziness.md#42-ltseqread_csv
[§4.5]: loading-and-laziness.md#45-ltseqfrom_pandas
[§4.6]: loading-and-laziness.md#46-ltseqfrom_dict-and-ltseqfrom_rows
[§6.2]: schema-and-table-operations.md#62-supported-types
[§6.3]: schema-and-table-operations.md#63-data-type-arguments-dtypelike
[§8]: expressions.md#8-expression-dsl
[§8.2]: expressions.md#82-operators
[§8.4]: expressions.md#84-math-functions
[§8.5]: expressions.md#85-general-expr-methods
[§9.4]: ordering.md#94-sort
[§10.1]: windows-and-grouping.md#101-window-methods
[§10.7]: windows-and-grouping.md#107-fold
[§12.1]: joins-and-sets.md#121-join
[§14]: aggregation.md#14-aggregation-partitioning-and-pivot
[§14.2]: aggregation.md#142-aggregate-expressions
[§14.5]: aggregation.md#145-partition
[§16]: streaming-and-output.md#16-output-and-interchange
[§16.2]: streaming-and-output.md#162-to_pandas
[§16.3]: streaming-and-output.md#163-to_dicts
[§16.4]: streaming-and-output.md#164-arrow-pycapsule-stream
[§16.5]: streaming-and-output.md#165-writers
[§17.1]: #171-integer-and-decimal-results-are-checked
[§17.2]: #172-literals
[§17.3]: #173-arithmetic-operators
[§17.4]: #174-types-of-mixed-operands
[§17.5]: #175-shared-values
[§17.6]: #176-explicit-casts
[§18]: #18-null-nan-and-boolean-logic
[§19.2]: #192-arithmetic-and-comparison
[§19.4]: #194-dt-methods
[§20.2]: errors-and-performance.md#202-stages
[§21.2]: errors-and-performance.md#212-fast-paths
[§23.18]: examples-semantics.md#2318-arrow-and-pandas-round-trip

<!-- /v0.5-modular:links -->
