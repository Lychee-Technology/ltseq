"""The literal grid: every literal kind in every position next to every context type (#243).

A cell is (context, literal, position). A context is a column of the fixture
table, or a CASE over two columns whose type DataFusion unifies. A literal is
a Python value. A position is where the literal meets the context: a
comparison (``eq``, ``gt``, and ``lt_mirror`` with the literal on the left),
membership (``isin`` alone, ``isin2`` beside the int ``1``), arithmetic
(``add``, ``sub``), a shared value (``fill``, ``coal``, ``coal_rev``,
``ifelse_t``, ``ifelse_f``), a ``shift`` default and, for temporal contexts,
``dt.diff``.

``run_cell`` derives the expression as column ``v`` and collects it. The
outcome is the Arrow type and the normalized values, or the error class, the
stage it was raised at and its first line. ``comparable`` drops the message;
it is what the gate in ``test_literal_grid.py`` compares.

The module imports ltseq only inside the functions that run cells, so

    python -I py-ltseq/tests/literal_grid/grid.py --commit SHA > expected/main.jsonl

records a baseline under whichever ltseq the interpreter imports. The first
line of the output names that build.
"""

import datetime as dt
import json
import math
import sys
import traceback
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow as pa

NY = ZoneInfo("America/New_York")
ROWS = 6
K = list(range(ROWS))


def _utc(seconds):
    return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)


# Six rows; rows 1 and 4 are NULL in every column so that value positions
# show the literal, and the others hold boundary values of the type.
COLUMNS = {
    "k": (K, pa.int64()),
    "i8": ([127, None, -128, 1, None, 0], pa.int8()),
    "i16": ([32767, None, -32768, 1, None, 0], pa.int16()),
    "i32": ([2**31 - 1, None, -(2**31), 2**24 + 1, None, 0], pa.int32()),
    "i64": ([2**63 - 1, None, -(2**63), 2**53 + 1, None, 0], pa.int64()),
    "u8": ([255, None, 0, 1, None, 128], pa.uint8()),
    "u32": ([2**32 - 1, None, 0, 2**24 + 1, None, 1], pa.uint32()),
    "u64": ([2**64 - 1, None, 0, 2**63, None, 1], pa.uint64()),
    "f32": ([16777216.0, None, 0.1, 1.5, None, 1e20], pa.float32()),
    "f64": ([2.0**53 + 2, None, 0.1, 1.5, None, 1e300], pa.float64()),
    "d32": ([Decimal("9999999.99"), None, Decimal("-0.01"), Decimal("1.50"), None, Decimal("0")], pa.decimal32(9, 2)),
    "d64": ([Decimal("999999999999999.999"), None, Decimal("-0.001"), Decimal("1.500"), None, Decimal("0")], pa.decimal64(18, 3)),
    "d5": ([Decimal("999.99"), None, Decimal("-0.50"), Decimal("1.50"), None, Decimal("0.00")], pa.decimal128(5, 2)),
    "d38": ([Decimal(10**27), None, Decimal("0.1234567890"), Decimal("-1.5"), None, Decimal("2.5")], pa.decimal128(38, 10)),
    "dneg": ([Decimal("100"), None, Decimal("-100"), Decimal("1E+9"), None, Decimal("0")], pa.decimal128(10, -2)),
    "d256": ([Decimal(10**44), None, Decimal("0.00001"), Decimal("1.5"), None, Decimal("0")], pa.decimal256(50, 5)),
    "date32": ([dt.date(1970, 1, 1), None, dt.date(2024, 1, 1), dt.date(1969, 12, 31), None, dt.date(2300, 1, 1)], pa.date32()),
    "date64": ([dt.date(1970, 1, 1), None, dt.date(2024, 1, 1), dt.date(1969, 12, 31), None, dt.date(2300, 1, 1)], pa.date64()),
    "ts_s": ([0, None, 1, -1, None, 1704067200], pa.timestamp("s")),
    "ts_ms": ([0, None, 1500, -1, None, 1704067200000], pa.timestamp("ms")),
    "ts_us": ([0, None, 1500000, -1, None, 1704067200000000], pa.timestamp("us")),
    "ts_ns": ([0, None, 1000000001, -1, None, 1704067200000000000], pa.timestamp("ns")),
    "ts_ny": ([_utc(18000), None, _utc(0), _utc(-1), None, _utc(1704067200)], pa.timestamp("us", "America/New_York")),
    "ts_utc": ([_utc(18000), None, _utc(0), _utc(-1), None, _utc(1704067200)], pa.timestamp("us", "UTC")),
    "str": (["1", None, "1.5", "a", None, "2024-01-01"], pa.string()),
    "bool": ([True, None, False, True, None, False], pa.bool_()),
}

# A CASE context takes column `a` where k > 2 and column `b` elsewhere; its
# type is DataFusion's unification of the two. DataFusion unifies a date with
# a timestamp as nanoseconds, so the date rows of `case_date_ts` are the first
# three, which lie within the nanosecond range (2300-01-01 does not).
CASES = {
    "case_i64_f64": ("i64", "f64"),
    "case_f64_i64": ("f64", "i64"),
    "case_d5_d38": ("d5", "d38"),
    "case_us_ns": ("ts_us", "ts_ns"),
    "case_i32_i64": ("i32", "i64"),
    "case_date_ts": ("ts_us", "date32"),
}

CONTEXTS = [name for name in COLUMNS if name != "k"] + ["dict_i64"] + list(CASES)
TEMPORAL_CONTEXTS = {name for name in CONTEXTS if name.startswith(("date", "ts_", "case_us", "case_date"))}

LITERALS = {
    "i0": 0, "i1": 1, "i5": 5, "im1": -1, "i127": 127, "i128": 128, "i255": 255, "i256": 256,
    "i2p24": 2**24, "i2p24p1": 2**24 + 1, "i2p31": 2**31, "i2p53": 2**53, "i2p53p1": 2**53 + 1,
    "imax": 2**63 - 1, "imin": -(2**63),
    "f0": 0.0, "fm0": -0.0, "f1": 1.0, "f1_5": 1.5, "f0_1": 0.1, "f2p53": 2.0**53,
    "f2p24": 16777216.0, "f2p24p1": 16777217.0, "f1e20": 1e20,
    "fnan": float("nan"), "finf": float("inf"), "fninf": float("-inf"),
    "D1_5": Decimal("1.5"), "D0_1": Decimal("0.1"), "D1_236": Decimal("1.236"), "D1E3": Decimal("1E+3"),
    "D1_50": Decimal("1.50"), "D38nines": Decimal("9" * 38), "Dfine33": Decimal("0." + "1" * 33),
    "D100": Decimal("100"), "D2p53p1": Decimal(2**53 + 1),
    "s1": "1", "s1_5": "1.5", "sdate": "2024-01-01", "bT": True, "none": None,
    "date2024": dt.date(2024, 1, 1), "date2300": dt.date(2300, 1, 1),
    "dt_mid": dt.datetime(2024, 1, 1), "dt_1_5s": dt.datetime(1970, 1, 1, 0, 0, 1, 500000),
    "dt_1500": dt.datetime(1500, 1, 1), "dt_utc": dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
    "dt_ny": dt.datetime(2024, 1, 1, tzinfo=NY),
}

POSITIONS = [
    "eq", "gt", "lt_mirror", "isin", "isin2", "add", "sub",
    "fill", "coal", "coal_rev", "ifelse_t", "ifelse_f", "shift_def", "dtdiff",
]
COMPARISON_POSITIONS = ["eq", "gt", "lt_mirror", "isin", "isin2"]
VALUE_POSITIONS = ["fill", "coal", "coal_rev", "ifelse_t", "ifelse_f"]
ARITHMETIC_POSITIONS = ["add", "sub"]


def literals():
    """The literal table, with the pandas nanosecond literal when pandas is installed."""
    table = dict(LITERALS)
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        return table
    table["pd_ns"] = pd.Timestamp(1_000_000_001, unit="ns")
    return table


def cells():
    """Every (context, literal, position), in a fixed order."""
    for ctx in CONTEXTS:
        for lit in literals():
            for pos in POSITIONS:
                if pos == "dtdiff" and ctx not in TEMPORAL_CONTEXTS:
                    continue
                yield ctx, lit, pos


def cell_id(ctx, lit, pos):
    return f"{ctx}/{lit}/{pos}"


def arrow_table():
    columns = {name: pa.array(values, typ) for name, (values, typ) in COLUMNS.items()}
    columns["dict_i64"] = columns["i64"].dictionary_encode()
    return pa.table(columns)


_TABLE = None


def table():
    global _TABLE
    if _TABLE is None:
        from ltseq import LTSeq

        _TABLE = LTSeq.from_arrow(arrow_table()).sort("k")
    return _TABLE


def context(name):
    """The context as a function of the row proxy."""
    from ltseq import if_else

    if name in CASES:
        a, b = CASES[name]
        return lambda r: if_else(r.k > 2, getattr(r, a), getattr(r, b))
    return lambda r: getattr(r, name)


def expression(ctx, lit, pos):
    """The cell's expression as a function of the row proxy."""
    return expression_on(context(ctx), lit, pos)


def expression_on(c, lit, pos):
    """The cell's expression with the context given as a function of the row proxy."""
    from ltseq import coalesce, if_else

    value = literals()[lit]
    return {
        "eq": lambda r: c(r) == value,
        "gt": lambda r: c(r) > value,
        "lt_mirror": lambda r: value < c(r),
        "isin": lambda r: c(r).is_in([value]),
        "isin2": lambda r: c(r).is_in([value, 1]),
        "add": lambda r: c(r) + value,
        "sub": lambda r: c(r) - value,
        "fill": lambda r: c(r).fill_null(value),
        "coal": lambda r: coalesce(c(r), value),
        "coal_rev": lambda r: coalesce(value, c(r)),
        "ifelse_t": lambda r: if_else(r.k % 2 == 0, value, c(r)),
        "ifelse_f": lambda r: if_else(r.k % 2 != 0, c(r), value),
        "shift_def": lambda r: c(r).shift(1, default=value),
        "dtdiff": lambda r: c(r).dt.diff(value, unit="second"),
    }[pos]


def normalize(value):
    """A JSON-ready, exact rendering of a Python value pyarrow produced."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        if value == 0 and math.copysign(1.0, value) < 0:
            return "-0.0"
        return value
    if isinstance(value, Decimal):
        return f"Decimal({value})"
    kind = type(value).__name__
    if kind == "Timestamp":  # pandas, for nanoseconds
        zone = "" if value.tz is None else f", tz={value.tz}"
        return f"Timestamp({value.value}{zone})"
    if kind == "Timedelta":
        return f"Timedelta({value.value})"
    if isinstance(value, dt.datetime):
        return f"datetime({value.isoformat()})"
    if isinstance(value, dt.date):
        return f"date({value.isoformat()})"
    if isinstance(value, dt.timedelta):
        return f"timedelta({value.days}, {value.seconds}, {value.microseconds})"
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return repr(value)


def _stage(error, phase):
    if phase == "collect":
        return "collect"
    frames = traceback.extract_tb(error.__traceback__)
    return "capture" if frames and "/ltseq/expr/" in frames[-1].filename.replace("\\", "/") else "plan"


def run_cell(ctx, lit, pos):
    """The normalized outcome of one cell on the imported ltseq."""
    return outcome(lambda: table().derive(v=expression(ctx, lit, pos)))


def outcome(derive):
    """The normalized outcome of ``derive()``, a table whose column ``v`` is collected."""
    phase = "plan"
    try:
        derived = derive()
        phase = "collect"
        column = derived.to_arrow().column("v")
        outcome = {"type": str(column.type), "values": normalize(column.to_pylist())}
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:  # a Rust panic is a BaseException
        message = str(error).splitlines()[0][:160] if str(error) else ""
        return {"error": {"class": type(error).__name__, "stage": _stage(error, phase), "msg": message}}
    # Round-trip through JSON so a live outcome compares equal to a recorded one.
    return json.loads(json.dumps(outcome))


def comparable(outcome):
    """The part of an outcome that must match: everything but the error message."""
    if "error" in outcome:
        return {"error": {"class": outcome["error"].get("class"), "stage": outcome["error"].get("stage")}}
    return outcome


def record(out, commit, only=None):
    import ltseq

    header = {"commit": commit, "ltseq": ltseq.__file__, "python": sys.version.split()[0]}
    out.write(json.dumps(header) + "\n")
    for ctx, lit, pos in cells():
        if only and ctx not in only:
            continue
        out.write(json.dumps({"cell": cell_id(ctx, lit, pos), **run_cell(ctx, lit, pos)}) + "\n")
        out.flush()


def load(path):
    """``{cell id: outcome}`` and the header of a recorded file."""
    lines = path.read_text().splitlines()
    header = json.loads(lines[0])
    outcomes = {}
    for line in lines[1:]:
        record_ = json.loads(line)
        outcomes[record_.pop("cell")] = record_
    return header, outcomes


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--commit", required=True, help="the commit of the build, stored in the header")
    parser.add_argument("--only", help="comma-separated contexts, for a partial run")
    args = parser.parse_args()
    record(sys.stdout, args.commit, args.only.split(",") if args.only else None)
