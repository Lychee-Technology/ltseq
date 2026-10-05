"""The literal wire contract (#145), driven by one fixture.

``fixtures/literal_protocol.json`` is the contract table: each ``encode`` row
names a Python value and the payload ``LiteralExpr`` must produce for it (or
the error it must raise at capture), and each accepted payload says what the
Rust decoder must turn it into, read back as an Arrow column. ``decode`` rows
are payloads Python never produces (hand-built or malformed) and what the
decoder must do with them. Both halves run against the real code: the Python
encoder, and the Rust decoder through ``LTSeq._inner``, so a rule that exists
on one side only fails a row.
"""

import json
import math
import subprocess
import sys
import textwrap
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import IntEnum
from fractions import Fraction
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ltseq import LTSeq
from ltseq.expr import LiteralExpr

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "literal_protocol.json").read_text())


class Color(IntEnum):
    RED = 1


class S(str):
    pass


SCOPE = {
    "np": np,
    "pd": pd,
    "date": date,
    "datetime": datetime,
    "time": time,
    "timedelta": timedelta,
    "timezone": timezone,
    "Decimal": Decimal,
    "Fraction": Fraction,
    "ZoneInfo": ZoneInfo,
    "Color": Color,
    "S": S,
}


def _evaluate(python: str):
    return eval(python, SCOPE)  # noqa: S307 - fixture expressions, written by us


def _same(a, b) -> bool:
    """Equal with exact types: True is not 1, 1 is not 1.0, NaN equals NaN."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, float):
        return (math.isnan(a) and math.isnan(b)) or (
            a == b and math.copysign(1, a) == math.copysign(1, b)
        )
    return a == b


def _derive_literal(payload: dict) -> pa.ChunkedArray:
    """Decode ``payload`` in Rust as a derived column and return that column."""
    table = LTSeq.from_arrow(pa.table({"i": pa.array([1], pa.int64())}))
    inner = table._inner.derive({"v": {"type": "Literal", **payload}})
    return LTSeq._from_inner(inner).to_arrow().column("v")


def _readback(column: pa.ChunkedArray):
    """The derived value in the fixture's readback form."""
    if pa.types.is_timestamp(column.type):
        return column.cast(pa.int64()).to_pylist()[0]
    if pa.types.is_date32(column.type):
        return column.cast(pa.int32()).to_pylist()[0]
    if pa.types.is_decimal(column.type):
        return column.to_pylist()[0]
    return column.to_pylist()[0]


def _check_arrow(column: pa.ChunkedArray, arrow: dict) -> None:
    assert str(column.type) == arrow["type"]
    got = _readback(column)
    want = arrow["value"]
    if pa.types.is_decimal(column.type):
        assert got == Decimal(want)
    elif isinstance(want, float):
        assert _same(float(got), want), (got, want)
    else:
        assert _same(got, want), (got, want)


def _ids(rows):
    return [row["id"] for row in rows]


ENCODE = FIXTURE["encode"]
ACCEPTED = [row for row in ENCODE if "payload" in row]
DECODE = FIXTURE["decode"]


@pytest.mark.parametrize("row", ENCODE, ids=_ids(ENCODE))
def test_encode(row):
    value = _evaluate(row["python"])
    if "error" in row:
        error_type = {"TypeError": TypeError, "ValueError": ValueError}[row["error"]["type"]]
        with pytest.raises(error_type, match=row["error"]["match"]) as raised:
            LiteralExpr(value)
        assert type(raised.value) is error_type
    else:
        payload = LiteralExpr(value).serialize()
        assert _same(payload, {"type": "Literal", **row["payload"]}), payload


@pytest.mark.parametrize("row", ACCEPTED, ids=_ids(ACCEPTED))
def test_accepted_payload_decodes_to_its_value(row):
    _check_arrow(_derive_literal(row["payload"]), row["arrow"])


@pytest.mark.parametrize("row", DECODE, ids=_ids(DECODE))
def test_decode(row):
    if "error" in row:
        with pytest.raises(ValueError, match=row["error"]):
            _derive_literal(row["payload"])
    else:
        _check_arrow(_derive_literal(row["payload"]), row["arrow"])


def test_unsupported_literal_fails_inside_the_lambda():
    """The error is raised where the user wrote the value, at capture."""
    table = LTSeq.from_arrow(pa.table({"a": pa.array([1, 2], pa.int64())}))
    with pytest.raises(TypeError, match="Unsupported literal type list"):
        table.filter(lambda r: r.a + [1, 2] > 0)
    with pytest.raises(TypeError, match=r"use \.is_in\(\[\.\.\.\]\) for membership"):
        table.filter(lambda r: r.a == [1, 2])


def test_plain_literals_encode_without_numpy_or_pandas():
    """numpy and pandas are optional: the encoder only touches them for their own scalars."""
    script = textwrap.dedent(
        """
        import sys

        class Block:
            def find_spec(self, name, path=None, target=None):
                if name.split(".")[0] in ("numpy", "pandas"):
                    raise ImportError(name)
                return None

        sys.meta_path.insert(0, Block())
        from datetime import date, datetime
        from decimal import Decimal
        from ltseq.expr import LiteralExpr

        for value in [1, 1.5, "a", None, True, Decimal("1.5"), date(2024, 1, 1), datetime(2024, 1, 1)]:
            LiteralExpr(value).serialize()
        assert "numpy" not in sys.modules and "pandas" not in sys.modules
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True, cwd=Path(__file__).parents[1])


def test_fixture_coverage():
    """Every dtype and field of the contract has a reject row of each kind."""
    dtype_fields = FIXTURE["dtype_fields"]
    rejected = [row for row in DECODE if "error" in row]

    def has_reject(predicate) -> bool:
        return any(predicate(row["payload"], row["error"]) for row in rejected)

    for dtype, fields in dtype_fields.items():
        assert any(row["payload"]["dtype"] == dtype for row in ACCEPTED), dtype
        assert has_reject(
            lambda p, e, d=dtype: p.get("dtype") == d and "unknown field" in e
        ), f"{dtype}: no unknown-key row"
        for field in fields:
            assert has_reject(
                lambda p, e, d=dtype, f=field: p.get("dtype") == d
                and f"field '{f}' must be" in e
            ), f"{dtype}.{field}: no wrong-type row"
            assert has_reject(
                lambda p, e, d=dtype, f=field: p.get("dtype") == d
                and f"missing field '{f}'" in e
            ), f"{dtype}.{field}: no missing-field row"
    # every accepted payload carries exactly its dtype's fields
    for row in ACCEPTED:
        payload = row["payload"]
        assert sorted(k for k in payload if k != "dtype") == dtype_fields[payload["dtype"]], row["id"]
