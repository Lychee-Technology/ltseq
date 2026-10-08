"""Run differential cases and normalize their outcomes.

A case module defines functions ``case_<id>()`` that build a table and
return it (it is collected here) or return a plain value. ``run_case``
turns the outcome into JSON-ready data: per column, the Arrow type and
the values; or the error class, a message, and the stage it was raised
at. Two outcomes are the same case result when ``comparable`` gives
equal values, which ignores error messages.

This module imports nothing from ltseq. The case modules do, so the
build under test is whichever ltseq the running interpreter imports.
``test_differential.py`` uses it on the current build, and
``tools/diffprobe`` runs it under several builds side by side.
"""

import datetime as dt
import importlib.util
import json
import math
import traceback
from decimal import Decimal
from pathlib import Path

CASES_DIR = Path(__file__).with_name("cases")
EXPECTED_DIR = Path(__file__).with_name("expected")


def load_module(path):
    path = Path(path)
    spec = importlib.util.spec_from_file_location(f"differential_case_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def case_functions(module):
    """``(id, function)`` for every case, in source order."""
    cases = [
        (name[len("case_"):], fn)
        for name, fn in vars(module).items()
        if name.startswith("case_") and callable(fn)
    ]
    return sorted(cases, key=lambda item: item[1].__code__.co_firstlineno)


def case_files():
    return sorted(CASES_DIR.glob("*.py"))


def normalize(value):
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        return value
    if isinstance(value, Decimal):
        return f"Decimal({value})"
    if isinstance(value, (dt.datetime, dt.date, dt.time, dt.timedelta)):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if hasattr(value, "isoformat"):  # pandas.Timestamp, pandas.Timedelta
        return repr(value)
    return value


def _stage(error, phase):
    if phase == "collect":
        return "collect"
    frames = traceback.extract_tb(error.__traceback__)
    return "capture" if frames and "/ltseq/expr/" in frames[-1].filename.replace("\\", "/") else "plan"


def run_case(fn):
    """The normalized outcome of one case function."""
    phase = "plan"
    try:
        out = fn()
        phase = "collect"
        if hasattr(out, "to_arrow"):
            arrow = out.to_arrow()
            result = {
                name: {"type": str(arrow.column(name).type), "values": normalize(arrow.column(name).to_pylist())}
                for name in arrow.column_names
            }
        else:
            result = normalize(out)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:  # a Rust panic is a BaseException
        message = str(error).splitlines()[0][:300] if str(error) else ""
        return {"error": {"class": type(error).__name__, "stage": _stage(error, phase), "msg": message}}
    # Round-trip through JSON so a live outcome compares equal to a recorded one.
    return json.loads(json.dumps({"result": result}, default=str))


def comparable(outcome):
    """The part of an outcome that must match: everything but the error message."""
    if "error" in outcome:
        return {"error": {"class": outcome["error"]["class"], "stage": outcome["error"]["stage"]}}
    return outcome


def run_file(path, only=None):
    module = load_module(path)
    for case_id, fn in case_functions(module):
        if only and only not in case_id:
            continue
        yield {"id": case_id, **run_case(fn)}
