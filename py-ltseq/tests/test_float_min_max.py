"""Float `min` and `max` in the contract's order on every path (#253).

Contract §14.2 orders `min` and `max` by §9.4: every NaN, whatever its sign
bit and payload, is greater than `+inf`, and `-0.0` equals `0.0`. So `max`
is NaN when a NaN takes part, `min` is NaN only when every value is NaN, and
both are NULL when no non-NULL value takes part. When the values equal to
the result differ in representation, the result is §18's representative:
`0.0` for the zeros, Python's `float("nan")` for the NaNs; otherwise it keeps
their bits. Results are compared bit for bit.

DataFusion 55 gave three answers: its grouped accumulators let a NaN replace
the running value (the grouped `min` of `[1.0, NaN, 2.0]` was `2.0`) and
started at the largest finite value (the grouped `max` of `[-inf, -inf]` was
`-1.797e308`); its other accumulators put a NaN with the sign bit set below
`-inf`; and an ungrouped aggregate over Parquet was answered from statistics,
which leave NaN out. The cases cover every row order of `[1.0, NaN, 2.0]`
with NaNs of both signs and another payload, the NaNs that `inf - inf` and
`0.0 / 0.0` compute, and the zeros, infinities and NULLs where they meet.
Each path runs at every engine layout of batch sizes 1 to 3 and from memory
and Parquet inputs; CSV layouts are left out, since CSV text has one NaN.
"""

from __future__ import annotations

import itertools
import math
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ltseq import LTSeq
from ltseq.expr.base import max_if, min_if

from . import layouts

NAN = math.nan
NEG_NAN = math.copysign(math.nan, -1.0)
# A quiet NaN whose payload is 1 rather than 0.
PAYLOAD_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF8_0000_0000_0001))[0]
INF = math.inf

ROW_ORDERS = [
    [1.0 if i == 0 else nan if i == 1 else 2.0 for i in order]
    for nan in (NAN, NEG_NAN, PAYLOAD_NAN)
    for order in itertools.permutations(range(3))
]
EDGES = [
    [NAN, NEG_NAN],
    [NEG_NAN, NEG_NAN],
    [None, None],
    [-INF, -INF],
    [INF],
    [-0.0, 0.0, -0.0],
    [-0.0, None, -0.0],
    [PAYLOAD_NAN, None, INF, -1.0],
]
CASES = ROW_ORDERS + EDGES

# Batch sizes 1 to 3 at one partition, two and twice the CPU count.
ENGINE_LAYOUTS = {
    f"{p_id}-b{batch_size}": (partitions, batch_size)
    for p_id, partitions in layouts.PARTITION_COUNTS.items()
    for batch_size in (1, 2, 3)
}
INPUTS = ("one_batch", "many_batches", "parquet_file", "parquet_row_groups", "parquet_files")
PARQUET_INPUTS = INPUTS[2:]


@pytest.fixture(params=list(ENGINE_LAYOUTS.values()), ids=list(ENGINE_LAYOUTS))
def small_batches(request, engine_config):
    partitions, batch_size = request.param
    return engine_config(target_partitions=partitions, batch_size=batch_size)


def bits(value):
    """A float's bits, so that NaNs and zeros compare by representation."""
    return None if value is None else struct.pack("<d", value)


def contract(values, extreme):
    """The contract's `min` or `max`, computed from its text."""
    present = [v for v in values if v is not None]
    if not present:
        return None
    nans = [v for v in present if math.isnan(v)]
    numbers = [v for v in present if not math.isnan(v)]
    if (extreme is max and nans) or not numbers:
        equal = nans
    else:
        best = extreme(numbers)
        equal = [v for v in numbers if v == best]
    if len({bits(v) for v in equal}) == 1:
        return equal[0]
    return NAN if math.isnan(equal[0]) else 0.0


def contract_min(values):
    return contract(values, min)


def contract_max(values):
    return contract(values, max)


def assert_same(actual, expected, context):
    assert bits(actual) == bits(expected), f"{context}: {actual!r} != {expected!r}"


def grouped_rows(cases):
    """Every case as a group `g` of consecutive rows, in table order `i`."""
    rows = [(g, x) for g, case in enumerate(cases) for x in case]
    return pa.table(
        {
            "g": pa.array([g for g, _ in rows], pa.int64()),
            "i": pa.array(range(len(rows)), pa.int64()),
            "c": pa.array(["v"] * len(rows)),
            "x": pa.array([x for _, x in rows], pa.float64()),
        }
    )


def by_group(rows, key="g"):
    return {row[key]: row for row in rows}


@pytest.mark.parametrize("source", INPUTS)
def test_grouped_paths_follow_the_contract(small_batches, source, tmp_path):
    t = layouts.build(source, grouped_rows(CASES), tmp_path)
    sorted_t = t.sort("i")
    nested = sorted_t.group_ordered(lambda r: r.g)

    results = {
        "group_by": by_group(
            t.group_by("g").agg(lo=lambda g: g.x.min(), hi=lambda g: g.x.max()).to_dicts()
        ),
        "agg(by=)": by_group(
            t.agg(by=lambda r: r.g, lo=lambda g: g.x.min(), hi=lambda g: g.x.max()).to_dicts()
        ),
        "group_ordered().agg": by_group(
            nested.agg(lambda g: {"g": g.first().g, "lo": g.min("x"), "hi": g.max("x")}).to_dicts()
        ),
    }
    pivot_lo = by_group(t.pivot(index="g", columns="c", values="x", agg_fn="min").to_dicts())
    pivot_hi = by_group(t.pivot(index="g", columns="c", values="x", agg_fn="max").to_dicts())
    results["pivot"] = {g: {"lo": pivot_lo[g]["v"], "hi": pivot_hi[g]["v"]} for g in pivot_lo}
    # Windows broadcast to every row: the group window, and a frame that
    # covers the whole group at its last row.
    derived = nested.derive(lambda g: {"lo": g.min("x"), "hi": g.max("x")}).to_dicts()
    windows = sorted_t.derive(
        rlo=lambda r: r.x.rolling(8, partition_by="g").min(),
        rhi=lambda r: r.x.rolling(8, partition_by="g").max(),
        clo=lambda r: r.x.cum_min(partition_by="g"),
        chi=lambda r: r.x.cum_max(partition_by="g"),
    ).to_dicts()
    last = by_group(windows)  # the last row of each group wins
    results["rolling"] = {g: {"lo": row["rlo"], "hi": row["rhi"]} for g, row in last.items()}
    results["cum_min/cum_max"] = {g: {"lo": row["clo"], "hi": row["chi"]} for g, row in last.items()}

    for path, result in results.items():
        assert sorted(result) == list(range(len(CASES))), path
    assert len(derived) == sum(map(len, CASES))
    for g, case in enumerate(CASES):
        expected_lo, expected_hi = contract_min(case), contract_max(case)
        for path, result in results.items():
            assert_same(result[g]["lo"], expected_lo, f"{path} min of {case}")
            assert_same(result[g]["hi"], expected_hi, f"{path} max of {case}")
        for row in (r for r in derived if r["g"] == g):
            assert_same(row["lo"], expected_lo, f"group_ordered().derive min of {case}")
            assert_same(row["hi"], expected_hi, f"group_ordered().derive max of {case}")


@pytest.mark.parametrize("case", CASES, ids=repr)
def test_ungrouped_follows_the_contract(small_batches, case):
    t = LTSeq.from_arrow(pa.table({"x": pa.array(case, pa.float64())}))
    [row] = t.agg(
        lo=lambda g: g.x.min(),
        hi=lambda g: g.x.max(),
        lo_if=lambda g: min_if(g.x.is_not_null(), g.x),
        hi_if=lambda g: max_if(g.x.is_not_null(), g.x),
    ).to_dicts()
    for key, expected in (("lo", contract_min(case)), ("hi", contract_max(case))):
        assert_same(row[key], expected, f"{key} of {case}")
        assert_same(row[f"{key}_if"], expected, f"{key}_if of {case}")


@pytest.mark.parametrize("source", PARQUET_INPUTS)
@pytest.mark.parametrize("partitions", [1, layouts.PARTITION_COUNTS["p2xcpu"]])
def test_ungrouped_parquet_is_not_answered_from_statistics(engine_config, partitions, source, tmp_path):
    """Parquet statistics leave NaN out, so a float min or max reads the rows."""
    engine_config(target_partitions=partitions)
    for index, case in enumerate(ROW_ORDERS + [[-INF, -INF], [-0.0, 0.0]]):
        rows = pa.table({"x": pa.array(case, pa.float64())})
        t = layouts.build(source, rows, tmp_path / str(index))
        [row] = t.agg(lo=lambda g: g.x.min(), hi=lambda g: g.x.max()).to_dicts()
        assert_same(row["lo"], contract_min(case), f"min of {case}")
        assert_same(row["hi"], contract_max(case), f"max of {case}")


@pytest.mark.parametrize("partitions", [2, layouts.PARTITION_COUNTS["p2xcpu"]])
@pytest.mark.parametrize(
    "case",
    [[-0.0, 1.0, 0.0], [2.0, 1.0, NEG_NAN], [2.0, 1.0, PAYLOAD_NAN, NAN]],
    ids=repr,
)
def test_ungrouped_parquet_scan_keeps_every_value(engine_config, partitions, case, tmp_path):
    """DataFusion pushes `x < min so far` or `x > max so far` into the scan
    of an ungrouped `min` or `max` over several partitions, and compares
    floats there in its own order, so later batches lost `0.0` once `-0.0`
    was seen, or a sign-bit NaN once `2.0` was. Each file is read one row at
    a time, so its first value sets that bound before the rest are read."""
    engine_config(target_partitions=partitions, batch_size=1)
    for index in range(2):
        pq.write_table(pa.table({"x": pa.array(case, pa.float64())}), tmp_path / f"{index}.parquet")
    t = LTSeq.read_parquet(str(tmp_path))
    # One aggregate each: `min` and `max` together pass a row outside both bounds.
    [lo] = t.agg(v=lambda g: g.x.min()).to_dicts()
    [hi] = t.agg(v=lambda g: g.x.max()).to_dicts()
    assert_same(lo["v"], contract_min(case), f"min of {case}")
    assert_same(hi["v"], contract_max(case), f"max of {case}")


@pytest.mark.parametrize(
    ("formula", "make_nan"),
    [
        ("inf - inf", lambda r: r.a - r.b),
        ("0.0 / 0.0", lambda r: r.a / r.b),
    ],
)
def test_computed_nans(small_batches, formula, make_nan):
    """`inf - inf` and `0.0 / 0.0` give a NaN with the sign bit set on x86-64.
    The expected values come from the computed column, read back as it is."""
    rows = []
    for g, order in enumerate(itertools.permutations(range(3))):
        for i in order:
            if formula == "inf - inf":
                rows.append((g, *[(1.0, 0.0), (INF, INF), (2.0, 0.0)][i]))
            else:
                rows.append((g, *[(1.0, 1.0), (0.0, 0.0), (2.0, 1.0)][i]))
    t = LTSeq.from_arrow(
        pa.table(
            {
                "g": pa.array([r[0] for r in rows], pa.int64()),
                "i": pa.array(range(len(rows)), pa.int64()),
                "a": pa.array([r[1] for r in rows], pa.float64()),
                "b": pa.array([r[2] for r in rows], pa.float64()),
            }
        )
    ).derive(x=make_nan)
    xs = {g: [] for g in range(6)}
    for row in t.sort("i").to_dicts():
        xs[row["g"]].append(row["x"])
    assert all(sum(math.isnan(x) for x in group) == 1 for group in xs.values())

    grouped = by_group(
        t.group_by("g").agg(lo=lambda g: g.x.min(), hi=lambda g: g.x.max()).to_dicts()
    )
    windows = by_group(t.sort("i").derive(chi=lambda r: r.x.cum_max(partition_by="g")).to_dicts())
    assert sorted(grouped) == list(range(6))
    for g, group in xs.items():
        assert_same(grouped[g]["lo"], 1.0, f"{formula}: grouped min of {group}")
        assert_same(grouped[g]["hi"], contract_max(group), f"{formula}: grouped max of {group}")
        assert_same(windows[g]["chi"], contract_max(group), f"{formula}: cum_max of {group}")
        [row] = t.filter(lambda r: r.g == g).agg(lo=lambda r: r.x.min(), hi=lambda r: r.x.max()).to_dicts()
        assert_same(row["lo"], 1.0, f"{formula}: ungrouped min of {group}")
        assert_same(row["hi"], contract_max(group), f"{formula}: ungrouped max of {group}")


def test_group_filter_compares_the_contract_min(small_batches):
    """`group_ordered().filter()` evaluates `g.min` as a group window.

    A NaN `max` keeps its sign bit when it is the only NaN, and how it then
    compares with a number is §18's order for comparisons, which #290 owns."""
    t = LTSeq.from_arrow(grouped_rows(ROW_ORDERS)).sort("i").group_ordered(lambda r: r.g)
    kept = t.filter(lambda g: g.min("x") == 1.0).flatten().to_dicts()
    assert sorted({row["g"] for row in kept}) == list(range(len(ROW_ORDERS)))


def test_rolling_frames_as_nans_enter_and_leave(small_batches):
    xs = [1.0, NEG_NAN, 2.0, None, -0.0, 0.0, NAN, 3.0, None, -INF, PAYLOAD_NAN, -0.0]
    t = LTSeq.from_arrow(
        pa.table({"i": pa.array(range(len(xs)), pa.int64()), "x": pa.array(xs, pa.float64())})
    ).sort("i")
    for width in (1, 2, 3, 4):
        rows = t.derive(
            lo=lambda r: r.x.rolling(width).min(), hi=lambda r: r.x.rolling(width).max()
        ).to_dicts()
        for row in rows:
            frame = xs[max(0, row["i"] - width + 1) : row["i"] + 1]
            assert_same(row["lo"], contract_min(frame), f"rolling({width}) min at {row['i']}")
            assert_same(row["hi"], contract_max(frame), f"rolling({width}) max at {row['i']}")


def sorted_rows(t, keys, desc):
    """`t` sorted by `keys`, whole and with a fetch (a TopK sort), as its
    keys with floats as bits: rows tied on the keys have the same bits."""
    sorted_t = t.sort(*keys, desc=desc)
    return [
        [
            tuple(bits(v) if isinstance(v, float) else v for v in map(row.get, keys))
            for row in table.to_dicts()
        ]
        for table in (sorted_t, sorted_t.slice(0, len(t) - 1))
    ]


@pytest.mark.parametrize("window", ["cum_min", "cum_max"])
def test_sorting_by_a_running_extreme_sorts(small_batches, window):
    """When an aggregate says its value only rises (`max`) or falls (`min`)
    as the frame grows, DataFusion takes the window column as sorted in its
    own order, totalOrder for floats, and drops a later sort by it. A float
    running extreme is monotonic only in the contract's order: the running
    `max` of `[1.0, -nan]` is `-nan`, below `1.0` in totalOrder, and that of
    `[PAYLOAD_NAN, nan]` is `float("nan")`, below `PAYLOAD_NAN`. So the sort
    runs, and the rows come back as they do once the same values are
    collected."""
    cases = CASES + [[PAYLOAD_NAN, NAN]]
    t = (
        LTSeq.from_arrow(grouped_rows(cases))
        .sort("g", "i")
        .derive(m=lambda r: getattr(r.x, window)(partition_by="g"))
    )
    snapshot = t.collect()
    for desc in (False, True):
        expected = sorted_rows(snapshot, ("g", "m"), [False, desc])
        assert sorted_rows(t, ("g", "m"), [False, desc]) == expected, f"desc={desc}"


@pytest.mark.parametrize("extreme", ["min", "max"])
def test_sorting_by_a_rolling_extreme_sorts(small_batches, extreme):
    """The same for a sliding frame over values in their sort order, where
    DataFusion also reads the aggregate's monotonicity. The rolling `max` of
    two NaNs whose payloads differ is `float("nan")`, below both in
    totalOrder."""
    payload_2 = struct.unpack("<d", struct.pack("<Q", 0x7FF8_0000_0000_0002))[0]
    xs = [1.0, NEG_NAN, 2.0, None, -0.0, 0.0, PAYLOAD_NAN, payload_2, -INF, INF]
    rows = pa.table({"x": pa.array(xs, pa.float64())})
    for x_desc in (False, True):
        t = (
            LTSeq.from_arrow(rows)
            .sort("x", desc=x_desc)
            .derive(m=lambda r: getattr(r.x.rolling(2), extreme)())
        )
        snapshot = t.collect()
        for desc in (False, True):
            expected = sorted_rows(snapshot, ("m",), desc)
            assert sorted_rows(t, ("m",), desc) == expected, f"x desc={x_desc}, desc={desc}"


def test_float32_and_dictionary_columns():
    values = [1.0, NEG_NAN, 2.0]
    t = LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array([1, 1, 1], pa.int64()),
                "f32": pa.array(values, pa.float32()),
                "dict": pa.array(values, pa.float64()).dictionary_encode(),
            }
        )
    )
    grouped = t.group_by("k").agg(
        lo32=lambda g: g.f32.min(),
        hi32=lambda g: g.f32.max(),
        lo_dict=lambda g: g.dict.min(),
        hi_dict=lambda g: g.dict.max(),
    )
    assert grouped.to_arrow().schema.field("lo32").type == pa.float32()
    [row] = grouped.to_dicts()
    for key in ("lo32", "lo_dict"):
        assert_same(row[key], 1.0, key)
    # The one NaN keeps its sign bit; float32's NaN widens to the same bits.
    for key in ("hi32", "hi_dict"):
        assert_same(row[key], NEG_NAN, key)


def test_statistics_still_answer_count_and_integer_min_max(tmp_path):
    """Only float min and max read the rows; count and integer min/max are
    answered from Parquet statistics, so their plan scans no file."""
    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"n": pa.array([3, 1, 2], pa.int64()), "x": [1.0, NAN, 2.0]}), path)
    t = LTSeq.read_parquet(str(path))

    from_stats = t.agg(rows=lambda g: g.n.count(), lo=lambda g: g.n.min(), hi=lambda g: g.n.max())
    assert from_stats.to_dicts() == [{"rows": 3, "lo": 1, "hi": 3}]
    assert "DataSourceExec" not in from_stats.explain_plan()[1]

    scanned = t.agg(hi=lambda g: g.x.max())
    assert_same(scanned.to_dicts()[0]["hi"], NAN, "float max")
    assert "DataSourceExec" in scanned.explain_plan()[1]
