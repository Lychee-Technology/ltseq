"""Check that each #251 prototype does what its measurement assumes.

``python benchmarks/m0/selfcheck.py`` (any build with the ``m0`` feature).
A prototype that silently did nothing, or did something else, would make
its timing meaningless, so ``run.py``'s numbers are only read after this
passes. Small inputs only; ClickBench checks are skipped when the files are
missing.
"""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cases  # noqa: E402
import inputs  # noqa: E402
import pipelines  # noqa: E402
from ltseq import LTSeq  # noqa: E402
from ltseq._testing import pinned_engine, prototypes  # noqa: E402

INT64_MAX = 2**63 - 1
CHECKS = []


def check(fn):
    CHECKS.append(fn)
    return fn


def raises(fn, text: str) -> str:
    try:
        fn()
    except Exception as error:  # noqa: BLE001 - the class is the point being checked
        message = f"{type(error).__name__}: {error}"
        assert text in message, f"expected {text!r} in {message!r}"
        return message
    raise AssertionError(f"expected an error containing {text!r}")


def plan(t: LTSeq) -> str:
    logical, physical = t._inner.explain_plan()
    return logical + "\n" + physical


@check
def m1_checked_arithmetic_raises_only_on_overflow():
    data = pa.table({"x": [1, INT64_MAX - 1, 5], "y": [2, 1, -3]})
    for mode in ("arrow", "fast"):
        with prototypes(checked=mode):
            t = LTSeq.from_arrow(data)
        ok = t.derive(s=lambda r: r.x + r.y, d=lambda r: r.x - r.y, m=lambda r: r.y * 3)
        assert "m0_checked_add" in plan(ok), plan(ok)
        assert ok.to_arrow().column("s").to_pylist() == [3, INT64_MAX, 2]
        raises(lambda: t.derive(s=lambda r: r.x + r.x).to_arrow(), "ArithmeticOverflowError")
        raises(lambda: t.derive(m=lambda r: r.x * 2).to_arrow(), "ArithmeticOverflowError")
        raises(lambda: t.derive(d=lambda r: r.y - r.x - r.x).to_arrow(), "ArithmeticOverflowError")
        # A sum whose running value overflows but whose total fits.
        total = t.agg(s=lambda g: g.x.sum())
        assert "m0_checked_sum" in plan(total), plan(total)
    with prototypes(checked="fast"):
        fits = LTSeq.from_arrow(pa.table({"x": [INT64_MAX, 5, -10]}))
        over = LTSeq.from_arrow(pa.table({"x": [INT64_MAX, 1]}))
    assert fits.agg(s=lambda g: g.x.sum()).to_arrow().column("s").to_pylist() == [INT64_MAX - 5]
    raises(lambda: over.agg(s=lambda g: g.x.sum()).to_arrow(), "ArithmeticOverflowError")
    # Today's kernels wrap.
    today = LTSeq.from_arrow(pa.table({"x": [INT64_MAX, 1]}))
    assert today.agg(s=lambda g: g.x.sum()).to_arrow().column("s").to_pylist() == [-(2**63)]
    # Every m1 pipeline gives today's values when nothing overflows.
    csv = str(inputs.bench_core_csv(inputs.WARMUP_ROWS))
    for name, pipeline in pipelines.M1.items():
        results = []
        for mode in ("off", "arrow", "fast"):
            with pinned_engine(target_partitions=1), prototypes(checked=mode):
                t = LTSeq.read_csv(csv).collect()
                if name == "m1.cum_sum":
                    t = t.sort("id")
                results.append(pipeline(t).to_arrow())
        assert results[0].equals(results[1]) and results[0].equals(results[2]), name


@check
def m2_exact_statistics_are_exact_and_order_independent():
    cancel = pa.table({"x": [1e16, 1.0, -1e16], "g": [1, 1, 1]})
    with prototypes(exact="exact"):
        t = LTSeq.from_arrow(cancel)
    q = t.agg(s=lambda g: g.x.sum(), m=lambda g: g.x.mean())
    assert "m0_exact_sum" in plan(q), plan(q)
    assert q.to_arrow().to_pylist() == [{"s": 1.0, "m": 1 / 3}]
    with prototypes(exact="compensated"):
        t = LTSeq.from_arrow(cancel)
    assert t.agg(s=lambda g: g.x.sum()).to_arrow().column("s").to_pylist() == [1.0]
    # Each m2 pipeline: exact results do not depend on the partition count,
    # and stay within rounding of today's.
    csv = str(inputs.bench_core_csv(inputs.WARMUP_ROWS))
    for name, pipeline in pipelines.M2.items():
        outs = {}
        for mode, p in (("off", 1), ("exact", 1), ("exact", 12), ("compensated", 4)):
            with pinned_engine(target_partitions=p), prototypes(exact=mode):
                t = LTSeq.read_csv(csv).collect().sort("id")
            out = pipeline(t)
            if mode != "off":
                assert "m0_exact" in plan(out), (name, plan(out))
            table = out.to_arrow()
            keys = [f.name for f in table.schema if not pa.types.is_floating(f.type)]
            rows = table.sort_by([(k, "ascending") for k in keys]).to_pylist() if keys else table.to_pylist()
            outs[(mode, p)] = rows
        assert outs[("exact", 1)] == outs[("exact", 12)], name
        for a, b in zip(outs[("off", 1)], outs[("exact", 1)]):
            for k, v in a.items():
                if isinstance(v, float) and v == v:
                    assert abs(v - b[k]) <= 1e-9 * max(1.0, abs(v)), (name, k, v, b[k])


@check
def m3_ordered_output_restores_file_then_row_order():
    with tempfile.TemporaryDirectory() as tmp:
        for f in range(3):
            ids = pa.array(range(f * 30_000, (f + 1) * 30_000), pa.int64())
            pq.write_table(pa.table({"id": ids, "v": ids}), f"{tmp}/part-{f}.parquet", row_group_size=4_000)
        expected = list(range(90_000))
        for mode in ("concat", "interleaved"):
            for slow in (0, 5):
                with pinned_engine(target_partitions=4, batch_size=1024), prototypes(order=mode, slow_ms=slow):
                    t = LTSeq.read_parquet(tmp)
                got = pipelines.M3["m3.r3_filter_select_derive"](t).to_arrow().column("id").to_pylist()
                assert got == [i for i in expected if i % 4 != 0], (mode, slow)
                assert "OrderedMergeExec" in plan(t.select("id")), plan(t)


@check
def m4_masked_operands_guard_each_other():
    for failing in (False, True):
        data = inputs.zones(20_000, 0.5, failing)
        demanded = int(pc.sum(data.column("g")).as_py())
        for name, pipeline in pipelines.M4.items():
            for mode in ("select", "errors", "optimistic"):
                with prototypes(checked="fast", masked=mode):
                    t = LTSeq.from_arrow(data)
                result = pipeline(t)
                assert "m0_masked" in plan(result), plan(result)
                rows = result.to_arrow().num_rows
                # `&` keeps the zone-0 rows; `|` of the negations keeps the rest.
                want = demanded if "_and_" in name else 20_000 - demanded
                assert rows == want, (name, mode, failing, rows, want)
        if failing:
            with prototypes(checked="fast"):
                t = LTSeq.from_arrow(data)
            raises(lambda: pipelines.M4["m4.fallible_and_guard"](t).to_arrow(), "ArithmeticOverflowError")
    # Each `&` form runs in its written order in every mode m4 measures.
    data = inputs.zones(20_000, 0.01, True)
    for name in ("m4.guard_and_fallible", "m4.fallible_and_guard"):
        for mode, flags in cases.M4_MODES.items():
            with prototypes(**flags):
                t = LTSeq.from_arrow(data)
            _, physical = pipelines.M4[name](t)._inner.explain_plan()
            line = next(line for line in physical.splitlines() if "FilterExec" in line)
            product_first = line.index("a1@") < line.index("g@")
            assert product_first == (name == "m4.fallible_and_guard"), (name, mode, line)
    # So at a 1% share today's checked plan raises only with the product
    # first: a guard first leaves under 20% of the rows open, and the product
    # runs only on those.
    with prototypes(checked="fast"):
        t = LTSeq.from_arrow(data)
    raises(lambda: pipelines.M4["m4.fallible_and_guard"](t).to_arrow(), "ArithmeticOverflowError")
    rows = pipelines.M4["m4.guard_and_fallible"](t).to_arrow().num_rows
    assert rows == pc.sum(data.column("g")).as_py(), rows


@check
def m4b_barrier_keeps_the_filter_above_sort_and_aggregate():
    # The control is today's pushdown with the scan's filtering off and
    # nothing else (`m0::tests` checks what `pruning` changes in a session).
    control = dict(cases.M4B_MODES["scan_filter_off"])
    assert control.pop("pruning") == "off" and control == cases.M4B_MODES["pushdown"], cases.M4B_MODES
    with tempfile.TemporaryDirectory() as tmp:
        for name, pipeline in pipelines.M4B.items():
            file = "hits_sorted.parquet" if name in pipelines.M4B_SORTED else "hits_sample.parquet"
            if not (inputs.CLICKBENCH / file).exists():
                print(f"  skipped {name}: no {file}")
                continue
            warm = inputs.clickbench_warmup(file)["path"]
            if name in pipelines.M4B_SORTED:
                # The warm-up rows hold no row of USER, which these filter on.
                user = pq.read_table(inputs.CLICKBENCH / file, filters=[("userid", "==", pipelines.USER)])
                warm = f"{tmp}/{name}.parquet"
                pq.write_table(pa.concat_tables([pq.read_table(inputs.clickbench_warmup(file)["path"]), user]), warm)
            results, repartitions = [], []
            for mode, flags in cases.M4B_MODES.items():
                if mode == "scan_filter_off" and name in pipelines.M4B_SORTED:
                    continue  # m4b runs the control on the sample only
                # Several partitions and small batches, so that a plan which
                # scattered an ordered stream would return it out of order.
                with (
                    pinned_engine(target_partitions=4, batch_size=256),
                    prototypes(checked="fast", **flags),
                ):
                    t = LTSeq.read_parquet(warm)
                q = pipeline(t)
                logical, physical = q._inner.explain_plan()
                assert ("M0Barrier" in logical) == (flags["restrict_pushdown"] != "off"), logical
                lines = physical.splitlines()

                def first(*names):
                    return min((i for i, line in enumerate(lines) if any(n in line for n in names)), default=None)

                # Today the filter becomes a scan predicate (DataFusion also
                # pushes filters physically); without the scan's row filter
                # it stays a FilterExec below the operator, and with a
                # barrier above the operator or between it and the barrier.
                filter_at, operator_at = first("FilterExec"), first("SortExec", "AggregateExec")
                barrier_at = first("M0BarrierExec")
                if mode == "pushdown":
                    assert filter_at is None, physical
                elif mode == "scan_filter_off":
                    assert barrier_at is None and operator_at < filter_at, physical
                elif mode == "operator":
                    assert filter_at < barrier_at < operator_at, physical
                else:
                    assert operator_at < filter_at < barrier_at, physical
                # Only the filter moves: every mode repartitions as today's
                # plan does, and a result with a declared order (the sort's)
                # comes back in it.
                repartitions.append(re.findall(r"RepartitionExec: partitioning=(\w+)", physical))
                table = q.to_arrow()
                if q._sort_keys:
                    order = [(key, "descending" if desc else "ascending") for key, desc in q._sort_keys]
                    # A stable sort of rows already in order leaves them in place.
                    moved = pc.sort_indices(table.select([key for key, _ in order]), sort_keys=order)
                    assert moved.to_pylist() == list(range(table.num_rows)), f"{name} with {mode}: rows out of order"
                results.append(sorted(map(str, table.to_pylist())))
            assert all(r == repartitions[0] for r in repartitions), (name, repartitions)
            assert all(r == results[0] for r in results) and results[0], name


@check
def m5_validation_catches_violations_and_disables_pruning():
    with tempfile.TemporaryDirectory() as tmp:
        good = pa.table({"x": pa.array(range(10_000), pa.int64())})
        pq.write_table(good, f"{tmp}/good.parquet", row_group_size=1_000)
        # One violation, straddling a row-group boundary.
        bad_x = list(range(10_000))
        bad_x[5_000], bad_x[4_999] = bad_x[4_999], bad_x[5_000]
        pq.write_table(pa.table({"x": pa.array(bad_x, pa.int64())}), f"{tmp}/bad.parquet", row_group_size=1_000)
        for p in (1, 4):
            with pinned_engine(target_partitions=p), prototypes(validate="validate"):
                ok = LTSeq.read_parquet(f"{tmp}/good.parquet").assume_sorted("x")
                bad = LTSeq.read_parquet(f"{tmp}/bad.parquet").assume_sorted("x")
            assert "ValidateSortedExec" in plan(ok.filter(lambda r: r.x > 9_000)), plan(ok)
            assert ok.filter(lambda r: r.x > 9_000).to_arrow().num_rows == 999
            # The filter would let statistics skip the bad row groups.
            raises(lambda: bad.filter(lambda r: r.x > 9_000).to_arrow(), "OrderViolationError")
        # Partition boundaries: each of 4 partitions sorted, out of order
        # between them.
        rows = [list(range(i * 1_000, (i + 1) * 1_000)) for i in (0, 2, 1, 3)]
        pq.write_table(pa.table({"x": pa.array(sum(rows, []), pa.int64())}), f"{tmp}/parts.parquet",
                       row_group_size=1_000)
        with pinned_engine(target_partitions=4), prototypes(validate="validate"):
            parts = LTSeq.read_parquet(f"{tmp}/parts.parquet").assume_sorted("x")
        raises(lambda: parts.to_arrow(), "OrderViolationError")
        # Two keys, the filter fixing the first: the order still checks both
        # (a scan predicate makes `a` constant in DataFusion's view of the
        # scan's ordering), and still every row, not only those that pass.
        a = [i // 100 for i in range(10_000)]
        b = [i % 100 for i in range(10_000)]
        pq.write_table(pa.table({"a": a, "b": b}), f"{tmp}/two.parquet", row_group_size=1_000)
        b[7_005], b[7_006] = b[7_006], b[7_005]
        pq.write_table(pa.table({"a": a, "b": b}), f"{tmp}/two_bad.parquet", row_group_size=1_000)
        for p in (1, 4):
            with pinned_engine(target_partitions=p), prototypes(validate="validate"):
                two = LTSeq.read_parquet(f"{tmp}/two.parquet").assume_sorted("a", "b")
                two_bad = LTSeq.read_parquet(f"{tmp}/two_bad.parquet").assume_sorted("a", "b")
            assert two.filter(lambda r: r.a == 50).to_arrow().num_rows == 100
            raises(lambda: two_bad.filter(lambda r: r.a == 50).to_arrow(), "OrderViolationError")
        # Several files: the check holds within one file only, so it refuses
        # them. Unrefused, `x >= 6` skips `a` by its statistics and the
        # out-of-order pair between the files goes unread.
        Path(f"{tmp}/files").mkdir()
        pq.write_table(pa.table({"x": pa.array([1, 2, 3, 4, 5], pa.int64())}), f"{tmp}/files/a.parquet")
        pq.write_table(pa.table({"x": pa.array([3, 4, 5, 6, 7], pa.int64())}), f"{tmp}/files/b.parquet")
        for p in (1, 4):
            with pinned_engine(target_partitions=p), prototypes(validate="validate"):
                files = LTSeq.read_parquet(f"{tmp}/files").assume_sorted("x")
            raises(lambda: files.filter(lambda r: r.x >= 6).to_arrow(), "one file")


@check
def m6_views_become_plain_strings():
    with tempfile.TemporaryDirectory() as tmp:
        pq.write_table(pa.table({"s": ["a", None, "ccc"], "n": [1, 2, 3]}), f"{tmp}/s.parquet")
        types = {}
        for mode in ("off", "plain", "cast"):
            with prototypes(views=mode):
                t = LTSeq.read_parquet(f"{tmp}/s.parquet")
            types[mode] = t.to_arrow().schema.field("s").type
            assert t.to_arrow().column("s").to_pylist() == ["a", None, "ccc"]
        assert types == {"off": pa.string_view(), "plain": pa.string(), "cast": pa.string()}, types


def main() -> int:
    failed = 0
    for fn in CHECKS:
        try:
            fn()
            print(f"ok    {fn.__name__}")
        except Exception as error:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {fn.__name__}: {type(error).__name__}: {error}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
