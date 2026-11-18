"""What #296's fix costs where it applies: compound membership filters
on 10 million Int64 and float64 rows, the median of 7 runs (#411)."""

import statistics
import time

import pyarrow as pa
import pyarrow.compute as pc

from ltseq import LTSeq

N = 10_000_000
CASES = {
    "x in A & x in B (intersection)": lambda r: r.x.is_in([1, 2, 5, 9]) & r.x.is_in([1, 3, 4, 6]),
    "x in A & ~x in [None, ...] (difference)": lambda r: r.x.is_in([1, 2, 5]) & ~r.x.is_in([None, 3, 4, 6]),
    "x in A | x in B (union)": lambda r: r.x.is_in([1, 2, 5, 9]) | r.x.is_in([11, 13, 14, 16]),
    "f in A & f in B, floats": lambda r: r.f.is_in([1.0, 2.0, 5.0, 9.0]) & r.f.is_in([1.0, 3.0, 4.0, 6.0]),
}


def main():
    x = pc.cast(pc.random(N), pa.float64())
    t = LTSeq.from_arrow(
        pa.table({"x": pc.cast(pc.floor(pc.multiply(x, 20)), pa.int64()), "f": pc.floor(pc.multiply(x, 20))})
    )
    for name, predicate in CASES.items():
        times = []
        for _ in range(7):
            start = time.perf_counter()
            t.filter(predicate).count()
            times.append(time.perf_counter() - start)
        print(f"| {name} | {statistics.median(times) * 1000:.1f} |")


if __name__ == "__main__":
    main()
