#!/usr/bin/env python3
"""
Consecutive Up Days Example (>= 9.5% for 3+ days)

Data source: stock.csv
- Multi-stock schema: CODE, DT, CL
- Single-stock schema: date, price
"""

import csv

from ltseq import LTSeq
from ltseq.expr import if_else


DATA_PATH: str = "StockRecords.csv"
THRESHOLD = 0.095


def _read_header(path: str) -> list[str]:
    with open(path, newline="") as f:
        return next(csv.reader(f))


def _analyze_multi_stock(path: str):
    t = LTSeq.read_csv(path)
    sorted_t = t.sort(lambda r: r.CODE, lambda r: r.DT)

    with_up = sorted_t.derive(
        lambda r: {
            "up": if_else(
                r.CL.shift(1).is_null() | (r.CODE != r.CODE.shift(1)),
                0,
                (r.CL - r.CL.shift(1)) / r.CL.shift(1),
            )
        }
    )

    with_hit = with_up.derive(
        lambda r: {"hit_flag": if_else(r.up >= THRESHOLD, 1, 0)}
    )

    with_segment = with_hit.derive(
        lambda r: {
            # Start a new segment when stock code changes or hit_flag flips.
            "segment_start": if_else(
                r.CODE.shift(1).is_null()
                | (r.CODE != r.CODE.shift(1))
                | (r.hit_flag != r.hit_flag.shift(1)),
                1,
                0,
            )
        }
    )

    with_segment_id = with_segment.cum_sum("segment_start")

    streaks = (
        with_segment_id
        .group_ordered(lambda r: r.segment_start_cumsum)
        .filter(lambda g: (g.first().hit_flag == 1) & (g.count() >= 3))
    )

    summary = (
        streaks
        .derive(
            lambda g: {
                "stock_code": g.first().CODE,
                "start_date": g.first().DT,
                "end_date": g.last().DT,
                "days": g.count(),
            }
        )
        .group_ordered(lambda r: r.segment_start_cumsum)
        .first()
        .select(lambda r: [r.stock_code, r.start_date, r.end_date, r.days])
    )

    codes = summary.select(lambda r: [r.stock_code]).distinct()
    return summary, codes


def _analyze_single_stock(path: str):
    t = LTSeq.read_csv(path)
    sorted_t = t.sort(lambda r: r.date)

    with_up = sorted_t.derive(
        lambda r: {
            "up": if_else(
                r.price.shift(1).is_null(),
                0,
                (r.price - r.price.shift(1)) / r.price.shift(1),
            )
        }
    )

    with_hit = with_up.derive(
        lambda r: {"hit_flag": if_else(r.up >= THRESHOLD, 1, 0)}
    )

    with_segment = with_hit.derive(
        lambda r: {
            "segment_start": if_else(
                r.hit_flag.shift(1).is_null()
                | (r.hit_flag != r.hit_flag.shift(1)),
                1,
                0,
            )
        }
    )

    with_segment_id = with_segment.cum_sum("segment_start")

    streaks = (
        with_segment_id
        .group_ordered(lambda r: r.segment_start_cumsum)
        .filter(lambda g: (g.first().hit_flag == 1) & (g.count() >= 3))
    )

    summary = (
        streaks
        .derive(
            lambda g: {
                "start_date": g.first().date,
                "end_date": g.last().date,
                "days": g.count(),
            }
        )
        .group_ordered(lambda r: r.segment_start_cumsum)
        .first()
        .select(lambda r: [r.start_date, r.end_date, r.days])
    )

    return summary


def main() -> None:
    header = _read_header(DATA_PATH)
    header_set = {name.strip() for name in header}

    if {"CODE", "DT", "CL"}.issubset(header_set):
        summary, codes = _analyze_multi_stock(DATA_PATH)
        print("Streaks (>= 3 days, >= 9.5% daily gain):")
        summary.show()
        print("\nStocks that meet the condition:")
        codes.show()
        return

    if {"date", "price"}.issubset(header_set):
        summary = _analyze_single_stock(DATA_PATH)
        print("Streaks (>= 3 days, >= 9.5% daily gain):")
        summary.show()
        return

    raise ValueError(
        "stock.csv must contain either CODE/DT/CL or date/price columns."
    )


if __name__ == "__main__":
    main()
