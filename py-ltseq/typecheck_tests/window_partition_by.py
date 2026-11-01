# pyright: reportUnnecessaryTypeIgnoreComment=true
"""shift(), diff() and rolling() take default= and partition_by= by keyword.

The engine reads both from the call's keyword arguments only, so a positional
value would be dropped without an error: `r.v.diff(1, "u")` diffs across the
groups. The stubs make them keyword-only, and each positional call below
carries an ignore for the error it has to produce; if the stubs start
accepting it, pyright reports the ignore as unnecessary and this check fails.
"""

from ltseq import LTSeq, rank

t = LTSeq.from_rows([{"user": "a", "ts": 1}, {"user": "a", "ts": 2}]).sort("ts")

shifted = t.derive(prev=lambda r: r.ts.shift(1, partition_by="user"))
defaulted = t.derive(prev=lambda r: r.ts.shift(1, default=0, partition_by="user"))
diffed = t.derive(gap=lambda r: r.ts.diff(1, partition_by=r.user))
rolled = t.derive(avg=lambda r: r.ts.rolling(2, partition_by="user").mean())
chained = t.derive(prev=lambda r: r.ts.rolling(2).mean().shift(1, partition_by="user"))
ranked = t.derive(rk=lambda r: rank().over(partition_by="user", order_by="ts"))


def positional_keywords(t: LTSeq) -> None:
    t.derive(v=lambda r: r.ts.shift(1, 0))  # pyright: ignore[reportCallIssue]
    t.derive(v=lambda r: r.ts.shift(1, None, "user"))  # pyright: ignore[reportCallIssue]
    t.derive(v=lambda r: r.ts.diff(1, "user"))  # pyright: ignore[reportCallIssue]
    t.derive(v=lambda r: r.ts.rolling(2, "user").mean())  # pyright: ignore[reportCallIssue]
    t.derive(v=lambda r: r.ts.rolling(2).mean().shift(1, 0))  # pyright: ignore[reportCallIssue]
    t.derive(v=lambda r: r.ts.rolling(2).mean().shift(1, None, "user"))  # pyright: ignore[reportCallIssue]
