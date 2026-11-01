# shift() and diff() take partition_by=, as rolling() does (#312).
from ltseq import LTSeq, rank

t = LTSeq.from_rows([{"user": "a", "ts": 1}, {"user": "a", "ts": 2}]).sort("ts")

shifted = t.derive(prev=lambda r: r.ts.shift(1, partition_by="user"))
diffed = t.derive(gap=lambda r: r.ts.diff(1, partition_by=r.user))
ranked = t.derive(rk=lambda r: rank().over(partition_by="user", order_by="ts"))
