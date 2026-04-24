from ltseq import LTSeq
from ltseq.grouping.nested_table import NestedTable
from ltseq.grouping.proxies import GroupProxy


t = LTSeq.from_rows([
    {"region": "East", "sales": 100, "is_active": True},
    {"region": "East", "sales": 200, "is_active": False},
    {"region": "West", "sales": 150, "is_active": True},
])

_schema: dict[str, str] = t._schema
_captured: dict[str, object] = t._capture_expr(lambda r: r.sales > 100)
_from_rows: LTSeq = LTSeq._from_rows([], {"id": "Int64"})

grouped: NestedTable = t.group_sorted(lambda r: r.region)
sorted_flag: bool = grouped._is_sorted

proxy = GroupProxy([
    {"sales": 100, "is_active": True},
    {"sales": 200, "is_active": False},
], None)

counted: int = proxy.count_if(lambda r: r.is_active)
summed: object = proxy.sum_if(lambda r: r.is_active, "sales")
