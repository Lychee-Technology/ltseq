from ltseq.core import LTSeq
from ltseq.aggregation import GroupBy
from ltseq.grouping.nested_table import NestedTable
from ltseq.linking import LinkedTable
from ltseq.partitioning import PartitionedTable, SQLPartitionedTable


t = LTSeq.from_rows([
    {"id": 1, "value": 10, "prod_name": "alpha"},
    {"id": 2, "value": 20, "prod_name": "beta"},
])

filtered: LTSeq = t.filter(lambda r: r.id > 0)
nested_filtered: LTSeq = t.filter(lambda r: r.prod.name == "alpha")
selected: LTSeq = t.select(lambda r: [r.id, r.value])
derived: LTSeq = t.derive(double=lambda r: r.value * 2)
first_match: LTSeq | None = t.search_first(lambda r: r.id == 1)

grouped = t.group_by("id")
groupby_obj: GroupBy = grouped
aggregated: LTSeq = grouped.agg(total=lambda r: r.value.sum())

ordered_groups: NestedTable = t.group_ordered(lambda r: r.id > 0)
partitions: PartitionedTable | SQLPartitionedTable = t.partition("id")

linked = t.link(t, on=lambda left, right: left.id == right.id, as_="prod")
linked_table: LinkedTable = linked
