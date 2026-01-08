from ltseq import LTSeq

t = LTSeq.read_csv("test_group_filter.csv")
print("Loaded table:")
t.show()

print("\nTest 1: Group by is_up, filter groups with count > 2")
result = (
    t.sort(lambda r: r.date)
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: g.count() > 2)
)
print("Success! Result:")
result.show()
print(
    "\nExpected: only groups with count > 2 (group 1 with 4 rows and group 3 with 3 rows)"
)
