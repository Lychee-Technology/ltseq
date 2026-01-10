from ltseq import LTSeq

t = LTSeq.read_csv("test_data/test_group_filter.csv")
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
