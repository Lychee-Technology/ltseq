from ltseq.grouping import NestedTable
from ltseq import LTSeq
import inspect
import ast

# Create test table
t = LTSeq.read_csv("test_data/test_group_filter.csv")
grouped = t.sort(lambda r: r.date).group_ordered(lambda r: r.is_up)


# Get the test lambda source and parse it
def test_predicate(g):
    return g.count() > 2


source = inspect.getsource(test_predicate)
print("Source:", repr(source))
tree = ast.parse(source)

# Print the AST
print("\nAST:")
for node in ast.walk(tree):
    if isinstance(node, ast.Compare):
        print(f"Compare node found")
        left = node.left
        print(f"  Left: {ast.dump(left)}")
        print(f"  Is Call?: {isinstance(left, ast.Call)}")
        if isinstance(left, ast.Call):
            print(f"    func: {ast.dump(left.func)}")
            if isinstance(left.func, ast.Attribute):
                print(f"    Attribute.attr: {left.func.attr}")
        ops = node.ops
        print(f"  Ops: {ops}")
        comparators = node.comparators
        print(f"  Comparators: {[ast.dump(c) for c in comparators]}")
        if comparators:
            c = comparators[0]
            print(f"    Is Constant?: {isinstance(c, ast.Constant)}")
            if isinstance(c, ast.Constant):
                print(f"    Value: {c.value}")

# Try extracting the filter condition
sql_where = grouped._extract_filter_condition(tree)
print("\nExtracted SQL WHERE:", repr(sql_where))
