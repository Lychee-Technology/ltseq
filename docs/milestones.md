# Development roadmap for LTSeq
This roadmap follows the "Walking Skeleton" methodology: we first build the end-to-end connection, and then progressively flesh out the "Sequence" capabilities.

## Milestone 1: The Walking Skeleton & Data Foundation
Objective: Establish the Rust-Python bridge using uv and maturin. Ensure data can be loaded from CSV via Rust (DataFusion) and displayed in Python.

### Step 1.1: Project Initialization
* Initialize ltseq structure with uv (Python) and cargo (Rust).
* Configure pyproject.toml and Cargo.toml.
* Implement the basic LTSeq wrapper class in Python and RustTable struct in Rust.

### Step 1.2: DataFusion Integration
* Embed SessionContext and DataFrame within RustTable.
* Implement LTSeq.read_csv(path) static method.
* Implement LTSeq.show() to print the Arrow RecordBatch to stdout.

### Deliverable Demo:

```Python
import ltseq
# Should load data via Rust and print a SQL-like table
t = ltseq.LTSeq.read_csv("data/sample.csv")
t.show()
````


## Milestone 2: The Symbolic Expression Engine

Objective: Enable the "Lambda DSL." Transform Python lambda functions into Rust Logical Plans to support filtering and projection.

### Step 2.1: Python AST Builder (The Proxy)

* Implement SchemaProxy and Expr classes in Python using Magic Methods (__getattr__, __gt__, __add__).
* Capture user intent: lambda r: r.age > 18 becomes an expression tree, not a function.

### Step 2.2: Rust Transpiler

* Define `PyExpr` enum in Rust to receive the serialized tree.
* Map `PyExpr` to DataFusion's `logical_expr::Expr`.
* Implement `filter()` and `select()` APIs in Rust.

### Deliverable Demo:

```Python
from ltseq import LTSeq
# The lambda is compiled to a Logical Plan, not executed by Python
t = LTSeq.read_csv("users.csv")
t.filter(lambda r: r.age > 18).select(lambda r: [r.name]).show()
```


## Milestone 3: Relative Position & Sequence Arithmetic

Objective: Implement the core "Sequence" capabilities that differentiate LTSeq from SQL/Pandas: accessing previous rows and sliding windows.

### Step 3.1: Shift & Lag Operators
* Implement r.col.shift(n) in the DSL.
* Map this to DataFusion's WindowFunction (Lag/Lead) or a custom physical plan for zero-copy offsets.
* Enable vector arithmetic: r.price - r.price.shift(1).

### Step 3.2: Rolling Windows
* Implement r.col.rolling(window=3).mean() syntax.
* Map to DataFusion's WindowFrame (Rows between N preceding).

### Deliverable Demo:

```Python
# Calculate daily growth: (Price - Prev_Price) / Prev_Price
t.derive(lambda r: {
    "growth": (r.price - r.price.shift(1)) / r.price.shift(1),
    "ma_3d":  r.price.rolling(3).mean()
}).show()
```


## Milestone 4: State-Aware Grouping (The Core Logic)

Objective: Implement Ordered Grouping (group@o), the crown jewel of the discrete dataset theory, allowing grouping by consecutive changes.

### Step 4.1: Cumulative Calculation (Scan)

* Implement cum_sum() operator.
* Logic: Used to generate unique Group IDs based on change flags.

### Step 4.2: The group_ordered Operator
* Rust Kernel: Implement the algorithm: Mask = col != col.shift(1) -> GroupID = CumSum(Mask).
* API: `LTSeq.group_ordered(by=...)`.
* This creates a sequence of sub-tables (Nested Data) based on contiguous segments.

### Deliverable Demo:

```Python
# Group consecutive identical records (e.g., consecutive rising days)
# Returns a sequence of sub-tables, not just aggregate results
groups = t.group_ordered(lambda r: r.trend_flag)
groups.filter(lambda g: g.count() > 3).show()
```

## Milestone 5: Pointer-Based Association (The Link)
Objective: Implement "Foreign Key Pointers" (switch/link) to replace expensive Hash Joins with high-speed direct memory access (Lookup).

### Step 5.1: The Link Operator
* Implement `LTSeq.link(target_table, on=...)`.
* Mechanism: Build a dictionary index (Take Kernel) mapping rows in Table A to row indices in Table B.

### Step 5.2: Object-Oriented Access

* Allow accessing the linked table's columns as attributes: r.linked_obj.name.
* Resolve these references to physical take operations in the execution plan.

### Deliverable Demo:
```Python
orders = LTSeq.read_csv("orders.csv")
products = LTSeq.read_csv("products.csv")

# Create a memory pointer, no immediate data copying
orders = orders.link(products, on=lambda o, p: o.pid == p.id, as_name="prod")

# Access product name directly via the pointer
orders.select(lambda r: [r.id, r.prod.name, r.prod.price]).show()
```


