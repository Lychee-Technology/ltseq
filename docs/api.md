Here is the **LTSeq Core API Definition**

This table maps the core capabilities of the original SPL (Structured Process Language) to the new Pythonic LTSeq API, leveraging **Lambda expressions** and **Type Hinting** for the best developer experience.

### **📝 Conventions**

* **t**: Represents the current LTSeq object (Immutable).  
* **r**: Represents the Row/Schema Proxy passed into the lambda.  
* **Return Value**: All operations return a new LTSeq instance to support method chaining.

---

#### **1\. Basic Relational Operations**

Standard dataframe operations found in SQL or Pandas, but optimized for the Rust kernel.

| Method | Lambda Signature Example | SPL Equivalent | Description |
| :---- | :---- | :---- | :---- |
| **filter** | filter(lambda r: r.amt \> 10\) | T.select(...) | **Row Filtering**. Predicates are pushed down to the Rust kernel. |
| **derive** | derive(lambda r: {"tax": r.price \* 0.1}) | T.derive(...) | **Derivation**. Adds new columns or modifies existing ones while keeping others. |
| **select** | select(lambda r: \[r.id, r.name\]) | T.new(...) | **Projection**. Keeps only the specified columns (pruning). |
| **sort** | sort(lambda r: \[r.date, r.id\]) | T.sort(...) | **Physical Sort**. Reorders data in memory; essential for subsequent ordered computing. |
| **distinct** | distinct(lambda r: r.client\_id) | T.id(...) | **Deduplication**. Removes duplicate rows based on specified keys. |
| **slice** | slice(offset=10, length=5) | T(\[10,15\]) | **Slicing**. Zero-copy selection of a specific range of rows. |

#### **2\. Ordered & Procedural Computing (The Core Differentiator)**

This is the soul of LTSeq, treating data as a sequence rather than an unordered set.

| Method / Expr | Lambda Signature Example | SPL Equivalent | Description |
| :---- | :---- | :---- | :---- |
| **shift(n)** | r.col.shift(1) | col\[-1\] | **Relative Position**. Reference values from previous (n\>0) or next (n\<0) rows. |
| **rolling(w)** | r.col.rolling(3).sum() | col{-1,1} | **Sliding Window**. Computes aggregates over a moving window of size w. |
| **cum\_sum()** | r.col.cum\_sum() | cum(col) | **Cumulative Calculation**. Running total from the first row to the current row. |
| **diff()** | r.col.diff() | col \- col\[-1\] | **Difference**. Shortcut for r.col \- r.col.shift(1). |
| **group\_ordered** | group\_ordered(lambda r: r.flag) | T.group@o | **Ordered Grouping**. **Crucial**: Groups only *consecutive* identical records. Does not sort the data. |
| **search\_first** | search\_first(lambda r: r.val \> 100\) | T.pselect | **Binary Search**. Quickly finds the first matching record in an ordered table. |

##### **Window Function Examples**

**Important**: Window functions (shift, rolling, cum_sum, diff) require a prior `.sort()` call to establish order.

```python
from ltseq import LTSeq

# Example: Stock price analysis
t = LTSeq.read_csv("stock.csv")

# 1. shift() - Access previous values
result = (
    t.sort(lambda r: r.date)
    .derive(lambda r: {
        "prev_close": r.close.shift(1),      # Previous row's close price
        "next_open": r.open.shift(-1),       # Next row's open price
        "price_change": r.close - r.close.shift(1)
    })
)

# 2. rolling() - Compute aggregates over sliding windows
result = (
    t.sort(lambda r: r.date)
    .derive(lambda r: {
        "ma_5": r.close.rolling(5).mean(),   # 5-day moving average
        "volume_sum_10": r.volume.rolling(10).sum(),  # 10-day sum
        "price_min_3": r.close.rolling(3).min(),      # 3-day minimum
        "price_max_3": r.close.rolling(3).max()       # 3-day maximum
    })
)

# 3. diff() - Calculate differences from previous values
result = (
    t.sort(lambda r: r.date)
    .derive(lambda r: {
        "daily_change": r.close.diff(),      # Close - previous close
        "volume_trend": r.volume.diff(2)     # Volume - volume from 2 rows ago
    })
)

# 4. cum_sum() - Cumulative calculations
result = (
    t.sort(lambda r: r.date)
    .cum_sum("volume", "amount")  # Adds cum_volume and cum_amount columns
)

# 5. Complex pipeline combining multiple operators
result = (
    t
    .sort(lambda r: r.date)
    .derive(lambda r: {
        "price_change": r.close - r.close.shift(1),
        "is_up": r.close > r.close.shift(1),
        "ma_20": r.close.rolling(20).mean(),
        "volatility": r.close.rolling(10).std()
    })
    .filter(lambda r: r.volatility > 2)  # High volatility days
    .select(lambda r: [r.date, r.close, r.is_up, r.ma_20, r.volatility])
)
```

##### **Important Notes on Window Functions**

1. **Sort Order is Required**: All window functions must follow a `.sort()` call
   ```python
   # ✅ CORRECT
   t.sort(lambda r: r.date).derive(lambda r: {"prev": r.col.shift(1)})
   
   # ❌ WRONG - Will fail
   t.derive(lambda r: {"prev": r.col.shift(1)})
   ```

2. **Sort Order is Preserved**: Window functions maintain the sort order established by the most recent `.sort()` call
   ```python
   t.sort(lambda r: r.date).derive(...).filter(...).derive(...)
   # Sort order by date is maintained throughout
   ```

3. **Dict-Based derive() API**: For complex multi-column operations
   ```python
   t.derive(lambda r: {
       "prev": r.col.shift(1),
       "next": r.col.shift(-1),
       "ma": r.col.rolling(5).mean()
   })
   ```

#### **3\. Set Algebra**

Operations treating tables as discrete sets of records.

| Method | Usage Example | SPL Equivalent | Description |
| :---- | :---- | :---- | :---- |
| **union** | t.union(other\_t) | A & B | **Union**. Vertically concatenates two tables. |
| **intersect** | t.intersect(other\_t, on=\["id"\]) | A ^ B | **Intersection**. Retains rows present in both tables. |
| **diff** | t.diff(other\_t, on=\["id"\]) | A \\ B | **Difference**. Retains rows in t that are not in other\_t. |
| **is\_subset** | t.is\_subset(other\_t) | n/a | **Subset Check**. Returns Boolean indicating if t is contained within other\_t. |

#### **4\. Association & Pointers (The Link)**

Replacing traditional Hash Joins with direct memory pointers for high performance.

| Method | Lambda Signature Example | SPL Equivalent | Description |
| :---- | :---- | :---- | :---- |
| **link** | link(target=DimT, on=lambda r, d: r.fk==d.pk, as\_="ptr") | T.switch | **Pointer Link**. Creates a virtual column pointing to objects in the target table (Foreign Key Objectification). |
| **join** | join(other, on=..., how="left") | join | **Standard Join**. Traditional SQL-style Hash Join. |
| **join\_merge** | join\_merge(other, on=...) | join@m | **Merge Join**. High-speed O(N) join for two tables that are already sorted. |
| **lookup** | derive(lambda r: {"name": r.fk.lookup(DimT)}) | T.find | **Direct Lookup**. Looks up a value in another table inside an expression (like VLOOKUP). |

#### **5\. Aggregation & Partitioning**

| Method | Lambda Signature Example | SPL Equivalent | Description |
| :---- | :---- | :---- | :---- |
| **agg** | agg(by=lambda r: r.area, sum\_v=lambda g: g.val.sum()) | T.groups | **Aggregation**. Returns a summary table with one row per group. |
| **partition** | partition(by=lambda r: r.area) | T.group | **Partition**. Returns a sequence of sub-tables (Nested Tables). No aggregation is performed. |
| **pivot** | pivot(index="date", columns="city", values="temp") | T.pivot | **Pivot**. Reshapes data from long format to wide format. |

---

### **⚡ Expression Capabilities (Inside Lambda)**

Within the lambda r: ... context, the expression builder supports the following vectorized operations:

* **Logical:** & (and), | (or), \~ (not), if\_else(cond, true\_val, false\_val)  
* **Arithmetic:** \+, \-, \*, /, // (floor div), % (mod)  
* **String:** r.s.contains(), r.s.starts\_with(), r.s.slice(), r.s.regex\_match()  
* **Temporal:** r.dt.year(), r.dt.month(), r.dt.add\_days(), r.dt.diff(other\_dt)  
* **Null Handling:** r.col.is\_null(), r.col.fill\_null(val)

---

### **🧬 DSL Showcase: The "Ordered" Difference**


```Python

from ltseq import LTSeq

# Task: Find all time intervals where a stock rose for more than 3 consecutive days.

t = LTSeq.read_csv("stock.csv")

result = (
    t
    .sort(lambda r: r.date)
    # 1. Procedural: Calculate rise/fall status relative to previous row
    .derive(lambda r: {
        "is_up": r.price > r.price.shift(1)
    })
    # 2. Ordered Grouping: Cut a new group only when 'is_up' changes
    #    (Groups consecutive True records together)
    .group_ordered(lambda r: r.is_up)
    
    # 3. Filtering on Sub-tables (Groups)
    #    'g' represents a sub-table object
    .filter(lambda g: 
        (g.first().is_up == True) &   # Must be a rising group
        (g.count() > 3)               # Must last > 3 days
    )
    
    # 4. Extract info from the group
    .derive(lambda g: {
        "start": g.first().date,
        "end":   g.last().date,
        "gain":  (g.last().price - g.first().price) / g.first().price
    })
)

```
