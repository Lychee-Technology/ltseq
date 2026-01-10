# LTSeq - Sequence Computing for Python

Fast, intuitive ordered computing on sequences of data. Process data as a sequence rather than an unordered set.

## Features

### Core Capabilities
- **Relational Operations**: filter, select, derive, sort, distinct, slice
- **Window Functions**: shift, rolling aggregates, cumulative sums, differences
- **Ranking Functions**: row_number, rank, dense_rank, ntile with window specifications
- **Ordered Computing**: group_ordered, search_first for processing sequences
- **Set Operations**: union, intersect, diff, subset checks
- **Joins**: Standard SQL joins, merge joins, semi/anti joins, as-of joins, lookups
- **Foreign Key Relationships**: Lightweight pointer-based linking
- **Aggregation**: group-by aggregation with statistical functions, partitioning, pivots
- **String Operations**: contains, replace, concat, pad, split, regex matching
- **Temporal Operations**: year, month, day, hour extraction, date arithmetic
- **Conditional Expressions**: if_else, null handling (fill_null, is_null)
- **Conditional Aggregations**: count_if, sum_if, avg_if, min_if, max_if
- **Streaming**: Cursor-based iteration for large datasets

### Performance
- Built on **DataFusion 51.0** - battle-tested SQL engine
- Pure Rust kernel for zero-copy operations
- Vectorized execution via Apache Arrow
- Pushes filters down to the storage layer

### Developer Experience
- **Pythonic API** - Lambda expressions, method chaining, intuitive syntax
- **Type hints** - Full IDE support and autocomplete
- **Lazy evaluation** - Build complex queries before execution
- **Zero setup** - Works with CSV, JSON, Parquet out of the box

## Quick Start

### Installation
```bash
pip install ltseq
```

### Basic Usage
```python
from ltseq import LTSeq

# Load data
t = LTSeq.read_csv("stock.csv")

# Sort by date (required for window functions)
t = t.sort(lambda r: r.date)

# Add derived columns with window functions
result = t.derive(lambda r: {
    "prev_close": r.close.shift(1),        # Previous row's value
    "price_change": r.close - r.close.shift(1),
    "ma_5": r.close.rolling(5).mean(),     # 5-day moving average
})

# Add cumulative sum columns
result = result.cum_sum("volume")  # Adds volume_cumsum column

# Filter and select
result = result.filter(lambda r: r.price_change > 0).select(lambda r: [
    r.date, r.close, r.price_change, r.ma_5
])

# Execute and collect results
df = result.to_pandas()
```

### DataFrame Operations

```python
# Filtering
t.filter(lambda r: r.amount > 100)

# Projection
t.select(lambda r: [r.id, r.name, r.amount])

# Sorting (required for window functions)
t.sort(lambda r: r.date)

# Adding computed columns
t.derive(lambda r: {"tax": r.amount * 0.1})

# Deduplication
t.distinct(lambda r: r.customer_id)

# Slicing
t.slice(offset=10, length=5)
```

### Pointer-Based Linking (Foreign Keys)

Link tables via foreign key relationships without materializing expensive joins:

```python
# Create a link between orders and products
orders = LTSeq.read_csv("orders.csv")        # id, product_id, quantity
products = LTSeq.read_csv("products.csv")    # product_id, name, price

linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.product_id,
    as_="prod"
)

# Filter on source columns (fast - no join yet)
filtered = linked.filter(lambda r: r.quantity > 10)

# Materialize when you need the joined data
result = filtered._materialize()
result.show()
```

**Features:**
- All four join types: `join_type="inner"` (default), `"left"`, `"right"`, `"full"`
- Composite keys: `on=lambda o, p: (o.year == p.year) & (o.product_id == p.product_id)`
- Lazy evaluation: Link created before join is executed
- See [Linking Guide](docs/LINKING_GUIDE.md) for detailed documentation


### Window Functions (Ordered Computing)

All window functions require a prior `.sort()` call.

```python
# Sort first!
t = t.sort(lambda r: r.date)

# Shift - Access values from adjacent rows
t.derive(lambda r: {
    "prev_value": r.col.shift(1),          # Previous row
    "next_value": r.col.shift(-1)          # Next row
})

# Rolling aggregates - Sliding window calculations
t.derive(lambda r: {
    "moving_avg": r.price.rolling(5).mean(),
    "moving_sum": r.volume.rolling(10).sum(),
    "moving_min": r.price.rolling(3).min(),
    "moving_max": r.price.rolling(3).max()
})

# Cumulative sum
t.cum_sum("revenue", "quantity")  # Adds cum_revenue and cum_quantity

# Difference
t.derive(lambda r: {
    "daily_change": r.price.diff(),        # price - previous price
    "change_2d": r.price.diff(2)           # price - price from 2 rows ago
})
```

### Ranking Functions

Assign positions or ranks to rows with `.over()` for window specification:

```python
from ltseq.expr import row_number, rank, dense_rank, ntile

t = LTSeq.read_csv("employees.csv")

# Row number within each department, ordered by salary
result = t.derive(lambda r: {
    "rn": row_number().over(partition_by=r.dept, order_by=r.salary, descending=True),
    "rnk": rank().over(partition_by=r.dept, order_by=r.salary, descending=True),
    "drnk": dense_rank().over(partition_by=r.dept, order_by=r.salary, descending=True),
    "quartile": ntile(4).over(partition_by=r.dept, order_by=r.salary)
})

# Simple global ranking
result = t.derive(lambda r: {
    "global_rank": rank().over(order_by=r.score)
})
```

### String Operations

Powerful string manipulation via the `.s` accessor:

```python
t = LTSeq.read_csv("data.csv")

result = t.derive(lambda r: {
    # Search and match
    "has_gmail": r.email.s.contains("gmail"),
    "is_order": r.code.s.starts_with("ORD"),
    "is_pdf": r.filename.s.ends_with(".pdf"),
    "valid_email": r.email.s.regex_match(r"^[a-z]+@[a-z]+\.[a-z]+$"),
    
    # Transform
    "lower_name": r.name.s.lower(),
    "upper_code": r.code.s.upper(),
    "clean_text": r.input.s.strip(),
    
    # Manipulate
    "clean_name": r.name.s.replace("-", "_"),
    "full_name": r.first.s.concat(" ", r.last),
    "padded_id": r.id.s.pad_left(5, "0"),
    "domain": r.email.s.split("@", 2),        # 1-based index
    "prefix": r.code.s.slice(0, 3)
})
```

### Temporal Operations

Date and time manipulation via the `.dt` accessor:

```python
from ltseq import LTSeq

t = LTSeq.read_csv("orders.csv")

result = t.derive(lambda r: {
    # Extract date components
    "year": r.order_date.dt.year(),
    "month": r.order_date.dt.month(),
    "day": r.order_date.dt.day(),
    
    # Extract time components  
    "hour": r.created_at.dt.hour(),
    "minute": r.created_at.dt.minute(),
    "second": r.created_at.dt.second(),
    
    # Date arithmetic
    "delivery_date": r.order_date.dt.add(days=5),
    "next_month": r.order_date.dt.add(months=1),
    "next_year": r.order_date.dt.add(years=1),
    
    # Date difference (in days)
    "days_since_order": r.ship_date.dt.diff(r.order_date)
})
```

### Conditional Expressions and Null Handling

```python
from ltseq import LTSeq
from ltseq.expr import if_else

t = LTSeq.read_csv("data.csv")

# Conditional expressions (SQL CASE WHEN)
result = t.derive(lambda r: {
    "category": if_else(r.price > 100, "expensive", "cheap"),
    "status": if_else(r.quantity > 0, "in_stock", "out_of_stock"),
    "discount": if_else(r.is_member, r.price * 0.1, 0)
})

# Null handling
result = t.derive(lambda r: {
    "safe_email": r.email.fill_null("unknown@example.com"),
    "safe_price": r.price.fill_null(0),
})

# Filter by null status
missing_emails = t.filter(lambda r: r.email.is_null())
valid_emails = t.filter(lambda r: r.email.is_not_null())
```

### Set Operations

```python
t1 = LTSeq.read_csv("dataset1.csv")
t2 = LTSeq.read_csv("dataset2.csv")

# Union (vertical concatenation, like SQL UNION ALL)
combined = t1.union(t2)

# Intersection (rows in both tables)
common = t1.intersect(t2, on=lambda r: r.id)

# Difference (rows in t1 but not in t2)
only_in_t1 = t1.diff(t2, on=lambda r: r.id)

# Subset check
is_sub = t1.is_subset(t2, on=lambda r: r.id)  # Returns bool
```

### Join Operations

```python
users = LTSeq.read_csv("users.csv")
orders = LTSeq.read_csv("orders.csv")
quotes = LTSeq.read_csv("quotes.csv")

# Standard hash join
joined = users.join(orders, on=lambda u, o: u.id == o.user_id, how="left")

# Merge join (for pre-sorted tables - more efficient)
users_sorted = users.sort("id")
orders_sorted = orders.sort("user_id")
merged = users_sorted.join_sorted(orders_sorted, on="id", how="inner")

# As-of join (time-series: find closest match)
trades = LTSeq.read_csv("trades.csv").sort("timestamp")
quotes = LTSeq.read_csv("quotes.csv").sort("timestamp")
with_quotes = trades.asof_join(
    quotes, 
    on=lambda t, q: t.symbol == q.symbol,
    direction="backward"  # Find most recent quote before trade
)
```

### Semi and Anti Joins

Filter rows based on key existence in another table:

```python
users = LTSeq.read_csv("users.csv")
orders = LTSeq.read_csv("orders.csv")

# Semi-join: Users who have placed at least one order
active_users = users.semi_join(orders, on=lambda u, o: u.id == o.user_id)

# Anti-join: Users who have never placed an order  
inactive_users = users.anti_join(orders, on=lambda u, o: u.id == o.user_id)
```

### Advanced Patterns

```python
# Financial metrics
t = (
    LTSeq.read_csv("daily_prices.csv")
    .sort(lambda r: r.date)
    .derive(lambda r: {
        "daily_return": (r.close - r.close.shift(1)) / r.close.shift(1),
        "volatility": r.close.rolling(20).std(),
        "ma_50": r.close.rolling(50).mean(),
        "rsi": r.close.rolling(14).mean()  # Simplified RSI
    })
    .filter(lambda r: r.volatility > 0.02)
)

# Ordered grouping (consecutive identical values)
t = (
    LTSeq.read_csv("events.csv")
    .sort(lambda r: r.timestamp)
    .derive(lambda r: {
        "status_changed": r.status != r.status.shift(1)
    })
    .group_ordered(lambda r: r.status_changed)
)

# Group aggregations with statistical methods
groups = t.sort("date").group_ordered(lambda r: r.category)
stats = groups.derive(lambda g: {
    "median_price": g.median("price"),
    "p95_price": g.percentile("price", 0.95),
    "price_std": g.std("price"),
    "price_var": g.variance("price"),
    "most_common": g.mode("status"),
    "top_3": g.top_k("amount", 3)
})

# Conditional aggregations within groups
stats = groups.derive(lambda g: {
    "high_value_count": g.count_if(lambda r: r.price > 100),
    "vip_total": g.sum_if(lambda r: r.is_vip, "amount"),
    "active_avg": g.avg_if(lambda r: r.status == "active", "score")
})

# Group quantifiers (all/any/none)
filtered_groups = groups.filter(lambda g: g.all(lambda r: r.price > 0))   # All positive
filtered_groups = groups.filter(lambda g: g.any(lambda r: r.is_vip))      # Has any VIP
filtered_groups = groups.filter(lambda g: g.none(lambda r: r.is_deleted)) # No deleted

# Method chaining
result = (
    t
    .sort(lambda r: r.date)
    .derive(lambda r: {"change": r.value.diff()})
    .filter(lambda r: r.change.is_null() == False)
    .select(lambda r: [r.date, r.value, r.change])
)
```

### Streaming Large Datasets

For datasets too large to fit in memory, use cursor-based streaming:

```python
from ltseq import LTSeq

# Create streaming cursor (doesn't load entire file)
cursor = LTSeq.scan("huge_file.csv")

# Process in batches
for batch in cursor:
    # batch is a RecordBatch (Arrow format)
    print(f"Processing {len(batch)} rows")
    
# Or materialize when needed
df = cursor.to_pandas()

# Parquet streaming
cursor = LTSeq.scan_parquet("data.parquet")
total_rows = cursor.count()  # Counts without loading all data
```

## Architecture

### Design Philosophy

LTSeq treats data as **sequences** rather than unordered sets. This distinction enables:

1. **Window Functions** - Reference adjacent rows (shift, rolling aggregates)
2. **Sequential Grouping** - Group only consecutive identical values
3. **Ordered Searches** - Binary search on sorted data
4. **State Machines** - Model processes as state transitions

Traditional dataframes are set-based (SQL, Pandas). LTSeq adds **sequence awareness** for temporal data, event logs, and state tracking.

### Technology Stack

- **Python Bindings**: PyO3 27.2 for seamless Python/Rust integration
- **SQL Engine**: Apache DataFusion 51.0 (powers Databricks, Apache Datafusion, etc.)
- **Data Format**: Apache Arrow for zero-copy columnar operations
- **Testing**: pytest with 999+ comprehensive tests

### Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| filter | O(n) | Vectorized, pushes to storage |
| select | O(n) | Projection pruning |
| sort | O(n log n) | In-memory sort |
| join | O(n + m) | Hash join |
| merge join | O(n + m) | For pre-sorted data |
| shift | O(n) | Window function, vectorized |
| rolling | O(n * w) | w = window size |
| cum_sum | O(n) | Single pass |

## API Reference

See [docs/api.md](docs/api.md) for the complete API specification including:
- Relational operations (filter, select, derive, sort, distinct, slice)
- Window functions (shift, rolling, cum_sum, diff)
- Ranking functions (row_number, rank, dense_rank, ntile)
- Ordered grouping (group_ordered, group_sorted, search_first)
- Set operations (union, intersect, diff, is_subset)
- Joins (join, join_merge, join_sorted, asof_join, semi_join, anti_join)
- Linking and lookups (link, lookup)
- Aggregation (agg, partition, pivot)
- String operations (contains, replace, concat, pad_left, pad_right, split, regex_match)
- Temporal operations (year, month, day, hour, minute, second, add, diff)
- Conditional expressions (if_else, fill_null, is_null, is_not_null)
- Conditional aggregations (count_if, sum_if, avg_if, min_if, max_if)
- GroupProxy methods (median, percentile, variance, std, mode, top_k, all, any, none)
- Streaming (scan, scan_parquet, Cursor)

## Testing

LTSeq includes a comprehensive test suite covering all functionality:

```bash
# Run all tests
pytest py-ltseq/tests/ -v

# Run specific test file
pytest py-ltseq/tests/test_linking_basic.py -v

# Current Status: 999+ tests passing
```

## Examples

### Example 1: Stock Price Analysis
```python
t = LTSeq.read_csv("stock.csv")

analysis = (
    t.sort(lambda r: r.date)
    .derive(lambda r: {
        "daily_change": r.close - r.close.shift(1),
        "is_up": r.close > r.close.shift(1),
        "ma_20": r.close.rolling(20).mean(),
        "ma_50": r.close.rolling(50).mean()
    })
    .filter(lambda r: r.is_up == True)
)

results = analysis.to_pandas()
```

### Example 2: Trend Detection
```python
t = LTSeq.read_csv("data.csv")

# Find periods with 3+ consecutive increases
trends = (
    t.sort(lambda r: r.date)
    .derive(lambda r: {"is_up": r.value > r.value.shift(1)})
    .group_ordered(lambda r: r.is_up)
    .filter(lambda g: (g.first().is_up == True) & (g.count() > 2))
    .derive(lambda g: {
        "trend_start": g.first().date,
        "trend_end": g.last().date,
        "change": g.last().value - g.first().value
    })
)
```

### Example 3: Event Duration Tracking
```python
t = LTSeq.read_csv("events.csv")

# Track consecutive event durations
durations = (
    t.sort(lambda r: r.timestamp)
    .derive(lambda r: {
        "status_changed": r.status != r.status.shift(1)
    })
    .group_ordered(lambda r: r.status_changed)
    .derive(lambda g: {
        "status": g.first().status,
        "start": g.first().timestamp,
        "end": g.last().timestamp,
        "duration": g.last().timestamp - g.first().timestamp,
        "event_count": g.count()
    })
)
```

## Limitations

- Window functions require an explicit `.sort()` call
- Ordered grouping (`group_ordered`) groups only consecutive identical values, not all identical values
- Large in-memory sorts are limited by available RAM
- Some advanced DataFusion features not yet exposed

## Contributing

Contributions welcome! Please refer to [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - See LICENSE file for details.

## Resources

- [Full API Documentation](docs/api.md)
- [Linking Guide](docs/LINKING_GUIDE.md) - Foreign key relationships and linking
- [Design Summary](docs/DESIGN_SUMMARY.md) - Architecture and design decisions
- **Examples** in `examples/` directory:
  - `linking_basic.py` - Basic order-product join
  - `linking_join_types.py` - All four join types
  - `linking_composite_keys.py` - Multi-column join keys
  - `linking_filtering.py` - Filtering patterns

## FAQ

**Q: How is this different from Pandas?**  
A: Pandas treats data as an unordered set with optional indices. LTSeq treats data as sequences with built-in window functions and ordered computing. Better for time series, event logs, and state tracking.

**Q: Do I need to sort before every window function?**  
A: Yes. Window functions require a `.sort()` call. The sort order is then preserved across subsequent operations.

**Q: What file formats are supported?**  
A: CSV, JSON, and Parquet. Via to_csv(), to_json(), and to_parquet() methods.

**Q: How do I join tables?**  
A: LTSeq offers two approaches:
1. **Linking**  - Lightweight pointer-based foreign keys with lazy evaluation
   - `orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_="prod")`
   - See [Linking Guide](docs/LINKING_GUIDE.md)
2. **Traditional SQL joins** - Full data materialization

**Q: Is this production-ready?**  
A: LTSeq is stable with 999+ passing tests covering all functionality. It's suitable for production use cases.

**Q: How does performance compare to Pandas?**  
A: LTSeq is generally faster for large datasets due to vectorized operations and the underlying DataFusion engine. For small datasets, both are comparable.

---

**Built with ❤️ using Rust and Python**
