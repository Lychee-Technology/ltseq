# LTSeq - Sequence Computing for Python

Fast, intuitive ordered computing on sequences of data. Process data as a sequence rather than an unordered set.

## Features

### Core Capabilities
- **Relational Operations**: filter, select, derive, sort, distinct, slice
- **Window Functions**: shift, rolling aggregates, cumulative sums, differences
- **Ordered Computing**: group_ordered, search_first for processing sequences
- **Set Operations**: union, intersect, diff, subset checks
- **Joins**: Standard SQL joins, merge joins, lookups
- **Foreign Key Relationships**: Lightweight pointer-based linking (Phase 8)
- **Aggregation**: group-by aggregation, partitioning, pivots

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
t = LTSeq.from_csv("stock.csv")

# Sort by date (required for window functions)
t = t.sort(lambda r: r.date)

# Add derived columns with window functions
result = t.derive(lambda r: {
    "prev_close": r.close.shift(1),        # Previous row's value
    "price_change": r.close - r.close.shift(1),
    "ma_5": r.close.rolling(5).mean(),     # 5-day moving average
    "cumulative_volume": r.volume.cum_sum()
})

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
- See [Phase 8 Linking Guide](docs/LINKING_GUIDE.md) for detailed documentation


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

### Advanced Patterns

```python
# Financial metrics
t = (
    LTSeq.from_csv("daily_prices.csv")
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
    LTSeq.from_csv("events.csv")
    .sort(lambda r: r.timestamp)
    .derive(lambda r: {
        "status_changed": r.status != r.status.shift(1)
    })
    .group_ordered(lambda r: r.status_changed)
)

# Method chaining
result = (
    t
    .sort(lambda r: r.date)
    .derive(lambda r: {"change": r.value.diff()})
    .filter(lambda r: r.change.is_null() == False)
    .select(lambda r: [r.date, r.value, r.change])
)
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
- **Testing**: pytest with 493+ comprehensive tests

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
- Ordered grouping (group_ordered, search_first)
- Set operations (union, intersect, diff)
- Joins and lookups (join, merge_join, lookup)
- Aggregation (agg, partition, pivot)

## Testing

LTSeq includes a comprehensive test suite covering all functionality:

```bash
# Run all tests
pytest py-ltseq/tests/ -v

# Run specific test file
pytest py-ltseq/tests/test_phase8_linking.py -v

# Current Status: 493+ tests passing
```

## Examples

### Example 1: Stock Price Analysis
```python
t = LTSeq.from_csv("stock.csv")

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
t = LTSeq.from_csv("data.csv")

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
t = LTSeq.from_csv("events.csv")

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
- [Phase 8 Linking Guide](docs/LINKING_GUIDE.md) - Foreign key relationships and linking
- [Phase 8J Linking Limitations](docs/phase8j_limitations.md) - Known limitations and workarounds
- [Phase 6 Implementation Details](docs/phase6_session_summary.md)
- [Design Decisions](docs/phase6_design_decisions.md)
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
1. **Linking** (Phase 8) - Lightweight pointer-based foreign keys with lazy evaluation
   - `orders.link(products, on=lambda o, p: o.product_id == p.product_id, as_="prod")`
   - See [Phase 8 Linking Guide](docs/phase8_linking_guide.md)
2. **Traditional SQL joins** - Full data materialization

**Q: Is this production-ready?**  
A: LTSeq is stable with 493+ passing tests covering all functionality. It's suitable for production use cases.

**Q: How does performance compare to Pandas?**  
A: LTSeq is generally faster for large datasets due to vectorized operations and the underlying DataFusion engine. For small datasets, both are comparable.

---

**Built with ❤️ using Rust and Python**
