# LTSeq API Extensions - Complete Reference

This document provides comprehensive coverage of all API extensions added in the latest release, achieving ~90% of planned API coverage expansion.

## Table of Contents

1. [Core Expression Extensions](#core-expression-extensions)
2. [String Operations](#string-operations)
3. [Temporal Operations](#temporal-operations)
4. [Lookup Pattern](#lookup-pattern)
5. [Examples](#examples)
6. [Migration Guide](#migration-guide)

---

## Core Expression Extensions

### if_else() Function

Conditional expression for creating values based on boolean conditions.

**Syntax:**
```python
from ltseq.expr import if_else

# In a derive or other expression context
if_else(condition, true_value, false_value)
```

**Parameters:**
- `condition`: Boolean expression (e.g., `r.age > 18`)
- `true_value`: Value/expression if condition is True
- `false_value`: Value/expression if condition is False

**Returns:**
- Expression that evaluates to true_value or false_value

**Example:**
```python
from ltseq import LTSeq
from ltseq.expr import if_else

# Load data
users = LTSeq.read_csv("users.csv")

# Derive a category based on age
categorized = users.derive(
    age_group=lambda r: if_else(r.age >= 18, "Adult", "Minor")
)
categorized.show()
# Output:
# | name | age | age_group |
# |------|-----|-----------|
# | Alice| 25  | Adult     |
# | Bob  | 17  | Minor     |
```

**SQL Translation:**
```sql
CASE WHEN age >= 18 THEN 'Adult' ELSE 'Minor' END
```

---

### fill_null() Method

Replace NULL values with a default value.

**Syntax:**
```python
# On a column in a lambda expression
r.column_name.fill_null(default_value)
```

**Parameters:**
- `default_value`: Value to use when column is NULL

**Returns:**
- Expression with NULLs replaced

**Example:**
```python
# Fill missing prices with 0
orders = LTSeq.read_csv("orders.csv")

filled = orders.derive(
    safe_price=lambda r: r.price.fill_null(0)
)
filled.show()
```

**SQL Translation:**
```sql
COALESCE(column_name, default_value)
```

---

### is_null() Method

Check if a value is NULL.

**Syntax:**
```python
# On a column in a lambda expression
r.column_name.is_null()
```

**Returns:**
- Boolean expression that is True when value is NULL

**Example:**
```python
# Filter rows where email is missing
users = LTSeq.read_csv("users.csv")

missing_emails = users.filter(lambda r: r.email.is_null())
missing_emails.show()
```

**SQL Translation:**
```sql
column_name IS NULL
```

---

### is_not_null() Method

Check if a value is NOT NULL.

**Syntax:**
```python
# On a column in a lambda expression
r.column_name.is_not_null()
```

**Returns:**
- Boolean expression that is True when value is NOT NULL

**Example:**
```python
# Filter rows where email exists
users = LTSeq.read_csv("users.csv")

valid_emails = users.filter(lambda r: r.email.is_not_null())
valid_emails.show()
```

**SQL Translation:**
```sql
column_name IS NOT NULL
```

---

## String Operations

String operations are accessed via the `.s` property on columns.

**Syntax:**
```python
r.column.s.operation()
```

### Available String Methods

#### contains(pattern)
Check if string contains a substring.

```python
# Filter for emails containing "gmail"
users = LTSeq.read_csv("users.csv")

gmail_users = users.filter(lambda r: r.email.s.contains("gmail"))
gmail_users.show()
```

**SQL Translation:**
```sql
POSITION('gmail' IN email) > 0
```

#### starts_with(prefix)
Check if string starts with a prefix.

```python
# Find orders with code starting with "ORD"
orders = LTSeq.read_csv("orders.csv")

order_codes = orders.filter(lambda r: r.code.s.starts_with("ORD"))
order_codes.show()
```

**SQL Translation:**
```sql
LEFT(code, LENGTH('ORD')) = 'ORD'
```

#### ends_with(suffix)
Check if string ends with a suffix.

```python
# Find files with .pdf extension
files = LTSeq.read_csv("files.csv")

pdfs = files.filter(lambda r: r.filename.s.ends_with(".pdf"))
pdfs.show()
```

**SQL Translation:**
```sql
RIGHT(filename, LENGTH('.pdf')) = '.pdf'
```

#### lower() / upper()
Convert case.

```python
# Create normalized email column
users = LTSeq.read_csv("users.csv")

normalized = users.derive(
    email_lower=lambda r: r.email.s.lower()
)
normalized.show()
```

**SQL Translation:**
```sql
LOWER(email)  -- or UPPER(email)
```

#### strip()
Remove leading/trailing whitespace.

```python
# Clean user names
users = LTSeq.read_csv("users.csv")

cleaned = users.derive(
    clean_name=lambda r: r.name.s.strip()
)
cleaned.show()
```

**SQL Translation:**
```sql
TRIM(name)
```

#### len()
Get string length.

```python
# Filter for long product names
products = LTSeq.read_csv("products.csv")

long_names = products.filter(lambda r: r.name.s.len() > 50)
long_names.show()
```

**SQL Translation:**
```sql
LENGTH(name)
```

#### slice(start, length)
Extract substring.

```python
# Extract year from date string "2024-01-15"
records = LTSeq.read_csv("records.csv")

years = records.derive(
    year=lambda r: r.date.s.slice(0, 4)
)
years.show()
```

**SQL Translation:**
```sql
SUBSTRING(date, start + 1, length)  -- Note: SQL uses 1-based indexing
```

#### regex_match(pattern)
Check if string matches a regular expression pattern (returns boolean).

```python
# Filter emails that match a pattern
users = LTSeq.read_csv("users.csv")

# Valid email pattern
valid_emails = users.filter(lambda r: r.email.s.regex_match(r'^[a-z]+@'))
valid_emails.show()

# Phone number pattern
contacts = LTSeq.read_csv("contacts.csv")
valid_phones = contacts.filter(lambda r: r.phone.s.regex_match(r'^\d{3}-\d{3}-\d{4}$'))
valid_phones.show()
```

**SQL Translation:**
```sql
REGEXP_LIKE(email, '^[a-z]+@')
```

---

## Temporal Operations

Temporal operations are accessed via the `.dt` property on date/datetime columns.

**Syntax:**
```python
r.column.dt.operation()
```

### Available Temporal Methods

#### year() / month() / day()
Extract date components.

```python
sales = LTSeq.read_csv("sales.csv")

# Extract components
by_date = sales.derive(
    year=lambda r: r.date.dt.year(),
    month=lambda r: r.date.dt.month(),
    day=lambda r: r.date.dt.day()
)
by_date.show()
```

**SQL Translation:**
```sql
EXTRACT(YEAR FROM date)
EXTRACT(MONTH FROM date)
EXTRACT(DAY FROM date)
```

#### hour() / minute() / second()
Extract time components.

```python
logs = LTSeq.read_csv("logs.csv")

# Extract time parts
with_time = logs.derive(
    hour=lambda r: r.timestamp.dt.hour(),
    minute=lambda r: r.timestamp.dt.minute()
)
with_time.show()
```

**SQL Translation:**
```sql
EXTRACT(HOUR FROM timestamp)
EXTRACT(MINUTE FROM timestamp)
EXTRACT(SECOND FROM timestamp)
```

#### add(days=0, months=0, years=0)
Add time intervals.

```python
orders = LTSeq.read_csv("orders.csv")

# Calculate delivery date (10 days from order)
with_delivery = orders.derive(
    delivery_date=lambda r: r.order_date.dt.add(days=10)
)
with_delivery.show()
```

**SQL Translation:**
```sql
date_col + INTERVAL '10' DAY + INTERVAL '0' MONTH + INTERVAL '0' YEAR
```

#### diff(other_date)
Calculate days between dates.

```python
orders = LTSeq.read_csv("orders.csv")

# Days between order and today (assuming 'today' column exists)
days_passed = orders.derive(
    days_since_order=lambda r: r.order_date.dt.diff(r.today)
)
days_passed.show()
```

**SQL Translation:**
```sql
DATEDIFF('day', order_date, today)
```

---

## Lookup Pattern

The lookup pattern provides a shorthand for performing simple lookups in related tables.

**Syntax:**
```python
r.key_column.lookup(target_table, "value_column")
```

**Parameters:**
- `target_table`: LTSeq instance to lookup from
- `"value_column"`: Column name to retrieve from target table
- `join_key` (optional): Column in target table to match against

**Example:**
```python
# Enrich orders with product information
orders = LTSeq.read_csv("orders.csv")  # Has: order_id, product_id, qty
products = LTSeq.read_csv("products.csv")  # Has: id, name, price

# Add product name and price to orders
enriched = orders.derive(
    product_name=lambda r: r.product_id.lookup(products, "name"),
    product_price=lambda r: r.product_id.lookup(products, "price")
)

enriched.show()
# Output:
# | order_id | product_id | qty | product_name | product_price |
# |----------|------------|-----|--------------|---------------|
# | 1        | 101        | 2   | Widget       | 29.99         |
# | 2        | 102        | 1   | Gadget       | 49.99         |
```

### Method Chaining with Lookup

Lookups can be chained with other operations:

```python
# Basic lookup - join key matches target table's primary key
enriched = orders.derive(
    product_name=lambda r: r.product_id.lookup(products, "name")
)

# Lookup with explicit join key (when target column name differs)
enriched = orders.derive(
    product_name=lambda r: r.prod_code.lookup(products, "name", join_key="product_code")
)
```

> **Note**: The lookup key should match the target table's key column.
> Transformations like `.lower()` on the key may cause lookup failures if
> the target table uses different casing.

---

## Examples

### Complete Workflow Example

```python
from ltseq import LTSeq
from ltseq.expr import if_else

# Load base data
orders = LTSeq.read_csv("orders.csv")
customers = LTSeq.read_csv("customers.csv")
products = LTSeq.read_csv("products.csv")

# Rich derivation using new features
enriched = orders.derive(
    # String operations
    clean_order_id=lambda r: r.order_id.s.strip(),
    is_premium_customer=lambda r: r.customer_id.s.starts_with("PREM"),
    
    # Lookup pattern
    customer_name=lambda r: r.customer_id.lookup(customers, "name"),
    product_name=lambda r: r.product_id.lookup(products, "name"),
    product_price=lambda r: r.product_id.lookup(products, "price"),
    
    # Temporal operations
    order_year=lambda r: r.order_date.dt.year(),
    order_month=lambda r: r.order_date.dt.month(),
    delivery_target=lambda r: r.order_date.dt.add(days=5),
    
    # Conditional logic
    order_status=lambda r: if_else(
        r.quantity > 10,
        "Bulk Order",
        "Standard Order"
    ),
    
    # Combined: String + Conditional
    category=lambda r: if_else(
        r.product_name.s.contains("Premium"),
        "High-End",
        "Standard"
    ),
    
    # Handle missing values
    discount=lambda r: r.discount_rate.fill_null(0)
)

# Filter and analyze
results = enriched.filter(
    lambda r: r.order_year == 2024
)

results.show()
```

### String Processing Example

```python
# Data cleanup and normalization
users = LTSeq.read_csv("users.csv")

cleaned = users.derive(
    # Normalize text
    first_name_clean=lambda r: r.first_name.s.strip().s.lower(),
    last_name_clean=lambda r: r.last_name.s.strip().s.upper(),
    
    # Validation checks
    email_valid=lambda r: if_else(
        r.email.s.contains("@"),
        "valid",
        "invalid"
    ),
    
    # Extract portions
    domain=lambda r: r.email.s.slice(r.email.s.len() - 10, 10),
    
    # Length checks
    has_long_name=lambda r: if_else(
        r.first_name.s.len() > 10,
        "Long",
        "Short"
    )
)

cleaned.show()
```

### Time Series Analysis

```python
# Analyze events with temporal context
events = LTSeq.read_csv("events.csv")

analyzed = events.derive(
    # Extract components
    event_year=lambda r: r.event_time.dt.year(),
    event_month=lambda r: r.event_time.dt.month(),
    event_hour=lambda r: r.event_time.dt.hour(),
    
    # Calculate offsets
    future_date=lambda r: r.event_time.dt.add(days=30),
    
    # Time-based categorization
    time_of_day=lambda r: if_else(
        r.event_time.dt.hour() < 12,
        "Morning",
        if_else(
            r.event_time.dt.hour() < 18,
            "Afternoon",
            "Evening"
        )
    )
)

analyzed.show()
```

---

## Migration Guide

### From Raw SQL to Expression Extensions

**Before (SQL):**
```python
# Had to use raw SQL for complex operations
results = table.filter_where(
    "email LIKE '%@gmail.com%' AND age >= 18"
)
```

**After (Expression Extensions):**
```python
# Clean, Pythonic expressions
results = table.filter(
    lambda r: (r.email.s.contains("@gmail.com")) & (r.age >= 18)
)
```

### From Pandas Operations to Expression API

**Before (Pandas):**
```python
df = table.to_pandas()
df['category'] = df['age'].apply(
    lambda x: 'Adult' if x >= 18 else 'Minor'
)
# Had to convert back...
```

**After (Expression API):**
```python
from ltseq.expr import if_else

table.derive(
    category=lambda r: if_else(r.age >= 18, "Adult", "Minor")
)
```

### Common Patterns

#### NULL Handling
```python
# Old: Required special pandas logic
# New: Simple and declarative
table.derive(
    safe_value=lambda r: r.value.fill_null(0)
)
```

#### String Normalization
```python
# Old: Pandas string methods
# New: Clean chaining
table.derive(
    clean_email=lambda r: r.email.s.lower().s.strip()
)
```

#### Date Extraction
```python
# Old: Required datetime parsing
# New: Simple temporal accessors
table.derive(
    year=lambda r: r.date.dt.year(),
    month=lambda r: r.date.dt.month()
)
```

#### Lookups
```python
# Old: Complicated pandas merge
# New: Simple lookup syntax
orders.derive(
    product_name=lambda r: r.product_id.lookup(products, "name")
)
```

---

## Performance Characteristics

All expression extensions are optimized for performance:

- **String Operations**: Mapped to SQL standard functions (POSITION, LENGTH, SUBSTRING, etc.)
- **Temporal Operations**: Use native SQL EXTRACT and INTERVAL arithmetic
- **if_else()**: Translates to SQL CASE WHEN (evaluated server-side)
- **fill_null()**: Translates to SQL COALESCE (efficient NULL handling)
- **Lookups**: Implemented as optimized join operations

### Execution Context

All expressions are:
1. **Serialized** to JSON representation
2. **Transpiled** to SQL/DataFusion expressions
3. **Executed** at the Rust/database layer
4. **Never** evaluated in Python (except lambda interception)

This ensures maximum performance and scalability.

---

## API Coverage Summary

| Feature | Status | SQL Translation |
|---------|--------|-----------------|
| if_else() | ✅ Complete | CASE WHEN ... THEN ... ELSE ... END |
| fill_null() | ✅ Complete | COALESCE(col, default) |
| is_null() | ✅ Complete | col IS NULL |
| is_not_null() | ✅ Complete | col IS NOT NULL |
| String: contains() | ✅ Complete | POSITION(str IN col) > 0 |
| String: starts_with() | ✅ Complete | LEFT(col, LEN(str)) = str |
| String: ends_with() | ✅ Complete | RIGHT(col, LEN(str)) = str |
| String: lower() | ✅ Complete | LOWER(col) |
| String: upper() | ✅ Complete | UPPER(col) |
| String: strip() | ✅ Complete | TRIM(col) |
| String: len() | ✅ Complete | LENGTH(col) |
| String: slice() | ✅ Complete | SUBSTRING(col, start, length) |
| String: regex_match() | ✅ Complete | REGEXP_LIKE(col, pattern) |
| Temporal: year() | ✅ Complete | EXTRACT(YEAR FROM col) |
| Temporal: month() | ✅ Complete | EXTRACT(MONTH FROM col) |
| Temporal: day() | ✅ Complete | EXTRACT(DAY FROM col) |
| Temporal: hour() | ✅ Complete | EXTRACT(HOUR FROM col) |
| Temporal: minute() | ✅ Complete | EXTRACT(MINUTE FROM col) |
| Temporal: second() | ✅ Complete | EXTRACT(SECOND FROM col) |
| Temporal: add() | ✅ Complete | col + INTERVAL ... |
| Temporal: diff() | ✅ Complete | DATEDIFF('day', col1, col2) |
| Lookup pattern | ✅ Complete | JOIN operation |

**Total Expression API Coverage: 100% of documented expressions implemented**

> **Note**: This covers expression-level APIs. See `api.md` for table-level operations.
> Window functions (shift, rolling, cum_sum, diff) have implementation but require further testing.

---

## Future Enhancements

All planned expression extensions have been implemented. Future work may include:
- Additional string methods (replace, split, join)
- Additional temporal methods (week, quarter, day of week)
- Expression-based alternatives for callable-dependent operations

See [ROADMAP.md](ROADMAP.md) for the overall development roadmap.
