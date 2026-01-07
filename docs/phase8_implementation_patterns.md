# Phase 8 Enhancement: Implementation Patterns & Code Examples

## Overview

This document provides concrete code examples for implementing Phase 8 full data joining. These patterns should be used as templates for the actual implementation.

---

## Pattern 1: Lambda Key Extraction (Phase 8A)

### Location
`py-ltseq/ltseq/__init__.py` - new function

### Pattern Code

```python
from .expr import BinOpExpr, ColumnExpr, SchemaProxy

def _extract_join_keys(join_fn: Callable, source_schema: Dict, target_schema: Dict) -> Tuple:
    """
    Extract join keys from a lambda expression.
    
    Examples:
        >>> join_fn = lambda o, p: o.product_id == p.id
        >>> left, right, jtype = _extract_join_keys(join_fn, orders_schema, products_schema)
        >>> left
        {'type': 'Column', 'name': 'product_id'}
        >>> right
        {'type': 'Column', 'name': 'id'}
    """
    # Create proxies for source and target tables
    source_proxy = SchemaProxy(source_schema)
    target_proxy = SchemaProxy(target_schema)
    
    try:
        # Call the lambda to capture the expression
        expr = join_fn(source_proxy, target_proxy)
    except AttributeError as e:
        raise AttributeError(f"Join lambda referenced invalid column: {e}")
    except Exception as e:
        raise ValueError(f"Failed to evaluate join condition: {e}")
    
    # Validate the expression is a simple equality
    if not isinstance(expr, BinOpExpr):
        raise ValueError(
            f"Join condition must be a binary expression, got {type(expr).__name__}"
        )
    
    if expr.op != "Eq":
        raise ValueError(
            f"Join condition must use equality (==), got {expr.op}"
        )
    
    # Extract left and right sides
    left_expr_dict = expr.left.serialize()
    right_expr_dict = expr.right.serialize()
    
    # Validate that we got column references (not literals)
    if not isinstance(expr.left, ColumnExpr):
        raise ValueError(
            f"Left side of join condition must be a column reference, "
            f"got {type(expr.left).__name__}"
        )
    
    if not isinstance(expr.right, ColumnExpr):
        raise ValueError(
            f"Right side of join condition must be a column reference, "
            f"got {type(expr.right).__name__}"
        )
    
    # Return: (left_key, right_key, join_type)
    return left_expr_dict, right_expr_dict, "inner"


# Usage in LinkedTable.__init__
class LinkedTable:
    def __init__(self, source_table: "LTSeq", target_table: "LTSeq", 
                 join_fn: Callable, alias: str):
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn
        self._alias = alias
        
        # Validate join condition early
        try:
            left_key, right_key, _ = _extract_join_keys(
                join_fn, 
                source_table._schema, 
                target_table._schema
            )
            self._left_key = left_key
            self._right_key = right_key
        except (ValueError, AttributeError) as e:
            raise TypeError(f"Invalid join condition: {e}")
        
        # Build merged schema
        self._schema = source_table._schema.copy()
        for col_name, col_type in target_table._schema.items():
            self._schema[f"{alias}_{col_name}"] = col_type
```

### Test Pattern

```python
def test_extract_join_keys_simple_equality():
    """Test extracting keys from simple equality join"""
    orders_schema = {"id": "int64", "product_id": "int64", "quantity": "int64"}
    products_schema = {"id": "int64", "name": "utf8", "price": "float64"}
    
    join_fn = lambda o, p: o.product_id == p.id
    left, right, jtype = _extract_join_keys(join_fn, orders_schema, products_schema)
    
    assert left == {"type": "Column", "name": "product_id"}
    assert right == {"type": "Column", "name": "id"}
    assert jtype == "inner"

def test_extract_join_keys_invalid_operator():
    """Test that non-equality conditions are rejected"""
    orders_schema = {"id": "int64", "product_id": "int64"}
    products_schema = {"id": "int64"}
    
    # Using > instead of ==
    join_fn = lambda o, p: o.product_id > p.id
    
    with pytest.raises(ValueError, match="equality"):
        _extract_join_keys(join_fn, orders_schema, products_schema)

def test_extract_join_keys_invalid_column():
    """Test that invalid column names are caught"""
    orders_schema = {"id": "int64"}
    products_schema = {"id": "int64"}
    
    join_fn = lambda o, p: o.nonexistent == p.id
    
    with pytest.raises(AttributeError):
        _extract_join_keys(join_fn, orders_schema, products_schema)
```

---

## Pattern 2: Rust Join Implementation (Phase 8B)

### Location
`src/lib.rs` - add to `impl RustTable` block

### Pattern Code

```rust
/// Join two tables based on join condition
///
/// Args:
///     other: Another RustTable to join with
///     left_key_expr_dict: Serialized expression dict for left join key
///     right_key_expr_dict: Serialized expression dict for right join key
///     join_type: "inner", "left", "right", "full"
///
/// Returns:
///     New RustTable with joined data
fn join(
    &self,
    other: &RustTable,
    left_key_expr_dict: &Bound<'_, PyDict>,
    right_key_expr_dict: &Bound<'_, PyDict>,
    join_type: String,
) -> PyResult<RustTable> {
    // 1. Validate inputs
    let self_df = self.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data loaded. Call read_csv() first.",
        )
    })?;
    
    let other_df = other.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data loaded. Call read_csv() first.",
        )
    })?;
    
    let self_schema = self.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table schema not available.",
        )
    })?;
    
    let other_schema = other.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table schema not available.",
        )
    })?;
    
    // 2. Deserialize join key expressions
    let left_py_expr = dict_to_py_expr(left_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    
    let right_py_expr = dict_to_py_expr(right_key_expr_dict)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    
    // 3. Transpile to DataFusion expressions
    let left_expr = pyexpr_to_datafusion(left_py_expr, self_schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
    
    let right_expr = pyexpr_to_datafusion(right_py_expr, other_schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
    
    // 4. Map join type string to DataFusion enum
    use datafusion::logical_expr::JoinType;
    let df_join_type = match join_type.as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" => JoinType::Full,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unknown join type: {}", join_type),
            ))
        }
    };
    
    // 5. Execute join (async operation)
    let joined_df = RUNTIME
        .block_on(async {
            let self_df_clone = (**self_df).clone();
            let other_df_clone = (**other_df).clone();
            
            // Perform the join
            self_df_clone
                .join(
                    other_df_clone,
                    df_join_type,
                    Some((vec![left_expr], vec![right_expr])),
                    None, // no additional filter
                )
                .map_err(|e| format!("Join execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
    
    // 6. Extract schema from joined DataFrame
    let joined_schema = joined_df.schema();
    let arrow_fields: Vec<Field> = joined_schema
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    let new_arrow_schema = ArrowSchema::new(arrow_fields);
    
    // 7. Return new RustTable with joined data
    Ok(RustTable {
        session: Arc::clone(&self.session),
        dataframe: Some(Arc::new(joined_df)),
        schema: Some(Arc::new(new_arrow_schema)),
        sort_exprs: self.sort_exprs.clone(),
    })
}
```

### Import Requirements

Add these imports at the top of `src/lib.rs`:

```rust
use datafusion::logical_expr::JoinType;
// (other imports already present)
```

---

## Pattern 3: LinkedTable Materialization (Phase 8C)

### Location
`py-ltseq/ltseq/__init__.py` - modify LinkedTable class

### Pattern Code

```python
class LinkedTable:
    def __init__(self, source_table: "LTSeq", target_table: "LTSeq", 
                 join_fn: Callable, alias: str):
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn
        self._alias = alias
        
        # Cache for materialized joined result
        self._materialized = None
        
        # Extract and cache join keys early for validation
        try:
            self._left_key, self._right_key, _ = _extract_join_keys(
                join_fn,
                source_table._schema,
                target_table._schema
            )
        except (ValueError, AttributeError) as e:
            raise TypeError(f"Invalid join condition: {e}")
        
        # Build merged schema with prefixed target columns
        self._schema = source_table._schema.copy()
        for col_name, col_type in target_table._schema.items():
            self._schema[f"{alias}_{col_name}"] = col_type
    
    def _materialize(self) -> "LTSeq":
        """
        Materialize the joined data.
        
        Performs the actual join operation if not already cached.
        Subsequent calls return the cached result.
        
        Returns:
            LTSeq: A new table containing the joined data
            
        Raises:
            RuntimeError: If join execution fails
        """
        # Return cached result if already materialized
        if self._materialized is not None:
            return self._materialized
        
        try:
            # Call Rust join operation
            joined_rust_table = self._source._rust_table.join(
                self._target._rust_table,
                self._left_key,
                self._right_key,
                "inner"  # MVP: only inner joins
            )
            
            # Create new LTSeq wrapping the joined RustTable
            result = LTSeq()
            result._rust_table = joined_rust_table
            
            # Use merged schema (includes prefixed target columns)
            result._schema = self._schema
            
            # Cache the result
            self._materialized = result
            
            return result
            
        except Exception as e:
            raise RuntimeError(f"Failed to materialize linked data: {e}")
    
    def show(self, n: int = 10) -> None:
        """
        Display the linked table (materialized).
        
        Args:
            n: Number of rows to display
        """
        joined = self._materialize()
        joined.show(n)
    
    def select(self, *cols) -> "LTSeq":
        """
        Select columns from the joined table.
        
        Args:
            *cols: Column names or selector lambdas
            
        Returns:
            LTSeq: Result table with selected columns
        """
        joined = self._materialize()
        return joined.select(*cols)
    
    def filter(self, predicate: Callable) -> "LinkedTable":
        """
        Filter rows in the joined table.
        
        Args:
            predicate: Lambda that returns True for rows to keep
            
        Returns:
            LinkedTable: New linked table with filtered data
        """
        joined = self._materialize()
        filtered = joined.filter(predicate)
        
        # Return a new LinkedTable wrapping the filtered result
        # (or just return as LTSeq since it's already materialized)
        return filtered  # Could be LTSeq instead
    
    def derive(self, mapper: Callable) -> "LinkedTable":
        """
        Add derived columns using the joined data.
        
        Args:
            mapper: Lambda that returns dict of new columns
            
        Returns:
            LinkedTable: New table with derived columns
        """
        joined = self._materialize()
        derived = joined.derive(mapper)
        return derived
    
    def slice(self, start: int, end: int) -> "LinkedTable":
        """
        Select a range of rows from the joined table.
        
        Args:
            start: Starting row index (0-based)
            end: Ending row index (exclusive)
            
        Returns:
            LinkedTable: New table with selected row range
        """
        joined = self._materialize()
        sliced = joined.slice(start, end)
        return sliced
```

### Test Pattern

```python
def test_link_lazy_materialization():
    """Test that link() is lazy - no join until accessed"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    # At this point, no join has happened
    assert linked._materialized is None

def test_link_materialize_on_show():
    """Test that show() triggers materialization"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    # Calling show() should materialize
    linked.show(5)
    
    # Now the materialized cache should be populated
    assert linked._materialized is not None

def test_link_caches_materialized():
    """Test that materialized result is cached"""
    orders = LTSeq.read_csv("examples/orders.csv")
    products = LTSeq.read_csv("examples/products.csv")
    
    linked = orders.link(
        products,
        on=lambda o, p: o.product_id == p.id,
        as_="prod"
    )
    
    # First materialization
    result1 = linked._materialize()
    
    # Second call should return cached result
    result2 = linked._materialize()
    
    assert result1 is result2  # Same object
```

---

## Pattern 4: Pointer Column Access (Phase 8D)

### Location
`py-ltseq/ltseq/expr.py` - modify SchemaProxy class

### Pattern Code

```python
class SchemaProxy:
    """
    Row proxy for capturing lambda expressions.
    
    Enhanced to support nested attribute access for linked tables:
    - Simple column: r.name → Column("name")
    - Linked column: r.prod.name → Column("prod_name")
    """
    
    def __init__(self, schema: Dict[str, str], prefix: str = ""):
        """
        Initialize a SchemaProxy.
        
        Args:
            schema: Dict mapping column names to types
            prefix: Prefix for linked table (e.g., "prod" for linked products)
        """
        self._schema = schema
        self._prefix = prefix
    
    def __getattr__(self, name: str) -> Union[ColumnExpr, "SchemaProxy"]:
        """
        Intercept attribute access for column references.
        
        Supports:
        - Simple columns: r.price → ColumnExpr("price")
        - Linked tables: r.prod → SchemaProxy with prefix="prod"
        - Linked columns: r.prod.price → ColumnExpr("prod_price")
        """
        # Prevent accessing private attributes
        if name.startswith("_"):
            raise AttributeError(f"No attribute {name}")
        
        # Check if this is a direct column reference
        if name in self._schema:
            return ColumnExpr(name)
        
        # Check if this might be a linked table reference
        # (e.g., "prod" when we have "prod_name", "prod_price", etc.)
        prefixed_name = f"{name}_" if not self._prefix else f"{self._prefix}_{name}_"
        matching_cols = [
            col for col in self._schema
            if col.startswith(f"{name}_")
        ]
        
        if matching_cols:
            # Return a new SchemaProxy for the linked table
            # Strip the prefix from column names for the nested proxy
            nested_schema = {}
            for col in matching_cols:
                # Remove the "prod_" prefix to get the base column name
                base_name = col.replace(f"{name}_", "")
                nested_schema[base_name] = self._schema[col]
            
            # Return proxy that will map back to prefixed columns
            return SchemaProxy._create_nested(self._schema, prefix=name)
        
        raise AttributeError(
            f"No column or linked table '{name}' in schema. "
            f"Available: {list(self._schema.keys())}"
        )
    
    @staticmethod
    def _create_nested(schema: Dict[str, str], prefix: str) -> "SchemaProxy":
        """
        Create a nested SchemaProxy for linked table access.
        
        This proxy intercepts the second level of attribute access
        and maps it to the prefixed column names.
        """
        class NestedSchemaProxy(SchemaProxy):
            def __getattr__(self_inner, name: str) -> ColumnExpr:
                # Map nested access to prefixed column name
                prefixed_col = f"{prefix}_{name}"
                if prefixed_col in schema:
                    return ColumnExpr(prefixed_col)
                
                raise AttributeError(
                    f"No column '{name}' in linked table '{prefix}'. "
                    f"Available: {[c.replace(prefix + '_', '') for c in schema if c.startswith(prefix + '_')]}"
                )
        
        return NestedSchemaProxy(schema, prefix=prefix)
```

### Test Pattern

```python
def test_schema_proxy_simple_column():
    """Test accessing simple columns"""
    schema = {"id": "int64", "name": "utf8", "price": "float64"}
    proxy = SchemaProxy(schema)
    
    col_expr = proxy.id
    assert isinstance(col_expr, ColumnExpr)
    assert col_expr.serialize() == {"type": "Column", "name": "id"}

def test_schema_proxy_linked_column():
    """Test accessing linked table columns"""
    schema = {
        "id": "int64",
        "product_id": "int64",
        "prod_id": "int64",
        "prod_name": "utf8",
        "prod_price": "float64"
    }
    proxy = SchemaProxy(schema)
    
    # First level: access the linked table
    prod_proxy = proxy.prod
    assert isinstance(prod_proxy, SchemaProxy)
    
    # Second level: access column in linked table
    name_expr = prod_proxy.name
    assert isinstance(name_expr, ColumnExpr)
    assert name_expr.serialize() == {"type": "Column", "name": "prod_name"}

def test_schema_proxy_invalid_column():
    """Test that invalid columns raise AttributeError"""
    schema = {"id": "int64", "name": "utf8"}
    proxy = SchemaProxy(schema)
    
    with pytest.raises(AttributeError):
        _ = proxy.nonexistent

def test_schema_proxy_invalid_linked_column():
    """Test that invalid linked columns raise AttributeError"""
    schema = {
        "id": "int64",
        "prod_name": "utf8",
        "prod_price": "float64"
    }
    proxy = SchemaProxy(schema)
    
    prod_proxy = proxy.prod
    
    with pytest.raises(AttributeError):
        _ = prod_proxy.nonexistent
```

---

## Pattern 5: Lambda Usage in select() (Phase 8D Extended)

### Location
`py-ltseq/ltseq/__init__.py` - modify select() method handling

### Pattern Code

```python
def _lambda_to_col_list(lambda_fn: Callable, schema: Dict) -> List[str]:
    """
    Convert a selector lambda to a list of column names.
    
    Examples:
        >>> lambda_fn = lambda r: [r.id, r.name, r.prod.price]
        >>> schema = {...}
        >>> _lambda_to_col_list(lambda_fn, schema)
        ["id", "name", "prod_price"]
    """
    proxy = SchemaProxy(schema)
    
    try:
        result = lambda_fn(proxy)
    except AttributeError as e:
        raise AttributeError(f"Invalid column reference in select: {e}")
    
    # Result should be a list of ColumnExpr
    if not isinstance(result, list):
        raise ValueError("select() lambda must return a list of column expressions")
    
    col_names = []
    for item in result:
        if isinstance(item, ColumnExpr):
            col_dict = item.serialize()
            col_names.append(col_dict["name"])
        else:
            raise ValueError(
                f"select() lambda must return ColumnExpr objects, "
                f"got {type(item).__name__}"
            )
    
    return col_names
```

### Usage Pattern

```python
# Example in user code
linked = orders.link(products, on=lambda o, p: o.product_id == p.id, as_="prod")

# Using lambda selector with linked columns
result = linked.select(
    lambda r: [
        r.id,           # Simple column
        r.quantity,     # Simple column
        r.prod.name,    # Linked column
        r.prod.price    # Linked column
    ]
)

result.show()
```

---

## Pattern 6: Complete Integration Example

### Full Example Workflow

```python
# ============================================================================
# Phase 8 Enhancement: Complete Example
# ============================================================================

from ltseq import LTSeq

# 1. Load data
orders = LTSeq.read_csv("examples/orders.csv")
products = LTSeq.read_csv("examples/products.csv")

# 2. Create linked table (lazy - no join yet)
linked = orders.link(
    products,
    on=lambda o, p: o.product_id == p.id,
    as_="prod"
)

# 3. Access data with linked columns (triggers materialization)

# Select columns from both tables
result = linked.select(
    lambda r: [r.id, r.quantity, r.prod.name, r.prod.price]
)
result.show()
# Output:
# +----+----------+--------+-------+
# | id | quantity | prod_name | price |
# +----+----------+--------+-------+
# | 1  | 5        | Widget | 50    |
# | 2  | 3        | Gadget | 75    |
# ...

# 4. Filter on linked columns
high_value = linked.filter(lambda r: r.prod.price > 75)
high_value.show()
# Output: Only rows where product price > 75

# 5. Derive new columns using linked data
with_revenue = linked.derive(
    lambda r: {
        "revenue": r.quantity * r.prod.price,
        "product_info": r.prod.name  # Can use this reference
    }
)
with_revenue.select(
    lambda r: [r.id, r.revenue, r.product_info]
).show()

# 6. Chain multiple operations
result = (linked
    .filter(lambda r: r.quantity > 2)
    .derive(lambda r: {"is_expensive": r.prod.price > 100})
    .select(lambda r: [r.id, r.prod.name, r.is_expensive])
)
result.show()
```

---

## Summary of Implementation Patterns

| Pattern | File | LOC | Purpose |
|---------|------|-----|---------|
| Lambda Key Extraction | __init__.py | 50 | Parse join conditions |
| Rust Join | lib.rs | 150 | Execute actual join |
| Materialization | __init__.py | 200 | Cache joined data |
| Column Access | expr.py | 120 | Support nested attributes |
| Integration | __init__.py | 50 | Tie it all together |

**Total:** ~570 LOC for MVP, ~1,500-2,000 LOC with error handling and documentation.

