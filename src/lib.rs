use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, SortExpr};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use lazy_static::lazy_static;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

// Global Tokio runtime for async operations

// Module declarations
mod error;
mod types;
mod helpers;

// Re-exports for convenience
pub use error::*;
pub use types::*;

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().expect("Failed to create Tokio runtime");
}

/// Error type for PyExpr operations
#[derive(Debug)]
enum PyExprError {
    MissingField(String),
    InvalidType(String),
    UnknownVariant(String),
    DeserializationFailed(String),
    TranspilationFailed(String),
}

impl std::fmt::Display for PyExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PyExprError::MissingField(field) => write!(f, "Missing field: {}", field),
            PyExprError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
            PyExprError::UnknownVariant(var) => write!(f, "Unknown expression type: {}", var),
            PyExprError::DeserializationFailed(msg) => write!(f, "Deserialization failed: {}", msg),
            PyExprError::TranspilationFailed(msg) => write!(f, "Transpilation failed: {}", msg),
        }
    }
}

impl std::error::Error for PyExprError {}

/// Represents a serialized Python expression for transpilation to DataFusion
#[derive(Debug, Clone)]
enum PyExpr {
    /// Column reference: {"type": "Column", "name": "age"}
    Column(String),

    /// Literal value: {"type": "Literal", "value": "18", "dtype": "Int64"}
    Literal { value: String, dtype: String },

    /// Binary operation: {"type": "BinOp", "op": "Gt", "left": {...}, "right": {...}}
    BinOp {
        op: String,
        left: Box<PyExpr>,
        right: Box<PyExpr>,
    },

    /// Unary operation: {"type": "UnaryOp", "op": "Not", "operand": {...}}
    UnaryOp { op: String, operand: Box<PyExpr> },

    /// Method call: {"type": "Call", "func": "shift", "args": [...], "kwargs": {...}, "on": {...}}
    Call {
        func: String,
        args: Vec<PyExpr>,
        kwargs: HashMap<String, PyExpr>,
        on: Box<PyExpr>,
    },
}

/// Recursively deserialize a Python dict to PyExpr
fn dict_to_py_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    // 1. Get "type" field
    let expr_type = dict
        .get_item("type")
        .map_err(|_| PyExprError::MissingField("type".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("type".to_string()))?
        .extract::<String>()
        .map_err(|_| PyExprError::InvalidType("type must be string".to_string()))?;

    // 2. Match on expression type
    match expr_type.as_str() {
        "Column" => {
            let name = dict
                .get_item("name")
                .map_err(|_| PyExprError::MissingField("name".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("name".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("name must be string".to_string()))?;
            Ok(PyExpr::Column(name))
        }

        "Literal" => {
            let value_obj = dict
                .get_item("value")
                .map_err(|_| PyExprError::MissingField("value".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("value".to_string()))?;

            // Convert Python value to string (handles int, float, str, bool, None)
            let value = value_obj.to_string();

            let dtype = dict
                .get_item("dtype")
                .map_err(|_| PyExprError::MissingField("dtype".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("dtype".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("dtype must be string".to_string()))?;
            Ok(PyExpr::Literal { value, dtype })
        }

        "BinOp" => {
            let op = dict
                .get_item("op")
                .map_err(|_| PyExprError::MissingField("op".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("op".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("op must be string".to_string()))?;

            let left_item = dict
                .get_item("left")
                .map_err(|_| PyExprError::MissingField("left".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("left".to_string()))?;
            let left_dict = left_item
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("left must be dict".to_string()))?;

            let right_item = dict
                .get_item("right")
                .map_err(|_| PyExprError::MissingField("right".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("right".to_string()))?;
            let right_dict = right_item
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("right must be dict".to_string()))?;

            let left = Box::new(dict_to_py_expr(&left_dict)?);
            let right = Box::new(dict_to_py_expr(&right_dict)?);
            Ok(PyExpr::BinOp { op, left, right })
        }

        "UnaryOp" => {
            let op = dict
                .get_item("op")
                .map_err(|_| PyExprError::MissingField("op".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("op".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("op must be string".to_string()))?;

            let operand_item = dict
                .get_item("operand")
                .map_err(|_| PyExprError::MissingField("operand".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("operand".to_string()))?;
            let operand_dict = operand_item
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("operand must be dict".to_string()))?;

            let operand = Box::new(dict_to_py_expr(&operand_dict)?);
            Ok(PyExpr::UnaryOp { op, operand })
        }

        "Call" => {
            let func = dict
                .get_item("func")
                .map_err(|_| PyExprError::MissingField("func".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("func".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("func must be string".to_string()))?;

            let on_item = dict
                .get_item("on")
                .map_err(|_| PyExprError::MissingField("on".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("on".to_string()))?;

            // Handle None value for 'on' (for function calls, not method calls)
            let on = if on_item.is_none() {
                // For function calls like abs(...), on is None
                // We'll create a dummy Column expression or handle this specially
                Box::new(PyExpr::Column("__dummy__".to_string()))
            } else {
                let on_dict = on_item
                    .cast::<PyDict>()
                    .map_err(|_| PyExprError::InvalidType("on must be dict or None".to_string()))?;
                Box::new(dict_to_py_expr(&on_dict)?)
            };

            // Deserialize args list
            let args_item = dict
                .get_item("args")
                .map_err(|_| PyExprError::MissingField("args".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("args".to_string()))?;
            let args_list = args_item
                .cast::<PyList>()
                .map_err(|_| PyExprError::InvalidType("args must be list".to_string()))?;
            let mut args = Vec::new();
            for arg_item in args_list.iter() {
                let arg_dict = arg_item
                    .cast::<PyDict>()
                    .map_err(|_| PyExprError::InvalidType("arg must be dict".to_string()))?;
                args.push(dict_to_py_expr(&arg_dict)?);
            }

            // Deserialize kwargs dict
            let kwargs_item = dict
                .get_item("kwargs")
                .map_err(|_| PyExprError::MissingField("kwargs".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("kwargs".to_string()))?;
            let kwargs_py = kwargs_item
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("kwargs must be dict".to_string()))?;
            let mut kwargs = HashMap::new();
            for (key, val) in kwargs_py.iter() {
                let key_str = key.extract::<String>().map_err(|_| {
                    PyExprError::InvalidType("kwargs key must be string".to_string())
                })?;
                let val_dict = val.cast::<PyDict>().map_err(|_| {
                    PyExprError::InvalidType("kwargs value must be dict".to_string())
                })?;
                kwargs.insert(key_str, dict_to_py_expr(&val_dict)?);
            }

            Ok(PyExpr::Call {
                func,
                args,
                kwargs,
                on,
            })
        }

        _ => Err(PyExprError::UnknownVariant(expr_type)),
    }
}

/// Convert PyExpr to DataFusion Expr
fn pyexpr_to_datafusion(py_expr: PyExpr, schema: &ArrowSchema) -> Result<Expr, String> {
    match py_expr {
        PyExpr::Column(name) => {
            // Validate column exists in schema
            if !schema.fields().iter().any(|f| f.name() == &name) {
                return Err(format!("Column '{}' not found in schema", name));
            }
            Ok(col(name))
        }

        PyExpr::Literal { value, dtype } => {
            // Parse dtype and convert value string to appropriate literal type
            match dtype.as_str() {
                "Int64" => {
                    let int_val = value
                        .parse::<i64>()
                        .map_err(|_| format!("Failed to parse '{}' as Int64", value))?;
                    Ok(lit(int_val))
                }
                "Int32" => {
                    let int_val = value
                        .parse::<i32>()
                        .map_err(|_| format!("Failed to parse '{}' as Int32", value))?;
                    Ok(lit(int_val))
                }
                "Float64" => {
                    let float_val = value
                        .parse::<f64>()
                        .map_err(|_| format!("Failed to parse '{}' as Float64", value))?;
                    Ok(lit(float_val))
                }
                "Float32" => {
                    let float_val = value
                        .parse::<f32>()
                        .map_err(|_| format!("Failed to parse '{}' as Float32", value))?;
                    Ok(lit(float_val))
                }
                "String" | "Utf8" => Ok(lit(value)),
                "Boolean" | "Bool" => {
                    let bool_val = value
                        .parse::<bool>()
                        .map_err(|_| format!("Failed to parse '{}' as Boolean", value))?;
                    Ok(lit(bool_val))
                }
                "Null" => {
                    // NULL literal - represents None in Python
                    // When comparing with NULL using == or !=, DataFusion handles it properly
                    // We use a null value which works with IS NULL / IS NOT NULL comparisons
                    Ok(lit(ScalarValue::Null))
                }
                _ => Err(format!("Unknown dtype: {}", dtype)),
            }
        }

        PyExpr::BinOp { op, left, right } => {
            let left_expr = pyexpr_to_datafusion(*left, schema)?;
            let right_expr = pyexpr_to_datafusion(*right, schema)?;

            // Map string ops to DataFusion Operator
            let operator = match op.as_str() {
                "Add" => Operator::Plus,
                "Sub" => Operator::Minus,
                "Mul" => Operator::Multiply,
                "Div" => Operator::Divide,
                "Mod" => Operator::Modulo,
                "Eq" => Operator::Eq,
                "Ne" => Operator::NotEq,
                "Lt" => Operator::Lt,
                "Le" => Operator::LtEq,
                "Gt" => Operator::Gt,
                "Ge" => Operator::GtEq,
                "And" => Operator::And,
                "Or" => Operator::Or,
                _ => return Err(format!("Unknown binary operator: {}", op)),
            };

            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr),
                operator,
                Box::new(right_expr),
            )))
        }

        PyExpr::UnaryOp { op, operand } => {
            let operand_expr = pyexpr_to_datafusion(*operand, schema)?;

            match op.as_str() {
                "Not" => Ok(operand_expr.not()),
                _ => Err(format!("Unknown unary operator: {}", op)),
            }
        }

        PyExpr::Call {
            func,
            args: _,
            kwargs: _,
            on,
        } => {
            // Phase 6: Window functions are now recognized but require special handling
            // They will be transpiled at the DataFrame level in derive()
            match func.as_str() {
                "is_null" => {
                    let on_expr = pyexpr_to_datafusion(*on, schema)?;
                    Ok(on_expr.is_null())
                }
                "is_not_null" => {
                    let on_expr = pyexpr_to_datafusion(*on, schema)?;
                    Ok(on_expr.is_not_null())
                }
                // Math functions that will be handled in SQL
                "abs" | "ceil" | "floor" | "round" => {
                    // These will be handled in the SQL transpilation path
                    Err(format!("Math function '{}' requires window context - should be handled in derive()", func))
                }
                // Window functions - these will be handled specially in derive()
                "shift" | "rolling" | "diff" | "cum_sum" | "mean" | "sum" | "min" | "max"
                | "count" => {
                    // For now, return an error indicating the window function needs DataFrame-level handling
                    Err(format!("Window function '{}' requires DataFrame context - should be handled in derive()", func))
                }
                _ => Err(format!("Method '{}' not yet supported", func)),
            }
        }
    }
}

/// Helper function to detect if a PyExpr contains a window function call
fn contains_window_function(py_expr: &PyExpr) -> bool {
    match py_expr {
        PyExpr::Call { func, on, .. } => {
            // Direct window functions
            if matches!(func.as_str(), "shift" | "rolling" | "diff" | "cum_sum") {
                return true;
            }
            // Aggregation functions applied to rolling windows
            if matches!(func.as_str(), "mean" | "sum" | "min" | "max" | "count") {
                // Check if this is applied to a rolling() call
                if let PyExpr::Call {
                    func: inner_func, ..
                } = &**on
                {
                    if matches!(inner_func.as_str(), "rolling") {
                        return true;
                    }
                }
            }
            // Check recursively in the `on` field
            contains_window_function(on)
        }
        PyExpr::BinOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        PyExpr::UnaryOp { operand, .. } => contains_window_function(operand),
        _ => false,
    }
}

/// Helper function to check if a DataType is numeric (can be summed)
fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Null // Allow Null type for empty tables
    )
}

/// Helper function to convert PyExpr to SQL string representation for window functions
fn pyexpr_to_sql(py_expr: &PyExpr, schema: &ArrowSchema) -> Result<String, String> {
    match py_expr {
        PyExpr::Column(name) => {
            if !schema.fields().iter().any(|f| f.name() == name) {
                return Err(format!("Column '{}' not found in schema", name));
            }
            Ok(format!("\"{}\"", name))
        }

        PyExpr::Literal { value, dtype } => match dtype.as_str() {
            "String" | "Utf8" => Ok(format!("'{}'", value)),
            _ => Ok(value.clone()),
        },

        PyExpr::BinOp { op, left, right } => {
            let left_sql = pyexpr_to_sql(left, schema)?;
            let right_sql = pyexpr_to_sql(right, schema)?;
            let op_str = match op.as_str() {
                "Add" => "+",
                "Sub" => "-",
                "Mul" => "*",
                "Div" => "/",
                "Mod" => "%",
                "Eq" => "=",
                "Ne" => "!=",
                "Lt" => "<",
                "Le" => "<=",
                "Gt" => ">",
                "Ge" => ">=",
                "And" => "AND",
                "Or" => "OR",
                _ => return Err(format!("Unknown binary operator: {}", op)),
            };
            Ok(format!("({} {} {})", left_sql, op_str, right_sql))
        }

        PyExpr::UnaryOp { op, operand } => {
            let operand_sql = pyexpr_to_sql(operand, schema)?;
            match op.as_str() {
                "Not" => Ok(format!("(NOT {})", operand_sql)),
                _ => Err(format!("Unknown unary operator: {}", op)),
            }
        }

        PyExpr::Call {
            func,
            args,
            kwargs: _,
            on,
        } => {
            match func.as_str() {
                "shift" => {
                    let on_sql = pyexpr_to_sql(on, schema)?;
                    let offset = if args.is_empty() {
                        1
                    } else if let PyExpr::Literal { value, .. } = &args[0] {
                        value.parse::<i32>().unwrap_or(1)
                    } else {
                        return Err("shift() offset must be a literal integer".to_string());
                    };

                    if offset >= 0 {
                        Ok(format!("LAG({}, {})", on_sql, offset))
                    } else {
                        Ok(format!("LEAD({}, {})", on_sql, -offset))
                    }
                }

                "rolling" => {
                    // rolling() should not be called directly - it's handled through the agg method
                    Err(
                        "rolling() should not reach pyexpr_to_sql - it's handled separately"
                            .to_string(),
                    )
                }

                "diff" => {
                    let on_sql = pyexpr_to_sql(on, schema)?;
                    let periods = if args.is_empty() {
                        1
                    } else if let PyExpr::Literal { value, .. } = &args[0] {
                        value.parse::<i32>().unwrap_or(1)
                    } else {
                        return Err("diff() periods must be a literal integer".to_string());
                    };

                    // diff(n) = col - LAG(col, n) - we'll handle the OVER clause in derive_with_window_functions
                    // Mark this as a diff operation so we know to apply the window frame to LAG
                    Ok(format!("__DIFF_{}__({})__{}", periods, on_sql, periods))
                }

                // Aggregation functions on rolling windows: mean(), sum(), min(), max(), count()
                "mean" | "sum" | "min" | "max" | "count" => {
                    // Check if this is applied to a rolling() call
                    if let PyExpr::Call {
                        func: inner_func,
                        args: inner_args,
                        on: inner_on,
                        ..
                    } = &**on
                    {
                        if inner_func == "rolling" {
                            // Get the window size from rolling() args
                            let window_size = if inner_args.is_empty() {
                                return Err("rolling() requires a window size".to_string());
                            } else if let PyExpr::Literal { value, .. } = &inner_args[0] {
                                value.parse::<i32>().unwrap_or(1)
                            } else {
                                return Err(
                                    "rolling() window size must be a literal integer".to_string()
                                );
                            };

                            // Get the column being aggregated
                            let col_sql = pyexpr_to_sql(inner_on, schema)?;

                            // Build the aggregation function name
                            let agg_func = match func.as_str() {
                                "mean" => "AVG",
                                "sum" => "SUM",
                                "min" => "MIN",
                                "max" => "MAX",
                                "count" => "COUNT",
                                _ => unreachable!(),
                            };

                            // Mark this as a rolling aggregation with window size and func
                            // The window frame will be added in derive_with_window_functions
                            Ok(format!(
                                "__ROLLING_{}__({})__{}",
                                agg_func, col_sql, window_size
                            ))
                        } else {
                            Err(format!(
                                "Aggregation function {} must be called on rolling()",
                                func
                            ))
                        }
                    } else {
                        Err(format!(
                            "Aggregation function {} requires rolling() context",
                            func
                        ))
                    }
                }

                "is_null" => {
                    let on_sql = pyexpr_to_sql(on, schema)?;
                    Ok(format!("({} IS NULL)", on_sql))
                }

                "is_not_null" => {
                    let on_sql = pyexpr_to_sql(on, schema)?;
                    Ok(format!("({} IS NOT NULL)", on_sql))
                }

                "abs" => {
                    let arg_sql = if args.is_empty() {
                        return Err("abs() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    Ok(format!("ABS({})", arg_sql))
                }

                "ceil" => {
                    let arg_sql = if args.is_empty() {
                        return Err("ceil() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    Ok(format!("CEIL({})", arg_sql))
                }

                "floor" => {
                    let arg_sql = if args.is_empty() {
                        return Err("floor() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    Ok(format!("FLOOR({})", arg_sql))
                }

                "round" => {
                    let arg_sql = if args.is_empty() {
                        return Err("round() requires an argument".to_string());
                    } else {
                        pyexpr_to_sql(&args[0], schema)?
                    };
                    let decimals = if args.len() > 1 {
                        if let PyExpr::Literal { value, .. } = &args[1] {
                            value.parse::<i32>().unwrap_or(0)
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    Ok(format!("ROUND({}, {})", arg_sql, decimals))
                }

                _ => Err(format!("Unsupported function in window context: {}", func)),
            }
        }
    }
}

/// Wrapper to convert PyExprError to PyErr
impl From<PyExprError> for PyErr {
    fn from(err: PyExprError) -> Self {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(err.to_string())
    }
}

/// RustTable: Holds DataFusion SessionContext and loaded data
/// This is the core Rust kernel backing LTSeq
#[pyclass]
pub struct RustTable {
    session: Arc<SessionContext>,
    dataframe: Option<Arc<DataFrame>>,
    schema: Option<Arc<ArrowSchema>>,
    sort_exprs: Vec<String>, // Column names used for sorting, for Phase 6 window functions
}

/// Helper function: Apply OVER clause to all nested LAG/LEAD functions in an expression
/// This recursively finds all LAG(...) and LEAD(...) calls and adds the proper OVER clause
fn apply_over_to_window_functions(expr: &str, order_by: &str) -> String {
    let mut result = String::new();
    let mut i = 0;
    let bytes = expr.as_bytes();

    while i < bytes.len() {
        // Look for LAG(
        if i + 4 <= bytes.len() && &expr[i..i + 4] == "LAG(" {
            result.push_str("LAG(");
            i += 4;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        }
        // Look for LEAD(
        else if i + 5 <= bytes.len() && &expr[i..i + 5] == "LEAD(" {
            result.push_str("LEAD(");
            i += 5;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                let ch = bytes[i] as char;
                result.push(ch);
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            // Add OVER clause
            if !order_by.is_empty() {
                result.push_str(&format!(" OVER (ORDER BY {})", order_by));
            } else {
                result.push_str(" OVER ()");
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    result
}

#[pymethods]
impl RustTable {
    #[new]
    fn new() -> Self {
        let session = SessionContext::new();
        RustTable {
            session: Arc::new(session),
            dataframe: None,
            schema: None,
            sort_exprs: Vec::new(),
        }
    }

    /// Read CSV file into DataFusion DataFrame
    ///
    /// Args:
    ///     path: Path to CSV file
    fn read_csv(&mut self, path: String) -> PyResult<()> {
        RUNTIME.block_on(async {
            // Use DataFusion's built-in CSV reader
            let df = self
                .session
                .read_csv(&path, CsvReadOptions::new())
                .await
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to read CSV: {}",
                        e
                    ))
                })?;

            // Get the DFSchema and convert to Arrow schema
            let df_schema = df.schema();
            let arrow_fields: Vec<arrow::datatypes::Field> =
                df_schema.fields().iter().map(|f| (**f).clone()).collect();
            let arrow_schema = ArrowSchema::new(arrow_fields);

            self.schema = Some(Arc::new(arrow_schema));
            self.dataframe = Some(Arc::new(df));
            Ok(())
        })
    }

    /// Display the data as a pretty-printed ASCII table
    ///
    /// Args:
    ///     n: Maximum number of rows to display
    ///
    /// Returns:
    ///     Formatted table as string
    fn show(&self, n: usize) -> PyResult<String> {
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available.")
        })?;

        RUNTIME.block_on(async {
            // Clone the DataFrame to collect data
            let df_clone = (**df).clone();
            let batches = df_clone.collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect data: {}",
                    e
                ))
            })?;

            let output = format_table(&batches, schema, n)?;
            Ok(output)
        })
    }

    /// Get basic info about the table
    fn hello(&self) -> String {
        match (&self.dataframe, &self.schema) {
            (Some(_), Some(schema)) => {
                format!(
                    "Hello from RustTable! Schema has {} columns",
                    schema.fields().len()
                )
            }
            _ => "Hello from RustTable! No data loaded yet.".to_string(),
        }
    }

    /// Get the number of rows in the table
    ///
    /// Returns:
    ///     The number of rows
    fn count(&self) -> PyResult<usize> {
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        RUNTIME.block_on(async {
            let df_clone = (**df).clone();
            let batches = df_clone.collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect data: {}",
                    e
                ))
            })?;

            let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
            Ok(total_rows)
        })
    }

    /// Filter rows based on predicate expression
    ///
    /// Args:
    ///     expr_dict: Serialized expression dict (from Python)
    ///
    /// Returns:
    ///     New RustTable with filtered data
    fn filter(&self, expr_dict: &Bound<'_, PyDict>) -> PyResult<RustTable> {
        // 1. Deserialize expression
        let py_expr = dict_to_py_expr(expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // 2. Get schema (required for transpilation)
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // 3. Transpile to DataFusion expr
        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // 4. Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // 5. Apply filter (async operation)
        let filtered_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .filter(df_expr)
                    .map_err(|e| format!("Filter execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 6. Return new RustTable with filtered data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(filtered_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
    }

    /// Select columns or derived expressions
    ///
    /// Args:
    ///     exprs: List of serialized expression dicts (from Python)
    ///
    /// Returns:
    ///     New RustTable with selected columns
    fn select(&self, exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // 1. Get schema
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // 2. Deserialize and transpile all expressions
        let mut df_exprs = Vec::new();

        for expr_dict in exprs {
            // Deserialize
            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Transpile
            let df_expr = pyexpr_to_datafusion(py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

            df_exprs.push(df_expr);
        }

        // 3. Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // 4. Apply select (async operation)
        let selected_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .select(df_exprs)
                    .map_err(|e| format!("Select execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 5. Update schema to reflect selected DataFrame
        // Get new schema from the resulting DataFrame
        let df_schema = selected_df.schema();
        let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_schema = ArrowSchema::new(arrow_fields);

        // 6. Return new RustTable with updated schema
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(selected_df)),
            schema: Some(Arc::new(new_schema)),
            sort_exprs: self.sort_exprs.clone(),
        })
    }

    /// Create derived columns based on expressions
    ///
    /// Args:
    ///     derived_cols: Dict mapping column names to serialized expression dicts
    ///
    /// Returns:
    ///     New RustTable with added derived columns
    fn derive(&self, derived_cols: &Bound<'_, PyDict>) -> PyResult<RustTable> {
        // 1. Get schema and DataFrame
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // 2. Check if any derived column contains window functions
        let mut has_window_functions = false;
        for (_, expr_item) in derived_cols.iter() {
            let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
            })?;

            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            if contains_window_function(&py_expr) {
                has_window_functions = true;
                break;
            }
        }

        // 3. Handle window functions using SQL
        if has_window_functions {
            return self.derive_with_window_functions(derived_cols);
        }

        // 4. Standard (non-window) derivation using DataFusion expressions
        // Build select expressions: keep all existing columns + add derived ones
        let mut select_exprs = Vec::new();

        // Add all existing columns
        for field in schema.fields() {
            select_exprs.push(col(field.name()));
        }

        // Process each derived column
        for (col_name, expr_item) in derived_cols.iter() {
            let col_name_str = col_name.extract::<String>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
            })?;

            let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
            })?;

            // Deserialize
            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Transpile
            let df_expr = pyexpr_to_datafusion(py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

            // Add to select with alias
            select_exprs.push(df_expr.alias(&col_name_str));
        }

        // Apply select to add derived columns
        let derived_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .select(select_exprs)
                    .map_err(|e| format!("Derive execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Get new schema from derived DataFrame (type inference!)
        let df_schema = derived_df.schema();
        let arrow_fields: Vec<Field> = df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);

        // Return new RustTable with updated schema
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(derived_df)),
            schema: Some(Arc::new(new_arrow_schema)),
            sort_exprs: self.sort_exprs.clone(),
        })
    }

    /// Helper method to derive columns with window functions using SQL
    fn derive_with_window_functions(
        &self,
        derived_cols: &Bound<'_, PyDict>,
    ) -> PyResult<RustTable> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Schema not available")
        })?;

        let df = self
            .dataframe
            .as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded"))?;

        RUNTIME.block_on(async {
            // Get the data from the current DataFrame as record batches
            let current_batches = (**df).clone().collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect current DataFrame: {}",
                    e
                ))
            })?;

            // Register a temporary table with the current data
            let temp_table_name = "__ltseq_temp";
            let arrow_schema = Arc::new((**schema).clone());
            let temp_table =
                MemTable::try_new(arrow_schema, vec![current_batches]).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create memory table: {}",
                        e
                    ))
                })?;

            self.session
                .register_table(temp_table_name, Arc::new(temp_table))
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to register temporary table: {}",
                        e
                    ))
                })?;

            // Build the SELECT clause with all columns + derived ones
            let mut select_parts = Vec::new();

            // Add all existing columns
            for field in schema.fields() {
                select_parts.push(format!("\"{}\"", field.name()));
            }

            // Add derived columns
            for (col_name, expr_item) in derived_cols.iter() {
                let col_name_str = col_name.extract::<String>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Column name must be string")
                })?;

                let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
                })?;

                let py_expr = dict_to_py_expr(&expr_dict)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

                // Convert expression to SQL
                let expr_sql = pyexpr_to_sql(&py_expr, schema)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

                // Check if this is a window function and handle rolling aggregations and diff
                let is_rolling_agg = expr_sql.contains("__ROLLING_");
                let is_diff = expr_sql.contains("__DIFF_");

                let full_expr =
                    if is_rolling_agg {
                        // Handle rolling aggregations: replace marker with actual window frame
                        // Pattern: __ROLLING_FUNC__(col)__size
                        // Replace with: FUNC(col) OVER (ORDER BY ... ROWS BETWEEN size-1 PRECEDING AND CURRENT ROW)
                        let window_size_start = expr_sql.rfind("__").unwrap_or(0) + 2;
                        let window_size_str = expr_sql[window_size_start..]
                            .trim_end_matches(|c: char| !c.is_ascii_digit());
                        let window_size = window_size_str.parse::<i32>().unwrap_or(1);

                        // Extract the FUNC(col) part
                        let start = expr_sql.find("__ROLLING_").unwrap_or(0) + 10; // len("__ROLLING_")
                        let end = expr_sql.find("__(").unwrap_or(start);
                        let func_part = &expr_sql[start..end];

                        let col_start = expr_sql.find("__(").unwrap_or(0) + 3; // len("__(")
                        let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
                        let col_part = &expr_sql[col_start..col_end];

                        let order_by = if !self.sort_exprs.is_empty() {
                            self.sort_exprs
                                .iter()
                                .map(|col| format!("\"{}\"", col))
                                .collect::<Vec<_>>()
                                .join(", ")
                        } else {
                            "".to_string()
                        };

                        if !order_by.is_empty() {
                            format!(
                              "{}({}) OVER (ORDER BY {} ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                              func_part, col_part, order_by, window_size - 1
                          )
                        } else {
                            format!(
                                "{}({}) OVER (ROWS BETWEEN {} PRECEDING AND CURRENT ROW)",
                                func_part,
                                col_part,
                                window_size - 1
                            )
                        }
                    } else if is_diff {
                        // Handle diff: replace marker with actual calculation
                        // Pattern: __DIFF_periods__(col)__periods
                        // Replace with: (col - LAG(col, periods)) OVER (ORDER BY ...)
                        let diff_start = expr_sql.find("__DIFF_").unwrap_or(0);
                        let periods_start = diff_start + 7; // len("__DIFF_")
                        let periods_end =
                            expr_sql[periods_start..].find("__").unwrap_or(0) + periods_start;
                        let periods_str = &expr_sql[periods_start..periods_end];

                        let col_start = expr_sql.find("__(").unwrap_or(0) + 3; // len("__(")
                        let col_end = expr_sql.rfind(")__").unwrap_or(expr_sql.len());
                        let col_part = &expr_sql[col_start..col_end];

                        let order_by = if !self.sort_exprs.is_empty() {
                            self.sort_exprs
                                .iter()
                                .map(|col| format!("\"{}\"", col))
                                .collect::<Vec<_>>()
                                .join(", ")
                        } else {
                            "".to_string()
                        };

                        // We need to apply OVER clause to LAG
                        let diff_expr = if !order_by.is_empty() {
                            format!(
                                "({} - LAG({}, {}) OVER (ORDER BY {}))",
                                col_part, col_part, periods_str, order_by
                            )
                        } else {
                            format!(
                                "({} - LAG({}, {}) OVER ())",
                                col_part, col_part, periods_str
                            )
                        };

                        diff_expr
                    } else if expr_sql.contains("LAG(") || expr_sql.contains("LEAD(") {
                        // Handle shift() window functions - LAG/LEAD may be nested in expressions
                        // Use helper function to apply OVER to ALL instances
                        let order_by = if !self.sort_exprs.is_empty() {
                            self.sort_exprs
                                .iter()
                                .map(|col| format!("\"{}\"", col))
                                .collect::<Vec<_>>()
                                .join(", ")
                        } else {
                            "".to_string()
                        };
                        apply_over_to_window_functions(&expr_sql, &order_by)
                    } else {
                        expr_sql
                    };

                select_parts.push(format!("{} AS \"{}\"", full_expr, col_name_str));
            }

            // Build and execute the SQL query
            let sql = format!(
                "SELECT {} FROM \"{}\"",
                select_parts.join(", "),
                temp_table_name
            );

            // Execute SQL using sql() which returns a DataFrame directly
            let new_df = self.session.sql(&sql).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Window function query failed: {}",
                    e
                ))
            })?;

            // Get schema from the new DataFrame
            let df_schema = new_df.schema();
            let arrow_fields: Vec<Field> =
                df_schema.fields().iter().map(|f| (**f).clone()).collect();
            let new_arrow_schema = ArrowSchema::new(arrow_fields);

            // Return new RustTable
            Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: Some(Arc::new(new_df)),
                schema: Some(Arc::new(new_arrow_schema)),
                sort_exprs: self.sort_exprs.clone(),
            })
        })
    }

    /// Sort rows by one or more key expressions
    ///
    /// Args:
    ///     sort_exprs: List of serialized expression dicts (from Python)
    ///
    /// Returns:
    ///     New RustTable with sorted data
    fn sort(&self, sort_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: Vec::new(),
            });
        }

        // Get schema (required for transpilation)
        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // Capture sort column names for Phase 6 window functions
        let mut captured_sort_keys = Vec::new();

        // Deserialize and transpile each sort expression
        let mut df_sort_exprs = Vec::new();
        for expr_dict in sort_exprs {
            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

            // Capture column name if this is a simple column reference
            if let PyExpr::Column(ref col_name) = py_expr {
                captured_sort_keys.push(col_name.clone());
            }

            let df_expr = pyexpr_to_datafusion(py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
            // Create SortExpr with ascending order (default)
            df_sort_exprs.push(SortExpr {
                expr: df_expr,
                asc: true,
                nulls_first: false,
            });
        }

        // Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // Apply sort (async operation)
        let sorted_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .sort(df_sort_exprs)
                    .map_err(|e| format!("Sort execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Return new RustTable with sorted data and captured sort keys
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(sorted_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: captured_sort_keys,
        })
    }

    /// Remove duplicate rows based on key columns
    ///
    /// Args:
    ///     key_exprs: List of serialized expression dicts (from Python)
    ///                If empty, considers all columns for uniqueness
    ///
    /// Returns:
    ///     New RustTable with unique rows
    fn distinct(&self, _key_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // Get schema (for now, we don't use it for distinct, but keep it for future)
        let _schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        // Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // For Phase 5, we implement simple distinct on all columns
        // TODO: In Phase 6+, support distinct with specific key columns
        // Note: key_exprs parameter is reserved for future use
        let distinct_df = RUNTIME
            .block_on(async {
                (**df)
                    .clone()
                    .distinct()
                    .map_err(|e| format!("Distinct execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Return new RustTable with distinct data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(distinct_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
    }

    /// Select a contiguous range of rows
    ///
    /// Args:
    ///     offset: Starting row index (0-based)
    ///     length: Number of rows to include (None = all rows from offset to end)
    ///
    /// Returns:
    ///     New RustTable with selected row range
    fn slice(&self, offset: i64, length: Option<i64>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        // Get DataFrame
        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        // Apply slice: use limit() with offset and fetch parameters
        let sliced_df = RUNTIME
            .block_on(async {
                // DataFusion's limit(skip: usize, fetch: Option<usize>)
                let skip = offset as usize;
                let fetch = length.map(|len| len as usize);

                (**df)
                    .clone()
                    .limit(skip, fetch)
                    .map_err(|e| format!("Slice execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // Return new RustTable with sliced data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(sliced_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
            sort_exprs: self.sort_exprs.clone(),
        })
    }

    /// Add cumulative sum columns for specified columns
    ///
    /// Args:
    ///     cum_exprs: List of serialized expression dicts (from Python)
    ///                Each expression identifies the column(s) to cumulate
    ///
    /// Returns:
    ///     New RustTable with cumulative sum columns added
    fn cum_sum(&self, cum_exprs: Vec<Bound<'_, PyDict>>) -> PyResult<RustTable> {
        // If no dataframe, return empty result (for unit tests)
        if self.dataframe.is_none() {
            return Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: None,
                schema: self.schema.as_ref().map(|s| Arc::clone(s)),
                sort_exprs: self.sort_exprs.clone(),
            });
        }

        let schema = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first.",
            )
        })?;

        let df = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first.",
            )
        })?;

        RUNTIME.block_on(async {
            // Get the data from the current DataFrame as record batches
            let current_batches = (**df).clone().collect().await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to collect current DataFrame: {}",
                    e
                ))
            })?;

            // Check if the table is empty - if so, just return with new schema
            let is_empty = current_batches.iter().all(|batch| batch.num_rows() == 0);
            if is_empty {
                // Build schema with cumulative sum columns added
                let mut new_fields = schema.fields().to_vec();
                for (idx, expr_item) in cum_exprs.iter().enumerate() {
                    let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
                    })?;

                    let py_expr = dict_to_py_expr(&expr_dict).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
                    })?;

                    let col_name = if let PyExpr::Column(name) = &py_expr {
                        name.clone()
                    } else {
                        format!("cum_sum_{}", idx)
                    };

                    // Add cumsum column with float64 type
                    let cumsum_col_name = format!("{}_cumsum", col_name);
                    new_fields.push(Arc::new(Field::new(
                        cumsum_col_name,
                        datafusion::arrow::datatypes::DataType::Float64,
                        true,
                    )));
                }

                let new_arrow_schema = ArrowSchema::new(new_fields);

                // Create empty MemTable with new schema
                let empty_table = MemTable::try_new(
                    Arc::new(new_arrow_schema.clone()),
                    vec![current_batches], // empty batches with new schema
                )
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create empty memory table: {}",
                        e
                    ))
                })?;

                // Register and query to get proper empty DataFrame
                let temp_table_name = "__ltseq_temp_empty";
                self.session
                    .register_table(temp_table_name, Arc::new(empty_table))
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to register empty table: {}",
                            e
                        ))
                    })?;

                // Build SELECT for all columns
                let mut select_parts = Vec::new();
                for field in schema.fields() {
                    select_parts.push(format!("\"{}\"", field.name()));
                }
                for (idx, expr_item) in cum_exprs.iter().enumerate() {
                    let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
                    })?;

                    let py_expr = dict_to_py_expr(&expr_dict).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
                    })?;

                    let col_name = if let PyExpr::Column(name) = &py_expr {
                        name.clone()
                    } else {
                        format!("cum_sum_{}", idx)
                    };

                    let cumsum_col_name = format!("{}_cumsum", col_name);
                    select_parts.push(format!("CAST(0.0 AS FLOAT) AS \"{}\"", cumsum_col_name));
                }

                let sql = format!(
                    "SELECT {} FROM \"{}\"",
                    select_parts.join(", "),
                    temp_table_name
                );

                let new_df = self.session.sql(&sql).await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "cum_sum query failed for empty table: {}",
                        e
                    ))
                })?;

                let df_schema = new_df.schema();
                let arrow_fields: Vec<Field> =
                    df_schema.fields().iter().map(|f| (**f).clone()).collect();
                let new_arrow_schema = ArrowSchema::new(arrow_fields);

                return Ok(RustTable {
                    session: Arc::clone(&self.session),
                    dataframe: Some(Arc::new(new_df)),
                    schema: Some(Arc::new(new_arrow_schema)),
                    sort_exprs: self.sort_exprs.clone(),
                });
            }

            // Register a temporary table with the current data
            let temp_table_name = "__ltseq_temp";
            let arrow_schema = Arc::new((**schema).clone());
            let temp_table =
                MemTable::try_new(arrow_schema, vec![current_batches]).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create memory table: {}",
                        e
                    ))
                })?;

            self.session
                .register_table(temp_table_name, Arc::new(temp_table))
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to register temporary table: {}",
                        e
                    ))
                })?;

            // Build the SELECT clause with all columns + cumulative sum columns
            let mut select_parts = Vec::new();

            // Add all existing columns
            for field in schema.fields() {
                select_parts.push(format!("\"{}\"", field.name()));
            }

            // Add cumulative sum columns for each input column
            for (idx, expr_item) in cum_exprs.iter().enumerate() {
                let expr_dict = expr_item.cast::<PyDict>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Expression must be dict")
                })?;

                let py_expr = dict_to_py_expr(&expr_dict)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

                // Convert expression to SQL
                let expr_sql = pyexpr_to_sql(&py_expr, schema)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

                // Extract column name from expression (if it's a simple Column)
                let col_name = if let PyExpr::Column(name) = &py_expr {
                    name.clone()
                } else {
                    format!("cum_sum_{}", idx)
                };

                // Validate that the column type is numeric (only for direct Column expressions)
                if let PyExpr::Column(col_name_ref) = &py_expr {
                    if let Some(field) = schema.field_with_name(col_name_ref).ok() {
                        let field_type = field.data_type();
                        if !is_numeric_type(field_type) {
                            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                                "cum_sum() requires numeric columns. Column '{}' has type {:?}",
                                col_name_ref, field_type
                            )));
                        }
                    }
                }

                // Build cumulative sum: SUM(col) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                let order_by = if !self.sort_exprs.is_empty() {
                    let order_cols = self
                        .sort_exprs
                        .iter()
                        .map(|col| format!("\"{}\"", col))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(" ORDER BY {}", order_cols)
                } else {
                    "".to_string()
                };

                let cumsum_expr = if !order_by.is_empty() {
                    format!(
                        "SUM({}) OVER ({}ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                        expr_sql, order_by
                    )
                } else {
                    format!(
                        "SUM({}) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                        expr_sql
                    )
                };

                let cumsum_col_name = format!("{}_cumsum", col_name);
                select_parts.push(format!("{} AS \"{}\"", cumsum_expr, cumsum_col_name));
            }

            // Build and execute the SQL query
            let sql = format!(
                "SELECT {} FROM \"{}\"",
                select_parts.join(", "),
                temp_table_name
            );

            // Execute SQL using sql() which returns a DataFrame directly
            let new_df = self.session.sql(&sql).await.map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "cum_sum query failed: {}",
                    e
                ))
            })?;

            // Get schema from the new DataFrame
            let df_schema = new_df.schema();
            let arrow_fields: Vec<Field> =
                df_schema.fields().iter().map(|f| (**f).clone()).collect();
            let new_arrow_schema = ArrowSchema::new(arrow_fields);

            // Return new RustTable
            Ok(RustTable {
                session: Arc::clone(&self.session),
                dataframe: Some(Arc::new(new_df)),
                schema: Some(Arc::new(new_arrow_schema)),
                sort_exprs: self.sort_exprs.clone(),
            })
        })
    }

    /// Phase 8B: Join two tables using pointer-based foreign keys
    ///
    /// This method implements an inner join between two tables based on specified join keys.
    /// It supports lazy evaluation - the join is only executed when the result is accessed.
    ///
    /// # DataFusion Schema Conflict Workaround
    ///
    /// DataFusion's join() API rejects tables with duplicate column names in the join input,
    /// even if the columns are from different tables. This implementation works around this
    /// limitation by:
    ///
    /// 1. Temporarily renaming the right table's join key to `__join_key_right__`
    /// 2. Executing the join on `left.join_key == right.__join_key_right__`
    /// 3. After joining, selecting all columns and renaming back to the desired schema with alias
    /// 4. The final schema has: `{all_left_cols, all_right_cols_prefixed_with_alias}`
    ///
    /// # Example
    ///
    /// For join(orders, products, left_key="product_id", right_key="product_id", alias="prod"):
    ///
    /// - orders schema: {id, product_id, quantity}
    /// - products schema: {product_id, name, price}
    /// - result schema: {id, product_id, quantity, prod_product_id, prod_name, prod_price}
    ///
    /// # Arguments
    ///
    /// * `other` - The table to join with (right table)
    /// * `left_key_expr_dict` - Serialized Column expression dict for left join key
    /// * `right_key_expr_dict` - Serialized Column expression dict for right join key
    /// * `join_type` - Type of join (currently only "inner" is supported in Phase 8B MVP)
    /// * `alias` - Prefix for right table columns (used to avoid name conflicts)
    ///
    /// # Returns
    ///
    /// A new RustTable containing the joined result with combined schema
    ///
    /// # Panics
    ///
    /// Returns PyValueError if:
    /// - Join key expressions are not simple Column references
    /// - Join operation fails due to schema/data issues
    fn join(
        &self,
        other: &RustTable,
        left_key_expr_dict: &Bound<'_, PyDict>,
        right_key_expr_dict: &Bound<'_, PyDict>,
        join_type: &str,
        alias: &str,
    ) -> PyResult<RustTable> {
        // 1. Validate both tables have data and schemas
        let df_left = self.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Left table has no data. Call read_csv() first.",
            )
        })?;

        let df_right = other.dataframe.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Right table has no data. Call read_csv() first.",
            )
        })?;

        let schema_left = self.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Left table schema not available.",
            )
        })?;

        let schema_right = other.schema.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Right table schema not available.",
            )
        })?;

        // 2. Deserialize join key expressions
        let left_key_expr = dict_to_py_expr(left_key_expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let right_key_expr = dict_to_py_expr(right_key_expr_dict)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        // 3. Extract column names from expressions (only Column variant supported for Phase 8B MVP)
        let left_col_name = match left_key_expr {
            PyExpr::Column(name) => name,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Phase 8B: Only simple column references are supported in join keys",
                ))
            }
        };

        let right_col_name = match right_key_expr {
            PyExpr::Column(name) => name,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Phase 8B: Only simple column references are supported in join keys",
                ))
            }
        };

        // 4. Validate columns exist in schemas
        if !schema_left.fields().iter().any(|f| f.name() == &left_col_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Column '{}' not found in left table", left_col_name),
            ));
        }

        if !schema_right.fields().iter().any(|f| f.name() == &right_col_name) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Column '{}' not found in right table", right_col_name),
            ));
        }

        // 5. Map join type string to DataFusion JoinType
        let df_join_type = match join_type {
            "inner" => datafusion::logical_expr::JoinType::Inner,
            "left" => datafusion::logical_expr::JoinType::Left,
            "right" => datafusion::logical_expr::JoinType::Right,
            "full" => datafusion::logical_expr::JoinType::Full,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Unknown join type: {}", join_type),
                ))
            }
        };

        // 6. Rename the join key from the right table to avoid schema conflicts
        // Create a select that renames the join key to add a temporary suffix
        let mut right_select_exprs = Vec::new();
        for field in schema_right.fields().iter() {
            let col_name = field.name();
            if col_name == &right_col_name {
                // Rename the join key temporarily
                let temp_join_key_name = format!("__join_key_right__");
                right_select_exprs.push(col(col_name).alias(temp_join_key_name));
            } else {
                // Keep other columns as-is
                right_select_exprs.push(col(col_name));
            }
        }

        // Apply the select to rename the join key
        let df_right_renamed = RUNTIME
            .block_on(async {
                (**df_right)
                    .clone()
                    .select(right_select_exprs)
                    .map_err(|e| format!("Right table join key rename failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 7. Execute the join with the renamed join key
        let joined_df = RUNTIME
            .block_on(async {
                (**df_left)
                    .clone()
                    .join(
                        df_right_renamed.clone(),
                        df_join_type,
                        &[left_col_name.as_str()],
                        &["__join_key_right__"],
                        None,
                    )
                    .map_err(|e| format!("Join execution failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 8. Select columns to create the final schema, renaming right table columns
        // to avoid conflicts and to match the expected schema with alias prefix
        let mut final_select_exprs = Vec::new();
        
        // Add all left table columns (they appear in the join result without qualification)
        for field in schema_left.fields().iter() {
            final_select_exprs.push(col(field.name()));
        }
        
        // Add right table columns with alias prefix (skip the temporary join key)
        // The right table columns appear with the "?table?" qualifier in the join result
        for field in schema_right.fields().iter() {
            let col_name = field.name();
            if col_name == &right_col_name {
                // The join key was renamed to "__join_key_right__", rename it back
                final_select_exprs.push(col("__join_key_right__").alias(&format!("{}_{}", alias, right_col_name)));
                continue;
            }
            // Rename other columns with alias prefix
            let new_col_name = format!("{}_{}", alias, col_name);
            final_select_exprs.push(
                col(col_name).alias(new_col_name)
            );
        }

        // Apply the final select
        let final_df = RUNTIME
            .block_on(async {
                joined_df
                    .clone()
                    .select(final_select_exprs)
                    .map_err(|e| format!("Final select failed: {}", e))
            })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

        // 9. Get schema from the final DataFrame
        let df_schema = final_df.schema();
        let arrow_fields: Vec<Field> =
            df_schema.fields().iter().map(|f| (**f).clone()).collect();
        let joined_arrow_schema = ArrowSchema::new(arrow_fields);

        // 9. Return new RustTable with joined data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(joined_df)),
            schema: Some(Arc::new(joined_arrow_schema)),
            sort_exprs: Vec::new(),
        })
    }
}

/// Format RecordBatches as a pretty-printed ASCII table
///
/// Shows up to `limit` rows, streaming through batches
fn format_table(batches: &[RecordBatch], schema: &ArrowSchema, limit: usize) -> PyResult<String> {
    let mut output = String::new();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Calculate column widths for pretty printing
    let mut col_widths: Vec<usize> = col_names.iter().map(|name| name.len()).collect();

    // Scan through batches to find max column widths
    let mut row_count = 0;
    for batch in batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            for (col_idx, _) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value_str = format_cell(col, row_idx);
                col_widths[col_idx] = col_widths[col_idx].max(value_str.len());
            }
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    // Limit width to 50 chars per column for readability
    for width in &mut col_widths {
        *width = (*width).min(50);
    }

    // Draw top border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Draw header
    output.push('|');
    for (i, col_name) in col_names.iter().enumerate() {
        output.push(' ');
        output.push_str(&format!("{:<width$}", col_name, width = col_widths[i]));
        output.push_str(" |");
    }
    output.push('\n');

    // Draw header border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Draw rows
    row_count = 0;
    for batch in batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            if row_count >= limit {
                break;
            }
            output.push('|');
            for (col_idx, _) in schema.fields().iter().enumerate() {
                output.push(' ');
                let col = batch.column(col_idx);
                let value_str = format_cell(col, row_idx);
                // Truncate long values
                let truncated = if value_str.len() > 50 {
                    format!("{}...", &value_str[..47])
                } else {
                    value_str
                };
                output.push_str(&format!(
                    "{:<width$}",
                    truncated,
                    width = col_widths[col_idx]
                ));
                output.push_str(" |");
            }
            output.push('\n');
            row_count += 1;
        }
        if row_count >= limit {
            break;
        }
    }

    // Draw bottom border
    output.push('+');
    for width in &col_widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    Ok(output)
}

/// Format a single cell value from an Arrow column
fn format_cell(column: &dyn arrow::array::Array, row_idx: usize) -> String {
    use arrow::array::*;

    // Handle null values
    if !column.is_valid(row_idx) {
        return "None".to_string();
    }

    // Match on column type and format accordingly
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int8Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int16Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt8Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt16Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
        arr.value(row_idx).to_string()
    } else if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
        format!("{}", arr.value(row_idx))
    } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
        format!("{}", arr.value(row_idx))
    } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
        arr.value(row_idx).to_string()
    } else {
        "[unsupported type]".to_string()
    }
}

#[pyfunction]
fn hello() -> PyResult<String> {
    Ok("hello from ltseq_core".to_string())
}

#[pymodule]
fn ltseq_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_class::<RustTable>()?;
    Ok(())
}
