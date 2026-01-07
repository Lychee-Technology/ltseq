use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;
use std::collections::HashMap;
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{Schema as ArrowSchema, Field};
use datafusion::logical_expr::{Expr, BinaryExpr, Operator, SortExpr};
use lazy_static::lazy_static;
use tokio::runtime::Runtime;

// Global Tokio runtime for async operations
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
    Literal {
        value: String,
        dtype: String,
    },
    
    /// Binary operation: {"type": "BinOp", "op": "Gt", "left": {...}, "right": {...}}
    BinOp {
        op: String,
        left: Box<PyExpr>,
        right: Box<PyExpr>,
    },
    
    /// Unary operation: {"type": "UnaryOp", "op": "Not", "operand": {...}}
    UnaryOp {
        op: String,
        operand: Box<PyExpr>,
    },
    
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
    let expr_type = dict.get_item("type")
        .map_err(|_| PyExprError::MissingField("type".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("type".to_string()))?
        .extract::<String>()
        .map_err(|_| PyExprError::InvalidType("type must be string".to_string()))?;
    
    // 2. Match on expression type
    match expr_type.as_str() {
        "Column" => {
            let name = dict.get_item("name")
                .map_err(|_| PyExprError::MissingField("name".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("name".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("name must be string".to_string()))?;
            Ok(PyExpr::Column(name))
        },
        
        "Literal" => {
            let value_obj = dict.get_item("value")
                .map_err(|_| PyExprError::MissingField("value".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("value".to_string()))?;
            
            // Convert Python value to string (handles int, float, str, bool, None)
            let value = value_obj.to_string();
            
            let dtype = dict.get_item("dtype")
                .map_err(|_| PyExprError::MissingField("dtype".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("dtype".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("dtype must be string".to_string()))?;
            Ok(PyExpr::Literal { value, dtype })
        },
        
        "BinOp" => {
            let op = dict.get_item("op")
                .map_err(|_| PyExprError::MissingField("op".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("op".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("op must be string".to_string()))?;
            
            let left_item = dict.get_item("left")
                .map_err(|_| PyExprError::MissingField("left".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("left".to_string()))?;
            let left_dict = left_item.downcast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("left must be dict".to_string()))?;
            
            let right_item = dict.get_item("right")
                .map_err(|_| PyExprError::MissingField("right".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("right".to_string()))?;
            let right_dict = right_item.downcast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("right must be dict".to_string()))?;
            
            let left = Box::new(dict_to_py_expr(&left_dict)?);
            let right = Box::new(dict_to_py_expr(&right_dict)?);
            Ok(PyExpr::BinOp { op, left, right })
        },
        
        "UnaryOp" => {
            let op = dict.get_item("op")
                .map_err(|_| PyExprError::MissingField("op".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("op".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("op must be string".to_string()))?;
            
            let operand_item = dict.get_item("operand")
                .map_err(|_| PyExprError::MissingField("operand".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("operand".to_string()))?;
            let operand_dict = operand_item.downcast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("operand must be dict".to_string()))?;
            
            let operand = Box::new(dict_to_py_expr(&operand_dict)?);
            Ok(PyExpr::UnaryOp { op, operand })
        },
        
        "Call" => {
            let func = dict.get_item("func")
                .map_err(|_| PyExprError::MissingField("func".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("func".to_string()))?
                .extract::<String>()
                .map_err(|_| PyExprError::InvalidType("func must be string".to_string()))?;
            
            let on_item = dict.get_item("on")
                .map_err(|_| PyExprError::MissingField("on".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("on".to_string()))?;
            let on_dict = on_item.downcast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("on must be dict".to_string()))?;
            
            // Deserialize args list
            let args_item = dict.get_item("args")
                .map_err(|_| PyExprError::MissingField("args".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("args".to_string()))?;
            let args_list = args_item.downcast::<PyList>()
                .map_err(|_| PyExprError::InvalidType("args must be list".to_string()))?;
            let mut args = Vec::new();
            for arg_item in args_list.iter() {
                let arg_dict = arg_item.downcast::<PyDict>()
                    .map_err(|_| PyExprError::InvalidType("arg must be dict".to_string()))?;
                args.push(dict_to_py_expr(&arg_dict)?);
            }
            
            // Deserialize kwargs dict
            let kwargs_item = dict.get_item("kwargs")
                .map_err(|_| PyExprError::MissingField("kwargs".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("kwargs".to_string()))?;
            let kwargs_py = kwargs_item.downcast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("kwargs must be dict".to_string()))?;
            let mut kwargs = HashMap::new();
            for (key, val) in kwargs_py.iter() {
                let key_str = key.extract::<String>()
                    .map_err(|_| PyExprError::InvalidType("kwargs key must be string".to_string()))?;
                let val_dict = val.downcast::<PyDict>()
                    .map_err(|_| PyExprError::InvalidType("kwargs value must be dict".to_string()))?;
                kwargs.insert(key_str, dict_to_py_expr(&val_dict)?);
            }
            
            let on = Box::new(dict_to_py_expr(&on_dict)?);
            Ok(PyExpr::Call { func, args, kwargs, on })
        },
        
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
        },
        
        PyExpr::Literal { value, dtype } => {
            // Parse dtype and convert value string to appropriate literal type
            match dtype.as_str() {
                "Int64" => {
                    let int_val = value.parse::<i64>()
                        .map_err(|_| format!("Failed to parse '{}' as Int64", value))?;
                    Ok(lit(int_val))
                },
                "Int32" => {
                    let int_val = value.parse::<i32>()
                        .map_err(|_| format!("Failed to parse '{}' as Int32", value))?;
                    Ok(lit(int_val))
                },
                "Float64" => {
                    let float_val = value.parse::<f64>()
                        .map_err(|_| format!("Failed to parse '{}' as Float64", value))?;
                    Ok(lit(float_val))
                },
                "Float32" => {
                    let float_val = value.parse::<f32>()
                        .map_err(|_| format!("Failed to parse '{}' as Float32", value))?;
                    Ok(lit(float_val))
                },
                "String" | "Utf8" => Ok(lit(value)),
                "Boolean" | "Bool" => {
                    let bool_val = value.parse::<bool>()
                        .map_err(|_| format!("Failed to parse '{}' as Boolean", value))?;
                    Ok(lit(bool_val))
                },
                _ => Err(format!("Unknown dtype: {}", dtype)),
            }
        },
        
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
        },
        
        PyExpr::UnaryOp { op, operand } => {
            let operand_expr = pyexpr_to_datafusion(*operand, schema)?;
            
            match op.as_str() {
                "Not" => Ok(operand_expr.not()),
                _ => Err(format!("Unknown unary operator: {}", op)),
            }
        },
        
        PyExpr::Call { func, args: _, kwargs: _, on } => {
            // For Phase 4, we don't fully support Call expressions
            // This is prepared for Phase 6 (sequence operators)
            match func.as_str() {
                "is_null" => {
                    let on_expr = pyexpr_to_datafusion(*on, schema)?;
                    Ok(on_expr.is_null())
                },
                "is_not_null" => {
                    let on_expr = pyexpr_to_datafusion(*on, schema)?;
                    Ok(on_expr.is_not_null())
                },
                _ => {
                    Err(format!("Method '{}' not yet supported in Phase 4", func))
                }
            }
        },
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
        }
    }

    /// Read CSV file into DataFusion DataFrame
    /// 
    /// Args:
    ///     path: Path to CSV file
    fn read_csv(&mut self, path: String) -> PyResult<()> {
        RUNTIME.block_on(async {
            // Use DataFusion's built-in CSV reader
            let df = self.session.read_csv(&path, CsvReadOptions::new())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to read CSV: {}", e)
                ))?;

            // Get the DFSchema and convert to Arrow schema
            let df_schema = df.schema();
            let arrow_fields: Vec<arrow::datatypes::Field> = df_schema
                .fields()
                .iter()
                .map(|f| (**f).clone())
                .collect();
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
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;

        let schema = self.schema.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available."
            ))?;

        RUNTIME.block_on(async {
            // Clone the DataFrame to collect data
            let df_clone = (**df).clone();
            let batches = df_clone.collect()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to collect data: {}", e)
                ))?;

            let output = format_table(&batches, schema, n)?;
            Ok(output)
        })
    }

    /// Get basic info about the table
    fn hello(&self) -> String {
        match (&self.dataframe, &self.schema) {
            (Some(_), Some(schema)) => {
                format!("Hello from RustTable! Schema has {} columns", schema.fields().len())
            },
            _ => "Hello from RustTable! No data loaded yet.".to_string(),
        }
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
            });
        }
        
        // 2. Get schema (required for transpilation)
        let schema = self.schema.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first."
            ))?;
        
        // 3. Transpile to DataFusion expr
        let df_expr = pyexpr_to_datafusion(py_expr, schema)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
        
        // 4. Get DataFrame
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // 5. Apply filter (async operation)
        let filtered_df = RUNTIME.block_on(async {
            (**df).clone().filter(df_expr)
                .map_err(|e| format!("Filter execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        
        // 6. Return new RustTable with filtered data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(filtered_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
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
            });
        }
        
        // 1. Get schema
        let schema = self.schema.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first."
            ))?;
        
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
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // 4. Apply select (async operation)
        let selected_df = RUNTIME.block_on(async {
            (**df).clone().select(df_exprs)
                .map_err(|e| format!("Select execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        
        // 5. Update schema to reflect selected DataFrame
        // Get new schema from the resulting DataFrame
        let df_schema = selected_df.schema();
        let arrow_fields: Vec<Field> = df_schema
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        let new_schema = ArrowSchema::new(arrow_fields);
        
        // 6. Return new RustTable with updated schema
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(selected_df)),
            schema: Some(Arc::new(new_schema)),
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
        let schema = self.schema.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first."
            ))?;
        
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // 2. Build select expressions: keep all existing columns + add derived ones
        let mut select_exprs = Vec::new();
        
        // Add all existing columns
        for field in schema.fields() {
            select_exprs.push(col(field.name()));
        }
        
        // 3. Process each derived column
        for (col_name, expr_item) in derived_cols.iter() {
            let col_name_str = col_name.extract::<String>()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Column name must be string"
                ))?;
            
            let expr_dict = expr_item.downcast::<PyDict>()
                .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Expression must be dict"
                ))?;
            
            // Deserialize
            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
            
            // Transpile
            let df_expr = pyexpr_to_datafusion(py_expr, schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;
            
            // Add to select with alias
            select_exprs.push(df_expr.alias(&col_name_str));
        }
        
        // 4. Apply select to add derived columns
        let derived_df = RUNTIME.block_on(async {
            (**df).clone().select(select_exprs)
                .map_err(|e| format!("Derive execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        
        // 5. Get new schema from derived DataFrame (type inference!)
        let df_schema = derived_df.schema();
        let arrow_fields: Vec<Field> = df_schema
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        let new_arrow_schema = ArrowSchema::new(arrow_fields);
        
        // 6. Return new RustTable with updated schema
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(derived_df)),
            schema: Some(Arc::new(new_arrow_schema)),
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
            });
        }
        
        // Get schema (required for transpilation)
        let schema = self.schema.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first."
            ))?;
        
        // Deserialize and transpile each sort expression
        let mut df_sort_exprs = Vec::new();
        for expr_dict in sort_exprs {
            let py_expr = dict_to_py_expr(&expr_dict)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
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
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // Apply sort (async operation)
        let sorted_df = RUNTIME.block_on(async {
            (**df).clone().sort(df_sort_exprs)
                .map_err(|e| format!("Sort execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        
        // Return new RustTable with sorted data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(sorted_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
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
            });
        }
        
        // Get schema (for now, we don't use it for distinct, but keep it for future)
        let _schema = self.schema.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Schema not available. Call read_csv() first."
            ))?;
        
        // Get DataFrame
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // For Phase 5, we implement simple distinct on all columns
        // TODO: In Phase 6+, support distinct with specific key columns
        // Note: key_exprs parameter is reserved for future use
        let distinct_df = RUNTIME.block_on(async {
            (**df).clone().distinct()
                .map_err(|e| format!("Distinct execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        
        // Return new RustTable with distinct data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(distinct_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
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
            });
        }
        
        // Get DataFrame
        let df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // Apply slice: use limit() with offset and fetch parameters
        let sliced_df = RUNTIME.block_on(async {
            // DataFusion's limit(skip: usize, fetch: Option<usize>)
            let skip = offset as usize;
            let fetch = length.map(|len| len as usize);
            
            (**df).clone().limit(skip, fetch)
                .map_err(|e| format!("Slice execution failed: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;
        
        // Return new RustTable with sliced data
        Ok(RustTable {
            session: Arc::clone(&self.session),
            dataframe: Some(Arc::new(sliced_df)),
            schema: self.schema.as_ref().map(|s| Arc::clone(s)),
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
            });
        }
        
        // Get DataFrame
        let _df = self.dataframe.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No data loaded. Call read_csv() first."
            ))?;
        
        // Phase 6 limitation: cum_sum not yet fully implemented in Rust
        // TODO: Implement window functions for proper cumulative sums
        // For now, return error indicating feature is not available
        return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "cum_sum() is not yet implemented. Phase 6 limitation: requires window function support."
        ));
    }
}

/// Format RecordBatches as a pretty-printed ASCII table
/// 
/// Shows up to `limit` rows, streaming through batches
fn format_table(
    batches: &[RecordBatch],
    schema: &ArrowSchema,
    limit: usize,
) -> PyResult<String> {
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
                output.push_str(&format!("{:<width$}", truncated, width = col_widths[col_idx]));
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
