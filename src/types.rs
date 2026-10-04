//! PyExpr type definition and deserialization

use crate::error::PyExprError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Represents a serialized Python expression for transpilation to DataFusion
#[derive(Debug, Clone, PartialEq)]
pub enum PyExpr {
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
        /// Receiver of a method-style call (`r.x.abs()`); `None` for a
        /// standalone function (`abs(r.x)`), whose inputs are all in `args`.
        on: Option<Box<PyExpr>>,
    },

    /// Window expression: {"type": "Window", "expr": {...}, "partition_by": {...}, "order_by": {...}, "descending": bool}
    Window {
        expr: Box<PyExpr>,
        partition_by: Option<Box<PyExpr>>,
        order_by: Option<Box<PyExpr>>,
        descending: bool,
    },

    /// Alias expression: {"type": "Alias", "expr": {...}, "alias": "new_name"}
    Alias { expr: Box<PyExpr>, alias: String },
}

/// A field that must be present.
fn required<'py>(dict: &Bound<'py, PyDict>, key: &str) -> Result<Bound<'py, PyAny>, PyExprError> {
    dict.get_item(key)
        .map_err(|_| PyExprError::MissingField(key.to_string()))?
        .ok_or_else(|| PyExprError::MissingField(key.to_string()))
}

/// A string field that must be present.
fn required_str(dict: &Bound<'_, PyDict>, key: &str) -> Result<String, PyExprError> {
    required(dict, key)?
        .extract::<String>()
        .map_err(|_| PyExprError::InvalidType(format!("{} must be string", key)))
}

/// A nested expression that must be present.
fn required_expr(dict: &Bound<'_, PyDict>, key: &str) -> Result<Box<PyExpr>, PyExprError> {
    let value = required(dict, key)?;
    let nested = value
        .cast::<PyDict>()
        .map_err(|_| PyExprError::InvalidType(format!("{} must be a dict", key)))?;
    Ok(Box::new(dict_to_py_expr(nested)?))
}

/// A nested expression that may be absent or None.
fn optional_expr(dict: &Bound<'_, PyDict>, key: &str) -> Result<Option<Box<PyExpr>>, PyExprError> {
    match dict
        .get_item(key)
        .map_err(|_| PyExprError::MissingField(key.to_string()))?
    {
        Some(value) if !value.is_none() => {
            let nested = value
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType(format!("{} must be a dict or None", key)))?;
            Ok(Some(Box::new(dict_to_py_expr(nested)?)))
        }
        _ => Ok(None),
    }
}

/// Deserialize a Column expression
fn parse_column_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    Ok(PyExpr::Column(required_str(dict, "name")?))
}

/// Deserialize a Literal expression
fn parse_literal_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    // Convert Python value to string (handles int, float, str, bool, None)
    let value = required(dict, "value")?.to_string();
    let dtype = required_str(dict, "dtype")?;
    Ok(PyExpr::Literal { value, dtype })
}

/// Deserialize a BinOp expression
fn parse_binop_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    Ok(PyExpr::BinOp {
        op: required_str(dict, "op")?,
        left: required_expr(dict, "left")?,
        right: required_expr(dict, "right")?,
    })
}

/// Deserialize a UnaryOp expression
fn parse_unaryop_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    Ok(PyExpr::UnaryOp {
        op: required_str(dict, "op")?,
        operand: required_expr(dict, "operand")?,
    })
}

/// Deserialize args list for a Call expression
fn parse_call_args(args_obj: &Bound<'_, pyo3::PyAny>) -> Result<Vec<PyExpr>, PyExprError> {
    let args_list = args_obj
        .cast::<pyo3::types::PyList>()
        .map_err(|_| PyExprError::InvalidType("args must be a list".to_string()))?;

    let mut args = Vec::new();
    for item in args_list.iter() {
        let arg_dict = item
            .cast::<PyDict>()
            .map_err(|_| PyExprError::InvalidType("args items must be dicts".to_string()))?;
        args.push(dict_to_py_expr(arg_dict)?);
    }
    Ok(args)
}

/// Deserialize kwargs dict for a Call expression
fn parse_call_kwargs(
    kwargs_obj: &Bound<'_, pyo3::PyAny>,
) -> Result<HashMap<String, PyExpr>, PyExprError> {
    let kwargs_dict = kwargs_obj
        .cast::<PyDict>()
        .map_err(|_| PyExprError::InvalidType("kwargs must be a dict".to_string()))?;

    let mut kwargs = HashMap::new();
    for (key, value) in kwargs_dict.iter() {
        let key_str = key
            .extract::<String>()
            .map_err(|_| PyExprError::InvalidType("kwargs keys must be strings".to_string()))?;

        let value_dict: &Bound<'_, PyDict> = value
            .cast::<PyDict>()
            .map_err(|_| PyExprError::InvalidType("kwargs values must be dicts".to_string()))?;
        kwargs.insert(key_str, dict_to_py_expr(value_dict)?);
    }
    Ok(kwargs)
}

/// Deserialize a Call expression
fn parse_call_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    Ok(PyExpr::Call {
        func: required_str(dict, "func")?,
        args: parse_call_args(&required(dict, "args")?)?,
        kwargs: parse_call_kwargs(&required(dict, "kwargs")?)?,
        // "on" is None (or absent) for standalone functions like abs(x),
        // whose inputs are all in args.
        on: optional_expr(dict, "on")?,
    })
}

/// Deserialize a Window expression
fn parse_window_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    // Parse descending (default false)
    let descending = dict
        .get_item("descending")
        .ok()
        .flatten()
        .and_then(|v| v.extract::<bool>().ok())
        .unwrap_or(false);

    Ok(PyExpr::Window {
        // The inner expression (e.g., row_number(), rank(), etc.)
        expr: required_expr(dict, "expr")?,
        partition_by: optional_expr(dict, "partition_by")?,
        order_by: optional_expr(dict, "order_by")?,
        descending,
    })
}

/// Deserialize an Alias expression
fn parse_alias_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    Ok(PyExpr::Alias {
        alias: required_str(dict, "alias")?,
        expr: required_expr(dict, "expr")?,
    })
}

/// Recursively deserialize a Python dict to PyExpr
pub fn dict_to_py_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    // Dispatch to type-specific parser
    let expr_type = required_str(dict, "type")?;
    match expr_type.as_str() {
        "Column" => parse_column_expr(dict),
        "Literal" => parse_literal_expr(dict),
        "BinOp" => parse_binop_expr(dict),
        "UnaryOp" => parse_unaryop_expr(dict),
        "Call" => parse_call_expr(dict),
        "Window" => parse_window_expr(dict),
        "Alias" => parse_alias_expr(dict),
        _ => Err(PyExprError::UnknownVariant(expr_type)),
    }
}
