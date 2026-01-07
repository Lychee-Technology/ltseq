//! PyExpr type definition and deserialization

use crate::error::PyExprError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Represents a serialized Python expression for transpilation to DataFusion
#[derive(Debug, Clone)]
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
        on: Box<PyExpr>,
    },
}

/// Recursively deserialize a Python dict to PyExpr
pub fn dict_to_py_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
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

            let left_dict_obj = dict
                .get_item("left")
                .map_err(|_| PyExprError::MissingField("left".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("left".to_string()))?;
            let left_dict = left_dict_obj
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("left must be a dict".to_string()))?;

            let right_dict_obj = dict
                .get_item("right")
                .map_err(|_| PyExprError::MissingField("right".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("right".to_string()))?;
            let right_dict = right_dict_obj
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("right must be a dict".to_string()))?;

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

            let operand_dict_obj = dict
                .get_item("operand")
                .map_err(|_| PyExprError::MissingField("operand".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("operand".to_string()))?;
            let operand_dict = operand_dict_obj
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("operand must be a dict".to_string()))?;

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

            // Parse args array
            let args_obj = dict
                .get_item("args")
                .map_err(|_| PyExprError::MissingField("args".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("args".to_string()))?;

            let args_list = args_obj
                .cast::<pyo3::types::PyList>()
                .map_err(|_| PyExprError::InvalidType("args must be a list".to_string()))?;

            let mut args = Vec::new();
            for item in args_list.iter() {
                let arg_dict = item.cast::<PyDict>().map_err(|_| {
                    PyExprError::InvalidType("args items must be dicts".to_string())
                })?;
                args.push(dict_to_py_expr(&arg_dict)?);
            }

            // Parse kwargs dict
            let kwargs_obj = dict
                .get_item("kwargs")
                .map_err(|_| PyExprError::MissingField("kwargs".to_string()))?
                .ok_or_else(|| PyExprError::MissingField("kwargs".to_string()))?;

            let kwargs_dict = kwargs_obj
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("kwargs must be a dict".to_string()))?;

            let mut kwargs = HashMap::new();
            for (key, value) in kwargs_dict.iter() {
                let key_str = key.extract::<String>().map_err(|_| {
                    PyExprError::InvalidType("kwargs keys must be strings".to_string())
                })?;

                let value_dict: &Bound<'_, PyDict> = value.cast::<PyDict>().map_err(|_| {
                    PyExprError::InvalidType("kwargs values must be dicts".to_string())
                })?;
                kwargs.insert(key_str, dict_to_py_expr(value_dict)?);
            }

            // Parse "on" field
            let on_obj = dict
                .get_item("on")
                .map_err(|_| PyExprError::MissingField("on".to_string()))?;

            let on = if let Some(on_val) = on_obj {
                let on_dict: &Bound<'_, PyDict> = on_val
                    .cast::<PyDict>()
                    .map_err(|_| PyExprError::InvalidType("on must be a dict".to_string()))?;
                Box::new(dict_to_py_expr(on_dict)?)
            } else {
                // If "on" is None, create a dummy column reference (will be handled by transpiler)
                Box::new(PyExpr::Column(String::new()))
            };

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
