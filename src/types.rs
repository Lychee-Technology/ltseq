//! PyExpr type definition and deserialization

use crate::error::PyExprError;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};
use std::collections::HashMap;
use std::sync::Arc;

/// Largest precision a Decimal128 literal can carry.
const DECIMAL128_MAX_PRECISION: u8 = 38;

/// A literal value decoded at the Python boundary into Rust-owned data.
///
/// Python encodes each literal as a typed payload (see `LiteralExpr` in
/// `core_types.py`); the parser extracts the matching native type once, so no
/// consumer ever re-parses a string or holds a Python object.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    /// Unscaled integer with the precision/scale of the Python `Decimal`.
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    /// Days since the Unix epoch.
    Date32(i32),
    /// Microseconds since the Unix epoch; `tz` is `None` for naive datetimes.
    TimestampMicrosecond {
        value: i64,
        tz: Option<String>,
    },
    /// Nanoseconds since the Unix epoch; used when a datetime has sub-microsecond precision.
    TimestampNanosecond {
        value: i64,
        tz: Option<String>,
    },
}

impl LiteralValue {
    /// The wire `dtype` name of this literal.
    pub fn dtype(&self) -> &'static str {
        match self {
            LiteralValue::Null => "Null",
            LiteralValue::Boolean(_) => "Boolean",
            LiteralValue::Int64(_) => "Int64",
            LiteralValue::Float64(_) => "Float64",
            LiteralValue::String(_) => "String",
            LiteralValue::Decimal128 { .. } => "Decimal128",
            LiteralValue::Date32(_) => "Date32",
            LiteralValue::TimestampMicrosecond { .. } => "TimestampMicrosecond",
            LiteralValue::TimestampNanosecond { .. } => "TimestampNanosecond",
        }
    }

    /// Convert to the DataFusion scalar this literal denotes.
    pub fn to_scalar_value(&self) -> ScalarValue {
        match self {
            LiteralValue::Null => ScalarValue::Null,
            LiteralValue::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            LiteralValue::Int64(v) => ScalarValue::Int64(Some(*v)),
            LiteralValue::Float64(v) => ScalarValue::Float64(Some(*v)),
            LiteralValue::String(v) => ScalarValue::Utf8(Some(v.clone())),
            LiteralValue::Decimal128 {
                value,
                precision,
                scale,
            } => ScalarValue::Decimal128(Some(*value), *precision, *scale),
            LiteralValue::Date32(v) => ScalarValue::Date32(Some(*v)),
            LiteralValue::TimestampMicrosecond { value, tz } => {
                ScalarValue::TimestampMicrosecond(Some(*value), tz.as_deref().map(Arc::from))
            }
            LiteralValue::TimestampNanosecond { value, tz } => {
                ScalarValue::TimestampNanosecond(Some(*value), tz.as_deref().map(Arc::from))
            }
        }
    }

    /// The value of an `Int64` literal.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            LiteralValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// The value of an `Int64` or `Float64` literal as a float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            LiteralValue::Int64(v) => Some(*v as f64),
            LiteralValue::Float64(v) => Some(*v),
            _ => None,
        }
    }
}

/// Represents a serialized Python expression for transpilation to DataFusion
#[derive(Debug, Clone, PartialEq)]
pub enum PyExpr {
    /// Column reference: {"type": "Column", "name": "age"}
    Column(String),

    /// Literal value: {"type": "Literal", "value": 18, "dtype": "Int64"}
    Literal(LiteralValue),

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

/// Deserialize a Column expression
fn parse_column_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    let name = dict
        .get_item("name")
        .map_err(|_| PyExprError::MissingField("name".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("name".to_string()))?
        .extract::<String>()
        .map_err(|_| PyExprError::InvalidType("name must be string".to_string()))?;
    Ok(PyExpr::Column(name))
}

/// Fetch a required field of a Literal payload.
fn literal_field<'py>(
    dict: &Bound<'py, PyDict>,
    field: &str,
) -> Result<Bound<'py, PyAny>, PyExprError> {
    dict.get_item(field)
        .map_err(|_| PyExprError::MissingField(field.to_string()))?
        .ok_or_else(|| PyExprError::MissingField(field.to_string()))
}

/// The Python type a Literal payload field must have on the wire.
///
/// PyO3's numeric extraction is coercive (`True` extracts as `1i64`, `1`
/// extracts as `1.0f64`), so the field's Python type is checked before the
/// value is extracted; a payload that contradicts its dtype is rejected.
#[derive(Clone, Copy)]
enum WireType {
    Bool,
    /// A Python `int`; `bool` is excluded even though it subclasses `int`.
    Int,
    Float,
    Str,
    /// A Python `str` or `None`.
    OptionalStr,
    None,
}

impl WireType {
    fn matches(self, obj: &Bound<'_, PyAny>) -> bool {
        match self {
            WireType::Bool => obj.is_instance_of::<PyBool>(),
            WireType::Int => obj.is_instance_of::<PyInt>() && !obj.is_instance_of::<PyBool>(),
            WireType::Float => obj.is_instance_of::<PyFloat>(),
            WireType::Str => obj.is_instance_of::<PyString>(),
            WireType::OptionalStr => obj.is_none() || obj.is_instance_of::<PyString>(),
            WireType::None => obj.is_none(),
        }
    }
}

/// The error for a Literal payload field whose value is not what its dtype requires.
fn literal_field_mismatch(
    obj: &Bound<'_, PyAny>,
    field: &str,
    dtype: &str,
    expected: &str,
) -> PyExprError {
    PyExprError::InvalidType(format!(
        "{dtype} literal '{field}' must be {expected}, got {}",
        obj.get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "<unknown>".to_string())
    ))
}

/// Fetch a required Literal payload field and check it has the `wire` Python type.
fn checked_literal_field<'py>(
    dict: &Bound<'py, PyDict>,
    field: &str,
    dtype: &str,
    wire: WireType,
    expected: &str,
) -> Result<Bound<'py, PyAny>, PyExprError> {
    let obj = literal_field(dict, field)?;
    if !wire.matches(&obj) {
        return Err(literal_field_mismatch(&obj, field, dtype, expected));
    }
    Ok(obj)
}

/// Extract a required Literal payload field as `T`, naming the dtype on failure.
///
/// The field must have the `wire` Python type; the extraction after that
/// check only fails on range (an int outside `T`), which `expected` names.
fn extract_literal_field<'py, T>(
    dict: &Bound<'py, PyDict>,
    field: &str,
    dtype: &str,
    wire: WireType,
    expected: &str,
) -> Result<T, PyExprError>
where
    T: for<'a> FromPyObject<'a, 'py>,
{
    let obj = checked_literal_field(dict, field, dtype, wire, expected)?;
    obj.extract::<T>()
        .map_err(|_| literal_field_mismatch(&obj, field, dtype, expected))
}

/// Decode the common payload fields for timestamp literals at any Arrow unit.
fn parse_timestamp_payload(
    dict: &Bound<'_, PyDict>,
    dtype: &str,
) -> Result<(i64, Option<String>), PyExprError> {
    let value = extract_literal_field(
        dict,
        "value",
        dtype,
        WireType::Int,
        "an int in the Int64 range",
    )?;
    let tz = extract_literal_field(dict, "tz", dtype, WireType::OptionalStr, "a str or None")?;
    Ok((value, tz))
}

/// Deserialize a Literal expression by extracting the native type its dtype names.
fn parse_literal_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    let dtype: String = literal_field(dict, "dtype")?
        .extract()
        .map_err(|_| PyExprError::InvalidType("dtype must be string".to_string()))?;

    let value = match dtype.as_str() {
        "Null" => {
            checked_literal_field(dict, "value", &dtype, WireType::None, "None")?;
            LiteralValue::Null
        }
        "Boolean" => LiteralValue::Boolean(extract_literal_field(
            dict,
            "value",
            &dtype,
            WireType::Bool,
            "a bool",
        )?),
        "Int64" => LiteralValue::Int64(extract_literal_field(
            dict,
            "value",
            &dtype,
            WireType::Int,
            "an int in the Int64 range",
        )?),
        "Float64" => LiteralValue::Float64(extract_literal_field(
            dict,
            "value",
            &dtype,
            WireType::Float,
            "a float",
        )?),
        "String" => LiteralValue::String(extract_literal_field(
            dict,
            "value",
            &dtype,
            WireType::Str,
            "a str",
        )?),
        "Decimal128" => {
            let precision: u8 =
                extract_literal_field(dict, "precision", &dtype, WireType::Int, "an int")?;
            let scale: i8 = extract_literal_field(dict, "scale", &dtype, WireType::Int, "an int")?;
            if !(1..=DECIMAL128_MAX_PRECISION).contains(&precision)
                || scale < 0
                || scale.unsigned_abs() > precision
            {
                return Err(PyExprError::InvalidType(format!(
                    "Decimal128 literal has invalid precision {precision} / scale {scale}"
                )));
            }
            let value: i128 = extract_literal_field(
                dict,
                "value",
                &dtype,
                WireType::Int,
                "an int in the Decimal128 range",
            )?;
            if value.unsigned_abs() >= 10u128.pow(u32::from(precision)) {
                return Err(PyExprError::InvalidType(format!(
                    "Decimal128 literal value {value} does not fit precision {precision}"
                )));
            }
            LiteralValue::Decimal128 {
                value,
                precision,
                scale,
            }
        }
        "Date32" => LiteralValue::Date32(extract_literal_field(
            dict,
            "value",
            &dtype,
            WireType::Int,
            "an int in the Int32 range",
        )?),
        "TimestampMicrosecond" => {
            let (value, tz) = parse_timestamp_payload(dict, &dtype)?;
            LiteralValue::TimestampMicrosecond { value, tz }
        }
        "TimestampNanosecond" => {
            let (value, tz) = parse_timestamp_payload(dict, &dtype)?;
            LiteralValue::TimestampNanosecond { value, tz }
        }
        other => {
            return Err(PyExprError::InvalidType(format!(
                "Unknown literal dtype: {other}"
            )))
        }
    };

    Ok(PyExpr::Literal(value))
}

/// Deserialize a BinOp expression
fn parse_binop_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
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

    let left = Box::new(dict_to_py_expr(left_dict)?);
    let right = Box::new(dict_to_py_expr(right_dict)?);

    Ok(PyExpr::BinOp { op, left, right })
}

/// Deserialize a UnaryOp expression
fn parse_unaryop_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
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

    let operand = Box::new(dict_to_py_expr(operand_dict)?);
    Ok(PyExpr::UnaryOp { op, operand })
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
    let args = parse_call_args(&args_obj)?;

    // Parse kwargs dict
    let kwargs_obj = dict
        .get_item("kwargs")
        .map_err(|_| PyExprError::MissingField("kwargs".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("kwargs".to_string()))?;
    let kwargs = parse_call_kwargs(&kwargs_obj)?;

    // Parse "on" field - can be None for standalone functions like abs()
    let on_obj = dict
        .get_item("on")
        .map_err(|_| PyExprError::MissingField("on".to_string()))?;

    let on = if let Some(on_val) = on_obj {
        // Check if it's Python None (null in JSON)
        if on_val.is_none() {
            // For standalone functions like abs(x), on is None and x is in args
            Box::new(PyExpr::Column(String::new()))
        } else {
            let on_dict: &Bound<'_, PyDict> = on_val
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("on must be a dict or None".to_string()))?;
            Box::new(dict_to_py_expr(on_dict)?)
        }
    } else {
        // If "on" key is missing, create a dummy column reference
        Box::new(PyExpr::Column(String::new()))
    };

    Ok(PyExpr::Call {
        func,
        args,
        kwargs,
        on,
    })
}

/// Deserialize a Window expression
fn parse_window_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    // Parse the inner expression (e.g., row_number(), rank(), etc.)
    let expr_obj = dict
        .get_item("expr")
        .map_err(|_| PyExprError::MissingField("expr".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("expr".to_string()))?;
    let expr_dict = expr_obj
        .cast::<PyDict>()
        .map_err(|_| PyExprError::InvalidType("expr must be a dict".to_string()))?;
    let expr = Box::new(dict_to_py_expr(expr_dict)?);

    // Parse partition_by (optional)
    let partition_by = if let Some(pb_obj) = dict
        .get_item("partition_by")
        .map_err(|_| PyExprError::MissingField("partition_by".to_string()))?
    {
        if pb_obj.is_none() {
            None
        } else {
            let pb_dict = pb_obj
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("partition_by must be a dict".to_string()))?;
            Some(Box::new(dict_to_py_expr(pb_dict)?))
        }
    } else {
        None
    };

    // Parse order_by (optional)
    let order_by = if let Some(ob_obj) = dict
        .get_item("order_by")
        .map_err(|_| PyExprError::MissingField("order_by".to_string()))?
    {
        if ob_obj.is_none() {
            None
        } else {
            let ob_dict = ob_obj
                .cast::<PyDict>()
                .map_err(|_| PyExprError::InvalidType("order_by must be a dict".to_string()))?;
            Some(Box::new(dict_to_py_expr(ob_dict)?))
        }
    } else {
        None
    };

    // Parse descending (default false)
    let descending = dict
        .get_item("descending")
        .ok()
        .flatten()
        .and_then(|v| v.extract::<bool>().ok())
        .unwrap_or(false);

    Ok(PyExpr::Window {
        expr,
        partition_by,
        order_by,
        descending,
    })
}

/// Deserialize an Alias expression
fn parse_alias_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    let alias = dict
        .get_item("alias")
        .map_err(|_| PyExprError::MissingField("alias".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("alias".to_string()))?
        .extract::<String>()
        .map_err(|_| PyExprError::InvalidType("alias must be string".to_string()))?;

    let expr_obj = dict
        .get_item("expr")
        .map_err(|_| PyExprError::MissingField("expr".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("expr".to_string()))?;
    let expr_dict = expr_obj
        .cast::<PyDict>()
        .map_err(|_| PyExprError::InvalidType("expr must be a dict".to_string()))?;
    let expr = Box::new(dict_to_py_expr(expr_dict)?);

    Ok(PyExpr::Alias { expr, alias })
}

/// Recursively deserialize a Python dict to PyExpr
pub fn dict_to_py_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    // Get "type" field
    let expr_type = dict
        .get_item("type")
        .map_err(|_| PyExprError::MissingField("type".to_string()))?
        .ok_or_else(|| PyExprError::MissingField("type".to_string()))?
        .extract::<String>()
        .map_err(|_| PyExprError::InvalidType("type must be string".to_string()))?;

    // Dispatch to type-specific parser
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
