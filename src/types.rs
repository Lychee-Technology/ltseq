//! PyExpr type definition and deserialization

use crate::error::{validation_error, LtseqError, PyExprError};
use crate::stack::MAX_EXPR_DEPTH;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Largest precision a Decimal128 literal can carry.
const DECIMAL128_MAX_PRECISION: u8 = 38;

/// A literal value, decoded at the Python boundary into Rust-owned data.
///
/// Python encodes each literal as a typed payload (`_encode_literal` in
/// `core_types.py`); the decoder below checks it against the wire contract
/// once, so no consumer re-parses a string or holds a Python object. Values
/// are read through `to_scalar_value` or the `require_*` accessors, which
/// return an error naming the argument instead of an `Option` a consumer
/// could silently default.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    /// Unscaled integer with the precision and scale of the Python `Decimal`'s digits.
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    /// Days since the Unix epoch.
    Date32(i32),
    /// Ticks of `unit` since the Unix epoch. For an aware value (`tz` set)
    /// the ticks are the UTC instant; for a naive one, wall-clock time.
    Timestamp {
        value: i64,
        unit: TimeUnit,
        tz: Option<Arc<str>>,
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
            LiteralValue::Timestamp { .. } => "Timestamp",
        }
    }

    /// The DataFusion scalar this literal denotes; the only such mapping.
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
            LiteralValue::Timestamp { value, unit, tz } => {
                timestamp_scalar(*unit, Some(*value), tz.clone())
            }
        }
    }

    /// An integer argument: an `Int64`, or a `Decimal128` with an integral
    /// value that fits. Anything else is an error naming `what`.
    pub fn require_i64(&self, what: &str) -> Result<i64, LtseqError> {
        match self {
            LiteralValue::Int64(v) => Ok(*v),
            LiteralValue::Decimal128 { value, scale, .. } => {
                let divisor = 10i128.pow(u32::from(scale.unsigned_abs()));
                if value % divisor == 0 {
                    if let Ok(v) = i64::try_from(value / divisor) {
                        return Ok(v);
                    }
                }
                Err(validation_error!("{what} must be an integer, got {self}"))
            }
            _ => Err(validation_error!("{what} must be an integer, got {self}")),
        }
    }

    /// A numeric argument: an `Int64`, `Float64` or `Decimal128` (the
    /// correctly rounded float of its decimal value).
    pub fn require_f64(&self, what: &str) -> Result<f64, LtseqError> {
        match self {
            LiteralValue::Int64(v) => Ok(*v as f64),
            LiteralValue::Float64(v) => Ok(*v),
            LiteralValue::Decimal128 { value, scale, .. } => Ok(decimal_to_f64(*value, *scale)),
            _ => Err(validation_error!("{what} must be a number, got {self}")),
        }
    }

    /// A string argument; only a `String` literal qualifies.
    pub fn require_str(&self, what: &str) -> Result<&str, LtseqError> {
        match self {
            LiteralValue::String(v) => Ok(v),
            _ => Err(validation_error!("{what} must be a string, got {self}")),
        }
    }
}

/// `"a String literal '1'"`, `"a Decimal128 literal 1.50"`: the dtype and the
/// value, for error messages. A `Decimal128` renders as plain decimal text.
impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dtype = self.dtype();
        match self {
            LiteralValue::Null => write!(f, "a Null literal"),
            LiteralValue::Boolean(v) => write!(f, "a {dtype} literal {}", if *v { "True" } else { "False" }),
            LiteralValue::Int64(v) => write!(f, "an {dtype} literal {v}"),
            LiteralValue::Float64(v) => write!(f, "a {dtype} literal {v:?}"),
            LiteralValue::String(v) => write!(f, "a {dtype} literal '{v}'"),
            LiteralValue::Decimal128 { value, scale, .. } => {
                write!(f, "a {dtype} literal {}", decimal_text(*value, *scale))
            }
            LiteralValue::Date32(v) => write!(f, "a {dtype} literal (day {v})"),
            LiteralValue::Timestamp { value, unit, tz } => match tz {
                Some(tz) => write!(f, "a {dtype} literal ({value} {unit:?}, {tz})"),
                None => write!(f, "a {dtype} literal ({value} {unit:?})"),
            },
        }
    }
}

/// `value / 10^scale` written out as plain decimal text (`-0.05`, `150`).
pub fn decimal_text(value: i128, scale: i8) -> String {
    let digits = value.unsigned_abs().to_string();
    let scale = usize::from(scale.unsigned_abs());
    let sign = if value < 0 { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int_part, frac_part) = padded.split_at(padded.len() - scale);
    format!("{sign}{int_part}.{frac_part}")
}

/// The float nearest to `value / 10^scale`: Rust's float parsing rounds
/// correctly, so this is the float Python's `float(Decimal)` gives.
pub fn decimal_to_f64(value: i128, scale: i8) -> f64 {
    decimal_text(value, scale)
        .parse()
        .unwrap_or_else(|_| unreachable!("decimal_text writes plain decimal digits"))
}

/// The timestamp scalar of `unit` for `value` ticks.
pub fn timestamp_scalar(unit: TimeUnit, value: Option<i64>, tz: Option<Arc<str>>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(value, tz),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, tz),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, tz),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, tz),
    }
}

/// A positional argument of a call, with "absent" and "present but not a
/// literal" kept apart, so a consumer that defaults an absent argument cannot
/// also default a wrong one.
#[derive(Debug, Clone, Copy)]
pub enum Arg<'a> {
    Absent,
    Literal(&'a LiteralValue),
    Expr(&'a PyExpr),
}

/// `args[i]` as an [`Arg`].
pub fn arg(args: &[PyExpr], i: usize) -> Arg<'_> {
    match args.get(i) {
        None => Arg::Absent,
        Some(PyExpr::Literal(value)) => Arg::Literal(value),
        Some(expr) => Arg::Expr(expr),
    }
}

/// Represents a serialized Python expression for transpilation to DataFusion
#[derive(Debug, Clone, PartialEq)]
pub enum PyExpr {
    /// Column reference: {"type": "Column", "name": "age"}
    Column(String),

    /// Literal value: {"type": "Literal", "dtype": "Int64", "value": 18}
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
        /// Receiver of a method-style call (`r.x.abs()`); `None` for a
        /// standalone function (`abs(r.x)`), whose inputs are all in `args`.
        on: Option<Box<PyExpr>>,
    },

    /// Window expression: {"type": "Window", "expr": {...}, "partition_by": {...}, "order_by": {...}, "descending": bool, "nulls_last": bool}
    Window {
        expr: Box<PyExpr>,
        partition_by: Option<Box<PyExpr>>,
        order_by: Option<Box<PyExpr>>,
        descending: bool,
        /// Where `order_by` puts NULL, as in `sort` (§9.4, §10.4).
        nulls_last: bool,
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

/// A Literal payload being decoded. The typed getters check each field's
/// exact Python type before extracting it (PyO3 alone would read `True` as
/// `1` and `1` as `1.0`) and record the field as read, so `finish` can reject
/// any field the dtype does not have.
struct LiteralPayload<'a, 'py> {
    dict: &'a Bound<'py, PyDict>,
    dtype: String,
    read: Vec<&'static str>,
}

impl<'a, 'py> LiteralPayload<'a, 'py> {
    fn new(dict: &'a Bound<'py, PyDict>) -> Result<Self, PyExprError> {
        let dtype = dict
            .get_item("dtype")
            .ok()
            .flatten()
            .ok_or_else(|| invalid("literal is missing field 'dtype'".to_string()))?;
        if !dtype.is_instance_of::<PyString>() {
            return Err(invalid(format!(
                "literal field 'dtype' must be a str, got {}",
                type_name(&dtype)
            )));
        }
        Ok(Self {
            dict,
            dtype: dtype.extract().map_err(py_err)?,
            read: vec![],
        })
    }

    fn field(&mut self, name: &'static str) -> Result<Bound<'py, PyAny>, PyExprError> {
        self.read.push(name);
        self.dict
            .get_item(name)
            .ok()
            .flatten()
            .ok_or_else(|| invalid(format!("{} literal is missing field '{name}'", self.dtype)))
    }

    fn wrong_type(&self, name: &str, expected: &str, got: &Bound<'_, PyAny>) -> PyExprError {
        invalid(format!(
            "{} literal field '{name}' must be {expected}, got {}",
            self.dtype,
            type_name(got)
        ))
    }

    /// An `int` field (never a `bool`) that fits `T`.
    fn int<T>(&mut self, name: &'static str) -> Result<T, PyExprError>
    where
        T: for<'b> FromPyObject<'b, 'py>,
    {
        let obj = self.field(name)?;
        if !obj.is_instance_of::<PyInt>() || obj.is_instance_of::<PyBool>() {
            return Err(self.wrong_type(name, "an int", &obj));
        }
        obj.extract::<T>().map_err(|_| {
            invalid(format!(
                "{} literal field '{name}' value {obj} is out of range",
                self.dtype
            ))
        })
    }

    fn float(&mut self, name: &'static str) -> Result<f64, PyExprError> {
        let obj = self.field(name)?;
        if !obj.is_instance_of::<PyFloat>() {
            return Err(self.wrong_type(name, "a float", &obj));
        }
        obj.extract().map_err(py_err)
    }

    fn bool(&mut self, name: &'static str) -> Result<bool, PyExprError> {
        let obj = self.field(name)?;
        if !obj.is_instance_of::<PyBool>() {
            return Err(self.wrong_type(name, "a bool", &obj));
        }
        obj.extract().map_err(py_err)
    }

    fn str(&mut self, name: &'static str) -> Result<String, PyExprError> {
        let obj = self.field(name)?;
        if !obj.is_instance_of::<PyString>() {
            return Err(self.wrong_type(name, "a str", &obj));
        }
        obj.extract().map_err(py_err)
    }

    fn opt_str(&mut self, name: &'static str) -> Result<Option<String>, PyExprError> {
        let obj = self.field(name)?;
        if obj.is_none() {
            return Ok(None);
        }
        if !obj.is_instance_of::<PyString>() {
            return Err(self.wrong_type(name, "a str or None", &obj));
        }
        obj.extract().map(Some).map_err(py_err)
    }

    fn none(&mut self, name: &'static str) -> Result<(), PyExprError> {
        let obj = self.field(name)?;
        if !obj.is_none() {
            return Err(self.wrong_type(name, "None", &obj));
        }
        Ok(())
    }

    /// Reject any field other than `type`, `dtype` and the ones read.
    fn finish(self) -> Result<(), PyExprError> {
        for key in self.dict.keys() {
            let key: String = key.extract().map_err(py_err)?;
            if key != "type" && key != "dtype" && !self.read.contains(&key.as_str()) {
                return Err(invalid(format!(
                    "unknown field '{key}' for {} literal",
                    self.dtype
                )));
            }
        }
        Ok(())
    }
}

fn invalid(message: String) -> PyExprError {
    PyExprError::InvalidLiteral(message)
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|name| name.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string())
}

/// A Python error raised while reading a field already type-checked above.
fn py_err(err: PyErr) -> PyExprError {
    invalid(err.to_string())
}

/// Check the parts of a Decimal128 literal against each other.
fn check_decimal(value: i128, precision: u8, scale: i8) -> Result<(), String> {
    if !(1..=DECIMAL128_MAX_PRECISION).contains(&precision) {
        return Err(format!(
            "Decimal128 literal precision {precision} is outside 1..={DECIMAL128_MAX_PRECISION}"
        ));
    }
    if scale < 0 || scale.unsigned_abs() > precision {
        return Err(format!(
            "Decimal128 literal scale {scale} is outside 0..={precision}"
        ));
    }
    if value.unsigned_abs() >= 10u128.pow(u32::from(precision)) {
        return Err(format!(
            "Decimal128 literal value {value} does not fit precision {precision}"
        ));
    }
    Ok(())
}

/// The Arrow time unit a Timestamp literal's `unit` field names.
fn parse_time_unit(unit: &str) -> Result<TimeUnit, String> {
    match unit {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        other => Err(format!(
            "Timestamp literal unit must be one of s, ms, us, ns, got '{other}'"
        )),
    }
}

/// A Timestamp literal's `tz`, which must name a zone Arrow can use.
fn parse_time_zone(tz: &str) -> Result<Arc<str>, String> {
    tz.parse::<Tz>()
        .map(|_| Arc::from(tz))
        .map_err(|_| format!("Timestamp literal tz '{tz}' is not a valid time zone"))
}

/// Deserialize a value that `insert` or `update` writes: a Literal
/// expression, decoded exactly as a literal inside an expression is.
pub fn dict_to_literal(dict: &Bound<'_, PyDict>) -> Result<LiteralValue, PyExprError> {
    let expr_type = required_str(dict, "type")?;
    if expr_type != "Literal" {
        return Err(PyExprError::InvalidType(format!(
            "a value to write must be a Literal expression, got {expr_type}"
        )));
    }
    parse_literal(dict)
}

/// Deserialize a Literal expression: the fields its dtype names, each of the
/// exact Python type the wire contract gives it, and nothing else.
fn parse_literal(dict: &Bound<'_, PyDict>) -> Result<LiteralValue, PyExprError> {
    let mut payload = LiteralPayload::new(dict)?;
    let value = match payload.dtype.as_str() {
        "Null" => {
            payload.none("value")?;
            LiteralValue::Null
        }
        "Boolean" => LiteralValue::Boolean(payload.bool("value")?),
        "Int64" => LiteralValue::Int64(payload.int("value")?),
        "Float64" => LiteralValue::Float64(payload.float("value")?),
        "String" => LiteralValue::String(payload.str("value")?),
        "Decimal128" => {
            let value = payload.int("value")?;
            let precision = payload.int("precision")?;
            let scale = payload.int("scale")?;
            check_decimal(value, precision, scale).map_err(invalid)?;
            LiteralValue::Decimal128 {
                value,
                precision,
                scale,
            }
        }
        "Date32" => LiteralValue::Date32(payload.int("value")?),
        "Timestamp" => {
            let value = payload.int("value")?;
            let unit = parse_time_unit(&payload.str("unit")?).map_err(invalid)?;
            let tz = match payload.opt_str("tz")? {
                Some(tz) => Some(parse_time_zone(&tz).map_err(invalid)?),
                None => None,
            };
            LiteralValue::Timestamp { value, unit, tz }
        }
        other => return Err(invalid(format!("Unknown literal dtype '{other}'"))),
    };
    payload.finish()?;
    Ok(value)
}

/// A child of an expression node: a nested expression, or an optional one
/// that is absent.
enum Child<'py> {
    Expr(Bound<'py, PyDict>),
    Absent,
}

/// Where the next child of a node is, looked up only when the walk reaches
/// it, so a node's children deserialize in order and an error names the
/// first malformed field in that order: `left` deserializes before `right`
/// is looked up.
enum Slot<'py> {
    /// A nested expression that must be present.
    Required(&'static str),
    /// A nested expression that may be absent or None.
    Optional(&'static str),
    /// A Call's args list, one child per item.
    Args,
    Arg(Bound<'py, PyAny>),
    /// A Call's kwargs dict, one child per value.
    Kwargs,
    Kwarg(Bound<'py, PyAny>, Bound<'py, PyAny>),
}

/// A nested expression that must be present.
fn required_child<'py>(dict: &Bound<'py, PyDict>, key: &str) -> Result<Child<'py>, PyExprError> {
    required(dict, key)?
        .cast_into::<PyDict>()
        .map(Child::Expr)
        .map_err(|_| PyExprError::InvalidType(format!("{} must be a dict", key)))
}

/// A nested expression that may be absent or None.
fn optional_child<'py>(dict: &Bound<'py, PyDict>, key: &str) -> Result<Child<'py>, PyExprError> {
    match dict
        .get_item(key)
        .map_err(|_| PyExprError::MissingField(key.to_string()))?
    {
        Some(value) if !value.is_none() => value
            .cast_into::<PyDict>()
            .map(Child::Expr)
            .map_err(|_| PyExprError::InvalidType(format!("{} must be a dict or None", key))),
        _ => Ok(Child::Absent),
    }
}

/// An expression node's own fields, read before its children.
enum Shell {
    BinOp {
        op: String,
    },
    UnaryOp {
        op: String,
    },
    /// `args` children, then one per kwarg name, then `on`.
    Call {
        func: String,
        args: usize,
        kwargs: Vec<String>,
    },
    /// `expr`, `partition_by`, then `order_by`.
    Window {
        descending: bool,
        nulls_last: bool,
    },
    Alias {
        alias: String,
    },
}

impl Shell {
    /// The node, given one parsed value per child slot, in slot order.
    fn build(self, children: Vec<Option<PyExpr>>) -> Result<PyExpr, PyExprError> {
        let mut children = Children(children.into_iter());
        Ok(match self {
            Shell::BinOp { op } => PyExpr::BinOp {
                op,
                left: children.required()?,
                right: children.required()?,
            },
            Shell::UnaryOp { op } => PyExpr::UnaryOp {
                op,
                operand: children.required()?,
            },
            Shell::Window {
                descending,
                nulls_last,
            } => PyExpr::Window {
                expr: children.required()?,
                partition_by: children.optional()?,
                order_by: children.optional()?,
                descending,
                nulls_last,
            },
            Shell::Alias { alias } => PyExpr::Alias {
                expr: children.required()?,
                alias,
            },
            Shell::Call { func, args, kwargs } => {
                let args = (0..args)
                    .map(|_| children.required().map(|arg| *arg))
                    .collect::<Result<Vec<_>, _>>()?;
                let kwargs = kwargs
                    .into_iter()
                    .map(|name| children.required().map(|value| (name, *value)))
                    .collect::<Result<HashMap<_, _>, _>>()?;
                PyExpr::Call {
                    func,
                    args,
                    kwargs,
                    on: children.optional()?,
                }
            }
        })
    }
}

/// The parsed children of a node, taken in slot order.
struct Children(std::vec::IntoIter<Option<PyExpr>>);

impl Children {
    fn optional(&mut self) -> Result<Option<Box<PyExpr>>, PyExprError> {
        self.0
            .next()
            .map(|child| child.map(Box::new))
            .ok_or_else(|| PyExprError::InvalidType("expression node lost a child".to_string()))
    }

    fn required(&mut self) -> Result<Box<PyExpr>, PyExprError> {
        self.optional()?
            .ok_or_else(|| PyExprError::InvalidType("expression node lost a child".to_string()))
    }
}

/// One node read from its dict: a leaf, or a node whose children follow.
enum Node {
    Leaf(PyExpr),
    Inner(Shell, Vec<Slot<'static>>),
}

/// Read one node of an expression dict, without its children.
fn read_node(dict: &Bound<'_, PyDict>) -> Result<Node, PyExprError> {
    let expr_type = required_str(dict, "type")?;
    Ok(match expr_type.as_str() {
        "Column" => Node::Leaf(PyExpr::Column(required_str(dict, "name")?)),
        "Literal" => Node::Leaf(PyExpr::Literal(parse_literal(dict)?)),
        "BinOp" => Node::Inner(
            Shell::BinOp {
                op: required_str(dict, "op")?,
            },
            vec![Slot::Required("left"), Slot::Required("right")],
        ),
        "UnaryOp" => Node::Inner(
            Shell::UnaryOp {
                op: required_str(dict, "op")?,
            },
            vec![Slot::Required("operand")],
        ),
        // "on" is None (or absent) for standalone functions like abs(x),
        // whose inputs are all in args.
        "Call" => Node::Inner(
            Shell::Call {
                func: required_str(dict, "func")?,
                args: 0,
                kwargs: Vec::new(),
            },
            vec![Slot::Args, Slot::Kwargs, Slot::Optional("on")],
        ),
        "Window" => {
            // NULL placement of `order_by` defaults to last (§9.4).
            let flag = |key: &str, default: bool| {
                dict.get_item(key)
                    .ok()
                    .flatten()
                    .and_then(|v| v.extract::<bool>().ok())
                    .unwrap_or(default)
            };
            Node::Inner(
                Shell::Window {
                    descending: flag("descending", false),
                    nulls_last: flag("nulls_last", true),
                },
                // The inner expression (e.g., row_number(), rank(), etc.)
                vec![
                    Slot::Required("expr"),
                    Slot::Optional("partition_by"),
                    Slot::Optional("order_by"),
                ],
            )
        }
        "Alias" => Node::Inner(
            Shell::Alias {
                alias: required_str(dict, "alias")?,
            },
            vec![Slot::Required("expr")],
        ),
        _ => return Err(PyExprError::UnknownVariant(expr_type)),
    })
}

/// A node whose children are being deserialized.
struct Open<'py> {
    dict: Bound<'py, PyDict>,
    shell: Shell,
    /// The slots still to read; the next is last.
    pending: Vec<Slot<'py>>,
    parsed: Vec<Option<PyExpr>>,
}

impl<'py> Open<'py> {
    fn new(dict: Bound<'py, PyDict>, shell: Shell, slots: Vec<Slot<'static>>) -> Self {
        Self {
            dict,
            shell,
            pending: slots.into_iter().rev().collect(),
            parsed: Vec::new(),
        }
    }

    /// The next child, looked up now, or `None` once every slot is read.
    fn next_child(&mut self) -> Result<Option<Child<'py>>, PyExprError> {
        while let Some(slot) = self.pending.pop() {
            match slot {
                Slot::Required(key) => return required_child(&self.dict, key).map(Some),
                Slot::Optional(key) => return optional_child(&self.dict, key).map(Some),
                Slot::Args => {
                    let args = required(&self.dict, "args")?;
                    let list = args
                        .cast::<pyo3::types::PyList>()
                        .map_err(|_| PyExprError::InvalidType("args must be a list".to_string()))?;
                    if let Shell::Call { args, .. } = &mut self.shell {
                        *args = list.len();
                    }
                    let items: Vec<_> = list.iter().map(Slot::Arg).collect();
                    self.pending.extend(items.into_iter().rev());
                }
                Slot::Arg(item) => {
                    return item
                        .cast_into::<PyDict>()
                        .map(|d| Some(Child::Expr(d)))
                        .map_err(|_| {
                            PyExprError::InvalidType("args items must be dicts".to_string())
                        })
                }
                Slot::Kwargs => {
                    let kwargs = required(&self.dict, "kwargs")?;
                    let dict = kwargs.cast::<PyDict>().map_err(|_| {
                        PyExprError::InvalidType("kwargs must be a dict".to_string())
                    })?;
                    let items: Vec<_> = dict.iter().map(|(k, v)| Slot::Kwarg(k, v)).collect();
                    self.pending.extend(items.into_iter().rev());
                }
                Slot::Kwarg(key, value) => {
                    let name = key.extract::<String>().map_err(|_| {
                        PyExprError::InvalidType("kwargs keys must be strings".to_string())
                    })?;
                    if let Shell::Call { kwargs, .. } = &mut self.shell {
                        kwargs.push(name);
                    }
                    return value
                        .cast_into::<PyDict>()
                        .map(|d| Some(Child::Expr(d)))
                        .map_err(|_| {
                            PyExprError::InvalidType("kwargs values must be dicts".to_string())
                        });
                }
            }
        }
        Ok(None)
    }
}

/// Deserialize a Python dict to PyExpr.
///
/// The walk keeps the open nodes in a vector rather than on the call stack,
/// since it reads Python objects and so cannot grow the stack
/// (`crate::stack`). It refuses an expression deeper than
/// [`MAX_EXPR_DEPTH`].
pub fn dict_to_py_expr(dict: &Bound<'_, PyDict>) -> Result<PyExpr, PyExprError> {
    dict_to_py_expr_within(dict, MAX_EXPR_DEPTH)
}

/// [`dict_to_py_expr`] for an expression nested in another, which leaves it
/// `levels` of the [`MAX_EXPR_DEPTH`] levels.
pub fn dict_to_py_expr_within(
    dict: &Bound<'_, PyDict>,
    levels: usize,
) -> Result<PyExpr, PyExprError> {
    let mut open = Vec::new();
    let mut depth = 0;
    let parsed = walk(dict, levels, &mut open, &mut depth);
    // Counting the levels above it, for an expression nested in another.
    let depth = MAX_EXPR_DEPTH.saturating_sub(levels) + depth;
    if parsed.is_ok() {
        crate::stack::saw_depth(depth);
    }
    // Drop what an error leaves parsed on a stack deep enough for it.
    let partial: Vec<_> = open.drain(..).map(|node| node.parsed).collect();
    if !partial.is_empty() {
        crate::stack::guarded_for(depth, move || drop(partial));
    }
    parsed
}

fn walk<'py>(
    dict: &Bound<'py, PyDict>,
    levels: usize,
    open: &mut Vec<Open<'py>>,
    depth: &mut usize,
) -> Result<PyExpr, PyExprError> {
    let mut next = dict.clone();
    loop {
        if open.len() >= levels {
            return Err(PyExprError::TooDeep(MAX_EXPR_DEPTH));
        }
        *depth = (*depth).max(open.len() + 1);
        let mut done = match read_node(&next)? {
            Node::Leaf(expr) => Some(expr),
            Node::Inner(shell, slots) => {
                open.push(Open::new(next, shell, slots));
                None
            }
        };
        // Hand the finished node to its parent, and find the next child to
        // read, closing each node whose children are all parsed.
        loop {
            let Some(top) = open.last_mut() else {
                return done
                    .ok_or_else(|| PyExprError::InvalidType("expression has no root".to_string()));
            };
            if let Some(expr) = done.take() {
                top.parsed.push(Some(expr));
            }
            match top.next_child()? {
                Some(Child::Expr(child)) => {
                    next = child;
                    break;
                }
                Some(Child::Absent) => top.parsed.push(None),
                None => {
                    if let Some(node) = open.pop() {
                        done = Some(node.shell.build(node.parsed)?);
                    }
                }
            }
        }
    }
}

// The Python-facing decoder needs an interpreter and is covered by
// py-ltseq/tests/test_literal_protocol.py; these pin the pure helpers.
#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(value: i128, precision: u8, scale: i8) -> LiteralValue {
        LiteralValue::Decimal128 {
            value,
            precision,
            scale,
        }
    }

    #[test]
    fn decimal_text_table() {
        let table = [
            (0, 0, "0"),
            (0, 5, "0.00000"),
            (150, 2, "1.50"),
            (-5, 2, "-0.05"),
            (1, 38, "0.00000000000000000000000000000000000001"),
            (-12345678, 3, "-12345.678"),
            (i128::MAX, 0, "170141183460469231731687303715884105727"),
        ];
        for (value, scale, expected) in table {
            assert_eq!(decimal_text(value, scale), expected, "{value} scale {scale}");
        }
    }

    /// A `require_*` result with its error as the message Python sees.
    fn msg<T>(result: Result<T, LtseqError>) -> Result<T, String> {
        result.map_err(|e| e.to_string())
    }

    #[test]
    fn require_i64_takes_integers_and_integral_decimals() {
        assert_eq!(msg(LiteralValue::Int64(-3).require_i64("k")), Ok(-3));
        assert_eq!(msg(decimal(30, 2, 1).require_i64("k")), Ok(3));
        assert_eq!(msg(decimal(-300, 3, 2).require_i64("k")), Ok(-3));
        let table = [
            (decimal(15, 2, 1), "k must be an integer, got a Decimal128 literal 1.5"),
            (
                decimal(i128::from(i64::MAX) + 1, 19, 0),
                "k must be an integer, got a Decimal128 literal 9223372036854775808",
            ),
            (LiteralValue::Float64(2.0), "k must be an integer, got a Float64 literal 2.0"),
            (
                LiteralValue::String("1".to_string()),
                "k must be an integer, got a String literal '1'",
            ),
            (LiteralValue::Boolean(true), "k must be an integer, got a Boolean literal True"),
            (LiteralValue::Null, "k must be an integer, got a Null literal"),
        ];
        for (value, expected) in table {
            assert_eq!(msg(value.require_i64("k")), Err(expected.to_string()), "{value:?}");
        }
    }

    #[test]
    fn require_f64_takes_every_number_kind() {
        assert_eq!(msg(LiteralValue::Int64(2).require_f64("p")), Ok(2.0));
        assert_eq!(msg(LiteralValue::Float64(0.5).require_f64("p")), Ok(0.5));
        assert_eq!(msg(decimal(95, 2, 2).require_f64("p")), Ok(0.95));
        // Correctly rounded, like Python's float(Decimal("0.1")).
        assert_eq!(msg(decimal(1, 1, 1).require_f64("p")), Ok(0.1));
        assert_eq!(
            msg(LiteralValue::String("0.5".to_string()).require_f64("p")),
            Err("p must be a number, got a String literal '0.5'".to_string())
        );
        assert_eq!(
            msg(LiteralValue::Date32(0).require_f64("p")),
            Err("p must be a number, got a Date32 literal (day 0)".to_string())
        );
    }

    #[test]
    fn require_str_takes_only_strings() {
        assert_eq!(msg(LiteralValue::String("day".to_string()).require_str("unit")), Ok("day"));
        assert_eq!(
            msg(LiteralValue::Int64(5).require_str("unit")),
            Err("unit must be a string, got an Int64 literal 5".to_string())
        );
    }

    #[test]
    fn arg_separates_absent_literal_and_expression() {
        let args = vec![
            PyExpr::Literal(LiteralValue::Int64(1)),
            PyExpr::Column("x".to_string()),
        ];
        assert!(matches!(arg(&args, 0), Arg::Literal(LiteralValue::Int64(1))));
        assert!(matches!(arg(&args, 1), Arg::Expr(PyExpr::Column(_))));
        assert!(matches!(arg(&args, 2), Arg::Absent));
    }

    #[test]
    fn decimal_parts_are_checked_against_each_other() {
        assert_eq!(check_decimal(99, 2, 0), Ok(()));
        assert_eq!(check_decimal(10i128.pow(38) - 1, 38, 38), Ok(()));
        let table = [
            ((1, 0, 0), "precision 0 is outside 1..=38"),
            ((1, 39, 0), "precision 39 is outside 1..=38"),
            ((1, 2, -1), "scale -1 is outside 0..=2"),
            ((1, 2, 3), "scale 3 is outside 0..=2"),
            ((100, 2, 0), "value 100 does not fit precision 2"),
            ((-100, 2, 0), "value -100 does not fit precision 2"),
        ];
        for ((value, precision, scale), expected) in table {
            let err = check_decimal(value, precision, scale).unwrap_err();
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn time_units_and_zones() {
        assert_eq!(parse_time_unit("ns"), Ok(TimeUnit::Nanosecond));
        assert!(parse_time_unit("min").unwrap_err().contains("must be one of s, ms, us, ns"));
        for good in ["UTC", "+02:00", "-03:30", "America/New_York", "Asia/Tokyo"] {
            assert_eq!(parse_time_zone(good).as_deref(), Ok(good), "{good}");
        }
        for bad in ["", "Not/AZone", "UTC+02:00"] {
            assert!(parse_time_zone(bad).unwrap_err().contains("is not a valid time zone"), "{bad}");
        }
    }
}
