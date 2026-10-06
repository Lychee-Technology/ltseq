//! Expression types come from DataFusion's own coercion.
//!
//! `Expr::get_type` on an expression that DataFusion has not coerced yet is
//! not always the type the expression executes as. A CASE reports its first
//! THEN branch's type while the analyzer later unifies every branch, and any
//! expression containing such a CASE inherits the wrong type;
//! `DataFrame::select` stores the same pre-coercion type in the projection
//! schema. DataFusion runs its analyzer only when a plan is optimized.
//!
//! [`Resolver`] applies the analyzer's expression coercion
//! (`TypeCoercionRewriter`) eagerly, against the input schema:
//!
//! - [`Resolver::data_type`] answers the type questions lowering has to ask,
//!   and [`Resolver::value_type`] the same about the values, whatever their
//!   encoding;
//! - [`Resolver::resolve`] gives the coerced expression that plan builders
//!   receive, so the schema DataFusion stores for a projection is the
//!   schema it executes.
//!
//! ltseq computes no common or widened type itself (design review of #225,
//! decision D-a). Nothing outside this module calls `get_type` on a lowered
//! expression.
//!
//! [`Resolver::literal`] decides what counts as a literal: a literal node,
//! or a constant expression DataFusion's simplifier folds into one, so the
//! literal rules see the values DataFusion will.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, ExprSchemable, Volatility};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::scalar::ScalarValue;

/// The input schema of one transpilation, and DataFusion's coercion over it.
pub(crate) struct Resolver<'a> {
    arrow: &'a ArrowSchema,
    schema: DFSchemaRef,
}

impl<'a> Resolver<'a> {
    pub(crate) fn new(arrow: &'a ArrowSchema) -> Result<Self, String> {
        let schema =
            DFSchema::try_from(arrow.clone()).map_err(|e| format!("Invalid input schema: {e}"))?;
        Ok(Self {
            arrow,
            schema: Arc::new(schema),
        })
    }

    pub(crate) fn arrow(&self) -> &'a ArrowSchema {
        self.arrow
    }

    /// `expr` coerced the way DataFusion's analyzer will coerce it, so its
    /// `get_type` is the type it executes as. Coercion is idempotent, so
    /// resolving a resolved expression changes nothing. An expression
    /// DataFusion cannot coerce is returned unchanged: DataFusion then
    /// reports the problem where it always has, when the plan is built or run.
    pub(crate) fn resolve(&self, expr: Expr) -> Expr {
        let mut rewriter = TypeCoercionRewriter::new(&self.schema);
        match expr.clone().rewrite(&mut rewriter) {
            Ok(transformed) => transformed.data,
            Err(_) => expr,
        }
    }

    /// The type `expr` executes as.
    pub(crate) fn data_type(&self, expr: &Expr) -> Result<DataType, String> {
        self.resolve(expr.clone())
            .get_type(&self.schema)
            .map_err(|e| e.to_string())
    }

    /// The type of the values `expr` executes as: [`Resolver::data_type`]
    /// without dictionary or run-end encoding. DataFusion coerces an encoded
    /// operand by its value type and decodes it losslessly
    /// (`dictionary_coercion`, `ree_coercion`), so a rule about values (how a
    /// literal reads, where it falls among the operand's values, whether a
    /// cast loses anything) is a rule about this type. Encoding is storage:
    /// a dictionary column must read a literal as its decoded column does.
    pub(crate) fn value_type(&self, expr: &Expr) -> Result<DataType, String> {
        let mut data_type = self.data_type(expr)?;
        loop {
            data_type = match data_type {
                DataType::Dictionary(_, value) => *value,
                DataType::RunEndEncoded(_, values) => values.data_type().clone(),
                decoded => return Ok(decoded),
            };
        }
    }

    /// The non-null value of `expr` when it is a literal, or a constant
    /// DataFusion folds into one (`if_else(True, a, b)`, `Decimal("1.5") + 0`).
    /// A constant is an expression with no column and only immutable
    /// functions: `now()` is left alone, since DataFusion reads it when the
    /// query runs, not when it is planned.
    pub(crate) fn literal(&self, expr: &Expr) -> Option<ScalarValue> {
        if let Expr::Literal(value, _) = expr {
            return (!value.is_null()).then(|| value.clone());
        }
        let constant = !expr
            .exists(|e| {
                Ok(match e {
                    Expr::Literal(..)
                    | Expr::BinaryExpr(_)
                    | Expr::Not(_)
                    | Expr::Negative(_)
                    | Expr::IsNull(_)
                    | Expr::IsNotNull(_)
                    | Expr::Case(_)
                    | Expr::Cast(_)
                    | Expr::TryCast(_)
                    | Expr::Alias(_) => false,
                    Expr::ScalarFunction(f) => {
                        f.func.signature().volatility != Volatility::Immutable
                    }
                    _ => true,
                })
            })
            .unwrap_or(true);
        if !constant {
            return None;
        }
        let context = SimplifyContext::builder()
            .with_schema(Arc::clone(&self.schema))
            .build();
        match ExprSimplifier::new(context).simplify(self.resolve(expr.clone())) {
            Ok(Expr::Literal(value, _)) if !value.is_null() => Some(value),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::functions::core::expr_fn::coalesce;
    use datafusion::logical_expr::{case, col, lit};

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("d", DataType::Decimal128(5, 2), true),
            Field::new("s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ])
    }

    fn when(then: &str, otherwise: &str) -> Expr {
        case(col("b"))
            .when(lit(true), col(then))
            .otherwise(col(otherwise))
            .unwrap()
    }

    #[test]
    fn a_case_is_typed_by_every_branch() {
        let arrow = schema();
        let rx = Resolver::new(&arrow).unwrap();
        // Uncoerced, DataFusion reports the first THEN branch's type.
        let df_schema = DFSchema::try_from(arrow.clone()).unwrap();
        assert_eq!(
            when("s", "us").get_type(&df_schema).unwrap(),
            DataType::Timestamp(TimeUnit::Second, None)
        );
        let cases = [
            (
                when("s", "us"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (when("i", "f"), DataType::Float64),
            (when("i", "f") + col("d"), DataType::Float64),
            (
                coalesce(vec![when("s", "us"), col("s")]),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        ];
        for (expr, expected) in cases {
            assert_eq!(rx.data_type(&expr).unwrap(), expected, "{expr}");
            assert_eq!(rx.resolve(expr).get_type(&df_schema).unwrap(), expected);
        }
    }

    #[test]
    fn a_value_type_has_no_encoding() {
        let decimal = DataType::Decimal128(38, 10);
        let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(decimal.clone()));
        let run_end = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", decimal.clone(), true)),
        );
        let arrow = ArrowSchema::new(vec![
            Field::new("dictionary", dictionary.clone(), true),
            Field::new("run_end", run_end, true),
        ]);
        let rx = Resolver::new(&arrow).unwrap();
        assert_eq!(rx.data_type(&col("dictionary")).unwrap(), dictionary);
        assert_eq!(rx.value_type(&col("dictionary")).unwrap(), decimal);
        assert_eq!(rx.value_type(&col("run_end")).unwrap(), decimal);
    }

    #[test]
    fn resolving_is_idempotent() {
        let arrow = schema();
        let rx = Resolver::new(&arrow).unwrap();
        let once = rx.resolve(when("i", "f") + col("d"));
        assert_eq!(rx.resolve(once.clone()), once);
    }

    #[test]
    fn a_constant_counts_as_the_literal_datafusion_folds_it_to() {
        use datafusion::functions::datetime::expr_fn::now;
        let arrow = schema();
        let rx = Resolver::new(&arrow).unwrap();
        let constant = case(lit(true))
            .when(lit(true), lit(2_i64))
            .otherwise(lit(3_i64))
            .unwrap();
        assert_eq!(rx.literal(&constant), Some(ScalarValue::Int64(Some(2))));
        assert_eq!(
            rx.literal(&(lit(1_i64) + lit(2_i64))),
            Some(ScalarValue::Int64(Some(3)))
        );
        assert_eq!(rx.literal(&lit(ScalarValue::Int64(None))), None);
        assert_eq!(rx.literal(&(col("i") + lit(1_i64))), None);
        // `now()` is read when the query runs, not when it is planned.
        assert_eq!(rx.literal(&now()), None);
    }

    #[test]
    fn an_expression_datafusion_cannot_coerce_is_left_for_datafusion_to_report() {
        let arrow = schema();
        let rx = Resolver::new(&arrow).unwrap();
        let expr = col("b") + col("s");
        assert_eq!(rx.resolve(expr.clone()), expr);
        assert!(rx.data_type(&expr).is_err());
    }
}
