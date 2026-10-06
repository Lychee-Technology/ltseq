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
//! - [`Resolver::data_type`] answers the type questions lowering has to ask;
//! - [`Resolver::resolve`] gives the coerced expression that plan builders
//!   receive, so the schema DataFusion stores for a projection is the
//!   schema it executes.
//!
//! ltseq computes no common or widened type itself (design review of #225,
//! decision D-a). Nothing outside this module calls `get_type` on a lowered
//! expression.

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;

/// The input schema of one transpilation, and DataFusion's coercion over it.
pub(crate) struct Resolver<'a> {
    arrow: &'a ArrowSchema,
    schema: DFSchema,
}

impl<'a> Resolver<'a> {
    pub(crate) fn new(arrow: &'a ArrowSchema) -> Result<Self, String> {
        let schema =
            DFSchema::try_from(arrow.clone()).map_err(|e| format!("Invalid input schema: {e}"))?;
        Ok(Self { arrow, schema })
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
    fn resolving_is_idempotent() {
        let arrow = schema();
        let rx = Resolver::new(&arrow).unwrap();
        let once = rx.resolve(when("i", "f") + col("d"));
        assert_eq!(rx.resolve(once.clone()), once);
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
