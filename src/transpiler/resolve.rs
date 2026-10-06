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
//!
//! Lowering asks about subexpressions bottom-up. Coercing the whole
//! subexpression for each question coerces a chain like `((x + 1) + 1) +
//! ...` a quadratic number of nodes, each of which walks its operands'
//! types, so planning grew cubically with depth (review finding F4 on
//! #225). [`Resolver::lowering`] remembers each lowered node with its
//! coerced form while its parent is lowered, and coercion substitutes those
//! forms instead of coercing them again: lowering coerces each node once,
//! and planning grows quadratically, as DataFusion's own analysis does.

use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
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
    /// Lowered nodes whose parent is being lowered, with their coerced forms.
    known: RefCell<Known>,
    /// Nodes coerced so far (not substituted from `known`).
    coerced: Cell<usize>,
}

impl<'a> Resolver<'a> {
    pub(crate) fn new(arrow: &'a ArrowSchema) -> Result<Self, String> {
        let schema =
            DFSchema::try_from(arrow.clone()).map_err(|e| format!("Invalid input schema: {e}"))?;
        Ok(Self {
            arrow,
            schema: Arc::new(schema),
            known: RefCell::default(),
            coerced: Cell::new(0),
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
        self.coerce(expr.clone())
            .map_or(expr, |coerced| coerced.data)
    }

    /// The analyzer's coercion of `expr`, node by node from the leaves up
    /// (`TypeCoercionRewriter`), except that a subexpression lowering
    /// remembers is replaced by its coerced form. Coercion is a function of
    /// the expression and the schema, so that is the form coercing it again
    /// would give.
    fn coerce(&self, expr: Expr) -> datafusion::common::Result<Transformed<Expr>> {
        let known = self.known.borrow();
        let mut rewriter = Coercion {
            analyzer: TypeCoercionRewriter::new(&self.schema),
            known: &known,
            substituted: false,
            coerced: &self.coerced,
        };
        expr.rewrite(&mut rewriter)
    }

    /// Lower one node: `lower` builds it, lowering its children through this
    /// method too. The node and its coerced form are remembered until its
    /// parent is lowered, which is when lowering asks about it (the type of
    /// an operand, a value, a list) and coerces the parent around it. A node
    /// lowering fails on, or DataFusion cannot coerce, is not remembered.
    pub(crate) fn lowering(
        &self,
        lower: impl FnOnce() -> Result<Expr, String>,
    ) -> Result<Expr, String> {
        let mark = self.known.borrow().len();
        let lowered = lower();
        let coerced = lowered
            .as_ref()
            .ok()
            .and_then(|expr| self.coerce(expr.clone()).ok());
        let mut known = self.known.borrow_mut();
        known.truncate(mark);
        if let (Ok(expr), Some(coerced)) = (&lowered, coerced) {
            // A coerced form that differs is asked about too: `is_in` builds
            // its IN from coerced equalities.
            if coerced.transformed {
                known.push(coerced.data.clone(), coerced.data.clone(), false);
            }
            known.push(expr.clone(), coerced.data, coerced.transformed);
        }
        lowered
    }

    /// Nodes coerced by this resolver, not counting substituted ones.
    #[cfg(test)]
    pub(crate) fn coerced_nodes(&self) -> usize {
        self.coerced.get()
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

/// The analyzer's coercion, with remembered subexpressions substituted.
struct Coercion<'r> {
    analyzer: TypeCoercionRewriter<'r>,
    known: &'r Known,
    /// The node `f_down` just replaced, which `f_up` then leaves alone.
    substituted: bool,
    coerced: &'r Cell<usize>,
}

impl TreeNodeRewriter for Coercion<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> datafusion::common::Result<Transformed<Expr>> {
        Ok(match self.known.get(&expr) {
            Some((coerced, changed)) => {
                self.substituted = true;
                Transformed::new(coerced.clone(), changed, TreeNodeRecursion::Jump)
            }
            None => Transformed::no(expr),
        })
    }

    fn f_up(&mut self, expr: Expr) -> datafusion::common::Result<Transformed<Expr>> {
        if std::mem::take(&mut self.substituted) {
            return Ok(Transformed::no(expr));
        }
        self.coerced.set(self.coerced.get() + 1);
        self.analyzer.f_up(expr)
    }
}

/// Expressions with their coerced forms (and whether coercion changed
/// them), as a stack that lowering truncates when it leaves a node, indexed
/// by hash.
#[derive(Default)]
struct Known {
    by_hash: HashMap<u64, Vec<(Expr, Expr, bool)>>,
    stack: Vec<u64>,
}

impl Known {
    fn len(&self) -> usize {
        self.stack.len()
    }

    fn push(&mut self, expr: Expr, coerced: Expr, changed: bool) {
        let hash = hash_of(&expr);
        self.by_hash
            .entry(hash)
            .or_default()
            .push((expr, coerced, changed));
        self.stack.push(hash);
    }

    fn truncate(&mut self, len: usize) {
        while self.stack.len() > len {
            let hash = self.stack.pop().expect("longer than len");
            let bucket = self.by_hash.get_mut(&hash).expect("pushed with this hash");
            bucket.pop();
            if bucket.is_empty() {
                self.by_hash.remove(&hash);
            }
        }
    }

    fn get(&self, expr: &Expr) -> Option<(&Expr, bool)> {
        if self.stack.is_empty() {
            return None;
        }
        let bucket = self.by_hash.get(&hash_of(expr))?;
        bucket
            .iter()
            .rev()
            .find(|(known, ..)| same(known, expr))
            .map(|(_, coerced, changed)| (coerced, *changed))
    }
}

fn hash_of(expr: &Expr) -> u64 {
    let mut hasher = DefaultHasher::new();
    expr.hash(&mut hasher);
    hasher.finish()
}

/// Whether `a` and `b` are the same expression. `Expr`'s equality compares
/// literals with `ScalarValue`'s, which ignores a timestamp's time zone, and
/// two literals that differ only there coerce differently; so the literals'
/// types are compared too.
fn same(a: &Expr, b: &Expr) -> bool {
    fn literal_types(expr: &Expr) -> Vec<DataType> {
        let mut types = Vec::new();
        expr.apply(|e| {
            if let Expr::Literal(value, _) = e {
                types.push(value.data_type());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("collecting literal types does not fail");
        types
    }
    a == b && literal_types(a) == literal_types(b)
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
