//! Window aggregates that claim no order of their output (#354).
//!
//! An aggregate's `set_monotonicity` tells DataFusion that its value only
//! rises (`max`, `count`, unsigned `sum`) or only falls (`min`) as values
//! join its set. DataFusion 55's `window_equivalence_properties` turns that
//! into an ordering of the window column, and `EnforceSorting` then drops a
//! later sort the claimed ordering satisfies. The claims it derives are wrong
//! for the windows LTSeq builds:
//!
//! - A sliding frame (`rolling(n)`) with no `partition_by` is claimed `ASC
//!   NULLS LAST` for an increasing aggregate (`DESC NULLS LAST` for `min`)
//!   whenever the argument is sorted, in either direction. A rolling `max`
//!   over a descending argument falls, and a rolling `count` falls once NULLs
//!   enter the frame, so `sort("m")` came back in the window's order.
//! - A running frame (`cum_max`, `cum_min`, `cum_sum`) is claimed `ASC NULLS
//!   LAST` for an increasing aggregate, but its value is NULL until the first
//!   non-NULL argument, so those NULL rows lead the column
//!   (apache/datafusion#27267).
//! - `sum` adds unsigned integers with wrapping, so a running or sliding
//!   unsigned sum falls when it overflows.
//!
//! `set_monotonicity` sees only the argument's type, not the frame or the
//! values, so it cannot tell these windows from the ones its claim holds for.
//! [`claiming_no_order`] wraps a window's aggregate so that it claims
//! nothing, and [`super::window_native::aggregate_to_window`], which builds
//! every window aggregate LTSeq plans, applies it. The cost is the sort a
//! running `max` or `min` of a non-NULL column made unnecessary; a sort by a
//! window column now always runs. Set monotonicity feeds nothing but these
//! window orderings in DataFusion 55, so grouped and ungrouped aggregates do
//! not change. Everything else is the wrapped aggregate's own: its name,
//! types, accumulators, statistics and rewrites.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr::{
    AggregateFunction, AggregateFunctionParams, WindowFunctionParams,
};
use datafusion::logical_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs,
};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Expr, GroupsAccumulator, Operator,
    ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
};

/// `udaf` as an aggregate that claims no order of a window's output.
pub(crate) fn claiming_no_order(udaf: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
    if udaf.inner().is::<NoOrderClaim>() {
        return udaf;
    }
    Arc::new(AggregateUDF::new_from_impl(NoOrderClaim { inner: udaf }))
}

/// An aggregate that is `inner` in every respect but its set monotonicity.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NoOrderClaim {
    inner: Arc<AggregateUDF>,
}

impl NoOrderClaim {
    fn inner(&self) -> &dyn AggregateUDFImpl {
        self.inner.inner().as_ref()
    }
}

impl AggregateUDFImpl for NoOrderClaim {
    /// The one difference from the wrapped aggregate (see the module docs).
    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {
        SetMonotonicity::NotMonotonic
    }

    fn name(&self) -> &str {
        self.inner().name()
    }

    fn aliases(&self) -> &[String] {
        self.inner().aliases()
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner().schema_name(params)
    }

    fn human_display(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner().human_display(params)
    }

    fn window_function_schema_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner().window_function_schema_name(params)
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> Result<String> {
        self.inner().display_name(params)
    }

    fn window_function_display_name(&self, params: &WindowFunctionParams) -> Result<String> {
        self.inner().window_function_display_name(params)
    }

    fn signature(&self) -> &Signature {
        self.inner().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner().return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        self.inner().return_field(arg_fields)
    }

    fn is_nullable(&self) -> bool {
        self.inner().is_nullable()
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner().accumulator(args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner().state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner().create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner().create_sliding_accumulator(args)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        let rewritten =
            Arc::clone(self.inner.inner()).with_beneficial_ordering(beneficial_ordering)?;
        Ok(rewritten.map(|inner| {
            Arc::new(NoOrderClaim {
                inner: Arc::new(AggregateUDF::new_from_shared_impl(inner)),
            }) as Arc<dyn AggregateUDFImpl>
        }))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner().order_sensitivity()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner().simplify()
    }

    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        arg_is_left: bool,
    ) -> Result<Option<Expr>> {
        self.inner()
            .simplify_expr_op_literal(agg_function, arg, op, lit, arg_is_left)
    }

    /// A reversed window keeps claiming nothing.
    fn reverse_expr(&self) -> ReversedUDAF {
        match self.inner().reverse_expr() {
            ReversedUDAF::Reversed(reversed) => ReversedUDAF::Reversed(claiming_no_order(reversed)),
            other => other,
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner().coerce_types(arg_types)
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner().is_descending()
    }

    fn value_from_stats(&self, args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner().value_from_stats(args)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner().default_value(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        self.inner().supports_null_handling_clause()
    }

    fn supports_within_group_clause(&self) -> bool {
        self.inner().supports_within_group_clause()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner().documentation()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::sum::sum_udaf;

    use crate::transpiler::min_max;

    fn udaf(expr: Expr) -> Arc<AggregateUDF> {
        match expr {
            Expr::AggregateFunction(AggregateFunction { func, .. }) => func,
            other => panic!("not an aggregate: {other:?}"),
        }
    }

    #[test]
    fn claims_nothing_and_keeps_the_rest() {
        let col = || datafusion::prelude::col("x");
        let cases = [
            (udaf(min_max::max(col())), DataType::Int64),
            (udaf(min_max::min(col())), DataType::Utf8),
            (count_udaf(), DataType::Int64),
            (sum_udaf(), DataType::UInt64),
        ];
        for (inner, data_type) in cases {
            assert_ne!(
                inner.inner().set_monotonicity(&data_type),
                SetMonotonicity::NotMonotonic,
                "{} claims an order of {data_type}",
                inner.name()
            );
            let wrapped = claiming_no_order(Arc::clone(&inner));
            assert_eq!(
                wrapped.inner().set_monotonicity(&data_type),
                SetMonotonicity::NotMonotonic
            );
            assert_eq!(wrapped.name(), inner.name());
            assert_eq!(wrapped.signature(), inner.signature());
            assert_eq!(wrapped.is_nullable(), inner.is_nullable());
            assert_eq!(
                wrapped.return_type(std::slice::from_ref(&data_type)).ok(),
                inner.return_type(std::slice::from_ref(&data_type)).ok()
            );
            assert!(matches!(wrapped.reverse_udf(), ReversedUDAF::Identical));
            // Wrapping twice is wrapping once.
            assert_eq!(claiming_no_order(Arc::clone(&wrapped)), wrapped);
        }
    }
}
