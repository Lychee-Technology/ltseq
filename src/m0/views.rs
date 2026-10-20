//! m6: `string_view` normalized to `string` (M18).
//!
//! DataFusion 55's Parquet scan decodes strings and binaries as views
//! (`schema_force_view_types`), and the contract shows plain types wherever
//! a schema is visible. `Views::Plain` turns the option off, so the scan
//! decodes plain arrays; `Views::Cast` keeps the views through the plan and
//! casts at its root, which is what a terminal would do.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::physical_expr::expressions::{cast, col};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

/// Cast every view column of the plan's output to its plain type.
#[derive(Debug)]
pub(crate) struct CastViewsAtRoot;

fn plain(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Utf8View => Some(DataType::Utf8),
        DataType::BinaryView => Some(DataType::Binary),
        _ => None,
    }
}

impl PhysicalOptimizerRule for CastViewsAtRoot {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema();
        if !schema
            .fields()
            .iter()
            .any(|f| plain(f.data_type()).is_some())
        {
            return Ok(plan);
        }
        let exprs = schema
            .fields()
            .iter()
            .map(|field| {
                let column = col(field.name(), &schema)?;
                let expr: Arc<dyn PhysicalExpr> = match plain(field.data_type()) {
                    Some(target) => cast(column, &schema, target)?,
                    None => column,
                };
                Ok((expr, field.name().clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    fn name(&self) -> &str {
        "ltseq_m0_cast_views_at_root"
    }

    fn schema_check(&self) -> bool {
        // The rule changes the output types on purpose.
        false
    }
}
