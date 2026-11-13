//! Pipeline diagnostics (#238): what the kernel planned for a table, as data.
//!
//! `explain_plan()` returns DataFusion's plans as text, and tests that ask
//! "how many sorts did this plan get?" used to search that text. This module
//! reports the same planning as structured values: the column environment,
//! the declared order, both plans, and the physical operator tree walked
//! node by node, so a test can count `SortExec`s or read a scan's partition
//! count without a regex.
//!
//! Two rules hold for everything here:
//!
//! - **Nothing executes.** Like `explain_plan()`, the work stops at physical
//!   planning (which may list files and read Parquet footers), and runs
//!   detached from the GIL.
//! - **Nothing is re-derived.** Every field reads the plan the table's
//!   terminals execute (`ordered_df`), planned once; the operator tree and
//!   the physical-plan text come from the same `ExecutionPlan`.
//!
//! The result is private (`LTSeq._explain_pipeline()`). The pipeline compiler
//! of #229 adds its own fields (IR, lowering decision, kernel eligibility,
//! decline reason) as it lands; ADR 0019 records that design.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;

/// One node of a physical plan, in the pre-order walk `physical_operators`
/// produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhysicalOperator {
    /// `ExecutionPlan::name()`, e.g. `SortExec`, `DataSourceExec`.
    pub name: String,
    /// Distance from the root, which is 0.
    pub depth: usize,
    /// The number of partitions the operator outputs. For a file scan this
    /// is its number of file groups.
    pub partitions: usize,
}

/// Walk a physical plan root first, children left to right.
pub(crate) fn physical_operators(plan: &Arc<dyn ExecutionPlan>) -> Vec<PhysicalOperator> {
    fn walk(node: &Arc<dyn ExecutionPlan>, depth: usize, out: &mut Vec<PhysicalOperator>) {
        out.push(PhysicalOperator {
            name: node.name().to_string(),
            depth,
            partitions: node.output_partitioning().partition_count(),
        });
        for child in node.children() {
            walk(child, depth + 1, out);
        }
    }
    let mut out = Vec::new();
    walk(plan, 0, &mut out);
    out
}

/// How many times each operator occurs in a plan, keyed by operator name.
pub(crate) fn operator_counts(operators: &[PhysicalOperator]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for op in operators {
        *counts.entry(op.name.clone()).or_insert(0) += 1;
    }
    counts
}

/// A column of the table's schema: name, LTSeq type string, nullability.
#[derive(Debug, Clone)]
pub(crate) struct ColumnInfo {
    pub name: String,
    pub dtype: String,
    pub nullable: bool,
}

/// What `_explain_pipeline()` reports for a table.
#[derive(Debug, Clone)]
pub(crate) struct PipelineExplanation {
    pub columns: Vec<ColumnInfo>,
    /// The declared order, as `get_sort_keys()` reports it.
    pub sort_keys: Vec<(String, bool)>,
    pub logical_plan: String,
    pub physical_plan: String,
    pub operators: Vec<PhysicalOperator>,
}

/// Plan the table as its terminals would, without executing it.
///
/// Returns `LtseqError` so it can run inside `gil::detached`.
pub(crate) fn explain_pipeline_impl(table: &LTSeqTable) -> Result<PipelineExplanation, LtseqError> {
    let schema = table.require_schema()?;
    let columns = schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name().to_string(),
            dtype: crate::datatype_to_schema_string(f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    let sort_keys = table
        .sort_specs
        .iter()
        .map(|s| (s.column.clone(), s.descending))
        .collect();

    let df = table.ordered_df()?;
    let logical = df
        .clone()
        .into_optimized_plan()
        .map_err(|e| LtseqError::with_context("Failed to optimize logical plan", e))?;
    let logical_plan = format!("{}", logical.display_indent());

    let plan = RUNTIME
        .block_on(df.create_physical_plan())
        .map_err(|e| LtseqError::with_context("Failed to create physical plan", e))?;
    let physical_plan = displayable(plan.as_ref()).indent(true).to_string();
    let operators = physical_operators(&plan);

    Ok(PipelineExplanation {
        columns,
        sort_keys,
        logical_plan,
        physical_plan,
        operators,
    })
}

impl PipelineExplanation {
    /// Convert to the Python dict `_explain_pipeline()` returns (under the GIL).
    pub(crate) fn into_py_dict(self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);

        let columns = PyList::empty(py);
        for c in &self.columns {
            let col = PyDict::new(py);
            col.set_item("name", &c.name)?;
            col.set_item("dtype", &c.dtype)?;
            col.set_item("nullable", c.nullable)?;
            columns.append(col)?;
        }
        dict.set_item("columns", columns)?;
        dict.set_item("sort_keys", &self.sort_keys)?;
        dict.set_item("logical_plan", &self.logical_plan)?;
        dict.set_item("physical_plan", &self.physical_plan)?;

        let operators = PyList::empty(py);
        for op in &self.operators {
            let node = PyDict::new(py);
            node.set_item("name", &op.name)?;
            node.set_item("depth", op.depth)?;
            node.set_item("partitions", op.partitions)?;
            operators.append(node)?;
        }
        dict.set_item("operators", operators)?;
        dict.set_item("operator_counts", operator_counts(&self.operators))?;

        Ok(dict.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::create_session_context;
    use crate::metadata::SortSpec;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn table(sort_specs: Vec<SortSpec>) -> LTSeqTable {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
        )
        .expect("batch");
        LTSeqTable::from_batches(create_session_context(), vec![batch], schema, sort_specs)
            .expect("table")
    }

    #[test]
    fn operator_walk_is_pre_order_and_counts_match_it() {
        let t = table(Vec::new());
        let df = (**t.require_df().expect("plan")).clone();
        let sorted = df
            .sort(vec![datafusion::prelude::col("x").sort(true, false)])
            .expect("sort");
        let plan = RUNTIME
            .block_on(sorted.create_physical_plan())
            .expect("physical plan");

        let ops = physical_operators(&plan);
        assert_eq!(ops[0].depth, 0);
        assert_eq!(ops[0].name, plan.name());
        // Each child sits one level below some earlier node.
        for (i, op) in ops.iter().enumerate().skip(1) {
            assert!(ops[..i].iter().any(|p| p.depth + 1 == op.depth), "{ops:?}");
        }
        let counts = operator_counts(&ops);
        assert_eq!(counts.values().sum::<usize>(), ops.len());
        assert_eq!(counts.get("SortExec"), Some(&1), "{ops:?}");
    }

    #[test]
    fn explanation_reads_the_plan_terminals_execute() {
        // A MemTable declared in order needs no sort at the root.
        let t = table(vec![SortSpec::new("x".to_string(), false, true, &datafusion::arrow::datatypes::DataType::Int64)]);
        let e = explain_pipeline_impl(&t).expect("explain");
        assert_eq!(e.sort_keys, vec![("x".to_string(), false)]);
        assert_eq!(e.columns.len(), 1);
        assert_eq!(e.columns[0].name, "x");
        assert_eq!(e.columns[0].dtype, "int64");
        assert!(e.columns[0].nullable);
        assert!(e.physical_plan.contains(&e.operators[0].name));
        assert_eq!(operator_counts(&e.operators).get("SortExec"), None);
    }

    #[test]
    fn a_table_without_a_plan_is_reported_as_such() {
        let t = LTSeqTable::unloaded(create_session_context());
        assert!(explain_pipeline_impl(&t).is_err());
    }
}
