//! Substrait serialization for GPU query plans.
//!
//! Converts a DataFusion `LogicalPlan` to serialised Substrait protobuf bytes.
//! These bytes are stored in `GpuExecNode` and will be forwarded to the
//! C++ libcudf engine in Phase 4.

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;

/// Serialise `plan` to Substrait protobuf bytes.
///
/// Returns the raw bytes that can be deserialised by any Substrait-compatible
/// engine (in our case, the C++ libcudf engine in Phase 4).
///
/// Returns `Err` if the plan contains operators not yet supported by
/// `datafusion-substrait`. Callers should treat errors as graceful degradation
/// (fall back to CPU-only execution).
pub fn to_substrait_bytes(plan: &LogicalPlan, state: &SessionState) -> Result<Vec<u8>> {
    let plan_proto = to_substrait_plan(plan, state)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(plan_proto.encode_to_vec())
}
