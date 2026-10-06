//! Aggregation operations for LTSeqTable
//!
//! Fully native DataFusion `df.aggregate()` path (issue #91): the plan stays
//! lazy — no collect, no MemTable, no SQL string round-trip. Aggregates that
//! SQL would express as scalar-over-aggregate (top_k, skew) are planned in
//! two stages: hidden aggregate parts in the Aggregate node, combined by a
//! post-aggregation projection.
//!
//! `filter_where_impl` keeps a `session.sql()` call by design, isolated in
//! `parse_where_clause`: it uses the SQL engine as a WHERE-clause parser
//! against an empty table (allowlisted in issue #91 — no data ever
//! round-trips).

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::types::{arg, dict_to_py_expr, Arg, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Column;
use datafusion::functions_aggregate::expr_fn as agg_fn;
use datafusion::logical_expr::{case, Expr, ExprFunctionExt, SortExpr};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

/// How one requested aggregate is planned.
enum AggPlan {
    /// A single aggregate expression (aliased with the output key by the caller).
    Plain(Expr),
    /// Aggregate parts computed under hidden aliases in the Aggregate node,
    /// combined into the final value by a post-aggregation projection.
    Staged {
        parts: Vec<(String, Expr)>,
        post: Expr,
    },
}

/// Every supported aggregate maps to a native plan — there is no fallback.
fn pyexpr_to_agg_plan(
    py_expr: &PyExpr,
    schema: &ArrowSchema,
    out_key: &str,
) -> Result<AggPlan, String> {
    let PyExpr::Call { func, on, args, .. } = py_expr else {
        return Err("Expected aggregate function call".to_string());
    };

    match func.as_str() {
        // Simple aggregations on a column
        "sum" | "count" | "min" | "max" | "avg" | "mean" | "median" | "variance" | "var"
        | "stddev" | "std" => {
            let col_expr = match on.as_deref() {
                Some(PyExpr::Column(col_name)) => {
                    Expr::Column(Column::new_unqualified(col_name))
                }
                _ if func == "count" => {
                    // count() without a specific column → count(*)
                    lit(1i64)
                }
                _ => return Err(format!("Aggregate '{}' requires a column reference", func)),
            };

            let agg_expr = match func.as_str() {
                "sum" => agg_fn::sum(col_expr),
                "count" => agg_fn::count(col_expr),
                "min" => agg_fn::min(col_expr),
                "max" => agg_fn::max(col_expr),
                "avg" | "mean" => agg_fn::avg(col_expr),
                "median" => agg_fn::median(col_expr),
                "variance" | "var" => agg_fn::var_sample(col_expr),
                "stddev" | "std" => agg_fn::stddev(col_expr),
                _ => unreachable!(),
            };
            Ok(AggPlan::Plain(agg_expr))
        }
        "percentile" => {
            let col_expr = match on.as_deref() {
                Some(PyExpr::Column(col_name)) => {
                    Expr::Column(Column::new_unqualified(col_name))
                }
                _ => return Err("percentile requires a column reference".to_string()),
            };
            // The default applies only when p is absent; a supplied argument
            // that is not a number in [0, 1] is an error, never the median.
            let p = match arg(args, 0) {
                Arg::Absent => 0.5,
                Arg::Literal(value) => value.require_f64("percentile() p")?,
                Arg::Expr(_) => return Err("percentile() p must be a literal number".to_string()),
            };
            if !(0.0..=1.0).contains(&p) {
                return Err(format!("percentile() p must be between 0 and 1, got {p}"));
            }
            let sort = datafusion::logical_expr::SortExpr::new(col_expr, true, false);
            Ok(AggPlan::Plain(agg_fn::approx_percentile_cont(sort, lit(p), None)))
        }
        // Conditional aggregations
        "count_if" => {
            let predicate = args.first().ok_or("count_if requires a predicate argument")?;
            let pred_expr = crate::transpiler::pyexpr_to_datafusion(predicate.clone(), schema)?;
            // count_if(cond) → SUM(CASE WHEN cond THEN 1 ELSE 0 END)
            let case_expr = case(pred_expr)
                .when(lit(true), lit(1i64))
                .otherwise(lit(0i64))
                .map_err(|e| format!("Failed to create CASE: {}", e))?;
            Ok(AggPlan::Plain(agg_fn::sum(case_expr)))
        }
        "sum_if" | "avg_if" | "min_if" | "max_if" => {
            if args.len() < 2 {
                return Err(format!("{} requires predicate and column arguments", func));
            }
            let pred_expr = crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?;
            let col_expr = crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?;

            let (true_val, false_val) = match func.as_str() {
                "sum_if" => (col_expr.clone(), lit(0i64)),
                _ => (col_expr.clone(), lit(ScalarValue::Null)),
            };

            let case_expr = case(pred_expr)
                .when(lit(true), true_val)
                .otherwise(false_val)
                .map_err(|e| format!("Failed to create CASE: {}", e))?;

            let agg_expr = match func.as_str() {
                "sum_if" => agg_fn::sum(case_expr),
                "avg_if" => agg_fn::avg(case_expr),
                "min_if" => agg_fn::min(case_expr),
                "max_if" => agg_fn::max(case_expr),
                _ => unreachable!(),
            };
            Ok(AggPlan::Plain(agg_expr))
        }
        // Statistical aggregates — native DataFusion path
        "corr" => {
            // corr(col_a, col_b) — both come from args for a standalone call
            let (col_a, col_b) = if let Some(on) = on {
                if args.is_empty() {
                    return Err("corr requires a second column argument".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion((**on).clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                )
            } else {
                if args.len() < 2 {
                    return Err("corr requires two column arguments".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?,
                )
            };
            Ok(AggPlan::Plain(agg_fn::corr(col_a, col_b)))
        }
        "covar" => {
            let (col_a, col_b) = if let Some(on) = on {
                if args.is_empty() {
                    return Err("covar requires a second column argument".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion((**on).clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                )
            } else {
                if args.len() < 2 {
                    return Err("covar requires two column arguments".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?,
                )
            };
            Ok(AggPlan::Plain(agg_fn::covar_samp(col_a, col_b)))
        }
        "concat_agg" => {
            // concat_agg(col, delimiter) — uses native string_agg UDAF
            let (col_expr, delim_expr) = if let Some(on) = on {
                let col = crate::transpiler::pyexpr_to_datafusion((**on).clone(), schema)?;
                let delim = if !args.is_empty() {
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?
                } else {
                    lit(",")
                };
                (col, delim)
            } else {
                if args.is_empty() {
                    return Err("concat_agg requires a column argument".to_string());
                }
                let col = crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?;
                let delim = if args.len() > 1 {
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?
                } else {
                    lit(",")
                };
                (col, delim)
            };
            use datafusion::functions_aggregate::string_agg::string_agg;
            Ok(AggPlan::Plain(string_agg(col_expr, delim_expr)))
        }
        // top_k(col, k) → ordered array_agg under a hidden alias, then
        // array_slice + array_to_string in the post-aggregation projection.
        // Output format (semicolon-joined doubles, descending) matches the
        // legacy SQL expression exactly.
        "top_k" => {
            let Some(PyExpr::Column(col_name)) = on.as_deref() else {
                return Err("top_k requires a column reference".to_string());
            };
            // The default applies only when k is absent; a supplied argument
            // that is not a positive integer is an error, never 10.
            let k = match arg(args, 0) {
                Arg::Absent => 10,
                Arg::Literal(value) => value.require_i64("top_k() k")?,
                Arg::Expr(_) => return Err("top_k() k must be a literal integer".to_string()),
            };
            if k < 1 {
                return Err(format!("top_k() k must be >= 1, got {k}"));
            }
            // array_slice takes an i32 end index; any k beyond it already
            // means "every value", so clamp instead of failing at collect.
            let k = k.min(i64::from(i32::MAX));

            let col_f64 = cast(
                Expr::Column(Column::new_unqualified(col_name)),
                DataType::Float64,
            );
            // ORDER BY col DESC (SQL default for DESC: nulls first)
            let sort = SortExpr::new(col_f64.clone(), false, true);
            let agg = agg_fn::array_agg(col_f64)
                .order_by(vec![sort])
                .build()
                .map_err(|e| format!("Failed to build top_k array_agg: {}", e))?;

            let hidden = format!("__{}_topk_arr", out_key);
            use datafusion::functions_nested::expr_fn::{array_slice, array_to_string};
            let post = array_to_string(
                array_slice(
                    Expr::Column(Column::new_unqualified(&hidden)),
                    lit(1i64),
                    lit(k),
                    None,
                ),
                lit(";"),
            );
            Ok(AggPlan::Staged {
                parts: vec![(hidden, agg)],
                post,
            })
        }
        // First/last row value within each group, ordered by an explicit order
        // column passed in args[0] (NestedTable.agg passes __rn__). "last" is
        // first_value over the reversed order.
        "first" | "last" => {
            let Some(PyExpr::Column(col_name)) = on.as_deref() else {
                return Err(format!("{} requires a column reference", func));
            };
            let col_expr = Expr::Column(Column::new_unqualified(col_name));
            let order_col = match args.first() {
                Some(PyExpr::Column(name)) if !name.is_empty() => name.clone(),
                _ => {
                    return Err(format!(
                        "{} requires an order column argument (in-group row order)",
                        func
                    ))
                }
            };
            let asc = func == "first";
            let sort = SortExpr::new(
                Expr::Column(Column::new_unqualified(&order_col)),
                asc,
                false,
            );
            Ok(AggPlan::Plain(agg_fn::first_value(col_expr, vec![sort])))
        }
        // mode keeps the legacy behavior (FIRST_VALUE ordered ascending —
        // i.e. min, not a true statistical mode). Real mode semantics are
        // deferred per issue #91 risk 3.
        "mode" => {
            let Some(PyExpr::Column(col_name)) = on.as_deref() else {
                return Err("mode requires a column reference".to_string());
            };
            let col_expr = Expr::Column(Column::new_unqualified(col_name));
            let sort = SortExpr::new(col_expr.clone(), true, false);
            let agg = agg_fn::first_value(col_expr, vec![sort]);
            Ok(AggPlan::Plain(agg))
        }
        // skew via the moment formula, composed from native aggregates over
        // hidden aliases: (E[x³] - 3·E[x²]·E[x] + 2·E[x]³) / stddev_pop(x)³.
        // Values are cast to Float64 up front to avoid integer overflow.
        "skew" => {
            // Method form g.col.skew() carries the column in `on`; the
            // exported free function skew(g.col) has no `on` and passes the
            // column in args[0].
            let col_name = match on.as_deref() {
                Some(PyExpr::Column(name)) => name.clone(),
                _ => match args.first() {
                    Some(PyExpr::Column(name)) if !name.is_empty() => name.clone(),
                    _ => return Err("skew requires a column reference".to_string()),
                },
            };
            let x = cast(
                Expr::Column(Column::new_unqualified(&col_name)),
                DataType::Float64,
            );

            let m1_name = format!("__{}_skew_m1", out_key);
            let m2_name = format!("__{}_skew_m2", out_key);
            let m3_name = format!("__{}_skew_m3", out_key);
            let sd_name = format!("__{}_skew_sd", out_key);

            let parts = vec![
                (m1_name.clone(), agg_fn::avg(x.clone())),
                (m2_name.clone(), agg_fn::avg(x.clone() * x.clone())),
                (m3_name.clone(), agg_fn::avg(x.clone() * x.clone() * x.clone())),
                (sd_name.clone(), agg_fn::stddev_pop(x)),
            ];

            let m1 = Expr::Column(Column::new_unqualified(&m1_name));
            let m2 = Expr::Column(Column::new_unqualified(&m2_name));
            let m3 = Expr::Column(Column::new_unqualified(&m3_name));
            let sd = Expr::Column(Column::new_unqualified(&sd_name));

            use datafusion::functions::expr_fn::{nullif, power};
            let numerator =
                m3 - lit(3.0) * m2 * m1.clone() + lit(2.0) * power(m1, lit(3.0));
            let post = numerator / nullif(power(sd, lit(3.0)), lit(0.0));
            Ok(AggPlan::Staged { parts, post })
        }
        _ => Err(format!("Unknown aggregate function: {}", func)),
    }
}

/// Parse group expression into DataFusion Expr(s)
fn parse_group_exprs(
    group_expr: Option<Bound<'_, PyDict>>,
    schema: &ArrowSchema,
) -> PyResult<Vec<Expr>> {
    let Some(group_expr_dict) = group_expr else {
        return Ok(Vec::new());
    };

    let py_expr = dict_to_py_expr(&group_expr_dict)
        .map_err(|e| LtseqError::Validation(format!("Failed to parse group: {}", e)))?;

    let df_expr = crate::transpiler::pyexpr_to_named_datafusion(py_expr, schema)
        .map_err(|e| LtseqError::Validation(format!("Transpile failed: {}", e)))?;

    Ok(vec![df_expr])
}

/// Aggregate rows into a summary table with one row per group.
/// Fully lazy: builds an Aggregate (plus, when needed, a post-aggregation
/// projection) on the logical plan and returns without collecting.
pub fn agg_impl(
    table: &LTSeqTable,
    group_expr: Option<Bound<'_, PyDict>>,
    agg_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Plan each requested aggregate.
    let mut agg_exprs: Vec<Expr> = Vec::new();
    // (output key, final expression over hidden columns) — empty when no
    // aggregate needs a post-aggregation projection.
    let mut staged_posts: Vec<(String, Option<Expr>)> = Vec::new();
    let mut any_staged = false;

    for (key, value) in agg_dict.iter() {
        let key_str = key
            .extract::<String>()
            .map_err(|_| LtseqError::TypeMismatch("Agg key must be string".into()))?;

        let val_dict = value
            .cast::<PyDict>()
            .map_err(|_| LtseqError::TypeMismatch("Agg value must be dict".into()))?;

        let py_expr = dict_to_py_expr(val_dict)
            .map_err(|e| LtseqError::Validation(format!("Failed to parse: {}", e)))?;

        match pyexpr_to_agg_plan(&py_expr, schema, &key_str)
            .map_err(LtseqError::Validation)?
        {
            AggPlan::Plain(expr) => {
                agg_exprs.push(expr.alias(&key_str));
                staged_posts.push((key_str, None));
            }
            AggPlan::Staged { parts, post } => {
                any_staged = true;
                for (hidden_name, part_expr) in parts {
                    agg_exprs.push(part_expr.alias(hidden_name));
                }
                staged_posts.push((key_str, Some(post)));
            }
        }
    }

    // Parse group expressions
    let group_exprs = parse_group_exprs(group_expr, schema)?;
    let n_group = group_exprs.len();

    // Build the lazy plan: Aggregate, then (only if needed) a projection that
    // combines hidden aggregate parts into the requested outputs.
    let agg_df = (**df)
        .clone()
        .aggregate(group_exprs, agg_exprs)
        .map_err(|e| LtseqError::Runtime(format!("Aggregate failed: {}", e)))?;

    let result_df = if any_staged {
        // Group columns come first in the Aggregate output schema.
        let group_cols: Vec<Expr> = agg_df
            .schema()
            .fields()
            .iter()
            .take(n_group)
            .map(|f| Expr::Column(Column::new_unqualified(f.name())))
            .collect();

        let mut select_exprs = group_cols;
        for (key, post) in staged_posts {
            match post {
                Some(post_expr) => select_exprs.push(post_expr.alias(&key)),
                None => select_exprs.push(Expr::Column(Column::new_unqualified(&key))),
            }
        }

        agg_df
            .select(select_exprs)
            .map_err(|e| LtseqError::Runtime(format!("Post-aggregation projection failed: {}", e)))?
    } else {
        agg_df
    };

    // Aggregation redefines row identity: no sort metadata, no fast-path token.
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        Vec::new(),
        None,
    ))
}

/// Drop the table qualifier from every column reference in `expr`, wherever
/// it sits in the tree.
fn strip_table_qualifiers(expr: Expr) -> datafusion::error::Result<Expr> {
    expr.transform(|e| match e {
        Expr::Column(c) if c.relation.is_some() => {
            Ok(Transformed::yes(Expr::Column(Column::new_unqualified(c.name))))
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
}

/// Filter rows using a raw SQL WHERE clause
///
/// Uses DataFusion's SQL parser to convert the WHERE clause into a native
/// expression, then applies it via DataFrame::filter() to preserve laziness.
pub fn filter_where_impl(table: &LTSeqTable, where_clause: &str) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let filter_expr =
        parse_where_clause(&table.session, schema, where_clause).map_err(LtseqError::Runtime)?;

    // Apply the filter natively — stays lazy
    let filtered_df = (**df)
        .clone()
        .filter(filter_expr)
        .map_err(|e| LtseqError::Runtime(format!("Failed to apply filter: {}", e)))?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        filtered_df,
        Arc::clone(schema),
        table.sort_specs.clone(),
        None, // row set / columns diverge from the raw file: drop fast-path token
    ))
}

/// Parse a SQL WHERE clause into a native expression over `schema`'s columns.
///
/// Plans `SELECT * FROM <empty table with this schema> WHERE <clause>`, takes
/// the Filter node's predicate, and strips the parse table's qualifier from
/// every column so the predicate applies to the original DataFrame.
fn parse_where_clause(
    session: &SessionContext,
    schema: &Arc<ArrowSchema>,
    where_clause: &str,
) -> Result<Expr, String> {
    RUNTIME.block_on(async {
        let temp_name = "__ltseq_filter_parse_tmp";
        let _ = session.deregister_table(temp_name);

        // Register an empty table with the same schema for parsing
        let empty_batch = datafusion::arrow::record_batch::RecordBatch::new_empty(Arc::clone(schema));
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(schema),
            vec![vec![empty_batch]],
        ).map_err(|e| format!("Failed to create parse table: {}", e))?;

        session
            .register_table(temp_name, Arc::new(mem_table))
            .map_err(|e| format!("Failed to register parse table: {}", e))?;

        // Parse the full SELECT to get the filter expression in context
        let parsed_df = session
            .sql(&format!("SELECT * FROM \"{}\" WHERE {}", temp_name, where_clause))
            .await
            .map_err(|e| format!("Failed to parse WHERE clause: {}", e))?;

        // Walk the logical plan to find the Filter node
        fn extract_filter_predicate(
            plan: &datafusion::logical_expr::LogicalPlan,
        ) -> Option<datafusion::logical_expr::Expr> {
            match plan {
                datafusion::logical_expr::LogicalPlan::Filter(filter) => {
                    Some(filter.predicate.clone())
                }
                datafusion::logical_expr::LogicalPlan::Projection(proj) => {
                    extract_filter_predicate(&proj.input)
                }
                _ => None,
            }
        }

        let predicate = extract_filter_predicate(parsed_df.logical_plan())
            .ok_or_else(|| format!("No filter expression found in: {}", where_clause))?;

        // Clean up temp table
        let _ = session.deregister_table(temp_name);

        // Columns parsed from SQL are qualified with the temp table name;
        // the original DataFrame needs them unqualified.
        strip_table_qualifiers(predicate)
            .map_err(|e| format!("Failed to unqualify WHERE clause columns: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::datasource::MemTable;

    /// `x` values of the rows a WHERE clause keeps, parsed the way
    /// `filter_where_impl` parses it and applied to a separate DataFrame.
    fn filtered_x(where_clause: &str) -> Vec<i64> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("x", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["apple", "banana", "avocado"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .expect("valid batch");
        let session = SessionContext::new();
        let df = session
            .read_table(Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("MemTable"),
            ))
            .expect("read MemTable");

        let predicate =
            parse_where_clause(&session, &schema, where_clause).expect("parse WHERE clause");
        let batches = RUNTIME
            .block_on(df.filter(predicate).expect("apply filter").collect())
            .expect("collect");
        batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("x")
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .expect("x column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// The parsed predicate's columns are qualified with the parse table's
    /// name wherever they sit; the old hand-written walk skipped expression
    /// kinds it did not list, such as `SIMILAR TO`, and the filter then failed
    /// with "No field named __ltseq_filter_parse_tmp.s".
    #[test]
    fn where_clause_columns_are_unqualified_in_every_expression_kind() {
        assert_eq!(filtered_x(r#""s" SIMILAR TO 'apple'"#), [1]);
        assert_eq!(
            filtered_x(r#"CASE WHEN "x" BETWEEN 2 AND 3 THEN "s" LIKE 'a%' ELSE "x" IN (1) END"#),
            [1, 3]
        );
        assert_eq!(filtered_x(r#"NOT ("s" IS NULL) AND abs(-"x") > 1"#), [2, 3]);
    }
}
