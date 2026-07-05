//! Aggregation operations for LTSeqTable
//!
//! Fully native DataFusion `df.aggregate()` path (issue #91): the plan stays
//! lazy — no collect, no MemTable, no SQL string round-trip. Aggregates that
//! SQL would express as scalar-over-aggregate (top_k, skew) are planned in
//! two stages: hidden aggregate parts in the Aggregate node, combined by a
//! post-aggregation projection.
//!
//! `filter_where_impl` keeps its `session.sql()` call by design: it uses the
//! SQL engine as a WHERE-clause parser against an empty table (allowlisted in
//! issue #91 — no data ever round-trips).

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::types::{dict_to_py_expr, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
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
        "sum" | "count" | "min" | "max" | "avg" | "median" | "variance" | "var" | "stddev"
        | "std" => {
            let col_expr = match on.as_ref() {
                PyExpr::Column(col_name) => {
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
                "avg" => agg_fn::avg(col_expr),
                "median" => agg_fn::median(col_expr),
                "variance" | "var" => agg_fn::var_sample(col_expr),
                "stddev" | "std" => agg_fn::stddev(col_expr),
                _ => unreachable!(),
            };
            Ok(AggPlan::Plain(agg_expr))
        }
        "percentile" => {
            let col_expr = match on.as_ref() {
                PyExpr::Column(col_name) => {
                    Expr::Column(Column::new_unqualified(col_name))
                }
                _ => return Err("percentile requires a column reference".to_string()),
            };
            let p = args.first().and_then(|arg| {
                if let PyExpr::Literal { value, .. } = arg {
                    value.parse::<f64>().ok()
                } else {
                    None
                }
            }).unwrap_or(0.5);
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
            // corr(col_a, col_b) — both come from args when on is empty
            let (col_a, col_b) = if matches!(on.as_ref(), PyExpr::Column(name) if name.is_empty()) {
                if args.len() < 2 {
                    return Err("corr requires two column arguments".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?,
                )
            } else {
                if args.is_empty() {
                    return Err("corr requires a second column argument".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(*on.clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                )
            };
            Ok(AggPlan::Plain(agg_fn::corr(col_a, col_b)))
        }
        "covar" => {
            let (col_a, col_b) = if matches!(on.as_ref(), PyExpr::Column(name) if name.is_empty()) {
                if args.len() < 2 {
                    return Err("covar requires two column arguments".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[1].clone(), schema)?,
                )
            } else {
                if args.is_empty() {
                    return Err("covar requires a second column argument".to_string());
                }
                (
                    crate::transpiler::pyexpr_to_datafusion(*on.clone(), schema)?,
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?,
                )
            };
            Ok(AggPlan::Plain(agg_fn::covar_samp(col_a, col_b)))
        }
        "concat_agg" => {
            // concat_agg(col, delimiter) — uses native string_agg UDAF
            let (col_expr, delim_expr) = if matches!(on.as_ref(), PyExpr::Column(name) if name.is_empty()) {
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
            } else {
                let col = crate::transpiler::pyexpr_to_datafusion(*on.clone(), schema)?;
                let delim = if !args.is_empty() {
                    crate::transpiler::pyexpr_to_datafusion(args[0].clone(), schema)?
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
            let PyExpr::Column(col_name) = on.as_ref() else {
                return Err("top_k requires a column reference".to_string());
            };
            let k = args
                .first()
                .and_then(|arg| {
                    if let PyExpr::Literal { value, .. } = arg {
                        value.parse::<i64>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(10);

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
        // mode keeps the legacy behavior (FIRST_VALUE ordered ascending —
        // i.e. min, not a true statistical mode). Real mode semantics are
        // deferred per issue #91 risk 3.
        "mode" => {
            let PyExpr::Column(col_name) = on.as_ref() else {
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
            // exported free function skew(g.col) leaves `on` as the empty
            // placeholder and passes the column in args[0].
            let col_name = match on.as_ref() {
                PyExpr::Column(name) if !name.is_empty() => name.clone(),
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

    let df_expr = crate::transpiler::pyexpr_to_datafusion(py_expr, schema)
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

/// Filter rows using a raw SQL WHERE clause
///
/// Uses DataFusion's SQL parser to convert the WHERE clause into a native
/// expression, then applies it via DataFrame::filter() to preserve laziness.
pub fn filter_where_impl(table: &LTSeqTable, where_clause: &str) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Parse the WHERE clause into a native expression by building a SQL query
    // against a temp table with the same schema, then extract the filter expression
    // and strip any table qualifiers so it works with the original DataFrame.
    let filter_expr = RUNTIME
        .block_on(async {
            let temp_name = "__ltseq_filter_parse_tmp";
            let _ = table.session.deregister_table(temp_name);

            // Register an empty table with the same schema for parsing
            let empty_batch = datafusion::arrow::record_batch::RecordBatch::new_empty(Arc::clone(schema));
            let mem_table = datafusion::datasource::MemTable::try_new(
                Arc::clone(schema),
                vec![vec![empty_batch]],
            ).map_err(|e| format!("Failed to create parse table: {}", e))?;

            table
                .session
                .register_table(temp_name, Arc::new(mem_table))
                .map_err(|e| format!("Failed to register parse table: {}", e))?;

            // Parse the full SELECT to get the filter expression in context
            let parsed_df = table
                .session
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
            let _ = table.session.deregister_table(temp_name);

            // Strip table qualifiers from the expression so it works with the original DataFrame.
            // Columns parsed from SQL will be qualified with the temp table name,
            // but we need unqualified columns for the native filter.
            fn strip_table_qualifiers(expr: datafusion::logical_expr::Expr) -> datafusion::logical_expr::Expr {
                use datafusion::logical_expr::Expr;
                match expr {
                    Expr::Column(col) => {
                        // Remove table qualifier
                        Expr::Column(datafusion::common::Column::new_unqualified(col.name))
                    }
                    Expr::Alias(alias) => {
                        Expr::Alias(datafusion::logical_expr::expr::Alias {
                            expr: Box::new(strip_table_qualifiers(*alias.expr)),
                            ..alias
                        })
                    }
                    Expr::BinaryExpr(binary) => {
                        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                            left: Box::new(strip_table_qualifiers(*binary.left)),
                            right: Box::new(strip_table_qualifiers(*binary.right)),
                            op: binary.op,
                        })
                    }
                    Expr::Like(like) => {
                        Expr::Like(datafusion::logical_expr::expr::Like {
                            negated: like.negated,
                            expr: Box::new(strip_table_qualifiers(*like.expr)),
                            pattern: Box::new(strip_table_qualifiers(*like.pattern)),
                            escape_char: like.escape_char,
                            case_insensitive: like.case_insensitive,
                        })
                    }
                    Expr::InList(in_list) => {
                        Expr::InList(datafusion::logical_expr::expr::InList {
                            expr: Box::new(strip_table_qualifiers(*in_list.expr)),
                            list: in_list.list.into_iter().map(strip_table_qualifiers).collect(),
                            negated: in_list.negated,
                        })
                    }
                    Expr::Between(between) => {
                        Expr::Between(datafusion::logical_expr::expr::Between {
                            expr: Box::new(strip_table_qualifiers(*between.expr)),
                            negated: between.negated,
                            low: Box::new(strip_table_qualifiers(*between.low)),
                            high: Box::new(strip_table_qualifiers(*between.high)),
                        })
                    }
                    Expr::Case(case) => {
                        Expr::Case(datafusion::logical_expr::expr::Case {
                            expr: case.expr.map(|e| Box::new(strip_table_qualifiers(*e))),
                            when_then_expr: case.when_then_expr.into_iter().map(|(w, t)| {
                                (Box::new(strip_table_qualifiers(*w)), Box::new(strip_table_qualifiers(*t)))
                            }).collect(),
                            else_expr: case.else_expr.map(|e| Box::new(strip_table_qualifiers(*e))),
                        })
                    }
                    Expr::ScalarFunction(func) => {
                        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
                            func: func.func,
                            args: func.args.into_iter().map(strip_table_qualifiers).collect(),
                        })
                    }
                    Expr::AggregateFunction(agg) => {
                        Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction {
                            func: agg.func,
                            params: datafusion::logical_expr::expr::AggregateFunctionParams {
                                args: agg.params.args.into_iter().map(strip_table_qualifiers).collect(),
                                filter: agg.params.filter.map(|e| Box::new(strip_table_qualifiers(*e))),
                                order_by: agg.params.order_by,
                                distinct: agg.params.distinct,
                                null_treatment: agg.params.null_treatment,
                            },
                        })
                    }
                    Expr::WindowFunction(win) => {
                        Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
                            fun: win.fun,
                            params: datafusion::logical_expr::expr::WindowFunctionParams {
                                args: win.params.args.into_iter().map(strip_table_qualifiers).collect(),
                                partition_by: win.params.partition_by.into_iter().map(strip_table_qualifiers).collect(),
                                order_by: win.params.order_by.into_iter().map(|s| datafusion::logical_expr::expr::Sort {
                                    expr: strip_table_qualifiers(s.expr),
                                    asc: s.asc,
                                    nulls_first: s.nulls_first,
                                }).collect(),
                                window_frame: win.params.window_frame,
                                filter: win.params.filter.map(|e| Box::new(strip_table_qualifiers(*e))),
                                null_treatment: win.params.null_treatment,
                                distinct: win.params.distinct,
                            },
                        }))
                    }
                    Expr::Cast(cast) => {
                        Expr::Cast(datafusion::logical_expr::expr::Cast {
                            expr: Box::new(strip_table_qualifiers(*cast.expr)),
                            data_type: cast.data_type,
                        })
                    }
                    Expr::TryCast(try_cast) => {
                        Expr::TryCast(datafusion::logical_expr::expr::TryCast {
                            expr: Box::new(strip_table_qualifiers(*try_cast.expr)),
                            data_type: try_cast.data_type,
                        })
                    }
                    Expr::Not(not) => {
                        Expr::Not(Box::new(strip_table_qualifiers(*not)))
                    }
                    Expr::IsNotNull(is_not_null) => {
                        Expr::IsNotNull(Box::new(strip_table_qualifiers(*is_not_null)))
                    }
                    Expr::IsNull(is_null) => {
                        Expr::IsNull(Box::new(strip_table_qualifiers(*is_null)))
                    }
                    Expr::IsTrue(is_true) => {
                        Expr::IsTrue(Box::new(strip_table_qualifiers(*is_true)))
                    }
                    Expr::IsFalse(is_false) => {
                        Expr::IsFalse(Box::new(strip_table_qualifiers(*is_false)))
                    }
                    Expr::IsUnknown(is_unknown) => {
                        Expr::IsUnknown(Box::new(strip_table_qualifiers(*is_unknown)))
                    }
                    Expr::IsNotTrue(is_not_true) => {
                        Expr::IsNotTrue(Box::new(strip_table_qualifiers(*is_not_true)))
                    }
                    Expr::IsNotFalse(is_not_false) => {
                        Expr::IsNotFalse(Box::new(strip_table_qualifiers(*is_not_false)))
                    }
                    Expr::IsNotUnknown(is_not_unknown) => {
                        Expr::IsNotUnknown(Box::new(strip_table_qualifiers(*is_not_unknown)))
                    }
                    Expr::Negative(neg) => {
                        Expr::Negative(Box::new(strip_table_qualifiers(*neg)))
                    }
                    // GetIndexedField removed in DataFusion 53
                    // Literals and other leaf expressions pass through unchanged
                    other => other,
                }
            }

            Ok::<_, String>(strip_table_qualifiers(predicate))
        })
        .map_err(LtseqError::Runtime)?;

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
