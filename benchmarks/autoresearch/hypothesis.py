"""
hypothesis.py — Generate optimization hypotheses from profiling hotspots.

Maps known Rust/Python function patterns to actionable suggestions.
"""

from __future__ import annotations

# Known optimization patterns: substring match → suggestion
# Each entry has:
#   hypothesis: what to try
#   files: relevant source files
#   effort: S/M/L (small/medium/large)
#   round: which benchmark round is most affected
KNOWN_PATTERNS: list[dict] = [
    {
        "match": "datafusion::physical_plan::aggregat",
        "label": "DataFusion GroupByExec: too many partitions",
        "hypothesis": (
            "target_partitions=32 causes GroupByExec to create 32 concurrent hash tables; "
            "the merge phase overhead outweighs the parallelism benefit for this workload. "
            "Suggestion: dynamically set target_partitions=CPU/4 for aggregation, "
            "or use a sequential session (target_partitions=1) specifically for agg() calls."
        ),
        "files": ["src/engine.rs:37", "src/ops/aggregation.rs"],
        "effort": "S",
        "round": "R1",
        "expected_gain": "R1 -15~25%",
    },
    {
        "match": "arrow::compute::concat",
        "label": "Arrow concat_batches: O(N) memory copy",
        "hypothesis": (
            "pattern_match.rs materializes all RecordBatches into one before pattern evaluation, "
            "causing an O(N) memcpy over the entire dataset. "
            "Suggestion: use a ChunkedArray or cross-batch index addressing to avoid the copy, "
            "or switch to batch-by-batch streaming evaluation."
        ),
        "files": ["src/ops/pattern_match.rs"],
        "effort": "M",
        "round": "R3",
        "expected_gain": "R3 -10~20%",
    },
    {
        "match": "ltseq_core::transpiler",
        "label": "Expression deserialization on every call",
        "hypothesis": (
            "Every filter()/derive() call fully deserializes the PyExpr dict from Python. "
            "For repeated calls with the same lambda this is redundant work. "
            "Suggestion: cache the serialized dict on the lambda object in Python, "
            "or intern identical dicts on the Rust side."
        ),
        "files": ["src/transpiler/mod.rs", "src/lib.rs"],
        "effort": "S",
        "round": "all",
        "expected_gain": "filter/derive -5~10% globally",
    },
    {
        "match": "ltseq::expr::proxy",
        "label": "SchemaProxy rebuilt on every call",
        "hypothesis": (
            "The Python layer constructs a new SchemaProxy and performs schema column lookup "
            "on every filter()/derive() call. "
            "Suggestion: cache the SchemaProxy instance on the LTSeq object and invalidate "
            "only when the schema changes."
        ),
        "files": ["py-ltseq/ltseq/expr/proxy.py", "py-ltseq/ltseq/transforms.py"],
        "effort": "S",
        "round": "all",
        "expected_gain": "filter/derive -3~8%",
    },
    {
        "match": "rayon",
        "label": "Rayon thread-pool overhead on small datasets",
        "hypothesis": (
            "parallel_scan.rs uses rayon parallelism unconditionally. "
            "For datasets smaller than ~1M rows, thread scheduling overhead exceeds "
            "the parallelism benefit. "
            "Suggestion: add an adaptive threshold — fall back to sequential scan below N rows."
        ),
        "files": ["src/ops/parallel_scan.rs"],
        "effort": "S",
        "round": "R3",
        "expected_gain": "R3 -5~15% on small data",
    },
    {
        "match": "parquet::arrow::async_reader",
        "label": "Parquet IO bottleneck",
        "hypothesis": (
            "Parquet reading dominates total time, suggesting the dataset is not cached "
            "in the OS page cache or read concurrency is insufficient. "
            "Suggestion: verify prefetch_size settings and apply projection pushdown "
            "to read only the columns needed by each round."
        ),
        "files": ["src/ops/io.rs", "src/engine.rs"],
        "effort": "M",
        "round": "all",
        "expected_gain": "IO-bound scenarios -10~30%",
    },
    {
        "match": "datafusion::physical_plan::sorts",
        "label": "Unexpected SortExec on pre-sorted data",
        "hypothesis": (
            "DataFusion is triggering SortExec even though assume_sorted was called. "
            "Suggestion: verify that sort_exprs exactly match the DataFusion "
            "EquivalenceProperties injected by assume_sorted, and that the injection "
            "path in lib.rs propagates correctly through all chained operations."
        ),
        "files": ["src/ops/sort.rs:107-114", "src/lib.rs"],
        "effort": "S",
        "round": "R2",
        "expected_gain": "R2 -5~15% (skip redundant sort)",
    },
    {
        "match": "starts_with",
        "label": "starts_with not using Arrow vectorized kernel",
        "hypothesis": (
            "The starts_with evaluation in pattern_match.rs goes through the generic "
            "eval_expr fallback path instead of arrow::compute::starts_with. "
            "Suggestion: add a dedicated fast path in eval_expr for "
            "StringMethod::StartsWith that calls arrow::compute::starts_with(array, scalar) directly."
        ),
        "files": ["src/ops/pattern_match.rs:459"],
        "effort": "S",
        "round": "R3",
        "expected_gain": "R3 starts_with eval -10~30%",
    },
]


def generate_hypotheses(hotspots: list[dict], max_results: int = 8) -> list[dict]:
    """Match hotspot function names against known patterns, return suggestions."""
    matched = []
    matched_patterns = set()

    for h in hotspots:
        fn = h.get("function", "")
        for pattern in KNOWN_PATTERNS:
            pid = pattern["match"]
            if pid in matched_patterns:
                continue
            if pattern["match"].lower() in fn.lower():
                matched.append(
                    {
                        "trigger_function": fn,
                        "trigger_self_pct": h.get("self_pct", 0),
                        **pattern,
                    }
                )
                matched_patterns.add(pid)

    # Sort by trigger_self_pct (most expensive first)
    matched.sort(key=lambda x: x["trigger_self_pct"], reverse=True)

    # Include unmatched hotspots for manual review
    matched_fns = {m["trigger_function"] for m in matched}
    unmatched = [
        {
            "trigger_function": h["function"],
            "trigger_self_pct": h.get("self_pct", 0),
            "label": "Unknown hotspot (manual review needed)",
            "hypothesis": "No known optimization pattern for this function. Inspect the flamegraph manually.",
            "files": [],
            "effort": "?",
            "round": "?",
            "expected_gain": "?",
        }
        for h in hotspots[:5]
        if h["function"] not in matched_fns
    ]

    return (matched + unmatched)[:max_results]


def render_hypotheses(hypotheses: list[dict]) -> str:
    """Format hypotheses as Markdown sections."""
    if not hypotheses:
        return "_No known optimization patterns matched. Inspect flamegraphs manually._\n"

    lines = []
    effort_label = {"S": "Small", "M": "Medium", "L": "Large", "?": "Unknown"}

    for i, h in enumerate(hypotheses, 1):
        pct = h.get("trigger_self_pct", 0)
        effort = effort_label.get(h.get("effort", "?"), "?")
        round_tag = h.get("round", "?")
        gain = h.get("expected_gain", "?")

        lines.append(f"### [{i}] {h['label']}")
        lines.append(f"- **Trigger**: `{h['trigger_function']}` ({pct:.1f}% self time)")
        lines.append(f"- **Round**: {round_tag} | **Effort**: {effort} | **Expected gain**: {gain}")
        lines.append(f"- **Hypothesis**: {h['hypothesis']}")
        if h.get("files"):
            file_list = ", ".join(f"`{f}`" for f in h["files"])
            lines.append(f"- **Files**: {file_list}")
        lines.append("")

    return "\n".join(lines)
