#!/usr/bin/env bash

set -euo pipefail

resolve_repo_root_dir() {
  local top_level

  top_level="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [[ -n "$top_level" ]]; then
    printf '%s\n' "$top_level"
    return 0
  fi

  local common_dir

  common_dir="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
  if [[ -n "$common_dir" && "$(basename "$common_dir")" == ".git" ]]; then
    dirname "$common_dir"
    return 0
  fi

  git rev-parse --show-toplevel
}

ROOT_DIR="$(resolve_repo_root_dir)"
AR_DIR="$ROOT_DIR/benchmarks/autoresearch/pilot"
REPORT_DIR="$AR_DIR/reports"

ensure_report_dirs() {
  mkdir -p "$REPORT_DIR/baseline" "$REPORT_DIR/candidates" "$REPORT_DIR/diff" "$REPORT_DIR/logs" "$REPORT_DIR/runs"
}

resolve_program_rules() {
  printf '%s\n' 'benchmarks/autoresearch/pilot/program-perf.md'
}

resolve_target_brief() {
  case "${1:-}" in
    clickbench_funnel)
      printf '%s\n' 'benchmarks/autoresearch/pilot/targets/clickbench_funnel_perf.md'
      ;;
    clickbench_sessionization)
      printf '%s\n' 'benchmarks/autoresearch/pilot/targets/clickbench_sessionization_perf.md'
      ;;
    *)
      printf 'unknown benchmark target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_sources() {
  case "${1:-}" in
    clickbench_funnel)
      printf '%s\n' 'src/ops/pattern_match.rs'
      ;;
    clickbench_sessionization)
      printf '%s\n' 'src/ops/window.rs src/ops/derive.rs'
      ;;
    *)
      printf 'unknown benchmark target: %s\n' "${1:-}" >&2
      exit 1
      ;;
  esac
}

resolve_target_source_paths() {
  local target="$1"
  local source_paths=()
  local source

  while IFS= read -r source; do
    [[ -z "$source" ]] && continue
    source_paths+=("$ROOT_DIR/$source")
  done < <(resolve_target_sources "$target" | tr ' ' '\n')

  printf '%s' "${source_paths[*]}"
}

resolve_target_report_dir() {
  local phase="$1"
  local target="$2"
  printf '%s\n' "$REPORT_DIR/$phase/$target"
}

benchmark_log_path() {
  local name="$1"
  printf '%s\n' "$REPORT_DIR/logs/${name}.log"
}

render_prompt_template_with_decision() {
  local template_path="$1"
  local target="$2"
  local decision_file="$3"
  local worktree_dir="${4:-$ROOT_DIR}"
  local rendered
  local brief_file
  local rules_file
  local candidate_script
  local gate_script
  local source_files

  brief_file="$worktree_dir/$(resolve_target_brief "$target")"
  rules_file="$worktree_dir/$(resolve_program_rules)"
  candidate_script="$worktree_dir/benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py"
  gate_script="$worktree_dir/benchmarks/autoresearch/pilot/scripts/benchmark_gate.py"
  source_files="$(resolve_target_source_paths "$target")"
  rendered="$(<"$template_path")"
  rendered="${rendered//\{\{TARGET\}\}/$target}"
  rendered="${rendered//\{\{SOURCE_FILES\}\}/$source_files}"
  rendered="${rendered//\{\{BRIEF_FILE\}\}/$brief_file}"
  rendered="${rendered//\{\{RULES_FILE\}\}/$rules_file}"
  rendered="${rendered//\{\{DECISION_FILE\}\}/$decision_file}"
  rendered="${rendered//\{\{CANDIDATE_SCRIPT\}\}/$candidate_script}"
  rendered="${rendered//\{\{GATE_SCRIPT\}\}/$gate_script}"
  rendered="${rendered//\{\{WORKTREE_DIR\}\}/$worktree_dir}"
  printf '%s' "$rendered"
}

run_or_print() {
  local dry_run="$1"
  shift
  if [[ "$dry_run" -eq 1 ]]; then
    printf 'dry-run command:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

sanitize_tsv_field() {
  printf '%s' "$1" | tr '\t\r\n' '   '
}
