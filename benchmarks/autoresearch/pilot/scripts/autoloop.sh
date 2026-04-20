#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=benchmarks/autoresearch/pilot/common.sh
source "$SCRIPT_DIR/../common.sh"

usage() {
  cat <<'EOF'
Usage:
  ./benchmarks/autoresearch/pilot/scripts/autoloop.sh [options]

Options:
  -m, --model MODEL            OpenCode model in provider/model form (default: github-copilot/gpt-5-mini)
  -t, --target TARGET          Benchmark target key (default: clickbench_funnel)
      --agent AGENT            Optional OpenCode agent name
      --iterations N           Number of candidate attempts (default: 3)
      --baseline               Run a fresh baseline before the loop
      --attach URL             Attach each run to an existing OpenCode server
      --session ID             Continue a specific OpenCode session across all runs
  -c, --continue               Continue the last OpenCode session across all runs
      --fork                   Fork when continuing a session
      --dangerously-skip-permissions
                               Pass through to OpenCode run
      --sample                 Use sample dataset for benchmark capture
      --data PATH              Custom benchmark dataset path
      --skip-build             Skip `maturin develop --release` in baseline/candidate runs
      --sleep-seconds N        Sleep between runs (default: 5)
      --log-prefix NAME        Prefix for loop log files (default: benchmark-autoloop)
      --print-prompt           Print the generated prompt and exit
      --dry-run                Print commands without executing
      --force                  Skip branch and main-worktree safety checks
  -h, --help                   Show this help
EOF
}

TARGET="clickbench_funnel"
MODEL="github-copilot/gpt-5-mini"
AGENT=""
ITERATIONS=3
RUN_BASELINE=0
ATTACH_URL=""
SESSION_ID=""
CONTINUE_LAST=0
FORK_SESSION=0
SKIP_PERMISSIONS=0
SLEEP_SECONDS=5
LOG_PREFIX="benchmark-autoloop"
PRINT_PROMPT=0
DRY_RUN=0
FORCE=0
USE_SAMPLE=0
DATA_PATH=""
SKIP_BUILD=0

default_benchmark_data_path() {
  if [[ -n "$DATA_PATH" ]]; then
    printf '%s\n' "$DATA_PATH"
    return 0
  fi

  if [[ "$USE_SAMPLE" -eq 1 ]]; then
    printf '%s\n' "$ROOT_DIR/benchmarks/data/hits_sample.parquet"
    return 0
  fi

  printf '%s\n' "$ROOT_DIR/benchmarks/data/hits_sorted.parquet"
}

preflight_python_modules() {
  python - <<'PY'
import importlib
import sys

missing = []
for name in ("duckdb", "psutil"):
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        missing.append(name)

if missing:
    print("missing-python-modules=" + ",".join(missing))
    sys.exit(1)
PY
}

run_preflight_checks() {
  local data_file
  local failures=()

  data_file="$(default_benchmark_data_path)"

  if [[ "$SKIP_BUILD" -ne 1 ]] && ! command -v maturin >/dev/null 2>&1; then
    failures+=("missing command: maturin (install it or rerun with --skip-build)")
  fi

  if ! preflight_python_modules >/tmp/benchmark-autoresearch-python-check.$$ 2>&1; then
    failures+=("python benchmark deps unavailable: $(tr '\n' ' ' < /tmp/benchmark-autoresearch-python-check.$$ | sed 's/[[:space:]]\+/ /g') (run 'uv sync --group bench' or 'uv sync --group autoresearch')")
  fi
  rm -f /tmp/benchmark-autoresearch-python-check.$$

  if [[ ! -f "$data_file" ]]; then
    failures+=("missing benchmark data: $data_file (run 'python benchmarks/prepare_data.py' or pass --data PATH)")
  fi

  if [[ ${#failures[@]} -ne 0 ]]; then
    printf 'benchmark autoresearch preflight failed:\n' >&2
    for failure in "${failures[@]}"; do
      printf '  - %s\n' "$failure" >&2
    done
    printf 'see docs/BENCHMARK_AUTORESEARCH.md for setup details\n' >&2
    return 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--model)
      MODEL="${2:-}"
      shift 2
      ;;
    -t|--target)
      TARGET="${2:-}"
      shift 2
      ;;
    --agent)
      AGENT="${2:-}"
      shift 2
      ;;
    --iterations)
      ITERATIONS="${2:-}"
      shift 2
      ;;
    --baseline)
      RUN_BASELINE=1
      shift
      ;;
    --attach)
      ATTACH_URL="${2:-}"
      shift 2
      ;;
    --session)
      SESSION_ID="${2:-}"
      shift 2
      ;;
    -c|--continue)
      CONTINUE_LAST=1
      shift
      ;;
    --fork)
      FORK_SESSION=1
      shift
      ;;
    --dangerously-skip-permissions)
      SKIP_PERMISSIONS=1
      shift
      ;;
    --sample)
      USE_SAMPLE=1
      shift
      ;;
    --data)
      DATA_PATH="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --sleep-seconds)
      SLEEP_SECONDS="${2:-}"
      shift 2
      ;;
    --log-prefix)
      LOG_PREFIX="${2:-}"
      shift 2
      ;;
    --print-prompt)
      PRINT_PROMPT=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

resolve_target_brief "$TARGET" >/dev/null

for value_name in ITERATIONS SLEEP_SECONDS; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -lt 0 ]]; then
    printf '%s must be a non-negative integer\n' "$value_name" >&2
    exit 1
  fi
done

if [[ "$ITERATIONS" -lt 1 ]]; then
  printf '--iterations must be at least 1\n' >&2
  exit 1
fi

if [[ -n "$SESSION_ID" && "$CONTINUE_LAST" -eq 1 ]]; then
  printf 'use either --continue or --session, not both\n' >&2
  exit 1
fi

if ! command -v opencode >/dev/null 2>&1; then
  printf 'opencode not found in PATH\n' >&2
  exit 1
fi

if [[ "$DRY_RUN" -ne 1 ]]; then
  run_preflight_checks || exit 1
fi

WORKTREE_DIR="$ROOT_DIR/.worktrees/autoresearch-benchmark-$TARGET"
BRANCH_DATE="$(date '+%Y%m%d')"
RESEARCH_BRANCH="autoresearch-benchmark/${TARGET}-${BRANCH_DATE}"
LOOP_LOG="$(benchmark_log_path "$LOG_PREFIX-$TARGET")"
results_file="$AR_DIR/results.tsv"
issues_file="$AR_DIR/issues.tsv"
base_overlay_manifest="$WORKTREE_DIR/.benchmark-autoresearch-base-overlay.txt"

append_loop_log() {
  local line="$1"
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$line" | tee -a "$LOOP_LOG"
}

is_target_allowed_perf_file() {
  local path="$1"

  case "$TARGET" in
    clickbench_funnel)
      case "$path" in
        src/ops/pattern_match.rs)
          return 0
          ;;
      esac
      ;;
    clickbench_sessionization)
      case "$path" in
        src/ops/window.rs|src/ops/derive.rs)
          return 0
          ;;
      esac
      ;;
  esac

  return 1
}

ensure_results_file() {
  if [[ ! -f "$results_file" ]]; then
    printf 'base_ref\ttarget\tmodel_status\trecommendation\thypothesis\ttarget_win\tprotected_status\tevidence\trun_dir\tpatch_path\n' > "$results_file"
  fi
}

ensure_issue_file() {
  if [[ ! -f "$issues_file" ]]; then
    printf 'id\tcategory\ttarget\tfile\ttitle\tevidence\tsuggested_fix\tstatus\trun_date\n' > "$issues_file"
  fi
}

is_registered_worktree() {
  git -C "$ROOT_DIR" worktree list --porcelain | rg -Fxq "worktree $WORKTREE_DIR"
}

init_worktree() {
  if [[ -d "$WORKTREE_DIR" ]]; then
    if ! is_registered_worktree; then
      git -C "$ROOT_DIR" worktree prune >/dev/null 2>&1 || true
    fi
    if ! is_registered_worktree; then
      printf 'worktree directory %s exists but is not registered as a git worktree\n' "$WORKTREE_DIR" >&2
      return 1
    fi
    printf 'using existing worktree: %s\n' "$WORKTREE_DIR"
    return 0
  fi

  printf 'creating new worktree: %s\n' "$WORKTREE_DIR"
  mkdir -p "$(dirname "$WORKTREE_DIR")"
  if ! git -C "$ROOT_DIR" rev-parse --verify "$RESEARCH_BRANCH" >/dev/null 2>&1; then
    git -C "$ROOT_DIR" branch "$RESEARCH_BRANCH"
  fi
  git -C "$ROOT_DIR" worktree add "$WORKTREE_DIR" "$RESEARCH_BRANCH"
}

ensure_main_clean() {
  local status
  status="$(git -C "$ROOT_DIR" status --porcelain)"
  if [[ -n "$status" && "$FORCE" -ne 1 ]]; then
    printf 'main worktree has uncommitted changes; commit/discard them or use --force\n' >&2
    return 1
  fi
}

ensure_worktree_clean() {
  local status
  status="$(git -C "$WORKTREE_DIR" status --porcelain)"
  if [[ -n "$status" ]]; then
    printf 'worktree %s is not clean\n' "$WORKTREE_DIR" >&2
    git -C "$WORKTREE_DIR" status --short >&2
    return 1
  fi
}

bootstrap_worktree_state() {
  if [[ ! -d "$WORKTREE_DIR/.git" && ! -f "$WORKTREE_DIR/.git" ]]; then
    return 0
  fi

  git -C "$WORKTREE_DIR" restore --staged --worktree . >/dev/null 2>&1 || true
  git -C "$WORKTREE_DIR" clean -fd >/dev/null 2>&1 || true
}

ensure_worktree_on_research() {
  local current_branch
  current_branch="$(git -C "$WORKTREE_DIR" symbolic-ref --short HEAD 2>/dev/null || git -C "$WORKTREE_DIR" rev-parse --short HEAD 2>/dev/null)"
  if [[ "$current_branch" != "$RESEARCH_BRANCH" ]]; then
    if [[ "$FORCE" -eq 1 ]]; then
      git -C "$WORKTREE_DIR" checkout "$RESEARCH_BRANCH"
    else
      printf 'worktree must be on %s, current=%s\n' "$RESEARCH_BRANCH" "$current_branch" >&2
      exit 1
    fi
  fi
}

sync_workspace_overlay() {
  local path
  : > "$base_overlay_manifest"

  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    case "$path" in
      .benchmark-autoresearch-*)
        continue
        ;;
      .opencode/*|benchmarks/autoresearch/pilot/*)
        continue
        ;;
    esac
    mkdir -p "$WORKTREE_DIR/$(dirname "$path")"
    cp "$ROOT_DIR/$path" "$WORKTREE_DIR/$path"
    printf '%s\n' "$path" >> "$base_overlay_manifest"
  done < <(git -C "$ROOT_DIR" diff --name-only HEAD -- .)
}

path_in_base_overlay() {
  local path="$1"
  [[ -f "$base_overlay_manifest" ]] || return 1
  rg -Fxq "$path" "$base_overlay_manifest"
}

sync_autoresearch_assets() {
  local dst_dir="$WORKTREE_DIR/benchmarks/autoresearch/pilot"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'sync autoresearch assets into: %s\n' "$dst_dir"
    return 0
  fi

  mkdir -p "$dst_dir"
  cp "$AR_DIR/README.md" "$AR_DIR/program-perf.md" "$AR_DIR/common.py" "$AR_DIR/common.sh" "$AR_DIR/results.tsv" "$AR_DIR/issues.tsv" "$dst_dir/"
  cp -R "$AR_DIR/prompts" "$AR_DIR/scripts" "$AR_DIR/targets" "$dst_dir/"
}

cleanup_synced_assets() {
  local synced_dir="$WORKTREE_DIR/benchmarks/autoresearch/pilot"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'cleanup synced autoresearch assets in: %s\n' "$synced_dir"
    return 0
  fi

  if git -C "$WORKTREE_DIR" ls-tree --name-only HEAD -- benchmarks/autoresearch/pilot >/dev/null 2>&1 && \
    [[ -n "$(git -C "$WORKTREE_DIR" ls-tree --name-only HEAD -- benchmarks/autoresearch/pilot)" ]]; then
    git -C "$WORKTREE_DIR" restore --worktree --source=HEAD -- benchmarks/autoresearch/pilot
    git -C "$WORKTREE_DIR" clean -fd -- benchmarks/autoresearch/pilot
    return 0
  fi

  rm -rf "$synced_dir"
}

sync_baseline_reports_to_worktree() {
  local root_baseline_dir="$REPORT_DIR/baseline/$TARGET"
  local worktree_baseline_dir="$WORKTREE_DIR/benchmarks/autoresearch/pilot/reports/baseline/$TARGET"

  if [[ ! -d "$root_baseline_dir" ]]; then
    return 0
  fi

  mkdir -p "$worktree_baseline_dir"
  cp -R "$root_baseline_dir/." "$worktree_baseline_dir/"
}

archive_baseline_reports_from_worktree() {
  local root_baseline_dir="$REPORT_DIR/baseline/$TARGET"
  local worktree_baseline_dir="$WORKTREE_DIR/benchmarks/autoresearch/pilot/reports/baseline/$TARGET"

  if [[ ! -d "$worktree_baseline_dir" ]]; then
    return 0
  fi

  rm -rf "$root_baseline_dir"
  mkdir -p "$root_baseline_dir"
  cp -R "$worktree_baseline_dir/." "$root_baseline_dir/"
}

build_benchmark_args() {
  local -n out_ref=$1
  out_ref=()
  if [[ "$USE_SAMPLE" -eq 1 ]]; then
    out_ref+=(--sample)
  fi
  if [[ -n "$DATA_PATH" ]]; then
    out_ref+=(--data "$DATA_PATH")
  fi
  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    out_ref+=(--skip-build)
  fi
}

print_prompt() {
  bash "$SCRIPT_DIR/opencode_autoresearch.sh" \
    --target "$TARGET" \
    --single-candidate \
    --decision-file /tmp/benchmark-autoresearch-decision.txt \
    --print-prompt
}

build_opencode_command() {
  local run_index="$1"
  local decision_file="$2"
  local -n out_ref=$3
  local prompt

  prompt="$(WORKTREE_DIR="$WORKTREE_DIR" bash "$SCRIPT_DIR/opencode_autoresearch.sh" \
    --target "$TARGET" \
    --single-candidate \
    --decision-file "$decision_file" \
    --print-prompt 2>/dev/null)"

  out_ref=(opencode run)
  if [[ -n "$MODEL" ]]; then
    out_ref+=(--model "$MODEL")
  fi
  if [[ -n "$AGENT" ]]; then
    out_ref+=(--agent "$AGENT")
  fi
  if [[ -n "$ATTACH_URL" ]]; then
    out_ref+=(--attach "$ATTACH_URL")
  fi
  if [[ "$SKIP_PERMISSIONS" -eq 1 ]]; then
    out_ref+=(--dangerously-skip-permissions)
  fi
  if [[ -n "$SESSION_ID" ]]; then
    out_ref+=(--session "$SESSION_ID")
  elif [[ "$CONTINUE_LAST" -eq 1 ]]; then
    out_ref+=(--continue)
  fi
  if [[ "$FORK_SESSION" -eq 1 ]]; then
    out_ref+=(--fork)
  fi
  out_ref+=(--title "benchmark-autoresearch:${TARGET}:${run_index}" --dir "$WORKTREE_DIR" -- "$prompt")
}

run_single_candidate() {
  local run_index="$1"
  local decision_file="$2"
  local stdout_log="$3"
  local cmd=()
  build_opencode_command "$run_index" "$decision_file" cmd

  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'run %d command:' "$run_index"
    printf ' %q' "${cmd[@]}"
    printf '\n'
    return 0
  fi

  rm -f "$stdout_log"
  {
    printf '=== run %d start ===\n' "$run_index"
    printf 'command:'
    printf ' %q' "${cmd[@]}"
    printf '\n'
    "${cmd[@]}"
  } 2>&1 | tee "$stdout_log"
}

recover_decision_from_stdout() {
  local stdout_log="$1"
  local decision_file="$2"
  python3 "$SCRIPT_DIR/extract_decision_from_stdout.py" "$stdout_log" "$decision_file"
}

get_field() {
  local file="$1"
  local key="$2"
  local line
  line="$(rg "^${key}=" "$file" | head -1 || true)"
  if [[ -z "$line" ]]; then
    return 0
  fi
  printf '%s' "${line#*=}" | tr -d '\r'
}

next_issue_id() {
  local max_id=0
  local id rest
  ensure_issue_file
  while IFS=$'\t' read -r id rest; do
    [[ "$id" == "id" ]] && continue
    if [[ "$id" =~ ^AR-([0-9]+)$ ]] && (( 10#${BASH_REMATCH[1]} > max_id )); then
      max_id=$((10#${BASH_REMATCH[1]}))
    fi
  done < "$issues_file"
  printf 'AR-%03d\n' "$((max_id + 1))"
}

issue_exists() {
  local category="$1"
  local target="$2"
  local file="$3"
  local title="$4"
  local id existing_category existing_target existing_file existing_title rest

  [[ -f "$issues_file" ]] || return 1
  while IFS=$'\t' read -r id existing_category existing_target existing_file existing_title rest; do
    [[ "$id" == "id" ]] && continue
    if [[ "$existing_category" == "$category" && "$existing_target" == "$target" && "$existing_file" == "$file" && "$existing_title" == "$title" ]]; then
      return 0
    fi
  done < "$issues_file"
  return 1
}

append_issue() {
  local category="$1"
  local file="$2"
  local title="$3"
  local evidence="$4"
  local suggested_fix="$5"
  local status="${6:-open}"
  local run_date="${7:-$(date '+%Y-%m-%d')}"
  local issue_id

  [[ -z "$category" || -z "$title" ]] && return 0
  category="$(sanitize_tsv_field "$category")"
  file="$(sanitize_tsv_field "${file:-n/a}")"
  title="$(sanitize_tsv_field "$title")"
  evidence="$(sanitize_tsv_field "${evidence:-n/a}")"
  suggested_fix="$(sanitize_tsv_field "${suggested_fix:-n/a}")"
  status="$(sanitize_tsv_field "$status")"
  run_date="$(sanitize_tsv_field "$run_date")"

  ensure_issue_file
  if issue_exists "$category" "$TARGET" "$file" "$title"; then
    return 0
  fi
  issue_id="$(next_issue_id)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$issue_id" "$category" "$TARGET" "$file" "$title" "$evidence" "$suggested_fix" "$status" "$run_date" >> "$issues_file"
}

record_issue_from_decision() {
  local decision_file="$1"
  append_issue \
    "$(get_field "$decision_file" issue_category)" \
    "$(get_field "$decision_file" issue_file)" \
    "$(get_field "$decision_file" issue_title)" \
    "$(get_field "$decision_file" issue_evidence)" \
    "$(get_field "$decision_file" issue_suggested_fix)"
}

run_baseline_if_needed() {
  local baseline_summary="$REPORT_DIR/baseline/$TARGET/benchmark-summary.json"
  local bench_args=()
  build_benchmark_args bench_args
  if [[ "$RUN_BASELINE" -eq 0 && -f "$baseline_summary" ]]; then
    return 0
  fi
  append_loop_log "running baseline for $TARGET"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'baseline command: (cd %q && python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py %q' "$WORKTREE_DIR" "$TARGET"
    printf ' %q' "${bench_args[@]}"
    printf ')\n'
    return 0
  fi
  (
    cd "$WORKTREE_DIR"
    python benchmarks/autoresearch/pilot/scripts/benchmark_baseline.py "$TARGET" "${bench_args[@]}"
  )
  archive_baseline_reports_from_worktree
}

validate_candidate_scope() {
  local out_file="$1"
  local status path invalid=0 changed=0
  : > "$out_file"
  while IFS= read -r status; do
    path="${status:3}"
    case "$path" in
      .benchmark-autoresearch-*)
        continue
        ;;
      benchmarks/autoresearch/pilot/*)
        continue
        ;;
      src/*|py-ltseq/*)
        if path_in_base_overlay "$path"; then
          continue
        fi
        if [[ "$path" == *"_test.py" || "$path" == *"_test.rs" ]] || ! is_target_allowed_perf_file "$path"; then
          printf '%s\n' "$path" >> "$out_file"
          invalid=1
        else
          changed=1
        fi
        ;;
      *)
        printf '%s\n' "$path" >> "$out_file"
        invalid=1
        ;;
    esac
  done < <(git -C "$WORKTREE_DIR" status --porcelain --untracked-files=all)

  if [[ "$changed" -eq 0 ]]; then
    printf 'no in-scope production changes detected\n' > "$out_file"
    return 1
  fi
  if [[ "$invalid" -ne 0 ]]; then
    return 1
  fi
  rm -f "$out_file"
  return 0
}

next_run_dir() {
  local target_runs_dir="$REPORT_DIR/runs/$TARGET"
  local max_index=0
  local path name value

  mkdir -p "$target_runs_dir"
  while IFS= read -r path; do
    name="$(basename "$path")"
    if [[ "$name" =~ ^run-([0-9]+)$ ]]; then
      value=$((10#${BASH_REMATCH[1]}))
      if (( value > max_index )); then
        max_index=$value
      fi
    fi
  done < <(python3 - <<'PY' "$target_runs_dir"
from pathlib import Path
import sys
for path in sorted(Path(sys.argv[1]).glob('run-*')):
    if path.is_dir():
        print(path)
PY
)

  printf '%s/run-%03d\n' "$target_runs_dir" "$((max_index + 1))"
}

archive_run_artifacts() {
  local run_index="$1"
  local decision_file="$2"
  local stdout_log="$3"
  local evaluation_file="$4"
  local run_dir
  local patch_path
  local worktree_report_root="$WORKTREE_DIR/benchmarks/autoresearch/pilot/reports"

  run_dir="$(next_run_dir)"
  patch_path="$run_dir/patch.diff"

  mkdir -p "$run_dir"
  cp "$decision_file" "$run_dir/decision.txt"
  cp "$stdout_log" "$run_dir/stdout.log"
  cp "$evaluation_file" "$run_dir/evaluation.txt"
  local -a diff_cmd=(git -C "$WORKTREE_DIR" diff -- . ':(exclude)benchmarks/autoresearch/pilot')
  if [[ -f "$base_overlay_manifest" ]]; then
    while IFS= read -r base_path; do
      [[ -z "$base_path" ]] && continue
      diff_cmd+=(":(exclude)$base_path")
    done < "$base_overlay_manifest"
  fi
  "${diff_cmd[@]}" > "$patch_path"

  if [[ -d "$worktree_report_root/candidates/$TARGET" ]]; then
    mkdir -p "$run_dir/candidate"
    cp -R "$worktree_report_root/candidates/$TARGET/." "$run_dir/candidate/"
  fi
  if [[ -d "$worktree_report_root/diff/$TARGET" ]]; then
    mkdir -p "$run_dir/diff"
    cp -R "$worktree_report_root/diff/$TARGET/." "$run_dir/diff/"
  fi

  printf '%s\n%s\n' "$run_dir" "$patch_path"
}

discard_candidate_state() {
  git -C "$WORKTREE_DIR" restore --worktree .
  git -C "$WORKTREE_DIR" clean -fd
}

record_result() {
  local base_ref="$1"
  local model_status="$2"
  local recommendation="$3"
  local hypothesis="$4"
  local target_win="$5"
  local protected_status="$6"
  local evidence="$7"
  local run_dir="$8"
  local patch_path="$9"

  ensure_results_file
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(sanitize_tsv_field "$base_ref")" \
    "$TARGET" \
    "$(sanitize_tsv_field "$model_status")" \
    "$(sanitize_tsv_field "$recommendation")" \
    "$(sanitize_tsv_field "$hypothesis")" \
    "$(sanitize_tsv_field "$target_win")" \
    "$(sanitize_tsv_field "$protected_status")" \
    "$(sanitize_tsv_field "$evidence")" \
    "$(sanitize_tsv_field "$run_dir")" \
    "$(sanitize_tsv_field "$patch_path")" >> "$results_file"
}

run_iteration() {
  local run_index="$1"
  local decision_file="$WORKTREE_DIR/.benchmark-autoresearch-decision-$run_index.txt"
  local stdout_log="$(benchmark_log_path "${LOG_PREFIX}-${TARGET}-run-${run_index}.stdout")"
  local eval_file="$(benchmark_log_path "${LOG_PREFIX}-${TARGET}-run-${run_index}.eval")"
  local scope_file="$(benchmark_log_path "${LOG_PREFIX}-${TARGET}-run-${run_index}.scope")"
  local status scenario reason evidence base_ref
  local run_dir patch_path recommendation target_win protected_status
  local bench_args=()

  rm -f "$decision_file" "$eval_file" "$scope_file"
  run_single_candidate "$run_index" "$decision_file" "$stdout_log" || true

  if [[ "$DRY_RUN" -eq 1 ]]; then
    return 0
  fi

  if [[ ! -f "$decision_file" ]]; then
    if ! recover_decision_from_stdout "$stdout_log" "$decision_file"; then
      append_issue "harness" "benchmarks/autoresearch/pilot/scripts/autoloop.sh" "Benchmark autoresearch run produced no usable decision artifact" "run ${run_index} did not emit a recoverable decision block" "Tighten prompt compliance or fallback parsing for benchmark controller"
      discard_candidate_state
      return 1
    fi
  fi

  status="$(get_field "$decision_file" status)"
  scenario="$(get_field "$decision_file" scenario)"
  reason="$(get_field "$decision_file" reason)"
  evidence="$(get_field "$decision_file" evidence)"
  base_ref="$(git -C "$WORKTREE_DIR" rev-parse --short HEAD)"

  record_issue_from_decision "$decision_file"

  if [[ "$status" != "keep" ]]; then
    printf 'recommendation=discard\nreason=%s\ntarget_win=none\nprotected_status=n/a\nevidence=%s\n' "$reason" "$evidence" > "$eval_file"
    mapfile -t archived < <(archive_run_artifacts "$run_index" "$decision_file" "$stdout_log" "$eval_file")
    run_dir="${archived[0]}"
    patch_path="${archived[1]}"
    record_result "$base_ref" "$status" "discard" "$scenario" "none" "n/a" "$reason" "$run_dir" "$patch_path"
    discard_candidate_state
    return 0
  fi

  if ! validate_candidate_scope "$scope_file"; then
    printf 'recommendation=discard\nreason=out-of-scope-or-empty-changes\ntarget_win=none\nprotected_status=n/a\nevidence=%s\n' "$(tr '\n' ';' < "$scope_file")" > "$eval_file"
    mapfile -t archived < <(archive_run_artifacts "$run_index" "$decision_file" "$stdout_log" "$eval_file")
    run_dir="${archived[0]}"
    patch_path="${archived[1]}"
    record_result "$base_ref" "$status" "discard" "$scenario" "none" "n/a" "out-of-scope-or-empty-changes" "$run_dir" "$patch_path"
    discard_candidate_state
    return 0
  fi

  build_benchmark_args bench_args
  append_loop_log "running benchmark candidate for iteration $run_index"
  if ! (
    cd "$WORKTREE_DIR"
    python benchmarks/autoresearch/pilot/scripts/benchmark_candidate.py "$TARGET" "${bench_args[@]}"
  ); then
    printf '{"recommendation":"discard","reason":"benchmark-candidate-command-failed","target_win":"none","protected_status":"n/a"}\n' > "$eval_file"
    mapfile -t archived < <(archive_run_artifacts "$run_index" "$decision_file" "$stdout_log" "$eval_file")
    run_dir="${archived[0]}"
    patch_path="${archived[1]}"
    record_result "$base_ref" "$status" "discard" "$scenario" "none" "n/a" "benchmark-candidate-command-failed" "$run_dir" "$patch_path"
    discard_candidate_state
    return 0
  fi

  if ! (
    cd "$WORKTREE_DIR"
    python benchmarks/autoresearch/pilot/scripts/benchmark_gate.py "$TARGET" >/dev/null
  ); then
    printf '{"recommendation":"discard","reason":"benchmark-gate-command-failed","target_win":"none","protected_status":"n/a"}\n' > "$eval_file"
    mapfile -t archived < <(archive_run_artifacts "$run_index" "$decision_file" "$stdout_log" "$eval_file")
    run_dir="${archived[0]}"
    patch_path="${archived[1]}"
    record_result "$base_ref" "$status" "discard" "$scenario" "none" "n/a" "benchmark-gate-command-failed" "$run_dir" "$patch_path"
    discard_candidate_state
    return 0
  fi

  python3 "$WORKTREE_DIR/benchmarks/autoresearch/pilot/scripts/evaluate_benchmark_candidate.py" \
    "$WORKTREE_DIR/benchmarks/autoresearch/pilot/reports/baseline/$TARGET/benchmark-summary.json" \
    "$WORKTREE_DIR/benchmarks/autoresearch/pilot/reports/candidates/$TARGET/benchmark-summary.json" \
    "$TARGET" > "$eval_file"

  mapfile -t archived < <(archive_run_artifacts "$run_index" "$decision_file" "$stdout_log" "$eval_file")
  run_dir="${archived[0]}"
  patch_path="${archived[1]}"
  recommendation="$(python3 - <<'PY' "$eval_file"
import json, sys
data = json.load(open(sys.argv[1]))
print(data.get('recommendation', 'unknown'))
PY
)"
  target_win="$(python3 - <<'PY' "$eval_file"
import json, sys
data = json.load(open(sys.argv[1]))
print(data.get('target_win', 'none'))
PY
)"
  protected_status="$(python3 - <<'PY' "$eval_file"
import json, sys
data = json.load(open(sys.argv[1]))
print(data.get('protected_status', 'n/a'))
PY
)"
  evidence="$(python3 - <<'PY' "$eval_file"
import json, sys
data = json.load(open(sys.argv[1]))
print(data.get('reason', 'n/a'))
PY
)"

  record_result "$base_ref" "$status" "$recommendation" "$scenario" "$target_win" "$protected_status" "$evidence" "$run_dir" "$patch_path"
  append_loop_log "iteration $run_index recommendation=$recommendation reason=$evidence run_dir=$run_dir"
  discard_candidate_state
}

autoloop_main() {
  cd "$ROOT_DIR"

  if [[ "$PRINT_PROMPT" -eq 1 ]]; then
    print_prompt
    return 0
  fi

  ensure_report_dirs
  ensure_results_file
  ensure_issue_file
  append_loop_log "target=$TARGET research_branch=$RESEARCH_BRANCH worktree=$WORKTREE_DIR model=${MODEL:-default}"

  if [[ "$DRY_RUN" -ne 1 ]]; then
    ensure_main_clean
    init_worktree
    bootstrap_worktree_state
    ensure_worktree_clean
    ensure_worktree_on_research
    sync_workspace_overlay
    sync_autoresearch_assets
    sync_baseline_reports_to_worktree
    run_baseline_if_needed
    cleanup_synced_assets
    discard_candidate_state
  else
    init_worktree || true
    bootstrap_worktree_state
    sync_workspace_overlay
    sync_autoresearch_assets
    sync_baseline_reports_to_worktree
    run_baseline_if_needed
    cleanup_synced_assets
  fi

  for ((run_index = 1; run_index <= ITERATIONS; run_index++)); do
    if [[ "$DRY_RUN" -ne 1 ]]; then
      bootstrap_worktree_state
      ensure_worktree_clean
      sync_workspace_overlay
      sync_autoresearch_assets
      sync_baseline_reports_to_worktree
    fi
    append_loop_log "=== iteration $run_index of $ITERATIONS ==="
    run_iteration "$run_index" || append_loop_log "iteration $run_index ended with controller error"
    cleanup_synced_assets
    if [[ "$run_index" -lt "$ITERATIONS" && "$SLEEP_SECONDS" -gt 0 ]]; then
      if [[ "$DRY_RUN" -eq 1 ]]; then
        printf 'sleep %s\n' "$SLEEP_SECONDS"
      else
        sleep "$SLEEP_SECONDS"
      fi
    fi
  done

  append_loop_log "benchmark autoresearch finished: $ITERATIONS iterations completed for target=$TARGET"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  autoloop_main "$@"
fi
