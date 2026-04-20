"""
profiler.py — Generate Rust + Python flamegraphs for benchmark rounds.

Two profiling approaches:
  1. py-spy (recommended): profiles Python + native Rust extension in one pass.
     Requires: pip install py-spy
     Advantage: no special Rust build needed for Python-level profiling;
                use --native flag to see Rust frames (debug symbols help).

  2. perf + inferno (Linux): kernel-level profiling with full Rust resolution.
     Requires: linux-perf, cargo install inferno
     Build with: maturin develop --profile flamegraph (adds debug=true)
     Advantage: more accurate Rust frame attribution.

Notes:
  - py-spy requires no elevated privileges on most systems.
  - perf may need: echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

BENCHMARKS_DIR = Path(__file__).parent.parent
REPO_ROOT = BENCHMARKS_DIR.parent


def _check_tool(name: str) -> bool:
    return shutil.which(name) is not None


def check_dependencies() -> dict[str, bool]:
    """Return availability of profiling tools."""
    return {
        "py-spy": _check_tool("py-spy"),
        "perf": _check_tool("perf"),
        "inferno-collapse-perf": _check_tool("inferno-collapse-perf"),
        "inferno-flamegraph": _check_tool("inferno-flamegraph"),
        "flamegraph": _check_tool("flamegraph"),  # cargo install flamegraph
    }


def print_install_hints(deps: dict[str, bool]) -> None:
    if not deps["py-spy"] and not deps["perf"]:
        print("  [profiler] Warning: no profiling tools found.")
        print("  Install py-spy:   pip install py-spy")
        print("  Or inferno:       cargo install inferno")


def profile_with_pyspy(
    round_num: int,
    output_svg: Path,
    data_flag: str = "--sample",
    extra_args: list[str] | None = None,
) -> bool:
    """Profile a single benchmark round using py-spy.

    Args:
        round_num: 1, 2, or 3
        output_svg: destination SVG path
        data_flag: '--sample' for 1M rows, '' for full dataset
        extra_args: additional bench_vs.py arguments

    Returns True on success.
    """
    bench_script = BENCHMARKS_DIR / "bench_vs.py"
    cmd = [
        "py-spy", "record",
        "--native",           # capture Rust frames (needs debug symbols for readable names)
        "--format", "flamegraph",
        "--output", str(output_svg),
        "--",
        sys.executable, str(bench_script),
        "--round", str(round_num),
        "--iterations", "1",
        "--warmup", "0",
    ]
    if data_flag:
        cmd.append(data_flag)
    if extra_args:
        cmd.extend(extra_args)

    print(f"  [profiler] py-spy: round {round_num} -> {output_svg.name}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            print(f"  [profiler] py-spy failed (rc={result.returncode}):")
            print(f"    {result.stderr[:500]}")
            return False
        return output_svg.exists()
    except subprocess.TimeoutExpired:
        print("  [profiler] py-spy timed out (300s)")
        return False
    except FileNotFoundError:
        print("  [profiler] py-spy not installed, skipping")
        return False


def profile_with_perf(
    round_num: int,
    output_svg: Path,
    data_flag: str = "--sample",
) -> bool:
    """Profile using Linux perf + inferno (more accurate Rust frame resolution).

    Requires:
      - perf installed
      - cargo install inferno
      - maturin develop --profile flamegraph  (for readable Rust symbols)
    """
    bench_script = BENCHMARKS_DIR / "bench_vs.py"
    perf_data = output_svg.parent / f"perf_r{round_num}.data"

    # Step 1: perf record
    record_cmd = [
        "perf", "record",
        "-F", "99",       # 99 Hz sampling frequency
        "-g",             # call graphs
        "--call-graph", "dwarf",
        "-o", str(perf_data),
        "--",
        sys.executable, str(bench_script),
        "--round", str(round_num),
        "--iterations", "1",
        "--warmup", "0",
    ]
    if data_flag:
        record_cmd.append(data_flag)

    print(f"  [profiler] perf record: round {round_num}...")
    try:
        result = subprocess.run(
            record_cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            print(f"  [profiler] perf record failed: {result.stderr[:300]}")
            return False
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"  [profiler] perf unavailable: {e}")
        return False

    # Step 2: perf script | inferno-collapse-perf | inferno-flamegraph
    print(f"  [profiler] generating flamegraph -> {output_svg.name}")
    try:
        perf_script = subprocess.run(
            ["perf", "script", "-i", str(perf_data)],
            cwd=str(REPO_ROOT), capture_output=True, timeout=120
        )
        collapse = subprocess.run(
            ["inferno-collapse-perf"],
            input=perf_script.stdout, capture_output=True, timeout=60
        )
        with open(output_svg, "wb") as f:
            flamegraph = subprocess.run(
                ["inferno-flamegraph"],
                input=collapse.stdout, stdout=f, timeout=60
            )
        perf_data.unlink(missing_ok=True)
        return output_svg.exists() and flamegraph.returncode == 0
    except Exception as e:
        print(f"  [profiler] flamegraph generation failed: {e}")
        return False


def profile_round(
    round_num: int,
    output_svg: Path,
    data_flag: str = "--sample",
    prefer: str = "pyspy",
) -> bool:
    """Profile a benchmark round using available tools.

    Args:
        prefer: 'pyspy' (default) or 'perf'
    """
    deps = check_dependencies()

    if prefer == "perf" and deps["perf"] and deps["inferno-collapse-perf"]:
        return profile_with_perf(round_num, output_svg, data_flag)

    if deps["py-spy"]:
        return profile_with_pyspy(round_num, output_svg, data_flag)

    if deps["perf"] and deps["inferno-collapse-perf"]:
        return profile_with_perf(round_num, output_svg, data_flag)

    print_install_hints(deps)
    return False


def profile_python_layer(output_svg: Path, data_flag: str = "--sample") -> bool:
    """Profile Python expression serialization overhead using bench_core.py."""
    bench_script = BENCHMARKS_DIR / "bench_core.py"
    deps = check_dependencies()

    if not deps["py-spy"]:
        print("  [profiler] py-spy not installed, skipping Python layer profiling")
        return False

    cmd = [
        "py-spy", "record",
        "--format", "flamegraph",
        "--output", str(output_svg),
        "--",
        sys.executable, str(bench_script),
    ]

    print(f"  [profiler] Python layer profiling -> {output_svg.name}")
    try:
        result = subprocess.run(
            cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=180
        )
        if result.returncode != 0:
            print(f"  [profiler] bench_core profiling failed: {result.stderr[:300]}")
            return False
        return output_svg.exists()
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"  [profiler] failed: {e}")
        return False
