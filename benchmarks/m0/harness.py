"""Runs #251's cells, each in a process of its own, and records the host.

A cell is one pipeline, terminal, input, layout and set of prototype flags
(``cases.py`` lists them). ``run_cell`` hands it to ``child.py`` as JSON and
reads back one JSON line; nothing a cell does survives into the next, so
peaks and allocator state are the cell's own.

The machine the measurements ran on is shared, so ``run_cell`` waits for
it to go quiet before each cell and records how much CPU other processes
took while the cell ran; ``report.py`` flags the cells they contended.
"""

from __future__ import annotations

import json
import os
import platform
import resource
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CHILD = HERE / "child.py"

# A cell whose resident memory grows past this is stopped and reported as
# aborted: the machine has 27 GiB, and the results must not swap.
ABORT_ABOVE = 8 << 30

# A cell starts once the whole machine is at most this busy over a second
# (about one of the 5600G's twelve threads), or after QUIET_TIMEOUT
# seconds of waiting, whichever comes first.
QUIET_SHARE = 0.08
QUIET_TIMEOUT = 1800


def _run(*args: str) -> str:
    # rustup's shims live in ~/.cargo/bin; RUSTUP_TOOLCHAIN, when the run
    # sets it, picks the toolchain the extension was built with.
    env = {**os.environ, "PATH": f"{Path.home() / '.cargo' / 'bin'}:{os.environ.get('PATH', '')}"}
    try:
        done = subprocess.run(args, capture_output=True, text=True, check=True, cwd=REPO, env=env)
        return done.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _cpu() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def _memory_bytes() -> int | None:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def host_info() -> dict:
    """What a reader needs to reproduce or compare the numbers."""
    import pyarrow

    sys.path.insert(0, str(REPO / "py-ltseq"))
    from ltseq import ltseq_core
    from ltseq._testing import prototypes_available

    build = ltseq_core._m0_build() if prototypes_available() else {}
    dirty = _run("git", "status", "--porcelain", "--untracked-files=no")
    return {
        "commit": _run("git", "rev-parse", "HEAD"),
        "worktree_dirty": bool(dirty) and dirty != "unknown",
        "branch": _run("git", "rev-parse", "--abbrev-ref", "HEAD"),
        "cpu": _cpu(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": _memory_bytes(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "pyarrow": pyarrow.__version__,
        "rustc": _run("rustc", "--version"),
        "m0_build": build,
        "command": " ".join([Path(sys.executable).name, *sys.argv]),
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def _cpu_seconds() -> tuple[float, float] | None:
    """Busy and total CPU seconds of every CPU since boot (``/proc/stat``)."""
    try:
        with open("/proc/stat") as f:
            # user nice system idle iowait irq softirq steal; guest time is
            # already counted in user.
            ticks = [int(v) for v in f.readline().split()[1:9]]
    except (OSError, ValueError):
        return None
    hz = os.sysconf("SC_CLK_TCK")
    total = sum(ticks) / hz
    return total - (ticks[3] + ticks[4]) / hz, total


def _children_cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def wait_for_quiet(share: float = QUIET_SHARE, timeout: float = QUIET_TIMEOUT) -> dict:
    """Sample the machine a second at a time until it is at most `share`
    busy; return the last sample and how long the wait took."""
    started = time.perf_counter()
    busy = None
    while True:
        before = _cpu_seconds()
        time.sleep(1)
        after = _cpu_seconds()
        if before is None or after is None:
            break
        busy = (after[0] - before[0]) / max(after[1] - before[1], 1e-9)
        if busy <= share or time.perf_counter() - started >= timeout:
            break
    return {"busy_before": busy, "quiet_wait": time.perf_counter() - started}


def run_cell(cell: dict, out_dir: Path, timeout: float = 1800) -> dict:
    """Run one cell in a fresh interpreter and return its result."""
    spec = {
        "out_dir": str(out_dir),
        "pipeline": cell["pipeline"],
        "terminal": cell["terminal"],
        "flags": cell.get("flags", {}),
        "target_partitions": cell["partitions"],
        "batch_size": cell.get("batch_size"),
        "warmup": cell["warmup"],
        "input": cell["input"],
        "repeat": cell.get("repeat", 3),
        "abort_above": cell.get("abort_above", ABORT_ABOVE),
    }
    quiet = wait_for_quiet()
    cpu_before, child_before = _cpu_seconds(), _children_cpu()
    started = time.perf_counter()
    try:
        done = subprocess.run(
            [sys.executable, str(CHILD), json.dumps(spec)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO,
        )
    except subprocess.TimeoutExpired:
        return {"aborted": True, "error": f"timed out after {timeout:.0f} s", "runs": []}
    lines = [line for line in done.stdout.splitlines() if line.startswith("{")]
    if done.returncode != 0 or not lines:
        tail = (done.stderr or done.stdout)[-2000:]
        return {"aborted": False, "error": f"child exited {done.returncode}: {tail}", "runs": []}
    result = json.loads(lines[-1])
    result["wall"] = time.perf_counter() - started
    result.update(quiet)
    cpu_after = _cpu_seconds()
    if cpu_before is not None and cpu_after is not None:
        # Whatever the machine was busy with besides the child, in cores
        # averaged over the cell; the kernel's own work for the child (page
        # cache, reclaim) counts here too.
        others = (cpu_after[0] - cpu_before[0]) - (_children_cpu() - child_before)
        result["other_cores"] = max(others, 0.0) / result["wall"]
    return result
