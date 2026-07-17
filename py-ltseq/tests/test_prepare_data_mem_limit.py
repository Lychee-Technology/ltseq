"""Regression tests for prepare_data.py's automatic DuckDB memory cap.

Guards review finding P2: the auto-detected ``memory_limit`` must stay strictly
below the detected cgroup/host ceiling, especially for small containers, so the
out-of-core sort spills instead of getting OOM-killed.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

MiB = 1024**2


def load_prepare_data_module():
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "benchmarks" / "prepare_data.py"
    spec = importlib.util.spec_from_file_location("ltseq_prepare_data", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "ceiling_mb",
    [64, 100, 200, 256, 300, 512, 1024, 4096, 32 * 1024],
)
def test_auto_limit_stays_below_ceiling(ceiling_mb):
    prepare_data = load_prepare_data_module()
    limit_mb = prepare_data._auto_memory_limit_mb(ceiling_mb * MiB)
    # The whole point of the cap: never configure at or above the ceiling.
    assert limit_mb < ceiling_mb, (
        f"ceiling={ceiling_mb}MiB -> limit={limit_mb}MiB exceeds/matches ceiling"
    )
    assert limit_mb >= 1


def test_small_container_uses_sixty_percent():
    """A small container is capped at 60% of its ceiling (no floor)."""
    prepare_data = load_prepare_data_module()
    assert prepare_data._auto_memory_limit_mb(100 * MiB) == 60


def test_mid_size_box_is_not_lifted_to_a_floor():
    """300 MiB ceiling -> 180 MiB (60%), never lifted to a 256 MiB floor.

    The old 256 MiB floor handed a 300 MiB cgroup 85% of its budget and left a
    257 MiB cgroup ~1 MiB of headroom, reintroducing the OOM the cap prevents.
    """
    prepare_data = load_prepare_data_module()
    assert prepare_data._auto_memory_limit_mb(300 * MiB) == 180


def test_tight_container_keeps_real_headroom():
    """A ~256 MiB cgroup must sit well below its ceiling, not at it."""
    prepare_data = load_prepare_data_module()
    limit = prepare_data._auto_memory_limit_mb(257 * MiB)
    assert limit == int(257 * 0.6)  # 154 MiB
    assert 257 - limit >= 64, "too little headroom for Python/DuckDB/FS overhead"


def test_large_box_uses_sixty_percent():
    prepare_data = load_prepare_data_module()
    assert prepare_data._auto_memory_limit_mb(32 * 1024 * MiB) == int(
        32 * 1024 * 0.6
    )
