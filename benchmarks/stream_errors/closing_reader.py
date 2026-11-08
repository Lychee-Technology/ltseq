"""Build and load OD-X6's native-subclass alternative (REPORT.md, option (e)).

`_closing_reader.pyx` subclasses `pyarrow.RecordBatchReader` in Cython so
that `close()` reaches the Rust iterator `from_batches` reads. Compiling it
needs Cython and a C++20 compiler, and the build is tied to the pyarrow it
was compiled against: it is written under
`target/stream_errors/closing_reader/pyarrow-<version>-<python>/` and rebuilt
when the source is newer.

    from closing_reader import load, to_batches
    reader = to_batches(t)          # a pyarrow.RecordBatchReader
    reader.close()                  # stops the execution
"""

from __future__ import annotations

import importlib.util
import sys
import sysconfig
from pathlib import Path
from types import ModuleType
from typing import Any

import pyarrow as pa

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "_closing_reader.pyx"
BUILD = (
    HERE.parents[1]
    / "target"
    / "stream_errors"
    / "closing_reader"
    / f"pyarrow-{pa.__version__}-{sys.implementation.cache_tag}"
)
NAME = "_closing_reader"

_module: ModuleType | None = None


def _library(directory: Path, name: str) -> str:
    """The linker flag for `lib<name>.so` in pyarrow's directory.

    A pyarrow wheel ships `libarrow.so.<abi>` without the unversioned
    symlink (`pyarrow.create_library_symlinks()` would write it into
    site-packages), so link the versioned file by name.
    """
    found = sorted(directory.glob(f"lib{name}.so*"), key=lambda p: len(p.name))
    if not found:
        raise RuntimeError(f"no lib{name}.so in {directory}")
    return f"-l:{found[0].name}"


def _built() -> Path | None:
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    path = BUILD / f"{NAME}{suffix}"
    if path.exists() and path.stat().st_mtime >= SOURCE.stat().st_mtime:
        return path
    return None


def build() -> Path:
    """Compile the extension for this pyarrow and Python; return its path."""
    from Cython.Build import cythonize
    from setuptools import Distribution, Extension

    libdir = Path(pa.get_library_dirs()[0])
    include = [pa.get_include()]
    try:
        import numpy

        include.append(numpy.get_include())
    except ImportError:  # pragma: no cover - pyarrow's headers need it only for numpy interop
        pass
    BUILD.mkdir(parents=True, exist_ok=True)
    ext = Extension(
        NAME,
        [str(SOURCE)],
        language="c++",
        include_dirs=include,
        extra_compile_args=["-std=c++20", "-O2", "-w"],
        extra_link_args=[_library(libdir, "arrow_python"), _library(libdir, "arrow")],
        library_dirs=[str(libdir)],
        runtime_library_dirs=[str(libdir)],
    )
    modules = cythonize(
        [ext], build_dir=str(BUILD / "cython"), quiet=True, compiler_directives={"language_level": 3}
    )
    dist = Distribution({"name": NAME, "ext_modules": modules})
    cmd = dist.get_command_obj("build_ext")
    cmd.build_lib = str(BUILD)
    cmd.build_temp = str(BUILD / "tmp")
    cmd.inplace = False
    cmd.ensure_finalized()
    cmd.run()
    path = _built()
    if path is None:
        raise RuntimeError(f"building {SOURCE.name} wrote no module under {BUILD}")
    return path


def load() -> ModuleType:
    """The compiled module, built first if there is none for this pyarrow."""
    global _module
    if _module is None:
        path = _built() or build()
        spec = importlib.util.spec_from_file_location(NAME, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _module = module
    return _module


def to_batches(table: Any, fault: Any = None, gil: str = "detach") -> pa.RecordBatchReader:
    """`_stream_spike.to_batches`, as a reader whose `close()` stops the execution."""
    from ltseq import _stream_spike as spike

    it = spike.batches(table, fault, gil)
    return load().closing_reader(pa.RecordBatchReader.from_batches(it.schema(), it), it)
