"""GpuTable: GPU-resident table backed by cuDF for zero-CPU Parquet IO.

Data path:
    read_parquet_gpu(path)
        → cudf.read_parquet()   ← libcudf GPU Parquet decoder (no CPU decode!)
        → cuDF DataFrame (GPU VRAM)
        → GpuTable

    GpuTable.filter(lambda r: r.x > 100)
        → captures expression dict (same system as LTSeq)
        → [simple Column CMP Literal on numeric type]
              → gpu_filter_raw() Rust PyFunction
              → CUDA kernel runs on cuDF's device ptr (no H2D!)
              → mask stays in GPU VRAM
              → cupy applies mask GPU-side (no D2H!)
        → [complex expression]
              → cudf_eval.py evaluates using cuDF API (all GPU)
        → new GpuTable (data stays in GPU VRAM)

    GpuTable.to_ltseq()
        → cudf_df.to_arrow()    ← single D2H (PCIe transfer, result only)
        → LTSeq.from_arrow()    ← Arrow IPC bridge to DataFusion
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from .core import LTSeq

try:
    import cudf
    _CUDF_AVAILABLE = True
except ImportError:
    _CUDF_AVAILABLE = False

try:
    from . import ltseq_core as _core
    _GPU_KERNELS_AVAILABLE = (
        _CUDF_AVAILABLE
        and hasattr(_core, "gpu_filter_raw")
        and hasattr(_core, "gpu_free_ptr")
        and _core.gpu_available()
    )
except Exception:
    _GPU_KERNELS_AVAILABLE = False

# cuDF dtype string → CUDA kernel dtype string
_DTYPE_MAP: dict[str, str] = {
    "int8":    "i32",
    "int16":   "i32",
    "int32":   "i32",
    "int64":   "i64",
    "uint8":   "i32",
    "uint16":  "i32",
    "uint32":  "i64",
    "uint64":  "i64",
    "float32": "f32",
    "float64": "f64",
}

# Comparison operators supported by the CUDA kernel path
_KERNEL_OPS: frozenset[str] = frozenset({"gt", "lt", "gte", "lte", "eq", "neq"})


class GpuGroupBy:
    """Intermediate object returned by :meth:`GpuTable.groupby`.

    Call :meth:`agg` to produce the aggregated :class:`GpuTable`.
    Data stays in GPU VRAM throughout.
    """

    def __init__(self, gdf: "cudf.DataFrame", keys: list[str]) -> None:
        self._gdf = gdf
        self._keys = keys

    def agg(self, spec: dict) -> "GpuTable":
        """Aggregate columns.

        Args:
            spec: Mapping of ``{col_name: agg_func}`` or
                  ``{col_name: [agg_func, ...]}``.
                  Supported functions: ``"sum"``, ``"count"``, ``"mean"``,
                  ``"min"``, ``"max"``.

        Returns:
            New GpuTable with group keys + aggregated columns.

        Example::
            result = t.groupby("region").agg({"revenue": "sum", "qty": "mean"})
        """
        gb = self._gdf.groupby(self._keys, as_index=False)
        result = gb.agg(spec)
        # Flatten multi-level column names produced by multi-agg (e.g. ["sum", "mean"])
        if hasattr(result.columns, "to_flat_index"):
            result.columns = [
                "_".join(str(c) for c in col).strip("_")
                for col in result.columns.to_flat_index()
            ]
        return GpuTable(result)


class GpuTable:
    """A GPU-resident table backed by cuDF.

    Operations (filter, select, derive, sort, limit, rename, groupby, join)
    stay in GPU VRAM. Call `.to_ltseq()` for a single D2H transfer and
    DataFusion integration.

    Typical usage::

        t = LTSeq.read_parquet_gpu("hits_sorted.parquet")
        result = (
            t.filter(lambda r: r.counterid > 5000)
             .groupby("region").agg({"revenue": "sum"})
             .sort("revenue", ascending=False)
             .limit(10)
             .to_ltseq()
        )
        result.show()
    """

    def __init__(self, gdf: "cudf.DataFrame") -> None:
        self._gdf = gdf

    # ── Factory ─────────────────────────────────────────────────────────────

    @classmethod
    def read_parquet(
        cls,
        path: str,
        columns: Optional[list[str]] = None,
    ) -> "GpuTable":
        """Read a Parquet file directly to GPU VRAM using cuDF's GPU Parquet decoder.

        Unlike `LTSeq.read_parquet()`, this bypasses CPU entirely: libcudf decodes
        the Parquet file (decompression + column decoding) directly on the GPU.

        Args:
            path:    Path to the Parquet file.
            columns: Optional list of column names to read (column pruning).

        Returns:
            GpuTable with data in GPU VRAM.

        Raises:
            RuntimeError: If cuDF is not installed.
        """
        if not _CUDF_AVAILABLE:
            raise RuntimeError(
                "cuDF is required for GPU Parquet reading. "
                "Install it with: pip install cudf-cu12 (or matching CUDA version)"
            )
        kwargs: dict = {}
        if columns is not None:
            kwargs["columns"] = columns
        gdf = cudf.read_parquet(path, **kwargs)
        return cls(gdf)

    # ── Core operations (all GPU-resident) ──────────────────────────────────

    def filter(self, predicate_fn: Callable) -> "GpuTable":
        """Apply a filter predicate, keeping matching rows in GPU VRAM.

        Uses the Rust CUDA kernel path (no H2D!) for simple `Column OP Literal`
        comparisons on numeric columns. Falls back to cuDF's GPU API for complex
        expressions.

        Args:
            predicate_fn: Lambda accepting a SchemaProxy row object.
                          Same syntax as `LTSeq.filter()`.

        Returns:
            New GpuTable with filtered rows, data stays in GPU VRAM.

        Example::
            filtered = t.filter(lambda r: r.counterid > 5000)
            filtered = t.filter(lambda r: (r.age >= 18) & (r.score < 100))
        """
        from .expr.proxy import SchemaProxy
        from .expr.cudf_eval import eval_expr_as_mask, extract_simple_cmp, is_simple_cmp

        proxy = SchemaProxy(list(self._gdf.columns))
        expr_obj = predicate_fn(proxy)
        expr_dict = expr_obj.serialize()

        if _GPU_KERNELS_AVAILABLE and is_simple_cmp(expr_dict):
            return self._filter_via_rust_kernel(expr_dict, extract_simple_cmp)
        else:
            mask = eval_expr_as_mask(self._gdf, expr_dict)
            return GpuTable(self._gdf[mask])

    def _filter_via_rust_kernel(self, expr_dict: dict, extract_fn: Callable) -> "GpuTable":
        """Apply filter using the Rust CUDA kernel on cuDF's raw device pointer.

        Data path (all GPU, zero H2D):
            cuDF column.data.ptr → Rust CUDA kernel → GPU mask → cupy apply → cuDF result
        """
        try:
            import cupy as cp
        except ImportError:
            raise RuntimeError(
                "cupy is required for the GPU kernel filter path. "
                "Install with: pip install cupy-cuda12x"
            )

        col_name, kernel_op, _, threshold = extract_fn(expr_dict)
        col = self._gdf[col_name]
        col_dtype_str = str(col.dtype)
        kernel_dtype = _DTYPE_MAP.get(col_dtype_str)

        if kernel_dtype is None or kernel_op not in _KERNEL_OPS:
            # Unsupported type/op — fall back to cuDF eval
            from .expr.cudf_eval import eval_expr_as_mask
            mask = eval_expr_as_mask(self._gdf, expr_dict)
            return GpuTable(self._gdf[mask])

        n = len(col)
        col_ptr = col.data.ptr  # raw CUDA device pointer (Python int)

        # Run Rust CUDA kernel directly on cuDF's device memory — no H2D!
        # Returns the device pointer of the u8 mask (1=keep, 0=drop).
        mask_ptr = _core.gpu_filter_raw(col_ptr, n, kernel_dtype, kernel_op, float(threshold))
        try:
            # Apply mask GPU-side via cupy — no D2H!
            # UnownedMemory wraps the raw device ptr without taking ownership.
            mask_mem = cp.cuda.UnownedMemory(mask_ptr, n, self._gdf)
            mask_ptr_obj = cp.cuda.MemoryPointer(mask_mem, 0)
            mask_cupy = cp.ndarray(n, dtype=cp.uint8, memptr=mask_ptr_obj).view(cp.bool_)
            filtered_gdf = self._gdf[cudf.Series(mask_cupy)]
        finally:
            # Always free the mask, even if cupy application raises
            _core.gpu_free_ptr(mask_ptr, n)

        return GpuTable(filtered_gdf)

    def select(self, *columns: str) -> "GpuTable":
        """Select a subset of columns. Data stays in GPU VRAM.

        Args:
            columns: Column names to keep.

        Returns:
            New GpuTable with selected columns.
        """
        return GpuTable(self._gdf[list(columns)])

    def derive(self, **kwargs: Callable) -> "GpuTable":
        """Add derived columns using GPU expressions. Data stays in GPU VRAM.

        Args:
            kwargs: {new_col_name: lambda r: <expression>}

        Returns:
            New GpuTable with additional columns.

        Example::
            t2 = t.derive(ratio=lambda r: r.clicks / r.impressions)
        """
        from .expr.proxy import SchemaProxy
        from .expr.cudf_eval import eval_expr_as_series

        gdf = self._gdf.copy()
        for col_name, fn in kwargs.items():
            proxy = SchemaProxy(list(gdf.columns))
            expr_dict = fn(proxy).serialize()
            gdf[col_name] = eval_expr_as_series(gdf, expr_dict)
        return GpuTable(gdf)

    def sort(self, by, ascending=True) -> "GpuTable":
        """Sort rows by one or more columns. Data stays in GPU VRAM.

        Args:
            by:        Column name or list of column names to sort by.
            ascending: True (default) for ascending, False for descending.
                       Can be a single bool (applied to all keys) or a list
                       matching the length of ``by``.

        Returns:
            New GpuTable sorted in the requested order.

        Example::
            t.sort("revenue", ascending=False)
            t.sort(["region", "revenue"], ascending=[True, False])
        """
        if isinstance(by, str):
            by = [by]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)
        return GpuTable(self._gdf.sort_values(by=by, ascending=ascending))

    def limit(self, n: int) -> "GpuTable":
        """Keep only the first ``n`` rows. Data stays in GPU VRAM.

        Args:
            n: Number of rows to keep.

        Returns:
            New GpuTable with at most ``n`` rows.
        """
        return GpuTable(self._gdf.head(n))

    def rename(self, **kwargs: str) -> "GpuTable":
        """Rename columns. Data stays in GPU VRAM.

        Args:
            kwargs: ``old_name=new_name`` keyword pairs.

        Returns:
            New GpuTable with renamed columns.

        Example::
            t.rename(l_extendedprice="price", l_quantity="qty")
        """
        return GpuTable(self._gdf.rename(columns=kwargs))

    def groupby(self, keys) -> "GpuGroupBy":
        """Start a groupby aggregation.

        Args:
            keys: Column name or list of column names to group by.

        Returns:
            :class:`GpuGroupBy` — call ``.agg()`` on it to get results.

        Example::
            t.groupby("region").agg({"revenue": "sum"})
            t.groupby(["region", "year"]).agg({"qty": ["sum", "mean"]})
        """
        if isinstance(keys, str):
            keys = [keys]
        return GpuGroupBy(self._gdf, keys)

    def join(self, other: "GpuTable", on, how: str = "inner") -> "GpuTable":
        """Hash join with another GpuTable. Both tables stay in GPU VRAM.

        Args:
            other: Right-hand GpuTable.
            on:    Column name or list of column names to join on.
            how:   Join type: ``"inner"`` (default), ``"left"``, ``"right"``,
                   or ``"outer"``.

        Returns:
            New GpuTable with joined rows, data stays in GPU VRAM.

        Example::
            orders.join(customers, on="custkey")
            lineitem.join(orders, on=["orderkey"], how="left")
        """
        return GpuTable(self._gdf.merge(other._gdf, on=on, how=how))

    # ── Materialization ─────────────────────────────────────────────────────

    def to_ltseq(self) -> "LTSeq":
        """Materialize to LTSeq via a single D2H transfer.

        Data flow:
            cuDF DataFrame (GPU VRAM)
            → cudf.to_arrow()          ← PCIe D2H (this is the ONLY D2H in the pipeline)
            → LTSeq.from_arrow()       ← Arrow IPC bridge → DataFusion MemTable

        Returns:
            LTSeq instance backed by DataFusion, ready for further operations.
        """
        from .io_ops import IOMixin  # avoid circular; use from_arrow class method

        arrow_table = self._gdf.to_arrow()

        # Reuse the existing from_arrow() bridge (Arrow IPC → Rust MemTable)
        # Import here to avoid circular dependency at module load time
        from .core import LTSeq
        return LTSeq.from_arrow(arrow_table)

    def to_pandas(self):
        """Convert to pandas DataFrame (triggers D2H transfer).

        Returns:
            pandas DataFrame.
        """
        return self._gdf.to_pandas()

    def to_arrow(self):
        """Convert to PyArrow Table (triggers D2H transfer).

        Returns:
            pyarrow.Table.
        """
        return self._gdf.to_arrow()

    # ── Inspection ──────────────────────────────────────────────────────────

    def show(self, n: int = 10) -> "GpuTable":
        """Print the first `n` rows.

        Args:
            n: Number of rows to display (default 10).

        Returns:
            self (for chaining).
        """
        print(self._gdf.head(n).to_string())
        return self

    def __len__(self) -> int:
        return len(self._gdf)

    def __repr__(self) -> str:
        return (
            f"GpuTable({len(self._gdf)} rows × {len(self._gdf.columns)} cols, "
            f"columns={list(self._gdf.columns)})"
        )

    @property
    def columns(self) -> list[str]:
        """List of column names."""
        return list(self._gdf.columns)

    @property
    def schema(self) -> dict[str, str]:
        """Column name → dtype string mapping."""
        return {col: str(self._gdf[col].dtype) for col in self._gdf.columns}
