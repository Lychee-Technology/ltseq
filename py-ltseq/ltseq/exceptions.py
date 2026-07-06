"""LTSeq exception hierarchy.

All library errors derive from ``LTSeqError`` so callers can catch the whole
family with one ``except``. Each concrete error also subclasses the builtin it
historically raised (``ValueError``/``AttributeError``), so existing
``except ValueError:`` / ``hasattr()`` code keeps working unchanged.
"""


class LTSeqError(Exception):
    """Base class for all LTSeq errors."""


class SortRequiredError(LTSeqError, ValueError):
    """An operation requires sorted input but the table is not sorted.

    Subclasses ``ValueError`` for backward compatibility (the sort checks used
    to raise ``ValueError``). The message carries a fix hint, e.g.
    "call .sort('date') first".
    """


class SchemaMismatchError(LTSeqError, ValueError):
    """Two tables have incompatible schemas for the requested operation."""


class ColumnNotFoundError(LTSeqError, ValueError, AttributeError):
    """A referenced column does not exist in the table schema.

    Multiple inheritance is load-bearing:
    - ``AttributeError`` keeps ``hasattr()`` / attribute-access probes working
      (SchemaProxy raises this from ``__getattr__``).
    - ``ValueError`` preserves ``except ValueError:`` call sites and the Rust
      ``ColumnNotFound`` mapping.
    """


__all__ = [
    "LTSeqError",
    "SortRequiredError",
    "SchemaMismatchError",
    "ColumnNotFoundError",
]
