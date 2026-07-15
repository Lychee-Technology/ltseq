"""LinkedTable: a deferred, prefix-aliased equi-join created by LTSeq.link()."""

from typing import TYPE_CHECKING, Callable

from .helpers import _extract_join_keys

if TYPE_CHECKING:
    from .core import LTSeq


class LinkedTable:
    """A deferred, prefix-aliased equi-join between a source and a target table.

    Created by :meth:`LTSeq.link`. ``link()`` records the join condition and
    the target's alias but executes nothing; it computes the joined schema up
    front — target columns namespaced as ``{alias}_{col}``, source columns
    kept as-is — so column references resolve without a round trip.

    Every transform (``filter``/``select``/``derive``/``sort``/``slice``/
    ``distinct``) builds the lazy join plan and runs on top of it, returning a
    plain :class:`LTSeq`. Because a transform applies AFTER the join, its
    row semantics follow the join: an inner/right/full join drops or adds
    unmatched rows, and a one-to-many match fans a source row out to several
    result rows — a following ``slice``/``filter`` sees the joined rows, not
    the original source rows. Reach for :meth:`to_ltseq` to get the lazy
    joined table explicitly, or :meth:`collect` to execute it.

    ``link()`` itself returns a new ``LinkedTable`` layered on top of the
    current join plan, so multi-hop links compose: the second link's
    condition may reference the first link's ``{alias}_{col}`` columns, and
    its schema is computed from the real joined plan (not a stitched preview).

    This is a lazy equi-join, not a pointer/take structure: there is no cheap
    per-row navigation, only a DataFusion join that stays lazy until
    materialized.
    """

    def __init__(
        self,
        source_table: "LTSeq",
        target_table: "LTSeq",
        join_fn: Callable,
        alias: str,
        join_type: str = "inner",
    ):
        """
        Initialize a LinkedTable from source and target tables.

        Args:
            source_table: The primary table (e.g., orders). For a chained
                link this is the previous link's joined plan.
            target_table: The table being linked (e.g., products)
            join_fn: Lambda specifying the equi-join condition
            alias: Prefix applied to every target column (e.g., "prod")
            join_type: One of "inner", "left", "right", "full". Default: "inner"
        """
        self._source = source_table
        self._target = target_table
        self._join_fn = join_fn
        self._alias = alias
        self._join_type = join_type
        # Ask Rust for the joined schema without executing the join — the same
        # build_prefixed_join_schema the join execution path uses, so the
        # {alias}_{col} naming convention has exactly one implementation.
        self._schema = source_table._inner.preview_join_schema(
            target_table._inner, alias
        )
        self._plan: "LTSeq | None" = None  # cached lazy joined LTSeq

    def _ensure_join_plan(self) -> "LTSeq":
        """
        Build (once) and return the lazy joined ``LTSeq``.

        No data is executed here: ``join_prefixed`` produces a DataFusion
        join LogicalPlan that stays lazy until the caller materializes it
        (collect / to_pandas / len / ...). The plan is cached so repeated
        transforms reuse one join node.
        """
        if self._plan is not None:
            return self._plan

        left_key_expr, right_key_expr, join_type = _extract_join_keys(
            self._join_fn,
            self._source._schema,
            self._target._schema,
            self._join_type,
        )

        # Prefix-aliased join: link() namespaces the whole target table as
        # `{alias}_col` (distinct from join()'s Polars-style conflict-only
        # suffix). The executed plan's schema comes from the Rust kernel.
        joined_inner = self._source._inner.join_prefixed(
            self._target._inner,
            left_key_expr,
            right_key_expr,
            join_type,
            self._alias,
        )

        from .core import LTSeq

        self._plan = LTSeq._from_inner(joined_inner)
        return self._plan

    def to_ltseq(self) -> "LTSeq":
        """Return the lazy joined table as a plain ``LTSeq`` (no execution)."""
        return self._ensure_join_plan()

    def collect(self) -> "LTSeq":
        """Execute the join and return an in-memory ``LTSeq`` (LTSeq.collect semantics)."""
        return self._ensure_join_plan().collect()

    def show(self, n: int = 10) -> None:
        """Display the joined table (materializes up to ``n`` rows)."""
        self._ensure_join_plan().show(n)

    def __len__(self) -> int:
        """Row count of the joined result (materializes the join)."""
        return len(self._ensure_join_plan())

    def filter(self, predicate: Callable) -> "LTSeq":
        """
        Filter the joined rows.

        The predicate resolves against the joined schema, so it may reference
        source columns (original names) and linked columns (``{alias}_col``).
        Returns a plain ``LTSeq``.
        """
        return self._ensure_join_plan().filter(predicate)

    def select(self, *cols: "str | Callable") -> "LTSeq":
        """
        Select columns from the joined table.

        Selecting source-only columns still executes the join, so unmatched
        rows and one-to-many fan-out are reflected in the result (there is no
        source-only shortcut). Returns a plain ``LTSeq``.
        """
        return self._ensure_join_plan().select(*cols)

    def derive(self, *args, **kwargs) -> "LTSeq":
        """Derive columns over the joined table; may reference linked columns."""
        return self._ensure_join_plan().derive(*args, **kwargs)

    def sort(self, *key_exprs, **kwargs) -> "LTSeq":
        """Sort the joined table (after the join, not the source)."""
        return self._ensure_join_plan().sort(*key_exprs, **kwargs)

    def slice(self, offset: int, length: int) -> "LTSeq":
        """Slice the joined rows (offset + length, over the joined result)."""
        return self._ensure_join_plan().slice(offset, length)

    def distinct(self, key_fn: Callable | None = None) -> "LTSeq":
        """Deduplicate the joined rows."""
        plan = self._ensure_join_plan()
        return plan.distinct() if key_fn is None else plan.distinct(key_fn)

    def link(
        self, target_table: "LTSeq", on: Callable, as_: str, join_type: str = "inner"
    ) -> "LinkedTable":
        """
        Chain another link on top of this one.

        The new link's source is this link's joined plan, so its ``on``
        condition may reference this link's ``{alias}_col`` columns and its
        schema is computed from the real joined plan.

        Args:
            target_table: The table to link to
            on: Lambda specifying the equi-join condition
            as_: Prefix for the new target's columns
            join_type: One of "inner", "left", "right", "full". Default: "inner"

        Returns:
            A new ``LinkedTable`` layered on the current join plan.
        """
        base = self._ensure_join_plan()
        return LinkedTable(base, target_table, on, as_, join_type)
