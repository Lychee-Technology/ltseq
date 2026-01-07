# LTSeq Python wrapper — requires native `ltseq_core` extension.
# This module intentionally does not provide Python fallbacks for read_csv/show
# to ensure we exercise the native DataFusion implementation during Phase 2.

from typing import Callable, Dict, Any, Optional
from .ltseq_core import RustTable  # type: ignore
from .expr import _lambda_to_expr


class LTSeq:
    """Python-visible LTSeq wrapper backed by the native Rust kernel."""

    def __init__(self):
        self._inner = RustTable()
        self._schema: Dict[str, str] = {}

    @classmethod
    def read_csv(cls, path: str) -> "LTSeq":
        t = cls()
        # Delegate to native implementation which will populate internal state
        t._inner.read_csv(path)
        # TODO: Extract schema from RustTable after it's populated
        # For now, schema remains empty until Phase 2 integration
        return t

    def show(self, n: int = 10) -> None:
        # Delegate to native show — native code is responsible for printing
        # a preview. We ignore the return string if any.
        out = self._inner.show(n)
        if out is not None:
            print(out)

    def hello(self) -> str:
        return self._inner.hello()

    def _capture_expr(self, fn: Callable) -> Dict[str, Any]:
        """
        Capture and serialize an expression tree from a lambda.

        This internal method intercepts Python lambdas and converts them to
        serializable expression dicts without executing Python logic.

        Args:
            fn: Lambda function, e.g., lambda r: r.age > 18

        Returns:
            Serialized expression dict ready for Rust deserialization

        Raises:
            TypeError: If lambda doesn't return an Expr
            AttributeError: If lambda references a non-existent column
            ValueError: If schema is not initialized (call read_csv first)

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> expr = t._capture_expr(lambda r: r.age > 18)
            >>> expr["type"]
            'BinOp'
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )
        return _lambda_to_expr(fn, self._schema)

    def filter(self, predicate: Callable) -> "LTSeq":
        """
        Filter rows where predicate lambda returns True.

        Args:
            predicate: Lambda that takes a row and returns a boolean Expr.
                      E.g., lambda r: r.age > 18

        Returns:
            A new LTSeq with filtered data

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return a boolean Expr
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> filtered = t.filter(lambda r: r.age > 18)
            >>> filtered.show()
        """
        expr_dict = self._capture_expr(predicate)

        # Create a new LTSeq with the filtered result
        result = LTSeq()
        result._schema = self._schema.copy()
        # Delegate filtering to Rust implementation
        result._inner = self._inner.filter(expr_dict)
        return result

    def select(self, *cols) -> "LTSeq":
        """
        Select specific columns or derived expressions.

        Supports both column references and computed columns via lambdas.

        Args:
            *cols: Either string column names or lambdas that compute new columns.
                   E.g., select("name", "age") or select("name", lambda r: r.price * r.qty)

        Returns:
            A new LTSeq with selected/computed columns

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return an Expr
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> selected = t.select("name", "age")
            >>> computed = t.select("name", lambda r: r.price * r.qty)
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        exprs = []
        for col in cols:
            if isinstance(col, str):
                # Column name reference
                if col not in self._schema:
                    raise AttributeError(
                        f"Column '{col}' not found in schema. "
                        f"Available columns: {list(self._schema.keys())}"
                    )
                exprs.append({"type": "Column", "name": col})
            elif callable(col):
                # Lambda that computes a derived column
                exprs.append(self._capture_expr(col))
            else:
                raise TypeError(
                    f"select() argument must be str or callable, got {type(col).__name__}"
                )

        # Create a new LTSeq with selected columns
        result = LTSeq()
        # Update schema based on selected columns
        # For now, we keep the full schema (proper schema reduction requires Rust support)
        result._schema = self._schema.copy()
        # Delegate selection to Rust implementation
        result._inner = self._inner.select(exprs)
        return result

    def derive(self, **kwargs: Callable) -> "LTSeq":
        """
        Create new derived columns based on lambda expressions.

        New columns are added to the dataframe; existing columns remain.

        Args:
            **kwargs: Column name -> lambda expression mapping.
                     E.g., derive(total=lambda r: r.price * r.qty, age_group=lambda r: r.age // 10)

        Returns:
            A new LTSeq with added derived columns

        Raises:
            ValueError: If schema is not initialized
            TypeError: If lambda doesn't return an Expr
            AttributeError: If lambda references a non-existent column

        Example:
            >>> t = LTSeq.read_csv("data.csv")
            >>> derived = t.derive(total=lambda r: r.price * r.qty)
            >>> derived.show()
        """
        if not self._schema:
            raise ValueError(
                "Schema not initialized. Call read_csv() first to populate the schema."
            )

        derived_cols = {}
        for col_name, fn in kwargs.items():
            if not callable(fn):
                raise TypeError(
                    f"derive() argument '{col_name}' must be callable, got {type(fn).__name__}"
                )
            derived_cols[col_name] = self._capture_expr(fn)

        # Create a new LTSeq with derived columns
        result = LTSeq()
        # Update schema: add derived columns (assuming Int64 for now as placeholder)
        result._schema = self._schema.copy()
        for col_name in derived_cols:
            # In Phase 2, we'd infer the dtype from the expression
            result._schema[col_name] = "Unknown"
        # Delegate derivation to Rust implementation
        result._inner = self._inner.derive(derived_cols)
        return result


__all__ = ["LTSeq"]
