# LTSeq Python wrapper — requires native `ltseq_core` extension.
# This module intentionally does not provide Python fallbacks for read_csv/show
# to ensure we exercise the native DataFusion implementation during Phase 2.

from .ltseq_core import RustTable  # type: ignore


class LTSeq:
    """Python-visible LTSeq wrapper backed by the native Rust kernel."""

    def __init__(self):
        self._inner = RustTable()

    @classmethod
    def read_csv(cls, path: str) -> "LTSeq":
        t = cls()
        # Delegate to native implementation which will populate internal state
        t._inner.read_csv(path)
        return t

    def show(self, n: int = 10) -> None:
        # Delegate to native show — native code is responsible for printing
        # a preview. We ignore the return string if any.
        out = self._inner.show(n)
        if out is not None:
            print(out)

    def hello(self) -> str:
        return self._inner.hello()


__all__ = ["LTSeq"]
