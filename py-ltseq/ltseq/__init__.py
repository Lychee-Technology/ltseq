from .ltseq_core import RustTable


class LTSeq:
    def __init__(self):
        self._inner = RustTable()

    def hello(self):
        return self._inner.hello()


__all__ = ["LTSeq"]
