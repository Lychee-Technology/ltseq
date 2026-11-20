# pyright: reportUnnecessaryTypeIgnoreComment=true
"""`LTSeq.show()` returns `None` (contract §7.9, #313).

The stub must say so, so that a chain continuing from `show()` is a type
error rather than an `AttributeError` at run time.
"""

from typing import assert_type

from ltseq import LTSeq


def show_returns_none(t: LTSeq) -> None:
    assert_type(t.show(), None)
    assert_type(t.show(5), None)


def no_chain_after_show(t: LTSeq) -> None:
    t.show().count()  # pyright: ignore[reportAttributeAccessIssue]
