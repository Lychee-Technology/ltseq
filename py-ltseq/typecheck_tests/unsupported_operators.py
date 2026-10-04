# pyright: reportUnnecessaryTypeIgnoreComment=true
"""Operators the DSL refuses at runtime are also type errors.

`-expr` and `**` raise NotImplementedError when the lambda is captured. The
stubs must keep rejecting them, so the mistake shows in the editor instead of
at run time. Each line below carries an ignore for the error it has to
produce; if the stubs start accepting the operator, pyright reports the
ignore as unnecessary and this check fails.
"""

from ltseq import LTSeq


def unsupported_operators(t: LTSeq) -> None:
    t.derive(v=lambda r: -r.x)  # pyright: ignore[reportOperatorIssue]
    t.derive(v=lambda r: r.x**2)  # pyright: ignore[reportOperatorIssue]
    t.derive(v=lambda r: 2**r.x)  # pyright: ignore[reportOperatorIssue]
    t.filter(lambda r: -r.x > 1)  # pyright: ignore[reportOperatorIssue]


def supported_operators(t: LTSeq) -> None:
    t.derive(v=lambda r: r.x // 2, w=lambda r: 7 // r.x, z=lambda r: 0 - r.x)
    t.filter(lambda r: abs(r.x) % 2 == 1)
