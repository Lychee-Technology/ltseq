"""Utility functions for LTSeq."""


def seq(start_or_stop, stop=None, step=1):
    """
    Generate an integer sequence as a single-column LTSeq.

    The interface mirrors Python's built-in ``range()``:

    - ``seq(5)``        → 0, 1, 2, 3, 4
    - ``seq(2, 7)``     → 2, 3, 4, 5, 6
    - ``seq(0, 10, 2)`` → 0, 2, 4, 6, 8

    Args:
        start_or_stop: If ``stop`` is None this is the stop value and start defaults
                       to 0.  Otherwise this is the start value.
        stop: Exclusive upper bound (optional).
        step: Step between successive values (default 1).

    Returns:
        LTSeq with a single column named ``"value"`` of type Int64.

    Example:
        >>> from ltseq import seq
        >>> seq(5).show()           # 0..4
        >>> seq(1, 6).show()        # 1..5
        >>> seq(0, 10, 2).show()    # 0,2,4,6,8
    """
    from .core import LTSeq

    if stop is None:
        start, stop_ = 0, start_or_stop
    else:
        start, stop_ = start_or_stop, stop

    rows = [{"value": i} for i in range(start, stop_, step)]
    schema = {"value": "Int64"}
    return LTSeq._from_rows(rows, schema)
