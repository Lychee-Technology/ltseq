"""The #251 measurement prototypes (cargo feature `m0`) and their switch.

A default build must not carry them: `prototypes()` refuses there, so a
measurement cannot silently time today's engine as a prototype. On a
measurement build the block sets exactly the flags it names and restores
the previous ones on exit.
"""

import pytest

from ltseq import ltseq_core
from ltseq._testing import prototypes, prototypes_available

measurement_build = pytest.mark.skipif(
    not prototypes_available(), reason="needs a measurement build (--features m0)"
)


def test_default_build_refuses_prototypes():
    if prototypes_available():
        pytest.skip("this extension is a measurement build (--features m0)")
    with pytest.raises(RuntimeError, match="--features m0"):
        with prototypes(checked="fast"):
            pass


@measurement_build
def test_flags_apply_inside_the_block_and_restore_on_exit():
    before = dict(ltseq_core._m0_prototypes())
    with prototypes(checked="fast", order="interleaved", order_window=4) as outer:
        assert outer["checked"] == "fast"
        assert outer["order"] == "interleaved"
        assert outer["order_window"] == "4"
        with prototypes(views="plain") as inner:
            # A flag the inner block leaves out is off, not inherited.
            assert inner["checked"] == "off"
            assert inner["views"] == "plain"
        assert dict(ltseq_core._m0_prototypes()) == outer
    assert dict(ltseq_core._m0_prototypes()) == before


@measurement_build
def test_unknown_flag_or_value_is_refused():
    before = dict(ltseq_core._m0_prototypes())
    with pytest.raises(ValueError, match="unknown prototype flag"):
        with prototypes(nope="1"):
            pass
    with pytest.raises(ValueError, match="sorted"):
        with prototypes(order="sorted"):
            pass
    assert dict(ltseq_core._m0_prototypes()) == before
