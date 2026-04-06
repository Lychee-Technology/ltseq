import pytest
from ltseq import LTSeq


def test_join_merge_removed():
    t = LTSeq.from_rows([{"id": 1}])
    assert not hasattr(t, "join_merge"), "join_merge should be removed"


def test_join_sorted_removed():
    t = LTSeq.from_rows([{"id": 1}])
    assert not hasattr(t, "join_sorted"), "join_sorted should be removed"


def test_diff_set_alias_removed():
    t = LTSeq.from_rows([{"id": 1}])
    assert hasattr(t, "except_"), "except_() must still exist"
    assert not hasattr(t, "diff"), "diff() set-alias should be removed"
