"""``LTSeq.show()`` prints the table and returns ``None`` (contract §7.9, #313).

It used to return the table, so a Jupyter cell ending in ``t.show()`` printed
the table and then rendered the returned table a second time through
``_repr_html_``.
"""

import pytest

from ltseq import LTSeq


def test_show_prints_and_returns_none(capsys):
    t = LTSeq.from_dict({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    assert t.show() is None
    rows = [line for line in capsys.readouterr().out.splitlines() if line.startswith("|")]
    # Header plus three rows.
    assert len(rows) == 4
    assert "id" in rows[0] and "v" in rows[0]


def test_show_n_returns_none(capsys):
    t = LTSeq.from_dict({"id": list(range(20))})
    assert t.show(3) is None
    rows = [line for line in capsys.readouterr().out.splitlines() if line.startswith("|")]
    # Header plus three rows.
    assert len(rows) == 4


def test_notebook_cell_ending_in_show_displays_the_table_once():
    nbformat = pytest.importorskip("nbformat")
    nbclient = pytest.importorskip("nbclient")
    pytest.importorskip("ipykernel")

    nb = nbformat.v4.new_notebook()
    nb.cells.append(
        nbformat.v4.new_code_cell(
            "from ltseq import LTSeq\n"
            't = LTSeq.from_dict({"id": [1, 2, 3], "v": ["a", "b", "c"]})\n'
            "t.show()"
        )
    )
    # The python3 kernel is the one `pytest --nbmake tutorials/` runs the
    # tutorials in.
    nbclient.NotebookClient(nb, timeout=120, kernel_name="python3").execute()

    outputs = nb.cells[0].outputs
    assert [o.output_type for o in outputs] == ["stream"], outputs
    assert outputs[0].name == "stdout"
    text = outputs[0].text
    assert sum(1 for line in text.splitlines() if line.startswith("|")) == 4  # header + 3 rows
