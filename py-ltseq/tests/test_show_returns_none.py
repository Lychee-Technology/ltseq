"""``LTSeq.show()`` prints the table and returns ``None`` (contract §7.9, #313).

It used to return the table, so a Jupyter cell ending in ``t.show()`` printed
the table and then rendered the returned table a second time through
``_repr_html_``.
"""

import json
import sys

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


def test_notebook_cell_ending_in_show_displays_the_table_once(tmp_path):
    nbformat = pytest.importorskip("nbformat")
    nbclient = pytest.importorskip("nbclient")
    pytest.importorskip("ipykernel")
    from jupyter_client.kernelspec import KernelSpecManager
    from jupyter_client.manager import KernelManager

    nb = nbformat.v4.new_notebook()
    nb.cells.append(
        nbformat.v4.new_code_cell(
            "from ltseq import LTSeq\n"
            't = LTSeq.from_dict({"id": [1, 2, 3], "v": ["a", "b", "c"]})\n'
            "t.show()"
        )
    )
    # Run the kernel on the interpreter running pytest, so it imports the
    # ltseq under test whatever `python3` kernelspec happens to be installed.
    # Only the private kernel directory is searched, and the native kernel
    # fallback is off, so no ambient kernelspec can take its place.
    spec_dir = tmp_path / "kernels" / "ltseq-test"
    spec_dir.mkdir(parents=True)
    (spec_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                "display_name": "ltseq test",
                "language": "python",
            }
        )
    )
    ksm = KernelSpecManager(kernel_dirs=[str(tmp_path / "kernels")], ensure_native_kernel=False)
    km = KernelManager(kernel_name="ltseq-test", kernel_spec_manager=ksm)
    assert km.kernel_spec.argv[0] == sys.executable
    nbclient.NotebookClient(nb, timeout=120, km=km).execute()

    outputs = nb.cells[0].outputs
    assert [o.output_type for o in outputs] == ["stream"], outputs
    assert outputs[0].name == "stdout"
    text = outputs[0].text
    assert sum(1 for line in text.splitlines() if line.startswith("|")) == 4  # header + 3 rows
