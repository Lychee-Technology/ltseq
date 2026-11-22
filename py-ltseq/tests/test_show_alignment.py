"""``show()`` measures, truncates, and pads cells and headers by display width (#316).

Previously ``calculate_column_widths`` used UTF-8 byte length (``str::len``) and
row/header formatting used character count (``{:<width$}``), misaligning tables
containing CJK text, combining marks, or other multi-byte characters.
"""

import unicodedata

import pyarrow as pa

from ltseq import LTSeq


def _display_width(text: str) -> int:
    """Display width for ASCII, CJK, and combining-mark strings used in tests."""
    total = 0
    for ch in text:
        if unicodedata.category(ch) in ("Mn", "Me"):
            continue
        total += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return total


def _divider_columns(line: str, marker: str) -> list[int]:
    cols = []
    pos = 0
    for ch in line:
        if ch == marker:
            cols.append(pos)
        if unicodedata.category(ch) not in ("Mn", "Me"):
            pos += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return cols


def _assert_aligned(table: str) -> None:
    lines = [line for line in table.splitlines() if line]
    assert len(lines) >= 4, table
    expected_width = _display_width(lines[0])
    expected_cols = _divider_columns(lines[0], "+")
    assert len(expected_cols) >= 2, table
    for idx, line in enumerate(lines):
        assert _display_width(line) == expected_width, f"line {idx} width mismatch:\n{table}"
        marker = "+" if line.startswith("+") else "|"
        assert _divider_columns(line, marker) == expected_cols, (
            f"line {idx} divider positions mismatch:\n{table}"
        )


def test_issue_316_cjk_and_ascii_align(capsys):
    t = LTSeq.from_rows(
        [
            {"pair": "啤酒 + 尿布", "n": 2},
            {"pair": "beer + chips", "n": 2},
        ]
    )
    t.show()
    out = capsys.readouterr().out
    expected = (
        "+--------------+---+\n"
        "| pair         | n |\n"
        "+--------------+---+\n"
        "| 啤酒 + 尿布  | 2 |\n"
        "| beer + chips | 2 |\n"
        "+--------------+---+\n"
    )
    assert out == expected + "\n"
    _assert_aligned(out)


def test_cjk_column_names_and_empty_table(capsys):
    t = LTSeq.from_rows(
        [
            {"商品": "a", "数量": 1},
            {"商品": "重要价值客户", "数量": 42},
        ]
    )
    t.show()
    out = capsys.readouterr().out
    expected = (
        "+--------------+------+\n"
        "| 商品         | 数量 |\n"
        "+--------------+------+\n"
        "| a            | 1    |\n"
        "| 重要价值客户 | 42   |\n"
        "+--------------+------+\n"
    )
    assert out == expected + "\n"
    _assert_aligned(out)

    empty = t.head(0)
    empty.show()
    empty_out = capsys.readouterr().out
    _assert_aligned(empty_out)
    assert "| 商品 | 数量 |" in empty_out


def test_accented_and_combining_marks_align(capsys):
    t = LTSeq.from_rows(
        [
            {"label": "café", "v": 1},
            {"label": "cafe\u0301", "v": 2},
            {"label": "abcd", "v": 3},
        ]
    )
    t.show()
    out = capsys.readouterr().out
    expected = (
        "+-------+---+\n"
        "| label | v |\n"
        "+-------+---+\n"
        "| café  | 1 |\n"
        "| cafe\u0301  | 2 |\n"
        "| abcd  | 3 |\n"
        "+-------+---+\n"
    )
    assert out == expected + "\n"
    _assert_aligned(out)


def test_display_width_truncation_cjk_boundary(capsys):
    fits = "中" * 25  # 50 display columns, 75 UTF-8 bytes — must not truncate
    too_wide = "中" * 26  # 52 display columns — truncates to 23 chars (46 cols) + "..." (49 cols)
    t = LTSeq.from_rows([{"s": fits}, {"s": too_wide}])
    t.show()
    out = capsys.readouterr().out
    _assert_aligned(out)
    lines = [line for line in out.splitlines() if line.startswith("|")]
    assert lines[1] == f"| {fits} |"
    assert lines[2] == f"| {'中' * 23}...  |"


def test_long_cjk_header_truncates_and_aligns(capsys):
    long_header = "列" * 30  # 60 display columns > 50
    t = LTSeq.from_arrow(pa.table({long_header: ["短", "中" * 30]}))
    t.show()
    out = capsys.readouterr().out
    _assert_aligned(out)
    lines = [line for line in out.splitlines() if line.startswith("|")]
    assert lines[0] == f"| {'列' * 23}...  |"


def test_repr_repr_html_and_linked_show_align(capsys):
    orders = LTSeq.from_rows([{"id": 1, "item": "啤酒"}, {"id": 2, "item": "beer"}])
    prices = LTSeq.from_rows([{"id": 1, "note": "促销"}, {"id": 2, "note": "sale"}])

    repr_table = "\n".join(repr(orders).splitlines()[2:])
    _assert_aligned(repr_table)

    html = orders._repr_html_()
    assert "| 啤酒 |" in html
    assert "| beer |" in html

    linked = orders.link(prices, on=lambda o, p: o.id == p.id, as_="p")
    linked.show()
    linked_out = capsys.readouterr().out
    _assert_aligned(linked_out)
