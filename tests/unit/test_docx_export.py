import pytest

from server.docx_export import _split_inline


@pytest.mark.unit
def test_split_inline_latex_bold_and_small_caps():
    segments = _split_inline(r"All \textsc{Artsiv}: \textbf{54.0}")

    assert ("small_caps", "Artsiv", None) in segments
    assert ("latex_bold", "54.0", None) in segments

