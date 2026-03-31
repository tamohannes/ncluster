"""Unit tests for logbook #id reference extraction."""

import pytest
from server.logbooks import _extract_entry_refs


@pytest.mark.unit
class TestExtractEntryRefs:
    def test_single_ref(self):
        assert _extract_entry_refs("See #42 for details") == [42]

    def test_multiple_refs(self):
        refs = _extract_entry_refs("Based on #10 and #20, see also #30")
        assert sorted(refs) == [10, 20, 30]

    def test_duplicate_refs_deduplicated(self):
        refs = _extract_entry_refs("Ref #5 and again #5")
        assert refs == [5]

    def test_no_refs(self):
        assert _extract_entry_refs("Plain text without references") == []

    def test_empty_body(self):
        assert _extract_entry_refs("") == []

    def test_none_body(self):
        assert _extract_entry_refs(None) == []

    def test_ref_in_code_block(self):
        refs = _extract_entry_refs("```\nprint(#1)\n```\nSee #2")
        assert 1 in refs
        assert 2 in refs

    def test_ref_at_start_of_line(self):
        assert _extract_entry_refs("#99 is the main plan") == [99]

    def test_non_numeric_hash_ignored(self):
        assert _extract_entry_refs("Use #heading for markdown") == []
