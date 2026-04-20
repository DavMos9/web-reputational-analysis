"""
tests/test_cleaner.py

Test per pipeline/cleaner.py.

Copertura:
- clean(): strip su campi required
- clean(): normalizzazione Unicode NFC
- clean(): decodifica HTML entities (title, text, author)
- clean(): campi optional — stringa vuota → None
- clean(): campi optional già None restano None
- clean(): Record già pulito non viene copiato inutilmente (stessa istanza)
- clean(): rimozione U+2028 LINE SEPARATOR e U+2029 PARAGRAPH SEPARATOR
- clean_all(): lista vuota e lista con record misti
"""

from __future__ import annotations

import pytest

from models import Record
from pipeline.cleaner import clean, clean_all


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(**overrides) -> Record:
    defaults = dict(
        source="news",
        query="test query",
        target="Test Target",
        title="Test Title",
        text="Test text.",
        date="2026-04-08",
        url="https://example.com/article",
        author=None,
        language=None,
        domain="example.com",
    )
    defaults.update(overrides)
    return Record(**defaults)


# ---------------------------------------------------------------------------
# Test: clean() — campi required
# ---------------------------------------------------------------------------

class TestCleanRequired:
    def test_strips_whitespace_from_title(self):
        r = _record(title="  Titolo con spazi  ")
        cleaned = clean(r)
        assert cleaned.title == "Titolo con spazi"

    def test_strips_whitespace_from_text(self):
        r = _record(text="\n  Testo con whitespace\t")
        cleaned = clean(r)
        assert cleaned.text == "Testo con whitespace"

    def test_strips_whitespace_from_url(self):
        r = _record(url="  https://example.com/article  ")
        cleaned = clean(r)
        assert cleaned.url == "https://example.com/article"

    def test_nfc_normalization(self):
        """Testo con caratteri Unicode composti viene normalizzato NFC."""
        # 'à' può essere rappresentata come U+00E0 (NFC) o come 'a' + U+0300 (NFD)
        nfd_char = "a\u0300"  # 'a' + combining grave accent (NFD)
        nfc_char = "\u00e0"   # 'à' precomposta (NFC)
        r = _record(title=nfd_char)
        cleaned = clean(r)
        assert cleaned.title == nfc_char

    def test_decodes_quot_entity_in_title(self):
        r = _record(title='Carlo Conti: &quot;Grazie a tutti&quot; - Sanremo 2026')
        cleaned = clean(r)
        assert cleaned.title == 'Carlo Conti: "Grazie a tutti" - Sanremo 2026'

    def test_decodes_amp_entity(self):
        r = _record(title="Economia &amp; Finanza")
        cleaned = clean(r)
        assert cleaned.title == "Economia & Finanza"

    def test_decodes_lt_gt_entities(self):
        r = _record(text="Valore &lt;100&gt; confermato")
        cleaned = clean(r)
        assert cleaned.text == "Valore <100> confermato"

    def test_decodes_apos_and_numeric_entity(self):
        r = _record(title="L&apos;articolo di &#39;oggi&#39;")
        cleaned = clean(r)
        assert cleaned.title == "L'articolo di 'oggi'"

    def test_decodes_numeric_unicode_entity(self):
        # &#8220; → " (left double quotation mark)
        r = _record(title="&#8220;Titolo tra virgolette&#8221;")
        cleaned = clean(r)
        assert cleaned.title == "\u201cTitolo tra virgolette\u201d"

    def test_decodes_nbsp_entity_to_space(self):
        # &nbsp; → \xa0 → spazio normale (whitespace orizzontale collassato)
        r = _record(title="Titolo&nbsp;con&nbsp;nbsp")
        cleaned = clean(r)
        assert cleaned.title == "Titolo con nbsp"

    def test_collapses_double_nbsp_separator(self):
        # Pattern Google News: "Titolo articolo\xa0\xa0Nome Fonte"
        r = _record(text="Paolo Ruffini: show\xa0\xa0Mediaset Infinity")
        cleaned = clean(r)
        assert cleaned.text == "Paolo Ruffini: show Mediaset Infinity"

    def test_collapses_multiple_spaces(self):
        r = _record(title="Titolo  con   spazi  multipli")
        cleaned = clean(r)
        assert cleaned.title == "Titolo con spazi multipli"

    def test_preserves_newlines_in_text(self):
        # \n è contenuto reale in commenti e post social — non va toccato
        r = _record(text="Prima riga\nSeconda riga\nTerza riga")
        cleaned = clean(r)
        assert cleaned.text == "Prima riga\nSeconda riga\nTerza riga"

    def test_removes_line_separator_u2028(self):
        """U+2028 LINE SEPARATOR causa warning VS Code nei file JSON esportati."""
        r = _record(text="testo con\u2028separatore di riga")
        cleaned = clean(r)
        assert "\u2028" not in cleaned.text
        assert cleaned.text == "testo conseparatore di riga"

    def test_removes_paragraph_separator_u2029(self):
        """U+2029 PARAGRAPH SEPARATOR — stesso problema di U+2028."""
        r = _record(title="titolo\u2029con PS")
        cleaned = clean(r)
        assert "\u2029" not in cleaned.title
        assert cleaned.title == "titolocon PS"

    def test_removes_both_unicode_separators_mixed(self):
        r = _record(text="a\u2028b\u2029c")
        cleaned = clean(r)
        assert cleaned.text == "abc"

    def test_decodes_html_entities_in_author(self):
        r = _record(author="Redazione &amp; Staff")
        cleaned = clean(r)
        assert cleaned.author == "Redazione & Staff"

    def test_already_clean_record_returns_equivalent(self):
        """Un Record già pulito non deve avere campi diversi dopo clean()."""
        r = _record()
        cleaned = clean(r)
        assert cleaned.title  == r.title
        assert cleaned.text   == r.text
        assert cleaned.url    == r.url
        assert cleaned.source == r.source


# ---------------------------------------------------------------------------
# Test: clean() — campi optional
# ---------------------------------------------------------------------------

class TestCleanOptional:
    def test_empty_author_becomes_none(self):
        r = _record(author="")
        cleaned = clean(r)
        assert cleaned.author is None

    def test_whitespace_only_author_becomes_none(self):
        r = _record(author="   ")
        cleaned = clean(r)
        assert cleaned.author is None

    def test_none_author_stays_none(self):
        r = _record(author=None)
        cleaned = clean(r)
        assert cleaned.author is None

    def test_valid_author_kept(self):
        r = _record(author="  Jane Doe  ")
        cleaned = clean(r)
        assert cleaned.author == "Jane Doe"

    def test_empty_language_becomes_none(self):
        r = _record(language="")
        cleaned = clean(r)
        assert cleaned.language is None

    def test_valid_language_kept(self):
        r = _record(language=" it ")
        cleaned = clean(r)
        assert cleaned.language == "it"


# ---------------------------------------------------------------------------
# Test: clean_all()
# ---------------------------------------------------------------------------

class TestCleanAll:
    def test_empty_list_returns_empty(self):
        assert clean_all([]) == []

    def test_returns_same_count(self):
        records = [_record(title=f"Title {i}") for i in range(5)]
        result = clean_all(records)
        assert len(result) == 5

    def test_all_records_are_cleaned(self):
        records = [
            _record(title="  Title A  ", author=""),
            _record(title="Title B",     author="  Author  "),
        ]
        result = clean_all(records)
        assert result[0].title  == "Title A"
        assert result[0].author is None
        assert result[1].author == "Author"
