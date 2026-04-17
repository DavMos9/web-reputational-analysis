"""
tests/test_deduplicator.py

Test per pipeline/deduplicator.py.

Copertura:
- lista vuota → lista vuota, 0 rimossi
- nessun duplicato → tutti i record passano
- URL identici → il secondo è rimosso (livello 1)
- URL con parametri tracking diversi → rimosso come duplicato (livello 1)
- URL AMP vs canonical con stesso titolo+dominio → rimosso (livello 2)
- stesso titolo + dominio, URL diverso → rimosso (livello 2)
- titolo duplicato ma dominio diverso → NON rimosso
- record con URL vuoto → incluso (non confrontato per URL)
- case insensitive su URL e titolo
"""

from __future__ import annotations

import pytest

from models import Record
from pipeline.deduplicator import deduplicate, _canonical_url, _canonical_title


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(
    url: str,
    title: str = "Default Title",
    domain: str = "example.com",
    source: str = "news",
) -> Record:
    return Record(
        source=source,
        query="test query",
        target="Test Target",
        title=title,
        text="Some text.",
        date="2026-04-08",
        url=url,
        domain=domain,
    )


# ---------------------------------------------------------------------------
# Test: _canonical_url
# ---------------------------------------------------------------------------

class TestCanonicalUrl:
    def test_removes_utm_params(self):
        url = "https://example.com/article?utm_source=twitter&utm_campaign=x"
        assert _canonical_url(url) == "https://example.com/article"

    def test_removes_trailing_slash(self):
        assert _canonical_url("https://example.com/path/") == "https://example.com/path"

    def test_removes_fragment(self):
        assert _canonical_url("https://example.com/path#section") == "https://example.com/path"

    def test_preserve_fragment_keeps_anchor(self):
        """Con preserve_fragment=True il fragment va conservato (caso wikitalk)."""
        url = "https://en.wikipedia.org/wiki/Talk:Donald_Trump#Current_consensus"
        assert _canonical_url(url, preserve_fragment=True) == url.lower()

    def test_lowercases_scheme_and_host(self):
        assert _canonical_url("HTTPS://Example.COM/Path") == "https://example.com/path"

    def test_preserves_non_tracking_params(self):
        url = "https://example.com/search?q=elon+musk&page=2"
        result = _canonical_url(url)
        assert "q=elon" in result
        assert "page=2" in result

    def test_empty_string_returns_empty(self):
        assert _canonical_url("") == ""


# ---------------------------------------------------------------------------
# Test: _canonical_title
# ---------------------------------------------------------------------------

class TestCanonicalTitle:
    def test_lowercases(self):
        assert _canonical_title("Elon Musk on Tesla") == "elon musk on tesla"

    def test_removes_punctuation(self):
        assert _canonical_title("Hello, World!") == "hello world"

    def test_collapses_whitespace(self):
        assert _canonical_title("  too   many   spaces  ") == "too many spaces"

    def test_empty_string_returns_empty(self):
        assert _canonical_title("") == ""


# ---------------------------------------------------------------------------
# Test: deduplicate()
# ---------------------------------------------------------------------------

class TestDeduplicateEmpty:
    def test_empty_list(self):
        unique, removed = deduplicate([])
        assert unique == []
        assert removed == 0


class TestDeduplicateNoDuplicates:
    def test_all_unique(self):
        records = [
            _record("https://example.com/a", title="Article A"),
            _record("https://example.com/b", title="Article B"),
            _record("https://example.com/c", title="Article C"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 3
        assert removed == 0


class TestDeduplicateLevelOne:
    """Livello 1: URL canonico identico."""

    def test_exact_same_url(self):
        records = [
            _record("https://example.com/article-1", title="Article A"),
            _record("https://example.com/article-1", title="Article A"),  # duplicato
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1

    def test_url_with_tracking_params(self):
        """Stesso URL con parametri UTM diversi → duplicato."""
        records = [
            _record("https://example.com/article?utm_source=twitter"),
            _record("https://example.com/article?utm_source=facebook"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1

    def test_url_with_trailing_slash(self):
        """URL con e senza trailing slash → duplicato."""
        records = [
            _record("https://example.com/article"),
            _record("https://example.com/article/"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1

    def test_url_case_insensitive(self):
        """URL con case diverso → duplicato."""
        records = [
            _record("https://EXAMPLE.com/Article"),
            _record("https://example.com/article"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1


class TestDeduplicateLevelTwo:
    """Livello 2: stesso titolo + stesso dominio, URL diverso."""

    def test_same_title_same_domain(self):
        records = [
            _record("https://example.com/article-v1", title="Breaking News Today", domain="example.com"),
            _record("https://example.com/article-v2", title="Breaking News Today", domain="example.com"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1

    def test_same_title_different_domain_not_removed(self):
        """Stesso titolo ma dominio diverso → NON è duplicato."""
        records = [
            _record("https://site-a.com/article", title="Same Title", domain="site-a.com"),
            _record("https://site-b.com/article", title="Same Title", domain="site-b.com"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 2
        assert removed == 0

    def test_title_punctuation_ignored(self):
        """Titoli che differiscono solo per punteggiatura sono duplicati."""
        records = [
            _record("https://example.com/v1", title="Breaking: News Today!", domain="example.com"),
            _record("https://example.com/v2", title="Breaking News Today",   domain="example.com"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1


class TestDeduplicateEdgeCases:
    def test_record_first_is_kept(self):
        """Il primo record è sempre mantenuto; i successivi duplicati sono rimossi."""
        records = [
            _record("https://example.com/article", title="Article A"),
            _record("https://example.com/article", title="Article A Copy"),  # URL dup
        ]
        unique, removed = deduplicate(records)
        assert unique[0].title == "Article A"

    def test_multiple_duplicates_of_same_record(self):
        """Tre copie dello stesso URL → 1 unico, 2 rimossi."""
        records = [
            _record("https://example.com/article"),
            _record("https://example.com/article"),
            _record("https://example.com/article"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 2

    def test_counter_is_accurate(self):
        """Il contatore removed riflette esattamente i duplicati eliminati."""
        records = [
            _record("https://a.com/1", title="A", domain="a.com"),
            _record("https://b.com/2", title="B", domain="b.com"),   # domain esplicito
            _record("https://a.com/1", title="A dup", domain="a.com"),  # URL dup → rimosso
            _record("https://c.com/3", title="B", domain="b.com"),       # title+domain dup → rimosso
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 2
        assert removed == 2


# ---------------------------------------------------------------------------
# Test: fragment preservation per wikitalk
# ---------------------------------------------------------------------------

class TestWikitalkFragmentPreservation:
    """
    Wikipedia Talk Pages usano lo stesso URL base con fragment #Section diversi.
    Ogni sezione è una conversazione distinta → dedup NON deve collassarle.
    """

    def _talk_record(self, section: str, title: str) -> Record:
        url = f"https://en.wikipedia.org/wiki/Talk:Donald_Trump#{section}"
        return Record(
            source="wikitalk",
            query="Donald Trump",
            target="Donald Trump",
            title=title,
            text="Discussion content for this specific section.",
            date=None,
            url=url,
            domain="en.wikipedia.org",
        )

    def test_wikitalk_sections_are_preserved(self):
        records = [
            self._talk_record("Current_consensus", "[Talk] Donald Trump: Current consensus"),
            self._talk_record("References", "[Talk] Donald Trump: References"),
            self._talk_record("Several_issues", "[Talk] Donald Trump: Several issues"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 3
        assert removed == 0

    def test_wikitalk_true_duplicate_still_removed(self):
        """Se due record wikitalk hanno lo stesso fragment, uno va rimosso comunque."""
        records = [
            self._talk_record("Current_consensus", "[Talk] Donald Trump: Current consensus"),
            self._talk_record("Current_consensus", "[Talk] Donald Trump: Current consensus"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1

    def test_non_wikitalk_fragment_still_stripped(self):
        """Per sorgenti diverse da wikitalk, il fragment è ancora scartato (regressione)."""
        records = [
            _record("https://example.com/article#intro", title="A"),
            _record("https://example.com/article#conclusion", title="A dup"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1


# ---------------------------------------------------------------------------
# Test: sorgenti parent-child (titolo ereditato dal contenitore)
# ---------------------------------------------------------------------------

class TestYoutubeCommentsDedup:
    """
    Tutti i commenti di uno stesso video YouTube ereditano il titolo del video.
    Il livello 2 del dedup (title+domain) li collasserebbe erroneamente a uno
    solo: per questa fonte il livello 2 va disattivato. L'identità è nel
    query-param `lc=<comment_id>` dell'URL (livello 1).
    """

    def _yt_comment(self, comment_id: str, text: str = "A distinct comment.") -> Record:
        return Record(
            source="youtube_comments",
            query="Donald Trump",
            target="Donald Trump",
            title="BREAKING NEWS: Trump Takes Reporters' Questions",
            text=text,
            date="2026-04-16",
            url=f"https://www.youtube.com/watch?v=2aa_6C4x8rc&lc={comment_id}",
            domain="youtube.com",
        )

    def test_comments_same_video_are_kept(self):
        """18 commenti allo stesso video → tutti e 18 sopravvivono."""
        records = [self._yt_comment(f"comment_{i}", f"Commento numero {i}") for i in range(18)]
        unique, removed = deduplicate(records)
        assert len(unique) == 18
        assert removed == 0

    def test_comment_vs_parent_video_both_kept(self):
        """Il video parent (source=youtube) e i suoi commenti (source=youtube_comments)
        condividono titolo+dominio: il video NON deve oscurare i commenti."""
        video = Record(
            source="youtube",
            query="Donald Trump",
            target="Donald Trump",
            title="BREAKING NEWS: Trump Takes Reporters' Questions",
            text="Video description.",
            date="2026-04-16",
            url="https://www.youtube.com/watch?v=2aa_6C4x8rc",
            domain="youtube.com",
        )
        comments = [self._yt_comment(f"c_{i}") for i in range(5)]
        records = [video, *comments]
        unique, removed = deduplicate(records)
        # video (1) + 5 commenti distinti per URL = 6
        assert len(unique) == 6
        assert removed == 0

    def test_comment_exact_url_duplicate_still_removed(self):
        """Livello 1 (URL canonico) resta attivo: URL identico → duplicato rimosso."""
        records = [
            self._yt_comment("same_id", text="originale"),
            self._yt_comment("same_id", text="duplicato"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1

    def test_other_source_title_dedup_still_active(self):
        """Regressione: per fonti NON whitelistate, il dedup livello 2 resta attivo."""
        records = [
            _record("https://a.com/1", title="Stesso Titolo", domain="example.com", source="news"),
            _record("https://a.com/2", title="Stesso Titolo", domain="example.com", source="news"),
        ]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 1
