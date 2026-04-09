"""
tests/test_slugify.py

Test per utils/slugify.py.

Copertura:
- target_slug(): spazi → underscore
- target_slug(): caratteri speciali rimossi
- target_slug(): lowercase
- target_slug(): underscore multipli collassati
- target_slug(): underscore iniziali/finali rimossi
- target_slug(): troncamento a max_len
- target_slug(): stringa vuota
- now_timestamp(): formato atteso "YYYYMMDDTHHMMSSz"
"""

from __future__ import annotations

import re
import pytest

from utils.slugify import target_slug, now_timestamp


class TestTargetSlug:
    def test_spaces_to_underscores(self):
        assert target_slug("Elon Musk") == "elon_musk"

    def test_lowercase(self):
        assert target_slug("ELON MUSK") == "elon_musk"

    def test_special_characters_removed(self):
        assert target_slug("Apple Inc.") == "apple_inc"

    def test_hyphens_to_underscores(self):
        assert target_slug("Coca-Cola") == "coca_cola"

    def test_multiple_spaces_collapsed(self):
        assert target_slug("Giorgia   Meloni") == "giorgia_meloni"

    def test_leading_trailing_underscores_removed(self):
        slug = target_slug("  Elon Musk  ")
        assert not slug.startswith("_")
        assert not slug.endswith("_")

    def test_consecutive_underscores_collapsed(self):
        result = target_slug("A -- B")
        assert "__" not in result

    def test_truncation_at_max_len(self):
        long_name = "A" * 50
        result = target_slug(long_name, max_len=10)
        assert len(result) <= 10

    def test_empty_string(self):
        assert target_slug("") == ""

    def test_single_word(self):
        assert target_slug("Ferrari") == "ferrari"


class TestNowTimestamp:
    def test_format_matches_pattern(self):
        """Il timestamp deve avere il formato YYYYMMDDTHHMMSSz."""
        ts = now_timestamp()
        pattern = re.compile(r"^\d{8}T\d{6}Z$")
        assert pattern.match(ts), f"Formato non atteso: {ts!r}"

    def test_returns_string(self):
        assert isinstance(now_timestamp(), str)

    def test_length(self):
        """Lunghezza attesa: 8 + 1 + 6 + 1 = 16 caratteri."""
        assert len(now_timestamp()) == 16
