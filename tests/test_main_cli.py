"""
tests/test_main_cli.py

Test di regressione sulle costanti di main.py che governano la selezione
delle fonti di default.

Obiettivo: garantire che le fonti opt-in (non adatte al caso d'uso generico)
non vengano incluse silenziosamente tra le fonti di default.
"""

from __future__ import annotations

import main


class TestSourceDefaults:
    def test_stackexchange_is_opt_in(self):
        """StackExchange non deve far parte delle fonti di default.

        Motivazione: SE fa full-text match sul body delle domande → nomi propri
        compaiono come stringhe di test in esempi di codice, generando record
        tematicamente irrilevanti. Resta richiamabile esplicitamente via
        --sources per target tech (librerie, framework, prodotti software).
        """
        assert "stackexchange" in main.OPT_IN_SOURCES
        assert "stackexchange" not in main.DEFAULT_SOURCES

    def test_default_sources_is_all_minus_opt_in(self):
        """DEFAULT_SOURCES = ALL_SOURCES \\ OPT_IN_SOURCES (coerenza)."""
        assert set(main.DEFAULT_SOURCES) == set(main.ALL_SOURCES) - main.OPT_IN_SOURCES

    def test_opt_in_sources_are_subset_of_all(self):
        """Ogni fonte opt-in deve comunque esistere nel registry."""
        assert main.OPT_IN_SOURCES.issubset(set(main.ALL_SOURCES))

    def test_all_sources_non_empty(self):
        """Sanity check: il registry non è vuoto."""
        assert len(main.ALL_SOURCES) > 0
        assert len(main.DEFAULT_SOURCES) > 0
