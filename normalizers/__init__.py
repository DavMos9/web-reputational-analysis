"""
normalizers/__init__.py

Entry point del package normalizers/.

Importa tutti i moduli source-specific, attivando le auto-registrazioni
nel registry. Il semplice `from normalizers import normalize` è sufficiente
per avere tutti i normalizer pronti.

--- Come aggiungere una nuova sorgente ---
1. Creare normalizers/<source>.py con la logica di estrazione
2. Chiamare register("<source_id>", <fn>) in fondo al nuovo file
3. Aggiungere l'import qui sotto
Nessun altro file va modificato.

--- Come rimuovere una sorgente ---
Commentare o rimuovere l'import corrispondente.
Il registry ignorerà automaticamente quella sorgente.
"""

from __future__ import annotations

from normalizers.registry import normalize, normalize_all, registered_sources

# Trigger delle auto-registrazioni — ordine non rilevante
import normalizers.news             # noqa: F401
import normalizers.gdelt            # noqa: F401
import normalizers.youtube          # noqa: F401
import normalizers.youtube_comments # noqa: F401
import normalizers.wikipedia        # noqa: F401
import normalizers.guardian         # noqa: F401
import normalizers.nyt              # noqa: F401
import normalizers.bluesky          # noqa: F401
import normalizers.stackexchange    # noqa: F401
import normalizers.mastodon         # noqa: F401
import normalizers.lemmy            # noqa: F401
import normalizers.wikitalk         # noqa: F401

__all__ = ["normalize", "normalize_all", "registered_sources"]
