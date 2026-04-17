"""
normalizers/__init__.py

Entry point del package normalizers/.

Scopre e importa automaticamente tutti i moduli source-specific tramite
pkgutil, attivando le auto-registrazioni nel registry.

--- Come aggiungere una nuova sorgente ---
1. Creare normalizers/<source>.py con la logica di estrazione
2. Chiamare register("<source_id>", <fn>) in fondo al nuovo file
Nessun altro file va modificato — il modulo viene scoperto automaticamente.

--- Come rimuovere una sorgente ---
Eliminare il file normalizers/<source>.py.
Il registry ignorerà automaticamente quella sorgente.

--- Moduli esclusi dall'auto-discovery ---
- registry.py  : il dispatcher centrale (non è un normalizer)
- utils.py     : helper condivisi (non è un normalizer)
"""

from __future__ import annotations

import importlib
import pkgutil

from normalizers.registry import normalize, normalize_all, registered_sources

# Moduli interni del package da non importare come normalizer
_EXCLUDED: frozenset[str] = frozenset({"registry", "utils"})

# Auto-discovery: importa ogni modulo non escluso, triggering register()
for _importer, _modname, _ispkg in pkgutil.iter_modules(__path__):
    if _modname not in _EXCLUDED:
        importlib.import_module(f"normalizers.{_modname}")

del _importer, _modname, _ispkg  # pulizia namespace

__all__ = ["normalize", "normalize_all", "registered_sources"]
