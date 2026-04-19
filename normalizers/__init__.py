"""
normalizers/__init__.py — Auto-discovery dei normalizer source-specific.

Per aggiungere una sorgente: creare normalizers/<source>.py e chiamare
register("<source_id>", <fn>) in fondo al file. Nessun altro file va modificato.
"""

from __future__ import annotations

import importlib
import pkgutil

from normalizers.registry import normalize, normalize_all, registered_sources, REGISTRY

_EXCLUDED: frozenset[str] = frozenset({"registry", "utils"})

# _modname è lasciato nel namespace per evitare NameError su iteratore vuoto.
for _importer, _modname, _ispkg in pkgutil.iter_modules(__path__):
    if _modname not in _EXCLUDED:
        importlib.import_module(f"normalizers.{_modname}")

__all__ = ["normalize", "normalize_all", "registered_sources", "REGISTRY"]
