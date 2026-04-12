"""
pipeline/normalizer.py

Shim di compatibilità: re-esporta normalize() e normalize_all()
dal package normalizers/.

La logica source-specific è in normalizers/<source>.py.
Per aggiungere una sorgente → vedere normalizers/__init__.py.
NON modificare questo file.
"""

from normalizers import normalize, normalize_all  # noqa: F401

__all__ = ["normalize", "normalize_all"]
