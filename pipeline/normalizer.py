"""pipeline/normalizer.py — Shim: re-esporta normalize() da normalizers/. NON modificare."""

from normalizers import normalize, normalize_all  # noqa: F401

__all__ = ["normalize", "normalize_all"]
