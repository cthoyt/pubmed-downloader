"""Automate downloading and processing PubMed."""

from .api import ensure_baselines, ensure_updates, iterate_articles

__all__ = [
    "ensure_baselines",
    "ensure_updates",
    "iterate_articles",
]
