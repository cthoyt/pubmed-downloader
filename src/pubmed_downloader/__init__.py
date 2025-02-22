"""Automate downloading and processing PubMed."""

from pubmed_downloader.constants import ISSN

from .api import (
    AbstractText,
    Article,
    Author,
    Heading,
    Journal,
    Qualifier,
    ensure_baselines,
    ensure_updates,
    iterate_ensure_articles,
    iterate_ensure_baselines,
    iterate_ensure_updates,
    iterate_process_articles,
    iterate_process_baselines,
    iterate_process_updates,
    process_articles,
    process_baselines,
    process_updates,
)

__all__ = [
    "ISSN",
    "AbstractText",
    "Article",
    "Author",
    "Heading",
    "Journal",
    "Qualifier",
    "ensure_baselines",
    "ensure_updates",
    "iterate_ensure_articles",
    "iterate_ensure_baselines",
    "iterate_ensure_updates",
    "iterate_process_articles",
    "iterate_process_baselines",
    "iterate_process_updates",
    "process_articles",
    "process_baselines",
    "process_updates",
]
