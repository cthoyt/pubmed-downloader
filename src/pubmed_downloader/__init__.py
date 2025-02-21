"""Automate downloading and processing PubMed."""

from .api import hello, square

# being explicit about exports is important!
__all__ = [
    "hello",
    "square",
]
