"""Command line interface for :mod:`pubmed_downloader`."""

import click

__all__ = [
    "main",
]


@click.command()
def main() -> None:
    """CLI for pubmed_downloader."""
    from .api import process_articles

    process_articles()


if __name__ == "__main__":
    main()
