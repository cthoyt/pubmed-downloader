"""Command line interface for :mod:`pubmed_downloader`."""

import click

from . import api, catalog

__all__ = [
    "main",
]


@click.group()
def main() -> None:
    """CLI for pubmed_downloader."""


main.add_command(api._main)
main.add_command(catalog._main)


if __name__ == "__main__":
    main()
