"""Command line interface for :mod:`pubmed_downloader`."""

import click

from . import api, catalog

__all__ = [
    "main",
]


@click.group()
def main() -> None:
    """CLI for pubmed_downloader."""


@main.command()
@click.option("--maximum", "-m", type=int, default=10_000)
def demo(maximum: int) -> None:
    """Test processing."""
    from tqdm import tqdm

    from pubmed_downloader import iterate_process_articles

    for _article, _ in zip(tqdm(iterate_process_articles()), range(maximum), strict=False):
        pass


main.add_command(api._main)
main.add_command(catalog._main)


if __name__ == "__main__":
    main()
