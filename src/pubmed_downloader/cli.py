"""Command line interface for :mod:`pubmed_downloader`."""

import click

__all__ = [
    "main",
]


@click.command()
@click.option("--name", required=True, help="The name of the person to say hello to")
def main(name: str) -> None:
    """CLI for pubmed_downloader."""
    # import inside the CLI to make running the --help command faster
    from .api import hello

    hello(name)

# If you want to have a multi-command CLI, see https://click.palletsprojects.com/en/latest/commands/


if __name__ == "__main__":
    main()
