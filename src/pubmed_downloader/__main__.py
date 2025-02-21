"""Entrypoint module, in case you use `python -m pubmed_downloader`."""

from .cli import main

if __name__ == "__main__":
    main()
