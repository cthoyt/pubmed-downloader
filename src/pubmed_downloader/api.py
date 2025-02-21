"""Downloading and processing functions for PubMed."""

from collections.abc import Iterable
from pathlib import Path

import click
import pystow
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel
from tqdm.contrib.concurrent import thread_map

__all__ = [
    "ensure_baselines",
    "ensure_updates",
    "iterate_articles",
]

BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
UPDATES_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"

MODULE = pystow.module("pubmed")
BASELINE_MODULE = MODULE.module("baseline")
UPDATES_MODULE = MODULE.module("updates")


def _download_baseline(url: str) -> Path:
    return BASELINE_MODULE.ensure(url=url)


def _download_updates(url: str) -> Path:
    return UPDATES_MODULE.ensure(url=url)


class Article(BaseModel):
    """Represents an article."""


def iterate_articles() -> Iterable[Article]:
    """Iterate over all parsed articles."""
    for path in ensure_baselines():
        yield from _parse(path)
    for path in ensure_updates():
        yield from _parse(path)


def _parse(path: Path) -> list[Article]:
    raise NotImplementedError


def ensure_baselines() -> list[Path]:
    """Ensure all the baseline files are downloaded."""
    return list(
        thread_map(
            _download_baseline,
            _get_urls(BASELINE_URL),
            desc="Downloading PubMed baseline",
        )
    )


def ensure_updates() -> list[Path]:
    """Ensure all the baseline files are downloaded."""
    return list(
        thread_map(
            _download_updates,
            _get_urls(UPDATES_URL),
            desc="Downloading PubMed updates",
        )
    )


def _main() -> None:
    res = ensure_baselines()
    click.echo(f"downloaded {len(res)} baseline files")

    res = ensure_updates()
    click.echo(f"downloaded {len(res)} update files")


def _get_urls(url: str) -> list[str]:
    res = requests.get(url, timeout=300)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    return [
        url + href  # type:ignore
        for link in soup.find_all("a")
        if (href := link.get("href")) and href.startswith("pubmed") and href.endswith(".xml.gz")  # type:ignore
    ]


if __name__ == "__main__":
    _main()
