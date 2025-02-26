"""Downloading and processing functions for PubMed."""

from __future__ import annotations

import datetime
import gzip
import itertools as itt
import json
import logging
from collections.abc import Iterable
from pathlib import Path
from xml.etree.ElementTree import Element

import requests
import ssslm
from bs4 import BeautifulSoup
from lxml import etree
from pydantic import BaseModel, Field
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from tqdm.contrib.logging import logging_redirect_tqdm

from .utils import (
    ISSN,
    MODULE,
    Author,
    Collective,
    Heading,
    _json_default,
    parse_author,
    parse_date,
    parse_mesh_heading,
)

__all__ = [
    "AbstractText",
    "Article",
    "Journal",
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

logger = logging.getLogger(__name__)
BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
UPDATES_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"

BASELINE_MODULE = MODULE.module("baseline")
UPDATES_MODULE = MODULE.module("updates")


def _download_baseline(url: str) -> Path:
    return BASELINE_MODULE.ensure(url=url)


def _download_updates(url: str) -> Path:
    return UPDATES_MODULE.ensure(url=url)


class Journal(BaseModel):
    """Represents a reference to a journal.

    Note, full information about a journal can be loaded elsewhere.
    """

    issn: str | None = Field(
        None, description="The ISSN used for linking, since there might be many"
    )
    nlm: str = Field(..., description="The NLM identifier for the journal")
    issns: list[ISSN] = Field(default_factory=list)


class AbstractText(BaseModel):
    """Represents an abstract text object."""

    text: str
    label: str | None = None
    category: str | None = None


class Article(BaseModel):
    """Represents an article."""

    pubmed: int
    title: str
    date_completed: datetime.date | None = None
    date_revised: datetime.date | None = None
    type_mesh_ids: list[str] = Field(
        default_factory=list, description="A list of MeSH LUIDs for article types"
    )
    headings: list[Heading] = Field(default_factory=list)
    journal: Journal
    abstract: list[AbstractText] = Field(default_factory=list)
    authors: list[Author | Collective]
    reference_pubmed_ids: list[str] = Field(default_factory=list)
    xrefs: list[Reference] = Field(default_factory=list)

    def get_abstract(self) -> str:
        """Get the full abstract."""
        return " ".join(a.text for a in self.abstract)


def _get_urls(url: str) -> list[str]:
    res = requests.get(url, timeout=300)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    return sorted(
        (
            url + href  # type:ignore
            for link in soup.find_all("a")
            if (href := link.get("href")) and href.startswith("pubmed") and href.endswith(".xml.gz")  # type:ignore
        ),
        reverse=True,
    )


def _parse_from_path(
    path: Path, *, ror_grounder: ssslm.Grounder, mesh_grounder: ssslm.Grounder
) -> Iterable[Article]:
    try:
        tree = etree.parse(path)  # noqa:S320
    except etree.XMLSyntaxError:
        tqdm.write(f"failed to parse {path}")
        return

    for pubmed_article in tree.findall("PubmedArticle"):
        article = _extract_article(
            pubmed_article, ror_grounder=ror_grounder, mesh_grounder=mesh_grounder
        )
        if article:
            yield article


def _extract_article(  # noqa:C901
    element: Element, *, ror_grounder: ssslm.Grounder, mesh_grounder: ssslm.Grounder
) -> Article | None:
    medline_citation: Element | None = element.find("MedlineCitation")
    if medline_citation is None:
        raise ValueError
    pmid_tag = medline_citation.find("PMID")
    if pmid_tag is None:
        raise ValueError

    if not pmid_tag.text:
        raise ValueError
    pubmed = int(pmid_tag.text)

    article = medline_citation.find("Article")
    if article is None:
        raise ValueError
    title_tag = article.find("ArticleTitle")
    if title_tag is None:
        raise ValueError
    title = title_tag.text
    if title is None:
        return None

    article.find("Abstract")

    pubmed_data = element.find("PubmedData")
    if pubmed_data is None:
        raise ValueError

    date_completed = parse_date(medline_citation.find("DateCompleted"))
    date_revised = parse_date(medline_citation.find("DateRevised"))

    types = sorted(
        x.attrib["UI"] for x in medline_citation.findall(".//PublicationTypeList/PublicationType")
    )

    headings = [
        heading
        for x in medline_citation.findall(".//MeshHeadingList/MeshHeading")
        if (heading := parse_mesh_heading(x, mesh_grounder=mesh_grounder))
    ]

    issns = [
        ISSN(value=x.text, type=x.attrib["IssnType"])
        for x in medline_citation.findall(".//Journal/ISSN")
    ]

    medline_journal = medline_citation.find("MedlineJournalInfo")
    if medline_journal is None:
        return None

    issn_linking = medline_journal.findtext("ISSNLinking")
    nlm_id = medline_journal.findtext("NlmUniqueID")

    journal = Journal(
        issn=issn_linking,
        nlm=nlm_id,
        issns=issns,
    )

    abstract = []
    for x in medline_citation.findall(".//Abstract/AbstractText"):
        if not x.text:
            continue
        t = AbstractText(
            text=x.text, label=x.attrib.get("Label"), category=x.attrib.get("NlmCategory")
        )
        abstract.append(t)

    authors = [
        author
        for i, x in enumerate(medline_citation.findall(".//AuthorList/Author"), start=1)
        if (author := parse_author(i, x, ror_grounder=ror_grounder))
    ]

    reference_pubmed_ids = [
        reference_pubmed_id
        for x in medline_citation.findall(".//ReferenceList/Reference")
        if (reference_pubmed_id := _parse_reference(x))
    ]

    xrefs = [
        Reference(prefix=x.attrib["IdType"], identifier=x.text)
        for x in medline_citation.findall(".//ArticleIdList/ArticleId")
    ]

    return Article(
        pubmed=pubmed,
        title=title,
        date_completed=date_completed,
        date_revised=date_revised,
        type_mesh_ids=types,
        headings=headings,
        journal=journal,
        abstract=abstract,
        authors=authors,
        xrefs=xrefs,
        reference_pubmed_ids=reference_pubmed_ids,
    )


def _parse_reference(reference_tag: Element) -> str | None:
    for article_id_tag in reference_tag.findall(".//ArticleIdList/ArticleId"):
        if article_id_tag.attrib["IdType"] == "pubmed":
            return article_id_tag.text
    return None


def ensure_baselines() -> list[Path]:
    """Ensure all the baseline files are downloaded."""
    return list(iterate_ensure_baselines())


def iterate_ensure_baselines() -> Iterable[Path]:
    """Ensure all the baseline files are downloaded."""
    yield from thread_map(
        _download_baseline,
        _get_urls(BASELINE_URL),
        desc="Downloading PubMed baseline",
        leave=False,
    )


def process_baselines() -> list[Article]:
    """Ensure and process all baseline files."""
    return list(iterate_process_baselines())


def iterate_process_baselines() -> Iterable[Article]:
    """Ensure and process all baseline files."""
    paths = ensure_baselines()

    import pyobo

    ror_grounder = pyobo.get_grounder("ror")
    mesh_grounder = pyobo.get_grounder("ror")

    return itt.chain.from_iterable(
        _process_xml_gz(path, ror_grounder=ror_grounder, mesh_grounder=mesh_grounder)
        for path in tqdm(paths, unit_scale=True, unit="baseline", desc="Processing baselines")
    )


def ensure_updates() -> list[Path]:
    """Ensure all the baseline files are downloaded."""
    return list(iterate_ensure_updates())


def iterate_ensure_updates() -> Iterable[Path]:
    """Ensure all the baseline files are downloaded."""
    yield from thread_map(
        _download_updates,
        _get_urls(UPDATES_URL),
        desc="Downloading PubMed updates",
        leave=False,
    )


def process_updates() -> list[Article]:
    """Ensure and process updates."""
    return list(iterate_process_updates())


def iterate_process_updates() -> Iterable[Article]:
    """Ensure and process updates."""
    paths = ensure_updates()

    import pyobo

    ror_grounder = pyobo.get_grounder("ror")
    mesh_grounder = pyobo.get_grounder("mesh")

    return itt.chain.from_iterable(
        _process_xml_gz(path, ror_grounder=ror_grounder, mesh_grounder=mesh_grounder)
        for path in tqdm(paths, unit_scale=True, unit="update", desc="Processing updates")
    )


def process_articles() -> list[Article]:
    """Ensure and process articles from baseline, then updates."""
    return list(iterate_process_articles())


def iterate_process_articles() -> Iterable[Article]:
    """Ensure and process articles from baseline, then updates."""
    yield from iterate_process_updates()
    yield from iterate_process_baselines()


def iterate_ensure_articles() -> Iterable[Path]:
    """Ensure articles from baseline, then updates."""
    yield from iterate_ensure_updates()
    yield from iterate_ensure_baselines()


def _process_xml_gz(
    path: Path, *, ror_grounder: ssslm.Grounder, mesh_grounder: ssslm.Grounder
) -> Iterable[Article]:
    """Process an XML file, cache a JSON version, and return it."""
    new_name = path.stem.removesuffix(".xml")
    new_path = path.with_stem(new_name).with_suffix(".json.gz")
    if new_path.is_file():
        with gzip.open(new_path, mode="rt") as file:
            for part in json.load(file):
                yield Article.model_validate(part)

    else:
        with logging_redirect_tqdm():
            models = list(
                _parse_from_path(path, ror_grounder=ror_grounder, mesh_grounder=mesh_grounder)
            )

        processed = [model.model_dump(exclude_none=True, exclude_defaults=True) for model in models]
        with gzip.open(new_path, mode="wt") as file:
            json.dump(processed, file, default=_json_default)

        yield from models
