"""Downloading and processing functions for PubMed."""

from __future__ import annotations

import datetime
import functools
import gzip
import itertools as itt
import json
import logging
import typing
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal, TextIO, TypeAlias
from xml.etree.ElementTree import Element

import click
import requests
import ssslm
from bs4 import BeautifulSoup
from curies import Reference, Triple
from curies import vocabulary as v
from curies.triples import read_triples, write_triples
from lxml import etree
from more_click import verbose_option
from pydantic import BaseModel, Field
from pystow.utils import safe_open_writer
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map, thread_map
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
EDGES_PATH = MODULE.join(name="edges.tsv.gz")
SSSOM_PATH = MODULE.join(name="articles.sssom.tsv.gz")
TEST_PATH = MODULE.join(name="articles-test.sssom.tsv")
UPDATES_PATH = MODULE.join(name="updates.html")
BASELINE_PATH = MODULE.join(name="baseline.html")


def _download_baseline(url: str) -> Path:
    return BASELINE_MODULE.ensure(url=url)


def _download_updates(url: str) -> Path:
    return UPDATES_MODULE.ensure(url=url)


class JournalIssue(BaseModel):
    """Represents the issue of a journal in which the article was published."""

    volume: str | None = None
    issue: str | None = None
    published: datetime.date | None = None


class Journal(BaseModel):
    """Represents a reference to a journal.

    Note, full information about a journal can be loaded elsewhere.
    """

    issn: str | None = Field(
        None, description="The ISSN used for linking, since there might be many"
    )
    nlm_catalog_id: str = Field(..., description="The NLM identifier for the journal")
    issns: list[ISSN] = Field(default_factory=list)


class AbstractText(BaseModel):
    """Represents an abstract text object."""

    text: str
    label: str | None = None
    category: str | None = None


class History(BaseModel):
    """Represents a history item."""

    status: Literal[
        "received",
        "accepted",
        "pubmed",
        "medline",
        "entrez",
        "pmc-release",
        "revised",
        "aheadofprint",
        "retracted",
        "ecollection",
    ]
    date: datetime.date


class Grant(BaseModel):
    """Represents a grant item."""

    id: str | None = None
    acronym: str | None = None
    agency: str  # use ROR to ground agency
    agency_reference: str | None = None
    country: str  # TODO use pydantic validation


#: aslo see edam:has_topic
HAS_TOPIC = Reference(prefix="biolink", identifier="has_topic")
#: also see biolink:published_in, EFO:0001796
IN_JOURNAL = Reference(prefix="uniprot.core", identifier="publishedIn")
CITES = Reference(prefix="cito", identifier="cites")


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
    journal_issue: JournalIssue
    abstract: list[AbstractText] = Field(default_factory=list)
    authors: list[Author | Collective] = Field(default_factory=list)
    cites_pubmed_ids: list[str] = Field(default_factory=list)
    xrefs: list[Reference] = Field(default_factory=list)
    history: list[History] = Field(default_factory=list)
    grants: list[Grant] = Field(default_factory=list)

    @property
    def date_published(self) -> datetime.date | None:
        """Get the date published from the journal issue."""
        return self.journal_issue.published

    def get_abstract(self) -> str:
        """Get the full abstract."""
        return " ".join(a.text for a in self.abstract)

    def _triples(self) -> Iterable[Triple]:
        s = Reference(prefix="pubmed", identifier=str(self.pubmed))
        for p, o in self._pos():
            yield Triple(subject=s, predicate=p, object=o)

    def _pos(self) -> Iterable[tuple[Reference, Reference]]:
        for type_mesh_id in self.type_mesh_ids:
            yield v.rdf_type, Reference(prefix="mesh", identifier=type_mesh_id)
        for heading in self.headings:
            yield HAS_TOPIC, Reference(prefix="mesh", identifier=heading.mesh_id)
        yield IN_JOURNAL, Reference(prefix="nlm", identifier=self.journal.nlm_catalog_id)
        for author in self.authors:
            match author:
                case Collective() as collective if collective.reference:
                    yield v.has_contributor, collective.reference
                case Author() as author if author.orcid:
                    yield v.has_contributor, Reference(prefix="orcid", identifier=author.orcid)
        for pubmed in self.cites_pubmed_ids:
            yield CITES, Reference(prefix="pubmed", identifier=pubmed)
        for xref in self.xrefs:
            yield v.exact_match, xref

    def is_retracted(self) -> bool:
        """Check if the article is retracted."""
        # see https://www.ncbi.nlm.nih.gov/mesh/68016441
        return "D016441" in self.type_mesh_ids


def _ensure_urls(url: str, cache_path: Path, *, force: bool) -> list[str]:
    if cache_path.is_file() and not force:
        text = cache_path.read_text()
    else:
        res = requests.get(url, timeout=300)
        res.raise_for_status()
        text = res.text
        cache_path.write_text(text)

    soup = BeautifulSoup(text, "html.parser")
    return sorted(
        (
            url + href  # type:ignore
            for link in soup.find_all("a")
            if (href := link.get("href")) and href.startswith("pubmed") and href.endswith(".xml.gz")  # type:ignore
        ),
        reverse=True,
    )


def _parse_from_path(
    path: Path,
    *,
    ror_grounder: ssslm.Grounder | None,
    mesh_grounder: ssslm.Grounder | None,
    author_grounder: ssslm.Grounder | None,
) -> Iterable[Article]:
    try:
        tree = etree.parse(path)
    except etree.XMLSyntaxError:
        tqdm.write(f"failed to parse {path}")
        return

    for pubmed_article in tree.findall("PubmedArticle"):
        article = _extract_article(
            pubmed_article,
            ror_grounder=ror_grounder,
            mesh_grounder=mesh_grounder,
            author_grounder=author_grounder,
        )
        if article:
            yield article


def _extract_article(  # noqa:C901
    element: Element,
    *,
    ror_grounder: ssslm.Grounder | None,
    mesh_grounder: ssslm.Grounder | None,
    author_grounder: ssslm.Grounder | None,
) -> Article | None:
    medline_citation: Element | None = element.find("MedlineCitation")
    if medline_citation is None:
        raise ValueError("article is missing MedlineCitation tag")
    pmid_tag = medline_citation.find("PMID")
    if pmid_tag is None:
        raise ValueError("article is missing PMID tag")

    if not pmid_tag.text:
        raise ValueError("article has an empty PMID tag")
    pubmed = int(pmid_tag.text)

    article = medline_citation.find("Article")
    if article is None:
        raise ValueError(f"[pubmed:{pubmed}] is missing an Article tag")
    title_tag = article.find("ArticleTitle")
    if title_tag is None:
        raise ValueError(f"[pubmed:{pubmed}] is missing an ArticleTitle tag")
    title = title_tag.text
    if title is None:
        logger.debug(
            "[pubmed:%s] has an empty ArticleTitle tag:%s",
            pubmed,
            etree.tostring(element, pretty_print=True, encoding="unicode"),
        )
        return None

    pubmed_data = element.find("PubmedData")
    if pubmed_data is None:
        raise ValueError(f"[pubmed:{pubmed}] is missing a PubmedData tag")

    date_completed = parse_date(medline_citation.find("DateCompleted"))
    date_revised = parse_date(medline_citation.find("DateRevised"))

    types = sorted(
        x.attrib["UI"]
        for x in medline_citation.findall(".//PublicationTypeList/PublicationType")
        if x.attrib["UI"]
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
        logger.debug("[pubmed:%s] missing MedlineJournalInfo section", pubmed)
        return None

    issn_linking = medline_journal.findtext("ISSNLinking")
    nlm_catalog_id = medline_journal.findtext("NlmUniqueID")

    journal = Journal(
        issn=issn_linking,
        nlm_catalog_id=nlm_catalog_id,
        issns=issns,
    )

    abstract_texts = []
    for abstract_text_tag in medline_citation.findall(".//Abstract/AbstractText"):
        if not abstract_text_tag.text:
            continue
        abstract_text = AbstractText(
            text=abstract_text_tag.text,
            label=abstract_text_tag.attrib.get("Label"),
            category=abstract_text_tag.attrib.get("NlmCategory"),
        )
        abstract_texts.append(abstract_text)

    authors = [
        author
        for i, author_tag in enumerate(medline_citation.findall(".//AuthorList/Author"), start=1)
        if (
            author := parse_author(
                i, author_tag, ror_grounder=ror_grounder, author_grounder=author_grounder
            )
        )
    ]

    grants = [
        _parse_grant(grant, ror_grounder=ror_grounder)
        for grant in medline_citation.findall("..//GrantList/Grant")
    ]

    cites_pubmed_ids = [
        cites_pubmed_id
        for citation_reference_tag in medline_citation.findall(".//ReferenceList/Reference")
        if (cites_pubmed_id := _parse_reference(citation_reference_tag))
    ]

    xrefs = [
        Reference(prefix=prefix, identifier=article_id_tag.text)
        for article_id_tag in pubmed_data.findall(".//ArticleIdList/ArticleId")
        # it duplicates its own reference here for some reason, skip PII since it's
        if article_id_tag.text and (prefix := article_id_tag.attrib["IdType"]) not in SKIP_PREFIXES
    ]

    history = [
        history
        for pubmed_date in pubmed_data.findall(".//History/PubMedPubDate")
        if (history := _parse_pub_date(pubmed_date))
    ]

    journal_issue = _get_journal_issue(article)

    return Article(
        pubmed=pubmed,
        title=title,
        date_completed=date_completed,
        date_revised=date_revised,
        type_mesh_ids=types,
        headings=headings,
        journal=journal,
        abstract=abstract_texts,
        authors=authors,
        xrefs=xrefs,
        cites_pubmed_ids=cites_pubmed_ids,
        history=history,
        journal_issue=journal_issue,
        grants=grants,
    )


def _get_journal_issue(article: Element) -> JournalIssue:
    volume = None
    issue = None
    publication_date = None
    if (journal_element := article.find("Journal")) is not None:
        if (journal_issue_element := journal_element.find("JournalIssue")) is not None:
            volume = journal_issue_element.findtext("Volume")
            # TODO create data model for issue? e.g., "1-2"
            issue = journal_issue_element.findtext("Issue")
            if (pubdate_element := journal_issue_element.find("PubDate")) is not None:
                publication_date = parse_date(pubdate_element)
    return JournalIssue(
        volume=volume,
        issue=issue,
        published=publication_date,
    )


def _parse_pub_date(element: Element) -> History | None:
    status = element.attrib.get("PubStatus")
    if status is None:
        tqdm.write(f"missing status: {etree.tostring(element)}")
        return None
    date = parse_date(element)
    if date is None:
        return None
    try:
        rv = History(status=status, date=date)
    except ValueError:
        tqdm.write(f"invalid status: {status}")
        return None
    else:
        return rv


SKIP_PREFIXES = {"pubmed"}


def _parse_reference(reference_tag: Element) -> str | None:
    for article_id_tag in reference_tag.findall(".//ArticleIdList/ArticleId"):
        if article_id_tag.attrib["IdType"] == "pubmed":
            return article_id_tag.text
    return None


def ensure_baselines(*, force: bool, source: Source | None = None) -> list[Path]:
    """Ensure all the baseline files are downloaded."""
    return list(iterate_ensure_baselines(force=force, source=source))


def iterate_ensure_baselines(
    *, source: Source | None = None, force: bool = False
) -> Iterable[Path]:
    """Ensure all the baseline files are downloaded."""
    if source == "remote" or source is None:
        yield from thread_map(
            _download_baseline,
            _ensure_urls(BASELINE_URL, BASELINE_PATH, force=force),
            desc="Downloading PubMed baseline",
            leave=False,
        )
    elif source == "local":
        yield from BASELINE_MODULE.base.glob("*.xml.gz")
    else:
        raise ValueError


def _parse_grant(element: Element, *, ror_grounder: ssslm.Grounder | None) -> Grant:
    grant_id = element.findtext("GrantID")
    acronym = element.findtext("Acronym")
    agency = element.findtext("Agency")

    if agency and ror_grounder is not None and (match := ror_grounder.get_best_match(agency)):
        agency_reference = match.reference
    else:
        agency_reference = None
    country = element.findtext("Country")
    return Grant(
        id=grant_id,
        acronym=acronym,
        agency=agency,
        agency_reference=agency_reference,
        country=country,
    )


Source: TypeAlias = Literal["remote", "local"]


def process_baselines(
    *, force_process: bool = False, source: Source | None = None
) -> list[Article]:
    """Ensure and process all baseline files."""
    return list(iterate_process_baselines(force_process=force_process, source=source))


def iterate_process_baselines(
    *,
    force_process: bool = False,
    multiprocessing: bool = False,
    ror_grounder: ssslm.Grounder | None = None,
    mesh_grounder: ssslm.Grounder | None = None,
    author_grounder: ssslm.Grounder | None = None,
    force_listing: bool = False,
    source: Source | None = None,
    ground: bool = True,
) -> Iterable[Article]:
    """Ensure and process all baseline files."""
    paths = ensure_baselines(force=force_listing, source=source)
    return _shared_process(
        paths=paths,
        ror_grounder=ror_grounder,
        mesh_grounder=mesh_grounder,
        author_grounder=author_grounder,
        force_process=force_process,
        multiprocessing=multiprocessing,
        ground=ground,
        unit="baseline",
    )


def _shared_process(
    paths: Iterable[Path],
    *,
    ror_grounder: ssslm.Grounder | None = None,
    mesh_grounder: ssslm.Grounder | None = None,
    author_grounder: ssslm.Grounder | None = None,
    force_process: bool = False,
    unit: str,
    multiprocessing: bool = False,
    ground: bool = True,
) -> Iterable[Article]:
    if ground:
        ror_grounder, mesh_grounder, author_grounder = _ensure_grounders(
            ror_grounder, mesh_grounder, author_grounder
        )
    else:
        ror_grounder, mesh_grounder = None, None

    tqdm_kwargs = {"unit_scale": True, "unit": unit, "desc": f"Processing {unit}s"}
    if multiprocessing:
        # multiprocessing can't return generators, needs to consumed into lists
        func = functools.partial(
            _process_xml_gz,
            ror_grounder=ror_grounder,
            mesh_grounder=mesh_grounder,
            author_grounder=author_grounder,
            force_process=force_process,
        )
        xxx = process_map(func, paths, **tqdm_kwargs, chunksize=3, max_workers=10)
    else:
        func = functools.partial(
            _iterate_process_xml_gz,
            ror_grounder=ror_grounder,
            mesh_grounder=mesh_grounder,
            author_grounder=author_grounder,
            force_process=force_process,
        )
        xxx = map(func, tqdm(paths, **tqdm_kwargs))

    return itt.chain.from_iterable(xxx)


def ensure_updates(
    *,
    force: bool,
    source: Source | None = None,
) -> list[Path]:
    """Ensure all the baseline files are downloaded."""
    return list(iterate_ensure_updates(force=force, source=source))


def iterate_ensure_updates(*, force: bool = False, source: Source | None = None) -> Iterable[Path]:
    """Ensure all the baseline files are downloaded."""
    if source is None or source == "remote":
        urls = _ensure_urls(UPDATES_URL, UPDATES_PATH, force=force)
        yield from thread_map(
            _download_updates,
            urls,
            desc="Downloading PubMed updates",
            leave=False,
        )
    elif source == "local":
        yield from UPDATES_MODULE.base.glob("*.xml.gz")
    else:
        raise ValueError(f"invalid source: {source}")


def process_updates(*, force_process: bool = False) -> list[Article]:
    """Ensure and process updates."""
    return list(iterate_process_updates(force_process=force_process))


def _ensure_grounders(
    ror_grounder: ssslm.Grounder | None = None,
    mesh_grounder: ssslm.Grounder | None = None,
    author_grounder: ssslm.Grounder | None = None,
) -> tuple[ssslm.Grounder, ssslm.Grounder, ssslm.Grounder]:
    if ror_grounder is None:
        import pyobo

        logger.info("getting ROR grounder")
        ror_grounder = pyobo.get_grounder("ror")
        logger.info("done getting ROR grounder")

    if mesh_grounder is None:
        import pyobo

        logger.info("getting MeSH grounder")
        mesh_grounder = pyobo.get_grounder("mesh")
        logger.info("done getting MeSH grounder")

    if author_grounder is None:
        from orcid_downloader.lexical import get_orcid_grounder

        logger.info("getting ORCiD grounder")
        author_grounder = get_orcid_grounder()
        logger.info("done getting ORCiD grounder")

    return ror_grounder, mesh_grounder, author_grounder


def iterate_process_updates(
    *,
    force_process: bool = False,
    multiprocessing: bool = False,
    ror_grounder: ssslm.Grounder | None = None,
    mesh_grounder: ssslm.Grounder | None = None,
    author_grounder: ssslm.Grounder | None = None,
    force_listing: bool = False,
    source: Source | None = None,
    ground: bool = True,
) -> Iterable[Article]:
    """Ensure and process updates."""
    paths = ensure_updates(force=force_listing, source=source)
    return _shared_process(
        paths=paths,
        ror_grounder=ror_grounder,
        mesh_grounder=mesh_grounder,
        author_grounder=author_grounder,
        force_process=force_process,
        multiprocessing=multiprocessing,
        ground=ground,
        unit="update",
    )


def process_articles(
    *,
    force_process: bool = False,
    multiprocessing: bool = False,
    force_listing: bool = False,
    source: Source | None = None,
) -> list[Article]:
    """Ensure and process articles from baseline, then updates."""
    return list(
        iterate_process_articles(
            force_process=force_process,
            multiprocessing=multiprocessing,
            force_listing=force_listing,
            source=source,
        )
    )


def iterate_process_articles(
    *,
    force_process: bool = False,
    ror_grounder: ssslm.Grounder | None = None,
    mesh_grounder: ssslm.Grounder | None = None,
    author_grounder: ssslm.Grounder | None = None,
    multiprocessing: bool = False,
    force_listing: bool = False,
    source: Source | None = None,
    ground: bool = True,
) -> Iterable[Article]:
    """Ensure and process articles from baseline, then updates."""
    """Ensure and process articles from baseline, then updates."""
    if ground:
        ror_grounder, mesh_grounder, author_grounder = _ensure_grounders(
            ror_grounder, mesh_grounder, author_grounder
        )
    else:
        ror_grounder, mesh_grounder, author_grounder = None, None, None
    yield from iterate_process_updates(
        force_process=force_process,
        ror_grounder=ror_grounder,
        mesh_grounder=mesh_grounder,
        author_grounder=author_grounder,
        multiprocessing=multiprocessing,
        force_listing=force_listing,
        source=source,
        ground=ground,
    )
    yield from iterate_process_baselines(
        force_process=force_process,
        ror_grounder=ror_grounder,
        mesh_grounder=mesh_grounder,
        author_grounder=author_grounder,
        multiprocessing=multiprocessing,
        force_listing=force_listing,
        source=source,
        ground=ground,
    )


def iterate_ensure_articles(*, force: bool = False, source: Source | None = None) -> Iterable[Path]:
    """Ensure articles from baseline, then updates."""
    yield from iterate_ensure_updates(force=force, source=source)
    yield from iterate_ensure_baselines(force=force, source=source)


def _process_xml_gz(
    path: Path,
    *,
    ror_grounder: ssslm.Grounder | None,
    mesh_grounder: ssslm.Grounder | None,
    author_grounder: ssslm.Grounder | None,
    force_process: bool = False,
) -> Iterable[Article]:
    """Process an XML file, cache a JSON version, and return it."""
    return list(
        _iterate_process_xml_gz(
            path=path,
            ror_grounder=ror_grounder,
            mesh_grounder=mesh_grounder,
            author_grounder=author_grounder,
            force_process=force_process,
        )
    )


def _iterate_process_xml_gz(
    path: Path,
    *,
    ror_grounder: ssslm.Grounder | None,
    mesh_grounder: ssslm.Grounder | None,
    author_grounder: ssslm.Grounder | None,
    force_process: bool = False,
) -> Iterable[Article]:
    """Process an XML file, cache a JSON version, and return it."""
    new_name = path.stem.removesuffix(".xml")
    new_path = path.with_stem(new_name).with_suffix(".json.gz")
    if new_path.is_file() and not force_process:
        with gzip.open(new_path, mode="rt") as file:
            for part in json.load(file):
                yield Article.model_validate(part)

    else:
        with logging_redirect_tqdm():
            models = list(
                _parse_from_path(
                    path,
                    ror_grounder=ror_grounder,
                    mesh_grounder=mesh_grounder,
                    author_grounder=author_grounder,
                )
            )

        processed = [model.model_dump(exclude_none=True, exclude_defaults=True) for model in models]
        with gzip.open(new_path, mode="wt") as file:
            json.dump(processed, file, default=_json_default)

        yield from models


def get_edges(*, force_process: bool = False, **kwargs: Any) -> list[Triple]:
    """Get edges from PubMed."""
    if EDGES_PATH.is_file() and not force_process:
        return read_triples(EDGES_PATH)
    rv = list(iterate_edges(force_process=force_process, **kwargs))
    write_triples(rv, EDGES_PATH)
    return rv


def iterate_edges(**kwargs: Any) -> Iterable[Triple]:
    """Iterate over edges from PubMed."""
    for article in iterate_process_articles(**kwargs):
        yield from article._triples()


def save_sssom(*, path: str | Path | TextIO | None = None, **kwargs: Any) -> None:
    """Save an SSSOM file for articles."""
    if path is None:
        path = SSSOM_PATH
    with safe_open_writer(path) as writer:
        for article in iterate_process_articles(**kwargs):
            p = f"pubmed:{article.pubmed}"
            for xref in article.xrefs:
                writer.writerow((p, xref.curie))


@click.command(name="articles")
@click.option("-f", "--force-process", is_flag=True)
@click.option("-m", "--multiprocessing", is_flag=True)
@click.option("--test", is_flag=True, help="Run a test file")
@click.option("--source", type=click.Choice(list(typing.get_args(Source))))
@verbose_option
def _main(force_process: bool, multiprocessing: bool, source: Source | None) -> None:
    """Download and process articles."""
    for _ in iterate_process_articles(
        force_process=force_process, multiprocessing=multiprocessing, source=source
    ):
        pass


if __name__ == "__main__":
    _main()
