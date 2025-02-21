"""Downloading and processing functions for PubMed."""

from __future__ import annotations

import datetime
import gzip
import itertools as itt
import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Literal
from xml.etree.ElementTree import Element

import click
import pystow
import requests
from bs4 import BeautifulSoup
from lxml import etree
from pydantic import BaseModel, Field
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from tqdm.contrib.logging import logging_redirect_tqdm

__all__ = [
    "ensure_baselines",
    "ensure_updates",
    "iterate_articles",
]

logger = logging.getLogger(__name__)
BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
UPDATES_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"

MODULE = pystow.module("pubmed")
BASELINE_MODULE = MODULE.module("baseline")
UPDATES_MODULE = MODULE.module("updates")
PARSED = MODULE.join(name="processed.json")


def _download_baseline(url: str) -> Path:
    return BASELINE_MODULE.ensure(url=url)


def _download_updates(url: str) -> Path:
    return UPDATES_MODULE.ensure(url=url)


class ISSN(BaseModel):
    value: str
    type: Literal["Print", "Electronic"]


class Qualifier(BaseModel):
    mesh: str
    major: bool = False


class Heading(BaseModel):
    descriptor: str
    major: bool = False
    qualifiers: list[Qualifier] | None = None


class Journal(BaseModel):
    issn: str | None = Field(
        None, description="The ISSN used for linking, since there might be many"
    )
    nlm: str = Field(..., description="The NLM identifier for the journal")
    issns: list[ISSN] = Field(default_factory=list)


class AbstractText(BaseModel):
    text: str
    label: str | None = None
    category: str | None = None


class Author(BaseModel):
    valid: bool = True
    affiliations: list[str] = Field(default_factory=list)
    # must have at least one of name/orcid
    name: str | None = None
    orcid: str | None = None


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
    authors: list[Author]


def iterate_articles() -> Iterable[Article]:
    """Iterate over all parsed articles."""
    for path in ensure_baselines():
        yield from _parse_from_path(path)
    for path in ensure_updates():
        yield from _parse_from_path(path)


def _get_urls(url: str) -> list[str]:
    res = requests.get(url, timeout=300)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    return [
        url + href  # type:ignore
        for link in soup.find_all("a")
        if (href := link.get("href")) and href.startswith("pubmed") and href.endswith(".xml.gz")  # type:ignore
    ]


def _parse_from_path(path: Path) -> Iterable[Article]:
    # with open(path, mode="rb") as file:
    tree = etree.parse(path)  # noqa:S320
    for pubmed_article in tree.findall("PubmedArticle"):
        article = _extract_article(pubmed_article)
        if article:
            yield article


def _extract_article(element: Element) -> Article | None:  # noqa:C901
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

    date_completed = _parse_date(medline_citation.find("DateCompleted"))
    date_revised = _parse_date(medline_citation.find("DateRevised"))

    types = sorted(
        x.attrib["UI"] for x in medline_citation.findall(".//PublicationTypeList/PublicationType")
    )

    headings = [
        _parse_mesh_heading(x) for x in medline_citation.findall(".//MeshHeadingList/MeshHeading")
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
        for x in medline_citation.findall(".//AuthorList/Author")
        if (author := _parse_author(pubmed, x))
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
    )


def _parse_yn(s: str) -> bool:
    match s:
        case "Y":
            return True
        case "N":
            return False
        case _:
            raise ValueError(s)


def _parse_author(pubmed: int, tag: Element) -> Author | None:  # noqa:C901
    affiliations = [a.text for a in tag.findall(".//AffiliationInfo/Affiliation") if a.text]
    valid = _parse_yn(tag.attrib["ValidYN"])

    orcid = None
    for it in tag.findall("Identifier"):
        source = it.attrib.get("Source")
        if source != "ORCID":
            logger.warning("unhandled identifier source: %s", source)
        elif not it.text:
            continue
        else:
            if it.text.startswith("https://orcid.org/"):
                orcid = it.text.removeprefix("https://orcid.org/")
            elif it.text.startswith("orcid.org/"):
                orcid = it.text.removeprefix("orcid.org/")
            elif it.text.startswith("http://orcid.org/"):
                orcid = it.text.removeprefix("http://orcid.org/")
            elif len(it.text) == 19:
                orcid = it.text
            elif len(it.text) == 18:
                # malformed, someone forgot the last value
                continue
            elif len(it.text) == 16 and it.text.isnumeric():
                # malformed, forgot dashes
                t = it.text
                orcid = f"{t[:4]}-{t[4:8]}-{t[8:12]}-{t[12:]}"
            elif len(it.text) == 20:
                # extra character got OCR'd, mostly from linking to affiliations
                orcid = it.text[:20]
            else:
                logger.warning(f"unhandled ORCID: {it.text}")

    last_name_tag = tag.find("LastName")
    forename_tag = tag.find("ForeName")
    initials_tag = tag.find("Initials")
    collective_name_tag = tag.find("CollectiveName")

    if collective_name_tag is not None:
        logger.debug(f"[pubmed:{pubmed}] skipping collective name: %s", collective_name_tag.text)
        return None

    if last_name_tag is None:
        if orcid is not None:
            return Author(
                valid=valid,
                affiliations=affiliations,
                orcid=orcid,
            )
        remainder = {
            subtag.tag
            for subtag in tag
            if subtag.tag not in {"LastName", "ForeName", "Initials", "AffiliationInfo"}
        }
        logger.warning(f"no last name given in {tag}. Other tags to check: {remainder}")
        return None

    if forename_tag is not None:
        name = f"{forename_tag.text} {last_name_tag.text}"
    elif initials_tag is not None:
        name = f"{initials_tag.text} {last_name_tag.text}"
    else:
        if orcid is not None:
            return Author(
                valid=valid,
                affiliations=affiliations,
                orcid=orcid,
            )
        remainder = {
            subtag.tag
            for subtag in tag
            if subtag.tag not in {"LastName", "ForeName", "Initials", "AffiliationInfo"}
        }
        # TOO can come back to this and do more debugging
        logger.debug(
            f"[pubmed:{pubmed}] no forename given in {tag} w/ last name {last_name_tag.text}. "
            f"Other tags to check: {remainder}"
        )
        return None

    return Author(
        valid=_parse_yn(tag.attrib["ValidYN"]),
        name=name,
        affiliations=affiliations,
        orcid=orcid,
    )


def _parse_mesh_heading(tag: Element) -> Heading | None:
    descriptor = tag.find("DescriptorName")
    if descriptor is None:
        return None
    mesh_id = descriptor.attrib["UI"]
    major = _parse_yn(descriptor.attrib["MajorTopicYN"])
    qualifiers = [
        Qualifier(mesh=qualifier.attrib["UI"], major=_parse_yn(qualifier.attrib["MajorTopicYN"]))
        for qualifier in tag.findall("QualifierName")
    ]
    return Heading(descriptor=mesh_id, major=major, qualifiers=qualifiers or None)


def _parse_date(date_tag: Element | None) -> datetime.date | None:
    if date_tag is None:
        return None
    year_tag = date_tag.find("Year")
    if year_tag is None or not year_tag.text:
        return None
    year = int(year_tag.text)
    month_tag = date_tag.find("Month")
    month = int(month_tag.text) if month_tag is not None and month_tag.text else None
    day_tag = date_tag.find("Day")
    day = int(day_tag.text) if day_tag is not None and day_tag.text else None
    return datetime.date(year=year, month=month, day=day)  # type:ignore


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


@click.command()
def _main() -> None:
    path = Path(__file__).parent.resolve().joinpath("pubmed25n1283.xml")
    rv = []
    for _x in list(_parse_from_path(path)):
        rv.append(_x)
        click.echo(repr(_x.journal))

    paths = itt.chain(
        tqdm(ensure_updates(), desc="Processing updates"),
        tqdm(ensure_baselines(), desc="Processing baselines"),
    )
    for path in paths:
        new_path = path.with_suffix(".json.gz")
        if new_path.is_file():
            continue
        with logging_redirect_tqdm():
            processed = [
                model.model_dump(exclude_none=True, exclude_defaults=True)
                for model in _parse_from_path(path)
            ]
        with gzip.open(new_path, mode="wt") as file:
            json.dump(
                processed,
                file,
                default=lambda o: o.isoformat()
                if isinstance(o, datetime.date | datetime.datetime)
                else o,
            )


if __name__ == "__main__":
    _main()
