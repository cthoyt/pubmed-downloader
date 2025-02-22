from __future__ import annotations

import csv
import gzip
import itertools as itt
import json
from collections.abc import Iterable
from pathlib import Path
from xml.etree.ElementTree import Element

import requests
from bs4 import BeautifulSoup
from lxml import etree
from pydantic import BaseModel, Field
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

from pubmed_downloader.constants import ISSN, MODULE

__all__ = [
    "ensure_catalog_provider_links",
    "ensure_catfile_catalog",
    "ensure_journal_overview",
    "ensure_serfile_catalog",
    "process_catalog_provider_links",
    "process_journal_overview",
]

CATALOG_TO_PUBLISHER = "https://ftp.ncbi.nlm.nih.gov/pubmed/xmlprovidernames.txt"
JOURNAL_INFO_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/jourcache.xml"
J_ENTREZ_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Entrez.txt"
J_MEDLINE_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Medline.txt"

CATALOG_CATFILE_MODULE = MODULE.module("catalog-catfile")
CATALOG_SERFILE_MODULE = MODULE.module("catalog-serfile")


class Journal(BaseModel):
    """Represents a journal (a subset of NLM Catalog Records)."""

    id: int
    nlm_catalog_id: str = Field(
        ...,
        description="The identifier for the journal in the NLM Catalog (https://www.ncbi.nlm.nih.gov/nlmcatalog)",
    )
    title: str
    abbreviation_medline: str | None = None
    abbreviation_iso: str | None = None
    issns: list[ISSN] = Field(default_factory=list)
    synonyms: list[str] = Field(default_factory=list)
    active: bool = True
    start_year: int | None
    end_year: int | None

    @property
    def nlm_url(self) -> str:
        return f"https://www.ncbi.nlm.nih.gov/nlmcatalog/{self.nlm_catalog_id}"


#: A remapping from internal journal keys to :class:`Journal` field names
REMAPPING = {
    "JrId": "id",
    "JournalTitle": "title",
    "MedAbbr": "abbreviation_medline",
    "IsoAbbr": "abbreviation_iso",
    "NlmId": "nlm_catalog_id",
}


def process_journal_overview(*, force: bool = False, include_entrez: bool = True) -> list[Journal]:
    """Get the list of journals appearing in PubMed/MEDLINE.

    :param force: Should the data be re-downloaded?
    :param include_entrez:
        If false, downloads only the PubMed/MEDLINE data. If true (default), downloads
        both the PubMed/MEDLINE and NCBI molecular biology database journals.
    :returns: A list of journal objects parsed from the overview file
    """
    path = ensure_journal_overview(force=force, include_entrez=include_entrez)
    return list(_parse_journals(path))


def ensure_journal_overview(*, force: bool = False, include_entrez: bool = True) -> Path:
    """Ensure the journal overview file is downloaded.

    :param force: Should the data be re-downloaded?
    :param include_entrez:
        If false, downloads only the PubMed/MEDLINE data. If true (default), downloads
        both the PubMed/MEDLINE and NCBI molecular biology database journals.
    :returns: A path to the journal overview file
    """
    if include_entrez:
        return MODULE.ensure(url=J_ENTREZ_PATH, force=force)
    else:
        return MODULE.ensure(url=J_MEDLINE_PATH, force=force)


def _parse_journals(path: Path) -> Iterable[Journal]:
    with path.open() as file:
        for is_delimiter, lines in itt.groupby(file, key=lambda line: line.startswith("---")):
            if is_delimiter:
                continue

            data = {}
            for line in lines:
                key, partition, value = (s.strip() for s in line.strip().partition(":"))
                if not partition:
                    raise ValueError(f"malformed line: {line}")
                if not value:
                    continue
                if key == "ISSN (Print)":
                    data.setdefault("issns", []).append(ISSN(value=value, type="Print"))
                elif key == "ISSN (Online)":
                    data.setdefault("issns", []).append(ISSN(value=value, type="Electronic"))
                else:
                    data[REMAPPING[key]] = value

            yield Journal.model_validate(data)


class CatalogProviderLink(BaseModel):
    """Represents a link between a NLM Catalog record and its provider."""

    nlm_catalog_id: str
    key: str = Field(..., description="Key for the NLM provider, corresponding to ")
    label: str


def process_catalog_provider_links(*, force: bool = False) -> list[CatalogProviderLink]:
    """Ensure and process catalog record - provider links file."""
    path = ensure_catalog_provider_links(force=force)
    with path.open() as file:
        return [
            CatalogProviderLink(nlm_catalog_id=nlm_catalog_id, key=key, label=name)
            for nlm_catalog_id, key, name in csv.reader(file, delimiter="|")
        ]


def ensure_catalog_provider_links(*, force: bool = False) -> Path:
    """Ensure the xmlprovidernames.txt file is downloaded."""
    return MODULE.ensure(url=CATALOG_TO_PUBLISHER, force=force)


def _iterate_journals(*, force: bool = False) -> Iterable[Journal]:
    process_journal_overview(force=force)
    process_catalog_provider_links(force=force)

    path = MODULE.ensure(url=JOURNAL_INFO_PATH, force=force)
    root = etree.parse(path).getroot()  # noqa:S320

    elements = root.findall("Journal")
    for element in elements:
        journal = _process_journal(element)
        if journal:
            yield journal


def _process_journal(element: Element) -> Journal | None:
    jrid = element.attrib["jrid"]

    nlm_catalog_id = element.findtext("NlmUniqueID")
    title = element.findtext("Name")
    issns = [
        ISSN(value=issn_tag.text, type=issn_tag.attrib["type"].capitalize())
        for issn_tag in element.findall("Issn")
    ]
    match element.findtext("ActivityFlag"):
        case "0":
            active = False
        case "1":
            active = True
        case _ as v:
            raise ValueError(f"unknown activity value: {v}")
    synonyms = [alias_tag.text for alias_tag in element.findall("Alias")]
    if start_year := element.findtext("StartYear"):
        if len(start_year) != 4:
            tqdm.write(f"[{nlm_catalog_id}] invalid start year: {start_year}")
            start_year = None
    if end_year := element.findtext("EndYear"):
        if len(end_year) != 4:
            tqdm.write(f"[{nlm_catalog_id}] invalid end year: {end_year}")
            end_year = None

    # TODO abbreviations?
    return Journal(
        id=jrid,
        title=title,
        nlm_catalog_id=nlm_catalog_id,
        active=active,
        start_year=start_year,
        end_year=end_year,
        issns=issns,
        synonyms=synonyms,
    )


class CatalogRecord(BaseModel):
    """Represents a record in the NLM Catalog."""


def process_catalog(*, force: bool = False, force_process: bool = False) -> list[CatalogRecord]:
    """Ensure and process the NLM Catalog."""
    return list(iterate_process_catalog(force=force, force_process=force_process))


def iterate_process_catalog(
    *, force: bool = False, force_process: bool = False
) -> Iterable[CatalogRecord]:
    """Iterate over records in the NLM Catalog."""
    for path in _iter_catfile_catalog(force=force):
        yield from _parse_catalog(path, force_process=force_process or force)


def ensure_catfile_catalog(*, force: bool = False) -> list[Path]:
    """Get the entire NLM Catalog via CatfilePlus files."""
    return list(_iter_catfile_catalog(force=force))


def ensure_serfile_catalog(*, force: bool = False) -> list[Path]:
    """Get the entire NLM Catalog via Serfile files."""
    return list(_iter_serfile_catalog(force=force))


def _parse_catalog(path: Path, *, force_process: bool = False) -> Iterable[CatalogRecord]:
    cache_path = path.with_suffix(".json.gz")
    if cache_path.is_file() and not force_process:
        with gzip.open(cache_path, mode="rt") as file:
            for d in json.load(file):
                yield CatalogRecord.model_validate(d)

    else:
        tree = etree.parse(path)  # noqa:S320
        catalog_records = []
        for tag in tree.findall("PubmedArticle"):
            catalog_record = _extract_catalog_record(tag)
            if catalog_record:
                catalog_records.append(catalog_record)

        with gzip.open(cache_path, mode="wt") as file:
            json.dump(
                [
                    catalog_record.model_dump(exclude_none=True, exclude_defaults=True)
                    for catalog_record in catalog_records
                ],
                file,
            )

        yield from catalog_records


def _extract_catalog_record(tag: Element) -> CatalogRecord | None:
    raise NotImplementedError


def _iter_catfile_catalog(*, force: bool = False) -> Iterable[Path]:
    return thread_map(
        lambda x: CATALOG_CATFILE_MODULE.ensure(url=x, force=force),
        _iter_catpluslease_urls(),
        desc="Downloading catalog catfiles",
        leave=False,
    )


def _iter_serfile_catalog(*, force: bool = False) -> Iterable[Path]:
    return thread_map(
        lambda x: CATALOG_SERFILE_MODULE.ensure(url=x, force=force),
        _iter_serfile_urls(),
        desc="Downloading catalog serfiles",
        leave=False,
    )


def _iter_catpluslease_urls() -> str:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    yield from _iter_catalog_urls(
        base="https://ftp.nlm.nih.gov/projects/catpluslease/",
        skip_prefix="catplusbase",
        include_prefix="catplus",
    )


def _iter_serfile_urls() -> str:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    yield from _iter_catalog_urls(
        base="https://ftp.nlm.nih.gov/projects/serfilelease/",
        skip_prefix="serfilebase",
        include_prefix="serfile",
    )


def _iter_catalog_urls(base: str, skip_prefix: str, include_prefix: str) -> str:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    res = requests.get(base, timeout=300)
    soup = BeautifulSoup(res.text, "html.parser")
    for link in soup.find_all("a"):
        href = link.attrs["href"]
        if href is None:
            tqdm.write(f"link: {link}")
            continue
        if (
            href.startswith(skip_prefix)
            or href.endswith(".marcxml.xml")
            or not href.startswith(include_prefix)
            or not href.endswith(".xml")
        ):
            continue
        yield base + href


if __name__ == "__main__":
    ensure_serfile_catalog()
