"""Download and parse catalog files from NLM."""

from __future__ import annotations

import csv
import datetime
import gzip
import itertools as itt
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Literal, TextIO
from xml.etree.ElementTree import Element

import click
import requests
import ssslm
from bs4 import BeautifulSoup
from curies import Reference
from lxml import etree
from pydantic import BaseModel, Field
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

from .utils import (
    ISSN,
    MODULE,
    Author,
    Collective,
    Heading,
    _get_mesh_id,
    _json_default,
    parse_author,
    parse_date,
    parse_mesh_heading,
)

__all__ = [
    "CatalogRecord",
    "ensure_catalog_provider_links",
    "ensure_catfile_catalog",
    "ensure_journal_overview",
    "ensure_serfile_catalog",
    "process_catalog",
    "process_catalog_provider_links",
    "process_journal_overview",
]

CATALOG_TO_PUBLISHER = "https://ftp.ncbi.nlm.nih.gov/pubmed/xmlprovidernames.txt"
JOURNAL_INFO_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/jourcache.xml"
J_ENTREZ_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Entrez.txt"
J_MEDLINE_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Medline.txt"

CATALOG_PROCESSED_GZ_PATH = MODULE.join(name="catalog.json.gz")


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
    def nlm_catalog_url(self) -> str:
        """Get the NLM Catalog URL."""
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

            data: dict[str, Any] = {}
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

    @property
    def nlm_catalog_url(self) -> str:
        """Get the NLM Catalog URL."""
        return f"https://www.ncbi.nlm.nih.gov/nlmcatalog/{self.nlm_catalog_id}"


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
    root = etree.parse(path).getroot()

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


class Resource(BaseModel):
    """Represents a resource annotation to a resource info annotation."""

    content_type: str
    media_type: str
    carrier_type: str


class ResourceInfo(BaseModel):
    """Represents a resource info annotation to a catalog record."""

    type: str
    # issuance only ever has one value: "continuing"
    issuance: str
    resource_units: list[str]
    resource: Resource | None = None


class Imprint(BaseModel):
    """Represents an imprint, which is like a brand for a publisher."""

    type: str | None = None
    function_type: str | None = None
    place: str | None = None
    entity: str | None = None


class Language(BaseModel):
    """Represents a language and its usage annotation."""

    # TODO is this supposed to be standardized with ISO 3-letter?
    value: str
    # this doesn't really make sense, it's one of Primary, Summary,
    # TableOfContents, Original, or Captions
    type: str


class TitleAlternative(BaseModel):
    """Represents an alternative title."""

    text: str
    source: str
    type: str
    sort: Literal["N"] | int


class TitleRelated(BaseModel):
    """Represents a related title."""

    text: str
    source: str
    type: str
    sort: Literal["N"] | int

    issns: list[ISSN] = Field(default_factory=list)
    xrefs: list[Reference] = Field(default_factory=list)


class CatalogRecord(BaseModel):
    """Represents a record in the NLM Catalog."""

    nlm_catalog_id: str
    title: str
    title_sort: Literal["N"] | int
    medline_short_title: str | None = None
    title_alternatives: list[TitleAlternative] = Field(default_factory=list)
    title_relatives: list[TitleRelated] = Field(default_factory=list)
    publication_type_mesh_ids: list[str] = Field(default_factory=list)
    mesh_headings: list[Heading] = Field(default_factory=list)
    date_created: datetime.date | None = None
    date_revised: datetime.date | None = None
    date_authorized: datetime.date | None = None
    date_completed: datetime.date | None = None
    date_revised_major: datetime.date | None = None
    xrefs: list[Reference] = Field(default_factory=list)
    start_year: int | None = None
    end_year: int | None = None
    issns: list[ISSN] = Field(default_factory=list)
    issn_linking: ISSN | None = None
    imprints: list[Imprint] = Field(default_factory=list)
    authors: list[Author] = Field(default_factory=list)
    collectives: list[Collective] = Field(default_factory=list)
    resource_info: ResourceInfo | None = None
    languages: list[Language] = Field(default_factory=list)
    elocations: list[str] = Field(default_factory=list)

    @property
    def nlm_catalog_url(self) -> str:
        """Get the NLM Catalog URL."""
        return f"https://www.ncbi.nlm.nih.gov/nlmcatalog/{self.nlm_catalog_id}"


def _process_elocation_tag(elt: Element) -> str | None:
    if elt.attrib["EIdType"] != "url":
        tqdm.write(f"unhandled elocation ID type: {elt.attrib['EIdType']}")
        return None
    if elt.attrib["ValidYN"] == "N":
        return None
    return elt.text


def _extract_alts(tag: Element) -> list[TitleAlternative]:
    # <TitleAlternate Owner="NLM" TitleType="Other">
    #     <Title Sort="N">Physiology, biochemistry and pharmacology</Title>
    # </TitleAlternate>
    rv = []
    for outer_tag in tag.findall("TitleAlternate"):
        inner_tag = outer_tag.find("Title")
        if inner_tag is None:
            continue

        title_type = outer_tag.attrib["TitleType"]
        title_source = outer_tag.attrib["Owner"]
        title_text = inner_tag.text
        title_sort = inner_tag.attrib["Sort"]

        rv.append(
            TitleAlternative(
                text=title_text,
                source=title_source,
                type=title_type,
                sort=title_sort,
            )
        )
    return rv


def _extract_rels(tag: Element) -> list[TitleRelated]:
    # <TitleRelated Owner="NLM" TitleType="SucceedingInPart">
    #     <Title Sort="N">Excerpta medica. Section 2B. Biochemistry</Title>
    #     <RecordID Source="LC">65009896</RecordID>
    #     <RecordID Source="OCLC">1778955</RecordID>
    #     <ISSN IssnType="Undetermined">0169-8028</ISSN>
    # </TitleRelated>
    rv = []
    for outer_tag in tag.findall("TitleRelated"):
        inner_tag = outer_tag.find("Title")
        if inner_tag is None:
            continue

        title_type = outer_tag.attrib["TitleType"]
        title_source = outer_tag.attrib["Owner"]
        title_text = inner_tag.text
        title_sort = inner_tag.attrib["Sort"]

        issns = [
            ISSN(value=issn_tag.text, type=issn_tag.attrib["IssnType"])
            for issn_tag in outer_tag.findall("ISSN")
        ]
        xrefs = [
            Reference(prefix=t.attrib["Source"], identifier=t.text)
            for t in outer_tag.findall("RecordID")
        ]
        rv.append(
            TitleRelated(
                text=title_text,
                source=title_source,
                type=title_type,
                sort=title_sort,
                issns=issns,
                xrefs=xrefs,
            )
        )
    return rv


def _extract_catalog_record(  # noqa:C901
    tag: Element, *, ror_grounder: ssslm.Grounder, mesh_grounder: ssslm.Grounder
) -> CatalogRecord | None:
    nlm_catalog_id = tag.findtext("NlmUniqueID")
    if not nlm_catalog_id:
        return None

    title_tags = tag.findall(".//TitleMain/Title")
    if len(title_tags) == 0:
        tqdm.write(f"[{nlm_catalog_id}] missing title")
        return None
    elif len(title_tags) > 1:
        tqdm.write(f"[{nlm_catalog_id}] multiple titles")
    title_tag = title_tags[0]
    title = title_tag.text
    if not title:
        tqdm.write(f"[{nlm_catalog_id}] no title text")
        return None

    title_sort = title_tag.attrib["Sort"]

    alts = _extract_alts(tag)
    rels = _extract_rels(tag)

    # TODO PhysicalDescription

    # <ELocationList>
    #         <ELocation>
    #             <ELocationID EIdType="url" ValidYN="Y">http://www.psychologicabelgica.com/</ELocationID>
    #         </ELocation>
    #         <ELocation>
    #             <ELocationID EIdType="url" ValidYN="Y">https://www.ncbi.nlm.nih.gov/pmc/journals/3396/</ELocationID>
    #         </ELocation>
    #     </ELocationList>
    elocations = [
        url
        for x in tag.findall(".//ELocationList/ELocation/ELocationID")
        if (url := _process_elocation_tag(x))
    ]

    # <Language LangType="Primary">eng</Language>
    languages = [Language(value=x.text, type=x.attrib["LangType"]) for x in tag.findall("Language")]

    publication_type_mesh_ids = sorted(
        # there are less than 30 instances of this data being broken where
        # the remove prefixes are necessary, but it has to be done
        mesh_id.removeprefix("(uri) http://id.nlm.nih.gov/mesh/").removeprefix(
            "http://id.nlm.nih.gov/mesh/"
        )
        for publication_type_tag in tag.findall(".//PublicationTypeList/PublicationType")
        if (mesh_id := _get_mesh_id(publication_type_tag))
    )

    mesh_headings = [
        heading
        for x in tag.findall(".//MeshHeadingList/MeshHeading")
        if (heading := parse_mesh_heading(x, mesh_grounder=mesh_grounder))
    ]

    xrefs = [xref for xref_tag in tag.findall("OtherID") if (xref := _process_other_id(xref_tag))]

    authors, collectives = [], []
    for i, x in enumerate(tag.findall(".//AuthorList/Author"), start=1):
        match parse_author(i, x, ror_grounder=ror_grounder):
            case Author() as author:
                authors.append(author)
            case Collective() as collective:
                collectives.append(collective)

    publication_info_tag = tag.find("PublicationInfo")
    start_year = None
    end_year = None
    if publication_info_tag is not None:
        start_year_ = publication_info_tag.findtext("PublicationFirstYear")
        if start_year_ and len(start_year_) == 4 and start_year_.isnumeric():
            start_year = int(start_year_)
        end_year_ = publication_info_tag.findtext("PublicationEndYear")
        if end_year_ and len(end_year_) == 4 and end_year_.isnumeric():
            end_year = int(end_year_)
        if end_year == 9999:
            end_year = None
        # TODO More information about publisher available here

        imprints = []
        for imprint_tag in publication_info_tag.findall("Imprint"):
            # also Place, DateIssued, and ImprintFull
            entity_tag = imprint_tag.find("Entity")
            if entity_tag is not None and entity_tag.text:
                entity = entity_tag.text.strip().strip(",").strip()
            else:
                entity = None
            imprints.append(
                Imprint(
                    entity=entity,
                    place=imprint_tag.findtext("Place"),
                    type=imprint_tag.attrib.get("ImprintType"),
                    function_type=imprint_tag.attrib.get("FunctionType"),
                )
            )

    issns = [
        ISSN(value=issn_tag.text, type=issn_tag.attrib["IssnType"])
        for issn_tag in tag.findall("ISSN")
    ]

    issn_linking = None
    if issn_linking_value := tag.findtext("ISSNLinking"):
        for issn in issns:
            if issn.value == issn_linking_value:
                issn_linking = issn
                break
        if issn_linking is None:
            issn_linking = ISSN(value=issn_linking_value, type="Linking")
            issns.append(issn_linking)

    return CatalogRecord(
        nlm_catalog_id=nlm_catalog_id,
        title=title.rstrip("."),
        title_sort=title_sort,
        title_alternatives=alts,
        title_relatives=rels,
        medline_short_title=tag.findtext("MedlineTA"),
        publication_type_mesh_ids=publication_type_mesh_ids,
        mesh_headings=mesh_headings,
        date_created=parse_date(tag.find("DateCreated")),
        date_revised=parse_date(tag.find("DateRevised")),
        date_authorized=parse_date(tag.find("DateAuthorized")),
        date_completed=parse_date(tag.find("DateCompleted")),
        date_revised_major=parse_date(tag.find("DateRevisedMajor")),
        xrefs=xrefs,
        start_year=start_year,
        end_year=end_year,
        issns=issns,
        issn_linking=issn_linking,
        imprints=imprints,
        authors=authors,
        collectives=collectives,
        resource_info=_get_resource_info(tag.find("ResourceInfo")),
        languages=languages,
        elocation=elocations,
    )


def _get_resource_info(resource_info_tag: Element | None) -> ResourceInfo | None:
    """Extract all resource info.

    :param resource_info_tag: The XML element
    :returns: A resource info object

    .. code-block:: xml

        <ResourceInfo>
            <TypeOfResource>Serial</TypeOfResource>
            <Issuance>continuing</Issuance>
            <ResourceUnit>remote electronic resource</ResourceUnit>
            <ResourceUnit>text</ResourceUnit>
            <Resource>
                <ContentType>text</ContentType>
                <MediaType>unmediated</MediaType>
                <CarrierType>volume</CarrierType>
            </Resource>
        </ResourceInfo>
    """
    if resource_info_tag is None:
        raise ValueError
    type = resource_info_tag.findtext("TypeOfResource")
    issuance = resource_info_tag.findtext("Issuance")
    resource_units: list[str] = [
        resource_unit_tag.text
        for resource_unit_tag in resource_info_tag.findall("ResourceUnit")
        if resource_unit_tag.text
    ]

    resource_tag = resource_info_tag.find("Resource")
    if resource_tag is None:
        resource = None
    else:
        resource = Resource(
            content_type=_replace(resource_tag.findtext("ContentType"), CONTENT_TYPE_REPLACE),
            media_type=_replace(resource_tag.findtext("MediaType"), MEDIA_TYPE_REPLACE),
            carrier_type=_replace(resource_tag.findtext("CarrierType"), CARRIER_TYPE_REPLACE),
        )
    return ResourceInfo(
        type=type,
        issuance=issuance,
        resource_units=resource_units,
        resource=resource,
    )


CONTENT_TYPE_REPLACE = {"Text": "text", None: "unspecified"}
MEDIA_TYPE_REPLACE: dict[str | None, str] = {
    "Computermedien": "computer",
    "informàtic": "unspecified",
    "unmmediated": "unmediated",  # typo
}
CARRIER_TYPE_REPLACE = {
    None: "unspecified",
    "Online-Ressource": "online resource",
    "online": "online resource",
    "other": "unspecified",
    "videocassette": "video cassette",
    "audiocassette": "audio cassette",
    "videodisc": "video disc",
}


def _replace(x: str | None, d: Mapping[str | None, str]) -> str | None:
    return d.get(x, x)


def _process_other_id(tag: Element) -> Reference | None:
    prefix = tag.attrib["Prefix"].lstrip("(").rstrip(")")
    # attrib also has 'Source',
    identifier = tag.text
    return Reference(prefix=prefix, identifier=identifier)


def process_catalog(*, force: bool = False, force_process: bool = False) -> list[CatalogRecord]:
    """Ensure and process the NLM Catalog."""
    if CATALOG_PROCESSED_GZ_PATH.is_file() and not force_process:
        return list(_read_catalog(CATALOG_PROCESSED_GZ_PATH))

    rv = list(iterate_process_catalog(force=force, force_process=force_process))
    with gzip.open(CATALOG_PROCESSED_GZ_PATH, mode="wt") as file:
        _dump_catalog(rv, file, indent=2)
    return rv


def iterate_process_catalog(
    *, force: bool = False, force_process: bool = False
) -> Iterable[CatalogRecord]:
    """Iterate over records in the NLM Catalog."""
    import pyobo

    ror_grounder = pyobo.get_grounder("ror")
    mesh_grounder = pyobo.get_grounder("mesh")

    for path in tqdm(ensure_serfile_catalog(force=force), desc="Processing NLM Catalog"):
        yield from _parse_catalog(
            path,
            force_process=force_process or force,
            ror_grounder=ror_grounder,
            mesh_grounder=mesh_grounder,
        )


def ensure_catfile_catalog(*, force: bool = False) -> list[Path]:
    """Get the entire NLM Catalog via CatfilePlus files."""
    return list(_iter_catfile_catalog(force=force))


def ensure_serfile_catalog(*, force: bool = False) -> list[Path]:
    """Get the entire NLM Catalog via Serfile files."""
    return list(_iter_serfile_catalog(force=force))


def _parse_catalog(
    path: Path,
    *,
    force_process: bool = False,
    ror_grounder: ssslm.Grounder,
    mesh_grounder: ssslm.Grounder,
) -> Iterable[CatalogRecord]:
    cache_path = path.with_suffix(".json.gz")
    if cache_path.is_file() and not force_process and False:
        yield from _read_catalog(cache_path)
    else:
        tree = etree.parse(path)
        catalog_records = []
        for tag in tree.findall("NLMCatalogRecord"):
            catalog_record = _extract_catalog_record(
                tag, ror_grounder=ror_grounder, mesh_grounder=mesh_grounder
            )
            if catalog_record:
                catalog_records.append(catalog_record)

        with gzip.open(cache_path, mode="wt") as file:
            _dump_catalog(catalog_records, file)

        yield from catalog_records


def _read_catalog(cache_path: Path) -> Iterable[CatalogRecord]:
    with gzip.open(cache_path, mode="rt") as file:
        for d in json.load(file):
            yield CatalogRecord.model_validate(d)


def _dump_catalog(catalog_records: list[CatalogRecord], file: TextIO, **kwargs: Any) -> None:
    json.dump(
        [
            catalog_record.model_dump(exclude_none=True, exclude_defaults=True)
            for catalog_record in catalog_records
        ],
        file,
        default=_json_default,
        **kwargs,
        ensure_ascii=False,
    )


def _iter_catfile_catalog(*, force: bool = False) -> Iterable[Path]:
    module = MODULE.module("catalog-catfile")
    return thread_map(  # type:ignore
        lambda x: module.ensure(url=x, force=force),
        _iter_catpluslease_urls(),
        desc="Downloading catalog catfiles",
        leave=False,
    )


def _iter_serfile_catalog(*, force: bool = False) -> Iterable[Path]:
    module = MODULE.module("catalog-serfile")
    return thread_map(  # type:ignore
        lambda x: module.ensure(url=x, force=force),
        _iter_serfile_urls(),
        desc="Downloading catalog serfiles",
        leave=False,
    )


def _iter_catpluslease_urls() -> Iterable[str]:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    yield from _iter_catalog_urls(
        base="https://ftp.nlm.nih.gov/projects/catpluslease/",
        skip_prefix="catplusbase",
        include_prefix="catplus",
    )


def _iter_serfile_urls() -> Iterable[str]:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    yield from _iter_catalog_urls(
        base="https://ftp.nlm.nih.gov/projects/serfilelease/",
        skip_prefix="serfilebase",
        include_prefix="serfile",
    )


def _iter_catalog_urls(base: str, skip_prefix: str, include_prefix: str) -> Iterable[str]:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    res = requests.get(base, timeout=300)
    soup = BeautifulSoup(res.text, "html.parser")
    for link in soup.find_all("a"):
        href = link.attrs["href"]  # type:ignore
        if not isinstance(href, str) or not href:
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


@click.command(name="catalog")
@click.option("-f", "--force-process", is_flag=True)
def _main(force_process: bool) -> None:
    """Download and process the NLM catalog."""
    from collections import Counter

    from tabulate import tabulate

    publication_type_counter: Counter[str] = Counter()
    imprint_type_counter: Counter[str | None] = Counter()
    language_counter: Counter[str] = Counter()
    language_type_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    issuance_counter: Counter[str] = Counter()
    resource_unit_counter: Counter[str] = Counter()
    content_type_counter: Counter[str] = Counter()
    media_type_counter: Counter[str] = Counter()
    carrier_type_counter: Counter[str] = Counter()

    records = process_catalog(force_process=force_process)
    click.echo(f"There are {len(records):,} catalog records")
    for record in records:
        resource_info = record.resource_info
        if not resource_info:
            continue
        for pt in record.publication_type_mesh_ids:
            publication_type_counter[pt] += 1

        for imprint in record.imprints:
            imprint_type_counter[imprint.type] += 1

        for lang in record.languages:
            language_counter[lang.value] += 1
            language_type_counter[lang.type] += 1

        type_counter[resource_info.type] += 1
        issuance_counter[resource_info.issuance] += 1
        for resource_unit in resource_info.resource_units:
            resource_unit_counter[resource_unit] += 1
        if resource_info.resource:
            content_type_counter[resource_info.resource.content_type] += 1
            media_type_counter[resource_info.resource.media_type] += 1
            carrier_type_counter[resource_info.resource.carrier_type] += 1

    click.secho("\nPublication Type Counter", fg="blue")
    click.echo(tabulate(publication_type_counter.most_common()))

    click.secho("\nImprint Type Counter", fg="blue")
    click.echo(tabulate(imprint_type_counter.most_common()))

    click.secho("\nLanguage Counter", fg="blue")
    click.echo(tabulate(language_counter.most_common()))

    click.secho("\nLanguage Type Counter", fg="blue")
    click.echo(tabulate(language_type_counter.most_common()))

    click.secho("\nResource Type Counter", fg="blue")
    click.echo(tabulate(type_counter.most_common()))

    click.secho("\nResource Issuance Counter", fg="blue")
    click.echo(tabulate(issuance_counter.most_common()))

    click.secho("\nResource Unit Counter", fg="blue")
    click.echo(tabulate(resource_unit_counter.most_common()))

    click.secho("\nContent Type Counter", fg="blue")
    click.echo(tabulate(content_type_counter.most_common()))

    click.secho("\nMedia Type Counter", fg="blue")
    click.echo(tabulate(media_type_counter.most_common()))

    click.secho("\nCarrier Type Counter", fg="blue")
    click.echo(tabulate(carrier_type_counter.most_common()))


if __name__ == "__main__":
    _main()
