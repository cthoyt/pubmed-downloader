"""Download and parse catalog files from NLM."""

from __future__ import annotations

import datetime
import itertools as itt
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast, overload
from xml.etree.ElementTree import Element

import click
import requests
import ssslm
from bs4 import BeautifulSoup
from curies import NamableReference, NamedReference, Reference
from lxml import etree
from pydantic import BaseModel, Field
from pydantic_extra_types.language_code import ISO639_3
from pystow.utils import iter_pydantic_jsonl, read_pydantic_jsonl, write_pydantic_jsonl
from ssslm import Grounder
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

from .utils import (
    ISSN,
    MODULE,
    Author,
    Collective,
    Heading,
    _get_mesh_id,
    parse_author,
    parse_date,
    parse_mesh_heading,
)

__all__ = [
    "CatalogRecord",
    "Journal",
    "ensure_catalog_provider_links",
    "ensure_catfile_catalog",
    "ensure_j_entrez",
    "ensure_j_medline",
    "ensure_journal_overview",
    "ensure_serfile_catalog",
    "get_catalog_to_publisher",
    "get_journals",
    "process_catalog",
    "process_journal_overview",
]

CATALOG_TO_PUBLISHER = "https://ftp.ncbi.nlm.nih.gov/pubmed/xmlprovidernames.txt"

# It appears that J_Entrez and J_Medline have the same data model -
# they both have explicit synonym types for MEDLINE and ISO annotations,
# but doesn't include start and end years. The jourcache.xml has start
# and end years, but doesn't annotate its synonym types
JOURNAL_INFO_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/jourcache.xml"
J_MEDLINE_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Medline.txt"
# The same content as J_Medline.txt plus NCBI molecular biology database journals
J_ENTREZ_PATH = "https://ftp.ncbi.nlm.nih.gov/pubmed/J_Entrez.txt"

CATALOG_PROCESSED_GZ_PATH = MODULE.join(name="catalog.jsonl.gz")

START_YEAR_FIXES: dict[str | None, str] = {"9918265998706676": "1992"}


def ensure_j_medline(*, force: bool = False) -> Path:
    """Ensure the overview file for PubMed/MEDLINE journals is downloaded."""
    return MODULE.ensure(url=J_MEDLINE_PATH, force=force)


def ensure_j_entrez(*, force: bool = False) -> Path:
    """Ensure the overview file for PubMed/MEDLINE extended with NCBI molecular biology database journals is downloaded."""  # noqa:E501
    return MODULE.ensure(url=J_ENTREZ_PATH, force=force)


class JournalShort(BaseModel):
    """Represents records in the J_Entrez and J_medline files."""

    id: int
    nlm_catalog_id: str
    title: str
    issns: list[ISSN] = Field(default_factory=list)
    abbreviation_medline: str | None = None
    abbreviation_iso: str | None = None

    @property
    def nlm_catalog_url(self) -> str:
        """Get the NLM Catalog URL."""
        return f"https://www.ncbi.nlm.nih.gov/nlmcatalog/{self.nlm_catalog_id}"


class Journal(JournalShort):
    """Represents a journal (a subset of NLM Catalog Records)."""

    synonyms: list[str] = Field(default_factory=list)
    active: bool = True
    start_year: int | None = None
    end_year: int | None = None
    publisher: NamedReference | None = None


#: A remapping from internal journal keys to :class:`Journal` field names
REMAPPING = {
    "JrId": "id",
    "JournalTitle": "title",
    "MedAbbr": "abbreviation_medline",
    "IsoAbbr": "abbreviation_iso",
    "NlmId": "nlm_catalog_id",
}


def process_journal_overview(
    *, force: bool = False, include_entrez: bool = True
) -> list[JournalShort]:
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


def _parse_journals(path: Path) -> Iterable[JournalShort]:
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

            yield JournalShort.model_validate(data, extra="forbid")


def get_catalog_to_publisher(*, force: bool = False) -> dict[str, NamedReference]:
    """Get a mapping from NLM Catalog identifier to NLM publisher reference."""
    path = ensure_catalog_provider_links(force=force)
    rv = {}
    with path.open() as file:
        for i, line in enumerate(file, start=1):
            try:
                catalog_id, publisher_id, publisher_name = line.strip().split("|")
            except ValueError:
                tqdm.write(f"failed on line {i}: {line}")
                continue
            rv[catalog_id] = NamedReference(
                prefix="nlm.publisher", identifier=publisher_id, name=publisher_name
            )
    return rv


def ensure_catalog_provider_links(*, force: bool = False) -> Path:
    """Ensure the xmlprovidernames.txt file is downloaded."""
    return MODULE.ensure(url=CATALOG_TO_PUBLISHER, force=force)


def get_journals(*, force: bool = False, progress: bool = True) -> list[Journal]:
    """Get the list of journals appearing in PubMed/MEDLINE.

    :param force: Should the data be re-downloaded?
    :returns: A list of journal objects parsed from the overview file
    """
    return list(_iterate_journals(force=force, progress=progress))


def _iterate_journals(*, force: bool = False, progress: bool = True) -> Iterable[Journal]:
    """Iterate over journals."""
    overview_summary = {
        journal.nlm_catalog_id: journal for journal in _parse_journals(ensure_j_entrez(force=force))
    }

    catalog_to_publisher = get_catalog_to_publisher(force=force)

    path = MODULE.ensure(url=JOURNAL_INFO_PATH, force=force)
    root = etree.parse(path).getroot()

    elements = root.findall("Journal")
    for element in tqdm(elements, disable=not progress, leave=False):
        if journal := _process_journal(
            element, overview_summary, catalog_to_publisher=catalog_to_publisher
        ):
            yield journal


def _process_journal(
    element: Element,
    journal_short_info: dict[str, JournalShort],
    catalog_to_publisher: dict[str, NamedReference],
) -> Journal | None:
    jrid = element.attrib["jrid"]

    nlm_catalog_id = element.findtext("NlmUniqueID")
    if nlm_catalog_id is None:
        raise ValueError("no NLM catalog ID")

    extra_info = journal_short_info.get(nlm_catalog_id)

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
    synonyms = {alias_tag.text for alias_tag in element.findall("Alias")}
    if extra_info is not None:
        synonyms.discard(extra_info.abbreviation_iso)
        synonyms.discard(extra_info.abbreviation_medline)
    if (start_year := element.findtext("StartYear")) and len(start_year) != 4:
        if nlm_catalog_id in START_YEAR_FIXES:
            start_year = START_YEAR_FIXES[nlm_catalog_id]
        else:
            tqdm.write(f"[{nlm_catalog_id}] - invalid start year: {start_year}")
    if (end_year := element.findtext("EndYear")) and len(end_year) != 4:
        tqdm.write(f"[{nlm_catalog_id}] - invalid end year: {end_year}")
        end_year = None
    return Journal(
        id=jrid,
        title=title,
        nlm_catalog_id=nlm_catalog_id,
        active=active,
        abbreviation_iso=extra_info and extra_info.abbreviation_iso,
        abbreviation_medline=extra_info and extra_info.abbreviation_medline,
        start_year=start_year,
        end_year=end_year,
        issns=issns,
        synonyms=synonyms,
        publisher=catalog_to_publisher.get(nlm_catalog_id),
    )


class Resource(BaseModel):
    """Represents a resource annotation to a resource info annotation."""

    content_type: str
    media_type: str
    carrier_type: str


ResourceType: TypeAlias = Literal[
    "Serial",
    "Nonmusical Sound Recording",
    "Visual Material",
    "Electronic Resource",
    "Kit",
    "Book",
    "Map",
    "Still Image",
]


class ResourceInfo(BaseModel):
    """Represents a resource info annotation to a catalog record."""

    type: ResourceType
    issuance: Literal["continuing"]
    resource_units: list[str]
    resource: Resource | None = None


class Imprint(BaseModel):
    """Represents an imprint, which is like a brand for a publisher."""

    type: Literal["Original", "Current"] | None = None
    function_type: str | None = None
    place: str | None = None
    name: str | None = None
    reference: NamableReference | None = None
    date_issued: str | None = None  # TODO parse in start/end?


class Language(BaseModel):
    """Represents a language and its usage annotation."""

    value: ISO639_3

    # this doesn't really make sense
    type: Literal["Primary", "Summary", "TableOfContents", "Original", "Captions"]


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


CatalogStatus: TypeAlias = Literal[
    "Completed",  # 553,846
    "Not-Our-Cataloging",  # 176,796
    "Withdrawn",  # 25,321
    "In-Process",  # 5,359
    "On-Order",  # 569
    "Brief",  # 133
    "Undetermined",  # 22
]

CatalogOwner: TypeAlias = Literal[
    "NLM",  # 762,024
    "Undetermined",  # 22
]


class CatalogRecord(BaseModel):
    """Represents a record in the NLM Catalog."""

    nlm_catalog_id: str
    owner: CatalogOwner
    status: CatalogStatus
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
    return [
        title_alternate
        for title_alternate_tag in tag.findall("TitleAlternate")
        if (title_alternate := _extract_alt(title_alternate_tag)) is not None
    ]


def _extract_alt(outer_tag: Element) -> TitleAlternative | None:
    inner_tag = outer_tag.find("Title")
    if inner_tag is None:
        return None
    title_text = inner_tag.text
    if not isinstance(title_text, str) or not title_text:
        return None
    return TitleAlternative(
        text=title_text,
        source=outer_tag.attrib["Owner"],
        type=outer_tag.attrib["TitleType"],
        sort=inner_tag.attrib["Sort"],
    )


def _extract_rels(tag: Element) -> list[TitleRelated]:
    # <TitleRelated Owner="NLM" TitleType="SucceedingInPart">
    #     <Title Sort="N">Excerpta medica. Section 2B. Biochemistry</Title>
    #     <RecordID Source="LC">65009896</RecordID>
    #     <RecordID Source="OCLC">1778955</RecordID>
    #     <ISSN IssnType="Undetermined">0169-8028</ISSN>
    # </TitleRelated>
    return [
        title_related
        for title_related_tag in tag.findall("TitleRelated")
        if (title_related := _extract_title_related(title_related_tag)) is not None
    ]


def _extract_title_related(tag: Element) -> TitleRelated | None:
    inner_tag = tag.find("Title")
    if inner_tag is None:
        return None

    if inner_tag.text is None:
        raise ValueError

    title_type = tag.attrib["TitleType"]
    title_source = tag.attrib["Owner"]
    title_sort = inner_tag.attrib["Sort"]

    issns = [
        ISSN(value=issn_tag.text, type=issn_tag.attrib["IssnType"])
        for issn_tag in tag.findall("ISSN")
    ]
    xrefs = []
    for record_id_tag in tag.findall("RecordID"):
        if record_id_tag.text is None:
            continue
        prefix = record_id_tag.attrib["Source"]
        try:
            # look into LC and sn 97039260
            xref = Reference(prefix=prefix, identifier=record_id_tag.text.replace(" ", ""))
        except ValueError:
            tqdm.write(f"failed to extract xref from {prefix} and {record_id_tag.text}")
        else:
            xrefs.append(xref)

    return TitleRelated(
        text=inner_tag.text,
        source=title_source,
        type=title_type,
        sort=title_sort,
        issns=issns,
        xrefs=xrefs,
    )


#: Legacy english language codes that can't be mapped to ISO three-letter codes
UNUSABLE_LEGACY_LANGUAGE_CODE = {
    "cai",  # Central American Indian (Other)
}
# remap from ISO 639-2/B (legacy english codes)
LEGACY_LANGUAGE_CODE_TO_STANDARD = {
    "ger": "deu",
    "cze": "ces",
    "fre": "fra",
    "dut": "nld",
    "chi": "zho",
    "slo": "slk",
    "rum": "ron",
    "mac": "mkd",
    "gre": "ell",
    "may": "msa",
    "ice": "isl",
    "per": "fas",
    "alb": "sqi",
    "arm": "hye",
    "geo": "kat",
    "wel": "cym",
    "tib": "bod",
    "baq": "eus",
    "mao": "mri",
    "bur": "mya",
}

ENTITY_MISSES: set[str] = set()


def _extract_catalog_record(  # noqa:C901
    tag: Element,
    *,
    ror_grounder: ssslm.Grounder,
    mesh_grounder: ssslm.Grounder,
    author_grounder: ssslm.Grounder,
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

    owner = tag.attrib["Owner"]
    status = tag.attrib["Status"]

    # TODO PhysicalDescription

    # <ELocationList>
    #     <ELocation>
    #         <ELocationID EIdType="url" ValidYN="Y">http://www.psychologicabelgica.com/</ELocationID>
    #     </ELocation>
    #     <ELocation>
    #         <ELocationID EIdType="url" ValidYN="Y">https://www.ncbi.nlm.nih.gov/pmc/journals/3396/</ELocationID>
    #     </ELocation>
    # </ELocationList>
    elocations = [
        url
        for elocation_id_tag in tag.findall(".//ELocationList/ELocation/ELocationID")
        if (url := _process_elocation_tag(elocation_id_tag)) is not None
    ]

    # <Language LangType="Primary">eng</Language>
    languages = [
        language
        for language_tag in tag.findall("Language")
        if (language := _get_language(language_tag)) is not None
    ]

    publication_type_mesh_ids = sorted(
        # there are less than 30 instances of this data being broken where
        # the remove prefixes are necessary, but it has to be done
        mesh_id
        for publication_type_tag in tag.findall(".//PublicationTypeList/PublicationType")
        if (mesh_id := _get_mesh_id(publication_type_tag)) is not None
    )

    mesh_headings = [
        heading
        for x in tag.findall(".//MeshHeadingList/MeshHeading")
        if (heading := parse_mesh_heading(x, mesh_grounder=mesh_grounder)) is not None
    ]

    xrefs = [xref for xref_tag in tag.findall("OtherID") if (xref := _process_other_id(xref_tag))]

    authors, collectives = [], []
    for i, author_tag in enumerate(tag.findall(".//AuthorList/Author"), start=1):
        match parse_author(
            i, author_tag, ror_grounder=ror_grounder, author_grounder=author_grounder
        ):
            case Author() as author:
                authors.append(author)
            case Collective() as collective:
                collectives.append(collective)

    publication_info_tag = tag.find("PublicationInfo")
    start_year = None
    end_year = None

    # there are only 70 that have more than one across the whole database,
    # so for simplicity, we drop the second on all of those by prioritizing
    # by ImprintType="Current"
    imprints: list[Imprint] = []
    if publication_info_tag is not None:
        start_year_ = publication_info_tag.findtext("PublicationFirstYear")
        if start_year_ and len(start_year_) == 4 and start_year_.isnumeric():
            start_year = int(start_year_)
        end_year_ = publication_info_tag.findtext("PublicationEndYear")
        if end_year_ and len(end_year_) == 4 and end_year_.isnumeric():
            end_year = int(end_year_)
        if end_year == 9999:
            end_year = None
        imprints.extend(
            _get_imprint(imprint_tag, ror_grounder=ror_grounder)
            for imprint_tag in publication_info_tag.findall("Imprint")
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
        owner=owner,
        status=status,
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
        elocations=elocations,
    )


def _get_imprint(imprint_tag: Element, ror_grounder: ssslm.Grounder) -> Imprint:
    """Extract information from an imprint.

    .. code-block:: xml

        <Imprint ImprintType="Original" FunctionType="Publication">
            <Place>Thousand Oaks, CA :</Place>
            <Entity>SAGE Publishing,</Entity>
            <DateIssued>[2023]-</DateIssued>
            <ImprintFull>Thousand Oaks, CA : SAGE Publishing, [2023]-</ImprintFull>
        </Imprint>
    """
    # TODO DateIssued (which might be a range?)
    entity_tag = imprint_tag.find("Entity")
    if entity_tag is not None and entity_tag.text:
        entity_name = entity_tag.text.strip().strip(",").strip()
        entity_match = ror_grounder.get_best_match(entity_name)
    else:
        entity_name = None
        entity_match = None

    place_tag = imprint_tag.find("Place")
    if place_tag is not None and place_tag.text:
        place = place_tag.text.strip().lstrip("[").rstrip(": ,.]")
    else:
        place = None

    return Imprint(
        name=entity_name,
        reference=entity_match.reference if entity_match else None,
        place=place,
        type=imprint_tag.attrib.get("ImprintType"),
        function_type=imprint_tag.attrib.get("FunctionType"),
        date_issued=imprint_tag.findtext("DateIssued"),
    )


def _get_language(language_tag: Element) -> Language | None:
    # legacy english bibliographic labels are used
    iso_639_2b = language_tag.text
    if iso_639_2b is None:
        return None
    iso_639_2b = iso_639_2b.strip().lower()
    if iso_639_2b in UNUSABLE_LEGACY_LANGUAGE_CODE:
        return None
    return Language(
        value=LEGACY_LANGUAGE_CODE_TO_STANDARD.get(iso_639_2b, iso_639_2b),
        type=language_tag.attrib["LangType"],
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
        if resource_unit_tag.text is not None
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


@overload
def _replace(x: str, d: Mapping[str | None, str]) -> str: ...


@overload
def _replace(x: None, d: Mapping[str | None, str]) -> str | None: ...


def _replace(x: str | None, d: Mapping[str | None, str]) -> str | None:
    return d.get(x, x)


def _process_other_id(tag: Element) -> Reference | None:
    prefix = tag.attrib.get("Prefix")
    identifier = tag.text
    if prefix is None or identifier is None:
        return None
    prefix = prefix.strip().lstrip("(").rstrip(")").strip()
    identifier = identifier.strip()
    # TODO attrib also has 'Source',
    return Reference(prefix=prefix, identifier=identifier)


def process_catalog(
    *, force_process: bool = False, refresh_index: bool = True
) -> list[CatalogRecord]:
    """Ensure and process the NLM Catalog."""
    if CATALOG_PROCESSED_GZ_PATH.is_file() and not force_process:
        return read_pydantic_jsonl(CATALOG_PROCESSED_GZ_PATH, CatalogRecord)
    catalog_records = list(
        iterate_process_catalog(force_process=force_process, refresh_index=refresh_index)
    )
    write_pydantic_jsonl(catalog_records, CATALOG_PROCESSED_GZ_PATH)
    return catalog_records


def iterate_process_catalog(
    *, force_process: bool = False, refresh_index: bool = True
) -> Iterable[CatalogRecord]:
    """Iterate over records in the NLM Catalog."""
    import pyobo
    from orcid_downloader.lexical import get_orcid_grounder

    ror_grounder = cast(Grounder, pyobo.get_grounder("ror"))
    mesh_grounder = cast(Grounder, pyobo.get_grounder("mesh"))
    author_grounder: Grounder = get_orcid_grounder()

    for path in tqdm(
        ensure_serfile_catalog(refresh_index=refresh_index),
        desc="Processing NLM Catalog",
        unit="file",
    ):
        yield from _parse_catalog(
            path,
            force_process=force_process,
            ror_grounder=ror_grounder,
            mesh_grounder=mesh_grounder,
            author_grounder=author_grounder,
        )


def ensure_catfile_catalog(*, refresh_index: bool = True) -> list[Path]:
    """Get the entire NLM Catalog via CatfilePlus files."""
    return list(_iter_catfile_catalog(refresh_index=refresh_index))


def ensure_serfile_catalog(*, refresh_index: bool = True) -> list[Path]:
    """Get the entire NLM Catalog via Serfile files."""
    return list(_iter_serfile_catalog(refresh_index=refresh_index))


def _parse_catalog(
    path: Path,
    *,
    force_process: bool = False,
    ror_grounder: ssslm.Grounder,
    mesh_grounder: ssslm.Grounder,
    author_grounder: ssslm.Grounder,
) -> Iterable[CatalogRecord]:
    cache_path = path.with_suffix(".jsonl.gz")
    if cache_path.is_file() and not force_process:
        yield from iter_pydantic_jsonl(cache_path, CatalogRecord)
    else:
        try:
            tree = etree.parse(path)
        except SyntaxError:
            tqdm.write(f"{path} failed to parse, skipping")
            return
        catalog_records = []
        for tag in tree.findall("NLMCatalogRecord"):
            catalog_record = _extract_catalog_record(
                tag,
                ror_grounder=ror_grounder,
                mesh_grounder=mesh_grounder,
                author_grounder=author_grounder,
            )
            if catalog_record:
                catalog_records.append(catalog_record)

        write_pydantic_jsonl(catalog_records, cache_path)
        yield from catalog_records


def _iter_catfile_catalog(*, refresh_index: bool = True) -> Iterable[Path]:
    module = MODULE.module("catalog-catfile")
    return thread_map(  # type:ignore
        lambda url: module.ensure(url=url),
        _iter_catpluslease_urls(refresh=refresh_index),
        desc="Downloading catalog catfiles",
        leave=False,
    )


def _iter_serfile_catalog(*, refresh_index: bool = True) -> Iterable[Path]:
    module = MODULE.module("catalog-serfile")
    return thread_map(  # type:ignore
        lambda url: module.ensure(url=url),
        _iter_serfile_urls(refresh=refresh_index),
        desc="Downloading catalog serfiles",
        leave=False,
    )


def _iter_catpluslease_urls(*, refresh: bool = True) -> Iterable[str]:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    yield from _iter_catalog_urls(
        base="https://ftp.nlm.nih.gov/projects/catpluslease/",
        skip_prefix="catplusbase",
        include_prefix="catplus",
        refresh=refresh,
    )


def _iter_serfile_urls(*, refresh: bool = True) -> Iterable[str]:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    yield from _iter_catalog_urls(
        base="https://ftp.nlm.nih.gov/projects/serfilelease/",
        skip_prefix="serfilebase",
        include_prefix="serfile",
        refresh=refresh,
    )


def _iter_catalog_urls(
    base: str, skip_prefix: str, include_prefix: str, *, refresh: bool = True
) -> Iterable[str]:
    path: Path = MODULE.join(name=f"{include_prefix}-index.txt")
    if path.is_file() and not refresh:
        yield from path.read_text().splitlines()
    else:
        urls = list(_iter_catalog_urls_helper(base, skip_prefix, include_prefix))
        path.write_text("\n".join(urls))
        yield from urls


def _iter_catalog_urls_helper(base: str, skip_prefix: str, include_prefix: str) -> Iterable[str]:
    # see https://www.nlm.nih.gov/databases/download/catalog.html
    res = requests.get(base, timeout=300)
    soup = BeautifulSoup(res.text, "html.parser")
    for link in soup.find_all("a"):
        href = link.attrs["href"]
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
@click.option("--refresh-index/--no-refresh-index", is_flag=True)
def _main(force_process: bool, refresh_index: bool) -> None:
    """Download and process the NLM catalog."""
    from collections import Counter

    from tabulate import tabulate

    publication_type_counter: Counter[str] = Counter()
    imprint_type_counter: Counter[str] = Counter()
    imprint_count_counter: Counter[int] = Counter()
    imprint_place_counter: Counter[str] = Counter()
    imprint_counter: Counter[str] = Counter()
    language_counter: Counter[str] = Counter()
    language_type_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    issuance_counter: Counter[str] = Counter()
    resource_unit_counter: Counter[str] = Counter()
    content_type_counter: Counter[str] = Counter()
    media_type_counter: Counter[str] = Counter()
    carrier_type_counter: Counter[str] = Counter()
    status_counter: Counter[str] = Counter()
    owner_counter: Counter[str] = Counter()

    records = process_catalog(force_process=force_process, refresh_index=refresh_index)
    click.echo(f"There are {len(records):,} catalog records")
    for record in records:
        resource_info = record.resource_info
        if not resource_info:
            continue
        for pt in record.publication_type_mesh_ids:
            publication_type_counter[pt] += 1

        for imprint in record.imprints:
            imprint_counter[imprint.name or "none"] += 1
            imprint_place_counter[imprint.place or "none"] += 1
            imprint_type_counter[imprint.type or "none"] += 1

        for lang in record.languages:
            language_counter[lang.value] += 1
            language_type_counter[lang.type] += 1

        imprint_count_counter[len(record.imprints or [])] += 1
        status_counter[record.status] += 1
        owner_counter[record.owner] += 1
        type_counter[resource_info.type] += 1
        issuance_counter[resource_info.issuance] += 1
        for resource_unit in resource_info.resource_units:
            resource_unit_counter[resource_unit] += 1
        if resource_info.resource:
            content_type_counter[resource_info.resource.content_type] += 1
            media_type_counter[resource_info.resource.media_type] += 1
            carrier_type_counter[resource_info.resource.carrier_type] += 1

    def _tabulate(counter: Counter[Any], title: str, *, n: int | None = None) -> None:
        click.echo()
        if n is not None:
            click.secho(f"showing top {n}", fg="yellow")
        click.echo(tabulate(counter.most_common(n=n), headers=[title, "Count"], tablefmt="github"))

    _tabulate(status_counter, "Publication Status")
    _tabulate(owner_counter, "Publication Owner")
    _tabulate(publication_type_counter, "Publication Type")
    _tabulate(imprint_counter, "Imprint", n=50)
    _tabulate(imprint_place_counter, "Imprint Place", n=50)
    _tabulate(imprint_type_counter, "Imprint Type")
    _tabulate(imprint_count_counter, "Imprint Arity")
    _tabulate(language_type_counter, "Language Type")
    _tabulate(type_counter, "Resource Type")
    _tabulate(issuance_counter, "Resource Issuance")
    _tabulate(resource_unit_counter, "Resource Unit")
    _tabulate(content_type_counter, "Content Type")
    _tabulate(media_type_counter, "Media Type")
    _tabulate(carrier_type_counter, "Carrier Type")


if __name__ == "__main__":
    _main()
