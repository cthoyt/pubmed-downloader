"""Constants for PubMed Downloader."""

from __future__ import annotations

import datetime
import logging
import re
from collections.abc import Iterable
from typing import Any, Literal
from xml.etree.ElementTree import Element

import pystow
import ssslm
from curies import NamableReference
from pydantic import BaseModel, Field
from tqdm import tqdm

__all__ = [
    "MODULE",
    "clean_pubmed_ids",
    "parse_date",
]

logger = logging.getLogger(__name__)
MODULE = pystow.module("pubmed")

ORCID_PREFIXES = [
    "https://orcid.org/",
    "http://orcid.org/",
    "https//orcid.org/",
    "https/orcid.org/",
    "http//orcid.org/",
    "http/orcid.org/",
    "orcid.org/",
    "https://orcid.org",
    "https://orcid.org-",
    "http://orcid/",
    "https://orcid.org ",
    "https://www.orcid.org/",
    "http://ORCID.org/",
]


class ISSN(BaseModel):
    """Represents an ISSN number, annotated with its type."""

    value: str
    type: Literal["Print", "Electronic", "Undetermined", "Linking"]


def parse_date(date_tag: Element | None) -> datetime.date | None:
    """Parse a date tag, if possible."""
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


class Author(BaseModel):
    """Represents an author."""

    position: int
    valid: bool = True
    affiliations: list[str] = Field(default_factory=list)
    # must have at least one of name/orcid
    name: str | None = None
    orcid: str | None = None
    roles: list[str] = Field(default_factory=list)


class Collective(BaseModel):
    """Represents an author."""

    position: int
    name: str
    reference: NamableReference | None = None
    roles: list[str] = Field(default_factory=list)


STARTS = (
    "https://open-na.hosted.exlibrisgroup.com/alma/01NLM_INST/authorities",
    "http://viaf.org/viaf/sourceID/",
    "http://id.loc.gov/authorities/names/",
    "http://id.worldcat.org/fast/",
    "http://id.nlm.nih.gov/mesh/",
)


def parse_author(  # noqa:C901
    position: int, tag: Element, *, doc_key: int | None = None, ror_grounder: ssslm.Grounder | None
) -> Author | Collective | None:
    """Parse an author XML object."""
    affiliations = [a.text for a in tag.findall(".//AffiliationInfo/Affiliation") if a.text]
    valid = _parse_yn(tag.attrib["ValidYN"]) if "ValidYN" in tag.attrib else True

    orcid = None
    for it in tag.findall("Identifier"):
        if not it.text:
            continue
        source = it.attrib.get("Source")
        if any(it.text.startswith(uri_prefix) for uri_prefix in STARTS):
            pass
        elif source == "FrPBN":
            pass  # this only happens once FrPBN:17723227, no context available
        elif source != "ORCID":
            logger.warning("unhandled identifier source: %s - %s (%s)", source, it.text, it.attrib)
        else:
            orcid = _clean_orcid(it.text)

    last_name_tag = tag.find("LastName")
    forename_tag = tag.find("ForeName")
    initials_tag = tag.find("Initials")
    collective_name_tag = tag.find("CollectiveName")

    roles = [role_tag.text for role_tag in tag.findall("Role")]

    if collective_name_tag is not None and collective_name_tag.text:
        name = collective_name_tag.text.rstrip(".")
        match = ror_grounder.get_best_match(name) if ror_grounder is not None else None
        return Collective(
            position=position, name=name, reference=match.reference if match else None, roles=roles
        )

    if last_name_tag is None:
        if orcid is not None:
            return Author(
                position=position,
                valid=valid,
                affiliations=affiliations,
                orcid=orcid,
                roles=roles,
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
                position=position,
                valid=valid,
                affiliations=affiliations,
                orcid=orcid,
                roles=roles,
            )
        remainder = {
            subtag.tag
            for subtag in tag
            if subtag.tag not in {"LastName", "ForeName", "Initials", "AffiliationInfo"}
        }
        # TODO can come back to this and do more debugging
        logger.debug(
            f"[{doc_key}] no forename given in {tag} w/ last name {last_name_tag.text}. "
            f"Other tags to check: {remainder}"
        )
        return None

    return Author(
        position=position,
        valid=valid,
        name=name,
        affiliations=affiliations,
        orcid=orcid,
        roles=roles,
    )


class Qualifier(BaseModel):
    """Represents a MeSH qualifier."""

    name: str
    mesh_id: str | None = None
    major: bool = False


class Heading(BaseModel):
    """Represents a MeSH heading annnotation."""

    name: str
    mesh_id: str
    major: bool = False
    qualifiers: list[Qualifier] | None = None


MESH_MISSES: set[str] = set()


def parse_mesh_heading(
    mesh_heading_tag: Element, *, mesh_grounder: ssslm.Grounder | None
) -> Heading | None:
    """Parse a MeSH heading."""
    descriptor_name_tag = mesh_heading_tag.find("DescriptorName")
    if descriptor_name_tag is None:
        return None

    descriptor_name = descriptor_name_tag.text
    descriptor_mesh_id = _get_mesh_id(descriptor_name_tag, mesh_heading_tag=mesh_heading_tag)

    if not descriptor_name and not descriptor_mesh_id:
        return None
    elif descriptor_name and not descriptor_mesh_id:
        best_match = (
            mesh_grounder.get_best_match(descriptor_name.rstrip("."))
            if mesh_grounder is not None
            else None
        )
        if best_match is not None:
            descriptor_mesh_id = best_match.identifier
        else:
            if descriptor_name not in MESH_MISSES:
                tqdm.write(f"could not ground mesh descriptor: {descriptor_name}")
                MESH_MISSES.add(descriptor_name)
            return None
    elif descriptor_mesh_id and not descriptor_name:
        raise NotImplementedError("need to lookup descriptor MeSH name automatically")
    # else, name and MeSH ID both available, and all good to continue

    major = _parse_yn(descriptor_name_tag.attrib["MajorTopicYN"])
    qualifiers = []
    # FIXME is this supposed to look in tag or descriptor_name_tag
    for qualifier_tag in mesh_heading_tag.findall("QualifierName"):
        qualifier_mesh_id = qualifier_tag.attrib.get("UI")
        qualifiers.append(
            Qualifier(
                name=qualifier_tag.text,
                mesh_id=qualifier_mesh_id,
                major=_parse_yn(qualifier_tag.attrib["MajorTopicYN"]),
            )
        )

    return Heading(
        mesh_id=descriptor_mesh_id,
        name=descriptor_name,
        major=major,
        qualifiers=qualifiers or None,
    )


MESH_RDF_URI_PREFIX = "https://id.nlm.nih.gov/mesh/"


def _get_mesh_id(
    descriptor_name_tag: Element, mesh_heading_tag: Element | None = None
) -> str | None:
    if "UI" in descriptor_name_tag.attrib:
        return descriptor_name_tag.attrib["UI"].removeprefix(MESH_RDF_URI_PREFIX)
    if "URI" in descriptor_name_tag.attrib:
        return descriptor_name_tag.attrib["URI"].removeprefix(MESH_RDF_URI_PREFIX)
    if mesh_heading_tag is not None and "URI" in mesh_heading_tag.attrib:
        return mesh_heading_tag.attrib["URI"].removeprefix(MESH_RDF_URI_PREFIX)
    return None


def _parse_yn(s: str) -> bool:
    match s:
        case "Y":
            return True
        case "N":
            return False
        case _:
            raise ValueError(s)


SPLOOSHED_RE = re.compile(r"^\d{15}(\d|X)$")


def _clean_orcid(s: str) -> str | None:
    for p in ORCID_PREFIXES:
        if s.startswith(p):
            return s[len(p) :]
    if len(s) == 19:
        return s
    elif len(s) == 18:
        # malformed, someone forgot the last value
        return None
    elif SPLOOSHED_RE.match(s):
        # malformed, forgot dashes
        return f"{s[:4]}-{s[4:8]}-{s[8:12]}-{s[12:]}"
    elif len(s) == 17 and s.startswith("s") and SPLOOSHED_RE.match(s[1:]):
        return f"{s[1:5]}-{s[5:9]}-{s[9:13]}-{s[13:]}"
    elif len(s) == 20:
        # extra character got OCR'd, mostly from linking to affiliations
        return s[:20]
    else:
        logger.debug(f"unhandled ORCID: {s}")
        return None


def _json_default(o: Any) -> Any:
    if isinstance(o, datetime.date | datetime.datetime):
        return o.isoformat()
    return o


def clean_pubmed_ids(pubmed_ids: Iterable[str | int]) -> Iterable[str]:
    """Clean a list of PubMed identifiers."""
    for pubmed_id in pubmed_ids:
        if isinstance(pubmed_id, int):
            yield str(pubmed_id)
        elif isinstance(pubmed_id, str):
            yield str(int(pubmed_id.strip()))
        else:
            raise TypeError
