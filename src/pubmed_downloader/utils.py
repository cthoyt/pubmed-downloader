"""Constants for PubMed Downloader."""

from __future__ import annotations

import datetime
import logging
from typing import Literal
from xml.etree.ElementTree import Element

import pystow
from pydantic import BaseModel, Field

__all__ = [
    "MODULE",
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
]


class ISSN(BaseModel):
    """Represents an ISSN number, annotated with its type."""

    value: str
    type: Literal["Print", "Electronic"]


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

    valid: bool = True
    affiliations: list[str] = Field(default_factory=list)
    # must have at least one of name/orcid
    name: str | None = None
    orcid: str | None = None


def parse_author(pubmed: int, tag: Element) -> Author | None:  # noqa:C901
    """Parse an author XML object."""
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
            orcid = _clean_orcid(it.text)
            if not orcid:
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


class Qualifier(BaseModel):
    """Represents a MeSH qualifier."""

    mesh: str
    major: bool = False


class Heading(BaseModel):
    """Represents a MeSH heading annnotation."""

    descriptor: str
    major: bool = False
    qualifiers: list[Qualifier] | None = None


def parse_mesh_heading(tag: Element) -> Heading | None:
    """Parse a MeSH heading."""
    descriptor_name_tag = tag.find("DescriptorName")
    if descriptor_name_tag is None:
        return None

    if "UI" in descriptor_name_tag.attrib:
        mesh_id = descriptor_name_tag.attrib["UI"]
    elif "URI" in descriptor_name_tag.attrib:
        mesh_id = descriptor_name_tag.attrib["URI"].removeprefix("https://id.nlm.nih.gov/mesh/")
    else:
        raise ValueError("unable to get MeSH ID for descriptor")

    major = _parse_yn(descriptor_name_tag.attrib["MajorTopicYN"])
    qualifiers = [
        Qualifier(mesh=qualifier.attrib["UI"], major=_parse_yn(qualifier.attrib["MajorTopicYN"]))
        for qualifier in tag.findall("QualifierName")
    ]
    return Heading(descriptor=mesh_id, major=major, qualifiers=qualifiers or None)


def _parse_yn(s: str) -> bool:
    match s:
        case "Y":
            return True
        case "N":
            return False
        case _:
            raise ValueError(s)


def _clean_orcid(s: str) -> str | None:
    for p in ORCID_PREFIXES:
        if s.startswith(p):
            return s[len(p) :]
    if len(s) == 19:
        return s
    elif len(s) == 18:
        # malformed, someone forgot the last value
        return None
    elif len(s) == 16 and s.isnumeric():
        # malformed, forgot dashes
        return f"{s[:4]}-{s[4:8]}-{s[8:12]}-{s[12:]}"
    elif len(s) == 17 and s.startswith("s") and s[1:].isnumeric():
        return f"{s[1:5]}-{s[5:9]}-{s[9:13]}-{s[13:]}"
    elif len(s) == 20:
        # extra character got OCR'd, mostly from linking to affiliations
        return s[:20]
    else:
        logger.warning(f"unhandled ORCID: {s}")
        return None
