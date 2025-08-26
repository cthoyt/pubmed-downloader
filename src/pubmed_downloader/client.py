"""Interact with NCBI rest."""

import logging
import os
import platform
import shlex
import stat
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal, TypeAlias, TypedDict, overload

import pystow
import requests
import ssslm
from lxml import etree
from more_itertools import batched
from pydantic import BaseModel
from ratelimit import limits, sleep_and_retry
from typing_extensions import NotRequired, Unpack

from .api import Article, _extract_article
from .utils import clean_pubmed_ids

__all__ = [
    "PubMedSearchKwargs",
    "SearchBackend",
    "count_search_results",
    "get_abstracts",
    "get_abstracts_dict",
    "get_articles",
    "get_titles",
    "get_titles_dict",
    "search",
    "search_with_api",
    "search_with_edirect",
]

logger = logging.getLogger(__name__)

PUBMED_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_FETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

URL = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/edirect.tar.gz"
URL_APPLE_SILICON = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/xtract.Silicon.gz"
URL_LINUX = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/xtract.Linux.gz"
MODULE = pystow.module("ncbi")

#: https://www.ncbi.nlm.nih.gov/books/NBK25497/ rate limit getting to the API
get = sleep_and_retry(limits(calls=3, period=1)(requests.get))


class PubMedSearchKwargs(TypedDict):
    """Keyword arguments for the PubMed search API.

    .. seealso:: https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch
    """

    use_text_word: NotRequired[bool]
    """
    Automatically add the ``[tw]`` type to the query to only search
    the title, abstract, and text fields. Useful to avoid spurious results.

    .. seealso:: https://www.nlm.nih.gov/bsd/disted/pubmedtutorial/020_760.html
    """
    retstart: NotRequired[int]
    retmax: NotRequired[int]
    reldate: NotRequired[int]
    maxdate: NotRequired[str]


class SearchResult(BaseModel):
    """Results from the PubMed search API."""

    count: int
    maximum: int
    start: int
    identifiers: list[str]
    query: str
    query_translation: str


#: The search backend
SearchBackend: TypeAlias = Literal["edirect", "api"]


def search(query: str, backend: SearchBackend | None = None, **kwargs: Any) -> list[str]:
    """Search PubMed."""
    if backend == "edirect":
        return search_with_edirect(query)
    elif backend == "api" or backend is None:
        return search_with_api(query, **kwargs)
    else:
        raise ValueError


def search_with_edirect(query: str) -> list[str]:
    """Get PubMed identifiers for a query."""
    injection = f"PATH={get_edirect_directory().as_posix()}:${{PATH}}"
    cmd = (
        f"{injection} esearch -db pubmed -query {shlex.quote(query)} "
        f"| {injection} efetch -format uid"
    )
    res = subprocess.getoutput(cmd)  # noqa:S605
    if "esearch: command not found" in res:
        raise RuntimeError("esearch is not properly on the filepath")
    if "efetch: command not found" in res:
        raise RuntimeError("efetch is not properly on the filepath")
    # If there are more than 10k IDs, the CLI outputs a . for each
    # iteration, these have to be filtered out
    pubmeds = [pubmed for pubmed in res.split("\n") if pubmed and "." not in pubmed]
    return pubmeds


def get_edirect_directory() -> Path:
    """Get path to eSearch tool."""
    path = MODULE.ensure_untar(url=URL)

    if platform.system() == "Darwin" and platform.machine() == "arm64":
        # if you're on an apple system, you need to download this,
        # and later enable it from the security preferences
        _ensure_xtract_command(URL_APPLE_SILICON)
    elif platform.system() == "Linux":
        _ensure_xtract_command(URL_LINUX)

    return path.joinpath("edirect")


def _ensure_xtract_command(url: str) -> Path:
    path = MODULE.ensure_gunzip("edirect", "edirect", url=url)

    # make sure that the file is executable
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


def search_with_api(
    query: str,
    **kwargs: Unpack[PubMedSearchKwargs],
) -> list[str]:
    """Search Pubmed for paper IDs given a search term.

    :param query:
        A term for which the PubMed search should be performed.
    :param kwargs:
        Additional keyword arguments to pass to the PubMed search as
        parameters. See https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch

    Here's an example XML response:

    .. code-block:: xml

        <?xml version="1.0" encoding="UTF-8" ?>
        <!DOCTYPE eSearchResult PUBLIC "-//NLM//DTD esearch 20060628//EN" "https://eutils.ncbi.nlm.nih.gov/eutils/dtd/20060628/esearch.dtd">
        <eSearchResult>
            <Count>422</Count>
            <RetMax>2</RetMax>
            <RetStart>0</RetStart>
            <IdList>
                <Id>40758384</Id>
                <Id>40535547</Id>
            </IdList>
            <TranslationSet/>
            <QueryTranslation>"Disease Ontology"[Text Word]</QueryTranslation>
        </eSearchResult>

    """
    result = _request_api(query, **kwargs)
    if len(result.identifiers) < result.count:
        logger.warning(
            "Not all PubMeds were returned for search `%s`. Limited by `retmax` of %d",
            query,
            result.maximum,
        )
    return result.identifiers


def count_search_results(query: str, **kwargs: Unpack[PubMedSearchKwargs]) -> int:
    """Count results."""
    return _request_api(query, **kwargs).count


def _request_api(query: str, **kwargs: Unpack[PubMedSearchKwargs]) -> SearchResult:
    if kwargs.pop("use_text_word", True):
        query += "[tw]"

    retmax = kwargs.pop("retmax", 10_000)
    if retmax <= 0:
        raise ValueError
    if retmax > 10_000:
        retmax = 10_000

    retstart = kwargs.pop("retstart", 0)
    if retstart < 0:
        raise ValueError

    params: dict[str, Any] = {
        "term": query,
        "retmax": retmax,
        "retstart": retstart,
        "db": "pubmed",
    }
    params.update(kwargs)
    res = get(PUBMED_SEARCH_URL, params=params, timeout=30)
    res.raise_for_status()
    tree = etree.fromstring(res.content)
    return SearchResult(
        count=int(tree.find("Count").text),
        maximum=int(tree.find("RetMax").text),
        start=int(tree.find("RetStart").text),
        query=query,
        query_translation=tree.find("QueryTranslation").text,
        identifiers=[element.text for element in tree.findall("IdList/Id")],
    )


ErrorStrategy: TypeAlias = Literal["raise", "none", "skip"]


# docstr-coverage:excused `overload`
@overload
def get_titles(
    pubmed_ids: Iterable[str | int], *, error_strategy: Literal["raise", "skip"] = ...
) -> list[str]: ...


# docstr-coverage:excused `overload`
@overload
def get_titles(
    pubmed_ids: Iterable[str | int], *, error_strategy: Literal["none"] = ...
) -> list[str | None]: ...


def get_titles(
    pubmed_ids: Iterable[str | int], *, error_strategy: ErrorStrategy = "raise"
) -> list[str] | list[str | None]:
    """Get titles."""
    return [
        article.title if article is not None else None
        for article in get_articles(pubmed_ids, error_strategy=error_strategy)
    ]


def get_titles_dict(pubmed_ids: Iterable[str | int]) -> dict[str, str]:
    """Get titles."""
    return {
        str(article.pubmed): article.title
        for article in get_articles(pubmed_ids, error_strategy="skip")
        if article.title
    }


# docstr-coverage:excused `overload`
@overload
def get_abstracts(
    pubmed_ids: Iterable[str | int], *, error_strategy: Literal["raise", "skip"] = ...
) -> list[str]: ...


# docstr-coverage:excused `overload`
@overload
def get_abstracts(
    pubmed_ids: Iterable[str | int], *, error_strategy: Literal["none"] = ...
) -> list[str | None]: ...


def get_abstracts(
    pubmed_ids: Iterable[str | int], *, error_strategy: ErrorStrategy = "raise"
) -> list[str] | list[str | None]:
    """Get abstracts."""
    return [
        article.get_abstract() if article is not None else None
        for article in get_articles(pubmed_ids, error_strategy=error_strategy)
    ]


def get_abstracts_dict(pubmed_ids: Iterable[str | int]) -> dict[str, str]:
    """Get abstracts."""
    return {
        str(article.pubmed): abstract
        for article in get_articles(pubmed_ids, error_strategy="skip")
        if (abstract := article.get_abstract())
    }


# docstr-coverage:excused `overload`
@overload
def get_articles(
    pubmed_ids: Iterable[str | int],
    *,
    ror_grounder: ssslm.Grounder | None = ...,
    mesh_grounder: ssslm.Grounder | None = ...,
    timeout: int | None = ...,
    error_strategy: Literal["raise", "skip"] = ...,
) -> Iterable[Article]: ...


# docstr-coverage:excused `overload`
@overload
def get_articles(
    pubmed_ids: Iterable[str | int],
    *,
    ror_grounder: ssslm.Grounder | None = ...,
    mesh_grounder: ssslm.Grounder | None = ...,
    timeout: int | None = ...,
    error_strategy: Literal["none"] = ...,
) -> Iterable[Article | None]: ...


def get_articles(  # noqa:C901
    pubmed_ids: Iterable[str | int],
    *,
    ror_grounder: ssslm.Grounder | None = None,
    mesh_grounder: ssslm.Grounder | None = None,
    timeout: int | None = None,
    error_strategy: ErrorStrategy = "none",
) -> Iterable[Article] | Iterable[Article | None]:
    """Get articles."""
    for subset in batched(pubmed_ids, 10_000):
        params = {"db": "pubmed", "id": ",".join(clean_pubmed_ids(subset)), "retmode": "xml"}
        response = get(PUBMED_FETCH_URL, params=params, timeout=timeout or 300)
        try:
            tree = etree.fromstring(response.text)
        except etree.XMLSyntaxError as e:
            if error_strategy == "skip":
                continue
            elif error_strategy == "none":
                yield None
            else:
                raise ValueError(f"could not extract article from response: {response.text}") from e
        else:
            for article_element in tree.findall("PubmedArticle"):
                article = _extract_article(
                    article_element, ror_grounder=ror_grounder, mesh_grounder=mesh_grounder
                )
                if article is not None:
                    yield article
                elif error_strategy == "skip":
                    continue
                elif error_strategy == "none":
                    yield None
                elif error_strategy == "raise":
                    raise ValueError(f"could not extract article from: {article_element}")
                else:
                    raise ValueError(f"invalid error strategy: {error_strategy}")
