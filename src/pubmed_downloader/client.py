"""Interact with NCBI rest."""

import os
import platform
import shlex
import stat
import subprocess
from pathlib import Path
from typing import Any, Literal

import pystow
import requests
from lxml import etree

__all__ = [
    "search_pubmed",
    "search_pubmed_api",
    "search_pubmed_edirect",
]

PUBMED_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
URL = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/edirect.tar.gz"
URL_APPLE_SILICON = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/xtract.Silicon.gz"
URL_LINUX = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/xtract.Linux.gz"
MODULE = pystow.module("ncbi")


def search_pubmed(
    query: str, backend: Literal["edirect", "api"] = "api", **kwargs: Any
) -> list[str]:
    """Search PubMed."""
    if backend == "edirect":
        return search_pubmed_edirect(query)
    elif backend == "api":
        return search_pubmed_api(query, **kwargs)
    else:
        raise ValueError


def search_pubmed_edirect(query: str) -> list[str]:
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


def search_pubmed_api(
    search_term: str,
    use_text_word: bool = True,
    retstart: int = 0,
    retmax: int = 10_000,
    reldate: int | None = None,
    maxdate: str | None = None,
    **kwargs: Any,
) -> list[str]:
    """Search Pubmed for paper IDs given a search term.

    Parameters
    ----------
    :param search_term:
        A term for which the PubMed search should be performed.
    :param use_text_word:
        Automatically add the ``[tw]`` type to the query to only search
        the title, abstract, and text fields. Useful to avoid spurious results.

        .. seealso:: https://www.nlm.nih.gov/bsd/disted/pubmedtutorial/020_760.html
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
    if use_text_word:
        search_term += "[tw]"
    params = {
        "term": search_term,
        "retmax": retmax,
        "retstart": retstart,
        "db": "pubmed",
    }
    if reldate:
        params["reldate"] = reldate
    if maxdate:
        params["maxdate"] = maxdate
    params.update(kwargs)
    res = requests.get(PUBMED_SEARCH_URL, params=params, timeout=30)
    res.raise_for_status()
    tree = etree.fromstring(res.content)
    return [element.text for element in tree.findall("IdList/Id")]
