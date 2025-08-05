"""Interact with NCBI rest."""

import os
import shlex
import stat
import subprocess
from pathlib import Path

import pystow

__all__ = [
    "search_pubmed",
]

URL = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/edirect.tar.gz"
URL_APPLE_SILICON = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/xtract.Silicon.gz"
MODULE = pystow.module("ncbi")


def search_pubmed(query: str) -> list[str]:
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
    pubmeds = [pubmed for pubmed in res.split("\n") if "." not in pubmed]
    return pubmeds


def get_edirect_directory() -> Path:
    """Get path to eSearch tool."""
    path = MODULE.ensure_untar(url=URL)

    # if you're on an apple system, you need to download this,
    # and later enable it from the security preferences
    filename = MODULE.ensure_gunzip("edirect", "edirect", url=URL_APPLE_SILICON)
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return path.joinpath("edirect")
