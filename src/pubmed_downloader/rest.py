"""Interact with NCBI rest."""

import os
import shlex
import stat
import subprocess
from pathlib import Path

import click
import pystow

__all__ = [
    "search_pubmed",
]

URL = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/edirect.tar.gz"
URL_APPLE_SILICON = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/xtract.Silicon.gz"
MODULE = pystow.module("ncbi")


def get_edirect_directory() -> Path:
    """Get path to eSearch tool."""
    path = MODULE.ensure_untar(url=URL)

    # if you're on an apple system, you need to download this,
    # and later enable it from the security preferences
    filename = MODULE.ensure_gunzip("edirect", "edirect", url=URL_APPLE_SILICON)
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return path.joinpath("edirect")


def get_esearch() -> Path:
    """Get path to eSearch tool."""
    return get_edirect_directory().joinpath("esearch")


def get_efetch() -> Path:
    """Get path to eSearch tool."""
    return get_edirect_directory().joinpath("efetch")


def search_pubmed(query: str) -> list[str]:
    """Get PubMed identifiers for a query."""
    search_exe = get_esearch()
    fetch_exe = get_esearch()
    # cmd = ['./esearch', '-db', 'pubmed', '-query', shlex.quote(query)]
    # print(cmd)
    # # cmd = f'{search_exe} -db pubmed -query {shlex.quote(query)} | {fetch_exe} -format uid'
    # res = subprocess.check_output(cmd, cwd=get_edirect_directory().as_posix())
    # print('got res', res)

    env = os.environ.copy()
    env["PATH"] = get_edirect_directory().as_posix() + os.pathsep + env["PATH"]

    p1 = subprocess.Popen(
        [search_exe.as_posix(), "-db", "pubmed", "-query", shlex.quote(query)],
        stdout=subprocess.PIPE,
        env=env,
    )
    p2 = subprocess.Popen(
        [fetch_exe.as_posix(), "-format", "uid"], stdin=p1.stdout, stdout=subprocess.PIPE, env=env
    )
    output, _ = p2.communicate()

    if not isinstance(output, str) or "not found" in output:
        raise RuntimeError

    # If there are more than 10k IDs, the CLI outputs a . for each
    # iteration, these have to be filtered out
    return [pubmed for pubmed in output.split("\n") if "." not in pubmed]


if __name__ == "__main__":
    click.echo(search_pubmed("bioregistry"))
