"""Constants for PubMed Downloader."""

from __future__ import annotations

from typing import Literal

import pystow

__all__ = [
    "MODULE",
]

from pydantic import BaseModel

MODULE = pystow.module("pubmed")


class ISSN(BaseModel):
    """Represents an ISSN number, annotated with its type."""

    value: str
    type: Literal["Print", "Electronic"]
