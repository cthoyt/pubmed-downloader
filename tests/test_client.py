"""Test eDirect utilities."""

import datetime
import unittest
from pathlib import Path

from lxml import etree

from pubmed_downloader import Author
from pubmed_downloader.api import Grant, History, _extract_article
from pubmed_downloader.client import (
    get_abstracts,
    get_edirect_directory,
    get_titles,
    search_with_api,
    search_with_edirect,
)

HERE = Path(__file__).parent.resolve()
SAMPLE_PATH = HERE.joinpath("sample.xml")


class TestEDirect(unittest.TestCase):
    """Test eDirect."""

    def test_search_with_edirect(self) -> None:
        """Test searching PubMed."""
        d = get_edirect_directory()
        self.assertIsInstance(d, Path)

        pubmeds = search_with_edirect("bioregistry")  # should be minimum 17 results
        self.assertTrue(all(pubmed.isnumeric() for pubmed in pubmeds), msg=f"Result: {pubmeds}")

        # This is `Unifying the identification of biomedical entities with the Bioregistry`
        self.assertIn("36402838", pubmeds)

    def test_search_with_api(self) -> None:
        """Test searching PubMed."""
        pubmeds = search_with_api("bioregistry")  # should be minimum 17 results
        self.assertTrue(all(pubmed.isnumeric() for pubmed in pubmeds), msg=f"Result: {pubmeds}")

        # This is `Unifying the identification of biomedical entities with the Bioregistry`
        self.assertIn("36402838", pubmeds)

    def test_titles(self) -> None:
        """Test getting titles."""
        self.assertEqual(
            [
                "Disease networks. Uncovering disease-disease relationships "
                "through the incomplete interactome.",
                "Early developmental conditioning of later health and disease: "
                "physiology or pathophysiology?",
            ],
            get_titles(["25700523", "25287859"]),
        )

    def test_abstracts(self) -> None:
        """Test getting abstracts."""
        a1, a2 = get_abstracts(["25700523", "25287859"])
        self.assertIn(
            "Here we derive mathematical conditions for the identifiability of disease", a1
        )
        self.assertIn("Extensive experimental animal studies and epidemiological obse", a2)

    def test_parse(self) -> None:
        """Test parsing."""
        try:
            from orcid_downloader.lexical import get_orcid_grounder
        except ImportError:
            author_grounder = None
        else:
            author_grounder = get_orcid_grounder()

        root = etree.parse(SAMPLE_PATH)
        article_element = root.find("PubmedArticle")
        article = _extract_article(
            article_element, ror_grounder=None, mesh_grounder=None, author_grounder=author_grounder
        )
        if article is None:
            raise ValueError
        self.assertIn(
            History(status="received", date=datetime.date(year=2022, month=7, day=16)),
            article.history,
        )

        author_orcids = {author.orcid for author in article.authors if isinstance(author, Author)}
        if author_grounder is not None:
            self.assertIn("0000-0003-4423-4370", author_orcids)

        # parsing out journal issue information
        self.assertEqual(datetime.date(year=2022, month=11, day=19), article.date_published)
        self.assertEqual("9", article.journal_issue.volume)
        self.assertEqual("1", article.journal_issue.issue)

        self.assertIn(
            Grant(id="R24 OD011883", acronym="OD", agency="NIH HHS", country="United States"),
            article.grants,
        )
