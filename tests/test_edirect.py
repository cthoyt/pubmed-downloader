"""Test eDirect utilities."""

import unittest
from pathlib import Path

from pubmed_downloader.edirect import get_edirect_directory, search_pubmed


class TestEDirect(unittest.TestCase):
    """Test eDirect."""

    def test_search_pubmed(self) -> None:
        """Test searching PubMed."""
        d = get_edirect_directory()
        self.assertIsInstance(d, Path)

        pubmeds = search_pubmed("bioregistry")  # should be minimum 17 results
        self.assertTrue(all(pubmed.isnumeric() for pubmed in pubmeds))
        self.assertLessEqual(17, len(pubmeds))
