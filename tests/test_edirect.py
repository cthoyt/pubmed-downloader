"""Test eDirect utilities."""

import unittest
from pubmed_downloader.rest import search_pubmed


class TestEDirect(unittest.TestCase):
    """Test eDirect."""

    def test_search_pubmed(self) -> None:
        """Test searching PubMed."""
        res = search_pubmed("bioregistry")  # should be minimum 17 results
        self.assertLessEqual(17, len(res))
