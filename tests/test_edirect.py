"""Test eDirect utilities."""

import unittest
from pathlib import Path

from pubmed_downloader.client import get_edirect_directory, search_with_api, search_with_edirect


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
