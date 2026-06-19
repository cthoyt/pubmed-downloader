"""Test parsing the NLM journal overview file."""

import unittest
from pathlib import Path

from pubmed_downloader.catalog import _parse_journals

HERE = Path(__file__).parent.resolve()
JOURNAL_OVERVIEW_PATH = HERE.joinpath("journal_overview_sample.txt")


class TestCatalog(unittest.TestCase):
    """Test NLM Catalog parsing."""

    def test_parse_journal_overview(self) -> None:
        """Parse a J_Entrez/J_Medline-format journal overview file.

        Regression test: ``process_journal_overview()`` (via ``_parse_journals``)
        raises a pydantic ``ValidationError`` on the real overview files because
        ``Journal.start_year`` / ``Journal.end_year`` are required, yet those
        files never provide them.
        """
        journals = list(_parse_journals(JOURNAL_OVERVIEW_PATH))
        by_nlm = {journal.nlm_catalog_id: journal for journal in journals}
        self.assertEqual({"7708172", "0410462"}, set(by_nlm))

        nature = by_nlm["0410462"]
        self.assertEqual("Nature", nature.title)
        self.assertEqual("Nature", nature.abbreviation_iso)
        self.assertEqual("Nature", nature.abbreviation_medline)
        # The overview file carries no publication years.
        self.assertIsNone(nature.start_year)
        self.assertIsNone(nature.end_year)
        self.assertIn("0028-0836", {issn.value for issn in nature.issns})
        self.assertIn("1476-4687", {issn.value for issn in nature.issns})
