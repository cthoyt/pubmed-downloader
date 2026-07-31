"""Test parsing the NLM journal overview file."""

import unittest
from pathlib import Path

from pubmed_downloader.catalog import _parse_journals

# The text fixture (journal_overview_sample.txt) consists of the first two entries from
# https://ftp.ncbi.nlm.nih.gov/pubmed/J_Entrez.txt as of July 31, 2026 (except that I
# changed the ISO abbreviation for JnId 2 to add " (ISO)" so I could confirm it was being
# read correctly).
HERE = Path(__file__).parent.resolve()
JOURNAL_OVERVIEW_PATH = HERE.joinpath("journal_overview_sample.txt")


class TestCatalog(unittest.TestCase):
    """Test NLM Catalog parsing."""

    def test_parse_journal_overview(self) -> None:
        """Test parsing a J_Entrez/J_Medline-format journal overview file.

        catalog.process_journal_overview() downloads one of two journal files (J_Entrez.txt
        or J_Medline.txt) from PubMed and parses them using _parse_journals(). This test
        ensures that a short example file (a test fixture at tests/journal_overview_sample.txt) can
        be parsed without throwing an error.

        Regression test: this test previously failed because the code never sets start_year and
        end_year for catalog.Journal() dataclass entries, where None values are allowed but no
        default values are provided. None of the entries in either file includes start and end years.

        PR https://github.com/cthoyt/pubmed-downloader/pull/16 adds a default `None` value for both
        fields so that validation passes.
        """
        journals = list(_parse_journals(JOURNAL_OVERVIEW_PATH))

        # Make sure both entries have loaded.
        by_nlm = {journal.nlm_catalog_id: journal for journal in journals}
        self.assertEqual({"7708172", "0431420"}, set(by_nlm))

        # Check whether JrId 2 (AANA J, NlmId 0431420) is read correctly.
        aana_j = by_nlm["0431420"]
        self.assertEqual(aana_j.title, "AANA journal")
        self.assertEqual(aana_j.abbreviation_iso, "AANA J (ISO)")
        self.assertEqual(aana_j.abbreviation_medline, "AANA J")
        self.assertEqual({"0094-6354", "2162-5239"}, {issn.value for issn in aana_j.issns})

        # The overview file carries no publication years.
        self.assertIsNone(aana_j.start_year)
        self.assertIsNone(aana_j.end_year)
