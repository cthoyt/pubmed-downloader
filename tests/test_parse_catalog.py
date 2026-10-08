"""Test parsing catalog entries."""

import unittest
from pathlib import Path
from typing import Any

from curies import NamableReference
from ssslm import EmptyGrounder, Grounder, Match

from pubmed_downloader.catalog import PERIODICAL, _parse_catalog_helper
from pubmed_downloader.utils import Heading

HERE = Path(__file__).parent.resolve()
EXAMPLE_PATH = HERE.joinpath("serfile.xml")

BIOLOGICAL_AVAILABILITY_REFERENCE = NamableReference(
    prefix="mesh", identifier="D001682", name="Biological Availability"
)
ENVIRONMENTAL_POLLUTANTS_REFERENCE = NamableReference(
    prefix="mesh", identifier="D004785", name="Environmental Pollutants"
)
REFS = {
    "Biological Availability": BIOLOGICAL_AVAILABILITY_REFERENCE,
    "Environmental Pollutants": ENVIRONMENTAL_POLLUTANTS_REFERENCE,
    "Periodical": PERIODICAL,
}


def get_mock_grounder(lookup: dict[str, NamableReference]) -> Grounder:
    """Get a grounder from a lookup."""

    class MockGrounder(EmptyGrounder):
        """A mock grounder."""

        def get_matches(self, text: str, *, strict: bool = False, **kwargs: Any) -> list[Match]:
            """Get mathces."""
            reference = lookup.get(text)
            if reference is not None:
                return [Match(reference=reference, score=1)]
            return []

    return MockGrounder()


class TestParseCatalog(unittest.TestCase):
    """Test parsing catalog entries."""

    def test_parse_catalog(self) -> None:
        """Test parsing catalog entries."""
        grounder = get_mock_grounder(REFS)
        records = list(
            _parse_catalog_helper(
                EXAMPLE_PATH,
                mesh_grounder=grounder,
                ror_grounder=grounder,
                author_grounder=grounder,
            )
        )
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual("9919264951506676", record.nlm_catalog_id)
        self.assertEqual(
            [
                Heading(major=False, reference=BIOLOGICAL_AVAILABILITY_REFERENCE),
                Heading(major=True, reference=ENVIRONMENTAL_POLLUTANTS_REFERENCE),
            ],
            record.headings,
        )
        self.assertEqual([PERIODICAL], record.publication_types)
        self.assertTrue(record.is_periodical)
        self.assertFalse(record.is_conference_proceedings)
