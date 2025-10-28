"""Explore grants.

This script makes a count summary of agencies
in one file and outputs the full grant list in another.

The purpose here is to identify new/existing Bioregistry
prefixes for different grant agencies.
"""

from collections import Counter

import pystow

from pubmed_downloader import iterate_process_articles

GRANTS_TSV_PATH = pystow.join("pubmed", name="grants.tsv")
AGENCY_TSV_PATH = pystow.join("pubmed", name="agencies.tsv")


def main() -> None:
    """Explore grants."""
    ids_counter = Counter()
    agency_counter = Counter()
    examples = {}
    for article in iterate_process_articles(source="local"):
        for grant in article.grants:
            agency_counter[grant.agency] += 1
            ids_counter[grant.agency, grant.id] += 1
            if grant.agency not in examples and grant.id:
                examples[grant.agency] = grant.id

    with AGENCY_TSV_PATH.open("w") as file:
        for agency, count in agency_counter.most_common():
            print(agency, count, examples.get(agency) or "", file=file, sep="\t")

    with GRANTS_TSV_PATH.open("w") as file:
        for (agency, grant_id), count in ids_counter.most_common():
            print(agency, grant_id, count, file=file, sep="\t")


if __name__ == "__main__":
    main()
