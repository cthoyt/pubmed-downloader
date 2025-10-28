"""Calculate statistics over the document types."""

from collections import Counter

import click
import pyobo
import pystow
from tabulate import tabulate

from pubmed_downloader import iterate_process_articles

DOCUMENT_TYPES_TSV_PATH = pystow.join("pubmed", name="document-types.tsv")


def main() -> None:
    """Calculate stats."""
    pyobo.get_name("mesh", "D016441")
    article_type_counter: Counter[str] = Counter()

    n = None
    it = iterate_process_articles(source="local", ground=False)
    if n is not None:
        it = (article for article, _ in zip(it, range(n), strict=False))

    for article in it:
        for x in article.type_mesh_ids:
            if not x:
                continue
            article_type_counter[x] += 1

    rows = [
        (mesh_id, pyobo.get_name("mesh", mesh_id), count)
        for mesh_id, count in article_type_counter.most_common()
    ]

    click.echo("Document Type Counter")
    click.echo(tabulate(rows))
    with DOCUMENT_TYPES_TSV_PATH.open("w") as file:
        for mesh_id, name, count in rows:
            print(mesh_id, name, count, sep="\t", file=file)


if __name__ == "__main__":
    main()
