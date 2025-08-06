"""Calculate stas."""

from collections import Counter

import click
from tabulate import tabulate

from pubmed_downloader import iterate_process_articles

RELABELLING = {
    "ASTRACT": "ABSTRACT",
    "ABSTRACT": "ABSTRACT",
    "Abstract": "ABSTRACT",

    "Background": "BACKGROUND",
    "BACKGROUND/AIMS": "BACKGROUND",
    "BACKGROUND & AIM": "BACKGROUND",
    "BACKGROUND AND PURPOSE": "BACKGROUND",

    "OBJECTIVE": "OBJECTIVES",
    "OBJECTIVES": "OBJECTIVES",

    "Introduction": "INTRODUCTION",


    "RESULT": "RESULTS",
    "THE RESULTS": "RESULTS",
    "MAIN RESULTS": "RESULTS",
    "Main Outcomes and Measures": "RESULTS",
    "FINDINGS": "RESULTS",
    "OUTCOMES": "RESULTS",

    "METHOD": "METHODS",
    "Methods": "METHODS",
    "RESEARCH DESIGN AND METHODS": "METHODS",
    "PATIENTS AND METHODS": "METHODS",
    "MATERIAL AND METHODS": "MATERIALS AND METHODS",
    "MATERIAL AND METHOD": "MATERIALS AND METHODS",
    "MATERIALS & METHODS": "MATERIALS AND METHODS",
    "METHODS AND STUDY DESIGN": "MATERIALS AND METHODS",
    "INVESTIGATION AND METHODOLOGIES": "MATERIALS AND METHODS",
    "METHODOLOGY": "MATERIALS AND METHODS",
    "EXPERIMENTAL DESIGN": "MATERIALS AND METHODS",

    "METHODS AND RESULTS": "METHODS AND RESULTS",

    "DISCUSSION": "DISCUSSION",
    "Discussion": "DISCUSSION",

    "DISCUSSION & CONCLUSION": "DISCUSSION AND CONCLUSION",
    "DISCUSSION AND CONCLUSION": "DISCUSSION AND CONCLUSION",

    "IN CONCLUSION": "CONCLUSION",
    "Conclusion": "CONCLUSION",
    "Conclusions": "CONCLUSION",
    "CONCLUSIONS": "CONCLUSION",
    "CONCLUSIONS AND IMPLICATIONS": "CONCLUSION",
    "Conclusions and Relevance": "CONCLUSION",
    "CONCLUSIONS AND RELEVANCE": "CONCLUSION",
    
    "FUNDING": "FUNDING",
    "Funding": "FUNDING",
}


def main() -> None:
    """Calculate stats."""
    label_counter: Counter[str] = Counter()
    category_counter: Counter[str] = Counter()

    n = 10_000_000
    n = 10_000
    for article, _ in zip(iterate_process_articles(), range(n), strict=False):
        for t in article.abstract:
            if t.label:
                label_counter[RELABELLING.get(t.label, t.label)] += 1
            if t.category:
                category_counter[t.category] += 1

    click.echo("Label Counter")
    click.echo(tabulate(label_counter.most_common()))

    click.echo("\nCategory Counter")
    click.echo(tabulate(category_counter.most_common()))


if __name__ == "__main__":
    main()
