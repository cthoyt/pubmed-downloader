"""Calculate stas."""

import textwrap
from collections import Counter

import click
import pystow
from tabulate import tabulate

from pubmed_downloader import iterate_process_articles

DOCUMENT_SECTION_TSV_PATH = pystow.join("pubmed", name="document-sections.tsv")


def _normalize(s: str) -> str:
    s = s.strip()
    s = s.replace(" & ", "and")
    s = s.lower()
    return s


XX = {
    "abstract": {
        "ASTRACT",
        "ABSTRACT",
        "Abstract",
    },
    "background": {
        "Background",
        "BACKGROUND/AIMS",
        "BACKGROUND & AIM",
        "background&aims",
        "BACKGROUND AND AIMS",
        "backgroundandaims",
        "BACKGROUND AND PURPOSE",
        "BACKGROUND & OBJECTIVE",
        "background and objectives",
        "BACKGROUND AND OBJECTIVE",
        "BACKGROUND AND AIM",
        "MOTIVATION",
        "Aims",
        "Motivation",
        "BACKGROUND/OBJECTIVES",
        "BACKGROUND/OBJECTIVE",
    },
    "objective": {
        "Objective",
        "OBJECTIVE",
        "OBJECTIVES",
        "study objectives",
        "rationaleandobjective",
        "purpose",
    },
    "introduction": {
        "Introduction",
        "INTRODUCTION AND IMPORTANCE",
        "BACKGROUND/INTRODUCTION",
        "INTRODUCTION/OBJECTIVES",
        "INTRODUCTION AND OBJECTIVES",
    },
    "materials and methods": {
        "METHOD",
        "Methods",
        "RESEARCH DESIGN AND METHODS",
        "PATIENTS AND METHODS",
        "MATERIAL AND METHODS",
        "MATERIAL AND METHOD",
        "materiel and method",
        "MATERIALS & METHODS",
        "METHODS AND STUDY DESIGN",
        "INVESTIGATION AND METHODOLOGIES",
        "METHODOLOGY",
        "EXPERIMENTAL DESIGN",
        "METHOD AND ANALYSIS",
        "STUDY DESIGN",
        "Design, Setting, and Participants",
        "STUDY DESIGN AND METHODS",
        "MATERIALS AND METHOD",
        "METHODS AND MATERIALS",
        "Method",
        "Methodology",
        "Materials and Methods",
        "MATERIALS",
        "MATERLALS AND METHODS",
        "DESIGN/METHODOLOGY/APPROACH",
    },
    "methods and results": {
        "METHODS AND RESULTS",
        "METHODS AND ANALYSIS",
        "APPROACH AND RESULTS",
        "methods and results",
        "METHODS AND FINDINGS",
    },
    "results": {
        "RESULT",
        "MEASUREMENTS AND MAIN RESULTS",
        "MAIN OUTCOME MEASURES",
        "Main Outcome and Measures",
        "MAIN OUTCOMES AND MEASURES",
        "Results",
        "MAIN OUTCOME MEASURE",
        "Design",
        "THE RESULTS",
        "MAIN RESULTS",
        "Main Outcomes and Measures",
        "FINDINGS",
        "OUTCOMES",
        "MAIN FINDINGS",
    },
    "results and discussion": {
        "RESULTS AND DISCUSSION",
        "RESULTS AND LIMITATIONS",
        "KEY FINDINGS AND LIMITATIONS",
    },
    "discussion": {
        "DISCUSSION",
        "Discussion",
        "Interpretation",
    },
    "discussion and conclusion": {
        "DISCUSSION & CONCLUSION",
        "DISSCUSSION AND CONCLUSION",
        "DISCUSSION AND CONCLUSIONS",
        "DISCUSSION AND CONCLUSION",
        "DISCUSSION/CONCLUSIONS",
        "discussion and conclusion",
        #
        "DISCUSSION AND IMPLICATIONS",
        #
        "LIMITATIONS",
        "Limitations",
        #
        "Future work",
    },
    "conclusion": {
        "IN CONCLUSION",
        "Conclusion",
        "AUTHORS' CONCLUSIONS",
        "Conclusions",
        "CONCLUSIONS",
        "take-home message",
        "take-home messages",
        #
        "CONCLUSION AND IMPLICATIONS",
        "CONCLUSIONS AND IMPLICATIONS",
        "CONCLUSIONS AND CLINICAL IMPLICATIONS",
        "Conclusions and Relevance",
        "CONCLUSIONS AND RELEVANCE",
        "conclusion and significance",
    },
    "funding": {
        "FUNDING",
        "Funding",
        "FUNDINGS",
    },
}
RELABELING = {_normalize(value): k for k, values in XX.items() for value in values}


def main() -> None:
    """Calculate stats."""
    label_counter: Counter[tuple[str, bool]] = Counter()
    category_counter: Counter[str] = Counter()

    n = 10_000
    for article, _ in zip(
        iterate_process_articles(source="local", ground=False), range(n), strict=False
    ):
        for t in article.abstract:
            if t.label:
                label = _normalize(t.label)
                if label in RELABELING:
                    label_counter[RELABELING[label], True] += 1
                else:
                    label_counter[label, False] += 1
            if t.category:
                category_counter[t.category] += 1

    click.echo("Label Counter")
    click.echo(
        tabulate(
            [
                (textwrap.shorten(a, 40), b, count)
                for (a, b), count in label_counter.most_common()
                if count > 1_000
            ]
        )
    )
    with DOCUMENT_SECTION_TSV_PATH.open("w") as file:
        for (a, b), c in label_counter.most_common():
            print(a, b, c, sep="\t", file=file)

    click.echo("\nCategory Counter")
    click.echo(tabulate(category_counter.most_common()))


if __name__ == "__main__":
    main()
