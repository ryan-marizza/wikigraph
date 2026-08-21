import json
import pathlib

import pytest

CORPUS = pathlib.Path("tests/fixtures/articles.jsonl")


@pytest.fixture(scope="session")
def articles():
    """Real articles sampled from the warehouse by scripts/sample_articles.py.

    Skips rather than fails when the corpus is absent, so CI (which has no
    warehouse) still runs layers 1 and 5.
    """
    if not CORPUS.exists():
        pytest.skip(f"{CORPUS} not present -- run scripts/sample_articles.py")
    with CORPUS.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


@pytest.fixture(scope="session")
def sample_wikitext(articles):
    return [a["wikitext"] for a in articles]