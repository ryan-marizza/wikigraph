"""Compare our extractor's link targets against mwparserfromhell's.

This does NOT assert equality -- the two disagree legitimately, and forcing
them to agree would mean adopting mwparserfromhell's judgment calls wholesale.
It asserts that the disagreement rate stays under a threshold, and it PRINTS
the disagreements so you can read them.

Read them. Every threshold bump should be a decision, not a reflex.
"""
import collections

import pytest

mwparserfromhell = pytest.importorskip("mwparserfromhell")

from wikigraph.links import extract_links

# Measured on the committed corpus. Tighten it as you fix real divergences;
# if it ever needs LOOSENING, something regressed.
MAX_DISAGREEMENT = 0.02


def _mwph_targets(wikitext):
    code = mwparserfromhell.parse(wikitext)
    out = collections.Counter()
    for link in code.ifilter_wikilinks():
        t = str(link.title).split("#", 1)[0].strip().lstrip(":").strip()
        if t:
            out[t] += 1
    return out


def _ours(wikitext):
    return collections.Counter(l["target_raw"] for l in extract_links(wikitext))


def test_target_sets_agree(articles, capsys):
    missing = collections.Counter()   # they found it, we didn't
    extra = collections.Counter()     # we found it, they didn't
    total = 0

    for a in articles:
        theirs, ours = _mwph_targets(a["wikitext"]), _ours(a["wikitext"])
        total += sum(theirs.values())
        for t, n in (theirs - ours).items():
            missing[t] += n
        for t, n in (ours - theirs).items():
            extra[t] += n

    rate = (sum(missing.values()) + sum(extra.values())) / max(total, 1)
    with capsys.disabled():
        print(f"\ncompared {total:,} link occurrences across {len(articles)} articles")
        print(f"we missed {sum(missing.values()):,}, we invented {sum(extra.values()):,}"
              f"  -> {rate:.3%}")
        for label, c in (("MISSING", missing), ("EXTRA", extra)):
            for t, n in c.most_common(15):
                print(f"  {label:<8} {n:>5}  {t[:70]}")

    assert rate < MAX_DISAGREEMENT