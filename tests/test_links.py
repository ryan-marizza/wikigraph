"""Unit tests for the wikilink extractor.

Each case is (name, wikitext, [(target, {flags that must be true})]). The flag
set is exact -- a case asserting {"in_parens"} fails if in_italics is also set.
That strictness is the point: it's how you catch a counter that never resets.
"""
import pytest

from wikigraph.links import extract_links

FLAGS = ("in_parens", "in_italics", "in_template", "in_table",
         "in_ref", "in_infobox", "in_file_caption")

CASES = [
    # --- the basic shapes -------------------------------------------------
    ("plain",          "A [[Dog]] barks.",                    [("Dog", set())]),
    ("piped",          "A [[Canis familiaris|dog]] barks.",   [("Canis familiaris", set())]),
    ("anchor",         "See [[Dog#Behavior]].",               [("Dog", set())]),
    ("piped anchor",   "See [[Dog#Behavior|how dogs act]].",  [("Dog", set())]),
    ("pipe trick",     "A [[Dog|]] barks.",                   [("Dog", set())]),

    # --- the six context flags -------------------------------------------
    ("parens",         "Dog (from [[Latin]] canis) barks.",   [("Latin", {"in_parens"})]),
    ("paren closes",   "Dog (from [[Latin]]) and [[Cat]].", [("Latin", {"in_parens"}), ("Cat", set())]),
    ("italics",        "The ''[[Canis]]'' genus and [[Dog]].",
     [("Canis", {"in_italics"}), ("Dog", set())]),
    ("bold is not italic", "'''[[Dog]]''' is a mammal.",      [("Dog", set())]),
    ("bold italic",    "'''''[[Dog]]''''' x",                 [("Dog", {"in_italics"})]),
    ("template",       "{{about|the animal|[[Dog (disambiguation)]]}} A [[Dog]].",
     [("Dog (disambiguation)", {"in_template"}), ("Dog", set())]),
    ("infobox",        "{{Infobox animal|genus=[[Canis]]}}\nThe [[Dog]].",
     [("Canis", {"in_template", "in_infobox"}), ("Dog", set())]),
    ("nested templates", "{{a|{{b|[[X]]}}}} [[Y]]",
     [("X", {"in_template"}), ("Y", set())]),
    ("table",          "{|\n|-\n| [[Row]]\n|}\n[[After]]",
     [("Row", {"in_table"}), ("After", set())]),
    ("ref",            "Dogs<ref>[[Cited]]</ref> are [[Nice]].",
     [("Cited", {"in_ref"}), ("Nice", set())]),
    ("self-closing ref", 'Dogs<ref name="a" /> are [[Nice]].', [("Nice", set())]),
    ("file caption",   "[[File:d.jpg|thumb|A [[puppy]]]]\nThe [[Dog]].",
     [("File:d.jpg", set()), ("puppy", {"in_file_caption"}), ("Dog", set())]),

    # --- namespaces and non-links ----------------------------------------
    ("category",       "Text [[Category:Mammals]]",           [("Category:Mammals", set())]),
    ("leading colon",  "See [[:Category:Mammals]].",          [("Category:Mammals", set())]),
    ("interwiki",      "[[fr:Chien]] [[Dog]]",
     [("fr:Chien", set()), ("Dog", set())]),
    ("colon in title", "[[Dog: A Story]]",                    [("Dog: A Story", set())]),
    ("intra-page anchor is not an edge", "See [[#History]] and [[Dog]].",
     [("Dog", set())]),

    # --- things that must NOT become links --------------------------------
    ("comment",        "A <!-- [[Hidden]] --> [[Real]].",     [("Real", set())]),
    ("nowiki",         "A <nowiki>[[NotALink]]</nowiki> [[Real]].", [("Real", set())]),
    ("unterminated",   "A [[Dog and more text",               []),

    # --- the traps --------------------------------------------------------
    ("paren inside title", "See [[Mercury (planet)]] then [[Sun]].",
     [("Mercury (planet)", set()), ("Sun", set())]),
    ("apostrophes in prose", "It's a [[Dog]] and Bob's [[Cat]].",
     [("Dog", set()), ("Cat", set())]),
    ("italics reset at newline", "''unclosed italic\nThe [[Dog]].", [("Dog", set())]),
    ("parens reset at newline",  "Unclosed (paren\nThe [[Dog]].",   [("Dog", set())]),
    ("template pipe is not a table close", "{{cite|a|}} then [[Dog]]", [("Dog", set())]),
    ("broken outer brackets", "[[Broken [[Good]] tail",       [("Good", set())]),
]


@pytest.mark.parametrize("name,wikitext,expected", CASES, ids=[c[0] for c in CASES])
def test_case(name, wikitext, expected):
    got = [(l["target_raw"], {f for f in FLAGS if l[f]})
           for l in extract_links(wikitext)]
    assert got == expected

def _check_invariants(rows, wikitext):
    """Structural properties that hold for any input whatsoever."""
    # Ordinals are 1..n with no gaps -- this is the PRIMARY KEY of
    # raw.pagelink. A gap or a duplicate is a load failure, not a data quirk.
    assert [r["ordinal"] for r in rows] == list(range(1, len(rows) + 1))

    # Document order, strictly. min(ordinal) as a relevance proxy in
    # fct_article_link is only meaningful if this holds.
    offs = [r["char_offset"] for r in rows]
    assert offs == sorted(offs) and len(set(offs)) == len(offs)

    for r in rows:
        # Offsets index the ORIGINAL wikitext, not the blanked copy.
        assert wikitext[r["char_offset"]:r["char_offset"] + 2] == "[["
        # A target that still contains brackets means the scanner mis-cut.
        assert not ({"[", "]"} & set(r["target_raw"]))
        assert r["target_raw"] == r["target_raw"].strip() and r["target_raw"]
        # The anchor is split off, never left on the target.
        assert "#" not in r["target_raw"]
        for f in FLAGS:
            assert isinstance(r[f], bool)


def test_invariants_on_synthetic(sample_wikitext):
    for wt in sample_wikitext:
        _check_invariants(extract_links(wt), wt)


def test_deterministic(sample_wikitext):
    for wt in sample_wikitext:
        assert extract_links(wt) == extract_links(wt)