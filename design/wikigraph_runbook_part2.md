# WikiGraph — Hands-On Runbook, Part 2: Links, Graphs, and Scale

**Goal of this document:** get you from "`mart.dim_article` has real Wikipedia articles in it" to "I have a plotted graph, a working fuzzy search, a first-link chain walker, and all 19 shards loaded."

**Where you are.** You finished Part 1's Step 16. Concretely, that means:

- `raw.page` holds 717,051 ns=0 pages for shard `p10p1400054`, 46% of them redirects
- `stg.stg_page` and `mart.dim_article` build clean via dbt, all tests green
- `wikigraph_ingest` and `wikigraph_transform` both run green from the UI
- `.\tasks.ps1 db-nuke` → `db-up` → `migrate` rebuilds the warehouse in under two minutes
- `NOTES.md` has your measured parse throughput, Parquet sizes, and redirect share

If any of those isn't true, go back — everything here compounds on top of them.

**Scope:** still one shard, right up until Step 26, which is where the other 18 arrive.

**How to read this:** same four-part structure as Part 1 — **Why**, **Do**, **Verify**, **If it breaks**. Same instruction: do not skip **Verify**.

**Time estimate:** Steps 17–20 are a weekend. Steps 21–25 are another. Step 26 is an hour of setup and then somewhere between six and fifteen hours of wall clock you spend doing something else.

---

## Table of contents

| Step | What                                                    | Est.        |
| ---- | ------------------------------------------------------- | ----------- |
| —    | Before you start: two more rules                        | 15 min      |
| 17   | The link extractor                                      | 3 hr        |
| 18   | Testing the extractor properly                          | 3 hr        |
| 19   | `raw.pagelink` and the link loader                      | 1.5 hr      |
| 20   | The links DAG                                           | 1 hr        |
| 21   | `stg_pagelink`, redirect resolution, `fct_article_link` | 2.5 hr      |
| 22   | Your first graph plot                                   | 2 hr        |
| 23   | `article_alias` and the search cascade                  | 2.5 hr      |
| 24   | `fct_first_link`                                        | 1 hr        |
| 25   | The in-memory functional-graph walk                     | 2.5 hr      |
| 26   | Backfilling the remaining 18 shards                     | 1 hr + wait |
| 27   | Full-scale verification, and the real answers           | 1.5 hr      |

---

## Before you start

### Two more rules

Part 1 gave you two. Here are the two that matter for this half.

**Rule 3: Python parses strings. SQL does set operations. Neither crosses the line.**

You met this in Part 1 as a note on the dbt models. From here on it is load-bearing, because the link extractor is the single place where it would be most tempting to break it.

The extractor is going to see `[[New_York#Culture|the city]]` and it will be *very* tempting to normalize that to `New York` right there in Python. Don't. `public.norm_title()` in `V002` is the one implementation of MediaWiki title rules in this project, and the moment there is a second one in Python, the two drift, and the join between `pagelink.target_norm` and `stg_page.norm_title` silently drops some percentage of your edges. Nothing errors. Every graph statistic you compute afterwards is quietly, confidently wrong, and you have no way to notice.

So the extractor emits `target_raw` — the string exactly as it appeared inside the brackets, with only the anchor and the leading colon split off — and SQL does the rest.

**Rule 4: every rule you invent gets a version number.**

"What counts as the first link in an article" is not a fact you can look up. It is a judgment call with about a dozen sub-decisions, and you will change your mind about three of them. If you overwrite the old answer each time, you can never say whether a change made things better or worse.

So `mart.fct_first_link` carries a `rule_version` column, the extractor module carries a `RULE_VERSION` constant, and when you change the rules you bump the number and compare the two path distributions instead of guessing.

### The shape of what you're building

```
raw.page.wikitext ──extract──▶ Parquet ──COPY──▶ raw.pagelink ──dbt──▶ stg_pagelink
   (Step 19)                   (staging)          (partitioned)         (view or table)
                                                                             │
                                        ┌────────────────────────────────────┤
                                        ▼                                    ▼
                              mart.fct_article_link                  mart.article_alias
                                   (Goal #1)                            (Goal #2)
                                        │                                    │
                                        ▼                                    ▼
                              ego network → plot            mart.article_search + GIN
                                        │
                                        ▼
                              mart.fct_first_link ──walk.py──▶ derived.first_link_walk
                                                                    (Goal #3)
```

One new schema appears: **`derived`**, owned by `etl`. It exists because the functional-graph walk in Step 25 is a *Python-computed artifact* — dbt can't produce it, but dbt needs to read it. `raw` would be a lie (nothing about it is raw) and `mart` belongs to dbt. Giving it its own schema keeps the ownership model honest: dbt still owns `stg` and `mart` exclusively, and consumes `derived` as a source.

### The one place I've deviated from the design doc's order

The design doc's §6 sequencing puts the backfill last, after all three goals. I've kept that, but you should know what it costs, because it affects how you read Steps 24 and 25.

**Goal #1 (the graph plot) works genuinely well on one shard.** Shards are page-ID ranges, and `p10p1400054` is the *lowest* range — the oldest pages on Wikipedia, which are also the most-linked ones. An ego network around a hub article will be dense and real.

**Goal #2 (search) works, but on 387,504 articles out of ~14 million.** You'll type things and get nothing back, and that's the data's fault, not the query's.

**Goal #3 (first-link chains) barely works at all on one shard.** Every chain that leaves the shard's page-ID range dies. *Philosophy* is `page_id` 13,692,155, which lives in `p9093926p14370073` — a different shard. You will not find the classic attractor until Step 26.

I've still put them in this order, for one reason: the code for Steps 24 and 25 has to be *proven* on something small before you spend twelve hours of wall clock feeding it nineteen shards. Their purpose at this stage is to demonstrate that the mechanism is correct, not to produce the answer. Step 27 is where you get the answer.

If you'd rather have the real numbers sooner, the legitimate alternative is to run Step 26 immediately after Step 22 — the ingest and links DAGs are per-shard idempotent and the marts are rebuilt by dbt regardless, so nothing you build later cares whether one shard is loaded or nineteen. A third option, if you're impatient and have the disk: kick off the 19-shard backfill overnight *while* you write Steps 23–25 against whatever has landed. The only cost is that your row counts move under you during **Verify**, which makes reconciliation confusing. I'd rather you didn't, the first time through.

---

## Step 17 — The link extractor

### Why

This is the hardest correctness problem in the project, and it's worth being precise about *why* it's hard, because the difficulty is not where people expect.

Finding `[[...]]` is trivial. `re.findall(r"\[\[(.*?)\]\]", text)` gets you 95% of the targets in one line. The hard part is the other thing you need: **where each link was**. A link inside `{{Infobox}}` is not a body link. A link inside `(from [[Latin]] canis)` is not a body link. A link inside `<ref>` is a citation. Without those six flags, `mart.fct_first_link` cannot be built at all — you'd be picking whatever `[[` happened to come first in the raw text, which on a typical article is a navigation template or an image.

And you cannot recover context with more regex. Consider:

```
{{Infobox settlement | leader = [[Mayor]] | motto = ''Semper (fortis) [[Latin|paratus]]'' }}
```

Two links. One is in a template and an infobox. The other is in a template, an infobox, *italics*, and — depending on how you count — near a parenthesis that closes before it. A regex that tries to answer "is this link inside braces" has to count balanced pairs, at which point you have written a parser, badly, in a language designed for the opposite job.

**So: a single left-to-right scan with depth counters.** Walk the text once. Maintain a counter for parenthesis depth, one for template depth, one for table depth, one for `<ref>` depth, a template-name stack (so you can tell an infobox from a citation), and two booleans for italic and bold. When you hit `[[`, the flags for that link are just a *snapshot of the counters at that instant*. No backtracking, no matching, no ambiguity. Nesting is handled for free because counters nest.

This is also why it's fast. The alternative — running six independent regexes over the text and asking "does any match span contain this offset" — is O(links × spans) and gets the wrong answer on nesting anyway.

**On `mwparserfromhell`.** Your Airflow image already installs it (Part 1, Step 10). It is a good library and it will build you a real parse tree. It is not what you want *here*, for three reasons: it doesn't model parentheses at all (they're just text, so `in_parens` is unavailable), it doesn't hand you character offsets into the original wikitext (so you can't show yourself the context during manual review), and it's meaningfully slower on 387,504 articles per shard. What it *is* excellent for is a second opinion — Step 18 uses it as a differential test oracle, which is a much better use of it than as the primary implementation.

**Two implementation details worth understanding before you copy the code.**

**(a) Blank the non-wikitext regions, don't delete them.** HTML comments, `<nowiki>`, `<math>`, `<pre>` and friends contain text that looks like wikitext but isn't. The obvious move is to strip them before scanning. Don't — that shifts every subsequent character offset, and `char_offset` is what lets you slice the original wikitext to see the context a link was found in. Without that, manual review in Step 18 is impossible. Replace each such region with an equal-length run of spaces instead, preserving newlines so line-based rules still work.

**(b) Tokenize, don't step.** A character-by-character Python loop over a 31 KB article is slow enough to matter at 7.4 million articles. Instead, compile one regex containing every construct that can change scanner state and iterate `finditer`. Between tokens the regex engine skips in C. I measured both on synthetic articles of realistic density; the token approach was roughly 20× faster and it's no harder to read.

### Do

**17a. Create `src/wikigraph/links.py`.**

```python
"""Extract wikilinks from MediaWiki wikitext, with the context flags the
first-link rules need.

One left-to-right scan. State is a handful of depth counters; a link's flags
are a snapshot of those counters at the moment its `[[` is seen. There is no
backtracking and no post-hoc matching, which is what makes the flags cheap and
what makes nesting impossible to get wrong.

This module deliberately does NOT normalize titles. It emits `target_raw`
exactly as written and lets public.norm_title() in SQL canonicalize it.
Two implementations of MediaWiki title rules is the worst bug available in
this project -- see Rule 3.
"""
from __future__ import annotations

import re

# Bump when you change what this module considers a link or how it flags one.
# Stored alongside every extraction so you can tell two runs apart.
RULE_VERSION = 1

# Templates whose contents count as "infobox" for first-link purposes. Navboxes
# and sidebars are in here because they are navigation furniture, not prose --
# the flag is named for the common case, not the whole set.
INFOBOX_PREFIXES = (
    "infobox", "taxobox", "chembox", "drugbox", "speciesbox",
    "automatic taxobox", "sidebar", "navbox",
)

FILE_PREFIXES = frozenset({"file", "image", "media"})

MAX_LINKS_PER_PAGE = 20_000
MAX_LINK_NESTING = 4

# --------------------------------------------------------------------------
# Pre-pass: regions whose contents are not wikitext.
#
# BLANKED, not deleted, so char_offset stays a valid index into the ORIGINAL
# wikitext. You can always slice the source to see the context a link was found
# in, which is what makes the manual review in Step 18 possible at all.
# --------------------------------------------------------------------------
_BLANK_RE = re.compile(
    r"<!--.*?-->"
    r"|<nowiki[^>]*>.*?</nowiki\s*>"
    r"|<pre[^>]*>.*?</pre\s*>"
    r"|<math[^>]*>.*?</math\s*>"
    r"|<chem[^>]*>.*?</chem\s*>"
    r"|<score[^>]*>.*?</score\s*>"
    r"|<timeline[^>]*>.*?</timeline\s*>"
    r"|<syntaxhighlight[^>]*>.*?</syntaxhighlight\s*>"
    r"|<source[^>]*>.*?</source\s*>",
    re.DOTALL | re.IGNORECASE,
)


def _blank(m: re.Match) -> str:
    """Replace a region with spaces, preserving newlines and total length."""
    return "".join("\n" if ch == "\n" else " " for ch in m.group(0))


# Every construct that can change scanner state, and nothing else.
#
# Order matters: two-character tokens must precede the single-character class,
# or `[[` would be read as a bare `[`. `{|` and `|}` are anchored to line start
# because MediaWiki requires that -- and because without the anchor, the `|}}`
# at the end of `{{template|}}` would be misread as a table close, which then
# eats the `}` that `}}` needed and unbalances the template stack for the rest
# of the article. That bug is very hard to see from the output.
_TOKEN_RE = re.compile(
    r"\[\[|\]\]|\{\{|\}\}"
    r"|^[ \t]*\{\|"
    r"|^[ \t]*\|\}"
    r"|</?ref\b"
    r"|'{2,}"
    r"|[()|\n]",
    re.MULTILINE,
)

_HEADING_RE = re.compile(r"[ \t]*(={2,6})[ \t]*(.+?)[ \t]*\1[ \t]*(?=\n|$)")
_TMPL_NAME_RE = re.compile(r"[^|}\n\[{]{0,80}")
_TRAIL_RE = re.compile(r"[a-z]+")


def extract_links(wikitext: str | None) -> list[dict]:
    """Return one dict per wikilink occurrence, in document order."""
    if not wikitext:
        return []

    text = _BLANK_RE.sub(_blank, wikitext)
    links: list[dict] = []

    tmpl_stack: list[str] = []
    frames: list[dict] = []          # currently-open [[ ... ]] contexts
    paren = table = ref = infobox = 0
    italic = bold = False
    section: str | None = None
    skip_until = 0

    for m in _TOKEN_RE.finditer(text):
        pos = m.start()
        if pos < skip_until:
            continue
        raw_tok = m.group(0)
        tok = raw_tok.lstrip(" \t") if raw_tok[:1] in " \t" else raw_tok

        if tok == "[[":
            close = text.find("]]", pos + 2)
            if close == -1:
                break                        # no closer anywhere: nothing left to find
            pipe = text.find("|", pos + 2)
            cut = pipe if 0 <= pipe < close else close
            # Bounded on purpose: an unbounded find("\n\n") scans to the end of
            # the document on every link, which is genuinely quadratic on a
            # 300 KB list article with no blank lines.
            gap = text.find("\n\n", pos + 2, close)
            if 0 <= gap < cut:
                continue                     # blank line inside a link: malformed
            if len(frames) >= MAX_LINK_NESTING:
                continue

            raw = text[pos + 2:cut]
            if "[" in raw or "]" in raw:
                # `[[Broken [[Good]]` -- this `[[` is noise and the real link
                # starts further in. Fall through WITHOUT setting skip_until so
                # the inner `[[` is still scanned.
                continue

            skip_until = cut                 # never re-scan the target itself:
                                             # `[[Mercury (planet)]]` must not
                                             # increment the paren counter
            leading_colon = raw.startswith(":")
            body = raw[1:] if leading_colon else raw
            body, _, anchor = body.partition("#")
            target = body.strip()

            if not target:                   # [[#Section]] -- an intra-page anchor,
                                             # not an edge. Push a frame anyway so
                                             # the closing ]] stays balanced.
                frames.append({"idx": None, "is_file": False,
                               "pipe": False, "dstart": -1})
                continue

            prefix = None
            if ":" in target:
                cand = target.split(":", 1)[0].strip().lower()
                if cand and len(cand) <= 32:
                    prefix = cand            # a CANDIDATE prefix. SQL decides
                                             # whether it is really a namespace.
            is_file = prefix in FILE_PREFIXES

            if len(links) < MAX_LINKS_PER_PAGE:
                links.append({
                    "ordinal":         len(links) + 1,
                    "target_raw":      target,
                    "target_prefix":   prefix,
                    "anchor":          (anchor.strip() or None),
                    "display_text":    None,     # filled in at the closing ]]
                    "section_name":    section,
                    "leading_colon":   leading_colon,
                    "in_parens":       paren > 0,
                    "in_italics":      italic,
                    "in_template":     bool(tmpl_stack),
                    "in_table":        table > 0,
                    "in_ref":          ref > 0,
                    "in_infobox":      infobox > 0,
                    "in_file_caption": any(f["is_file"] for f in frames),
                    "char_offset":     pos,
                })
                idx = len(links) - 1
            else:
                idx = None
            frames.append({"idx": idx, "is_file": is_file,
                           "pipe": False, "dstart": -1})

        elif tok == "]]":
            if not frames:
                continue
            f = frames.pop()
            if f["idx"] is not None:
                lk = links[f["idx"]]
                if f["is_file"]:
                    pass                     # a caption is not an alias for the file
                elif f["pipe"]:
                    lk["display_text"] = text[f["dstart"]:pos].strip() or None
                else:
                    trail = _TRAIL_RE.match(text, pos + 2)
                    if trail:                # [[dog]]s renders as "dogs"
                        lk["display_text"] = lk["target_raw"] + trail.group(0)

        elif tok == "|":
            if frames and not frames[-1]["pipe"]:
                frames[-1]["pipe"] = True
                frames[-1]["dstart"] = pos + 1

        elif tok == "{{":
            name_m = _TMPL_NAME_RE.match(text, pos + 2)
            name = name_m.group(0).strip().lower() if name_m else ""
            skip_until = pos + 2 + (len(name_m.group(0)) if name_m else 0)
            tmpl_stack.append(name)
            if name.startswith(INFOBOX_PREFIXES):
                infobox += 1

        elif tok == "}}":
            if tmpl_stack:
                if tmpl_stack.pop().startswith(INFOBOX_PREFIXES):
                    infobox -= 1

        elif tok == "{|":
            table += 1
        elif tok == "|}":
            if table:
                table -= 1

        elif tok == "<ref":
            gt = text.find(">", pos)
            if gt == -1:
                continue
            skip_until = gt + 1
            if text[gt - 1] != "/":          # <ref name=x /> has no content
                ref += 1
        elif tok == "</ref":
            gt = text.find(">", pos)
            skip_until = gt + 1 if gt != -1 else pos + 5
            if ref:
                ref -= 1

        elif tok[0] == "'":
            n = len(tok)
            if n == 2:
                italic = not italic
            elif n in (3, 4):                # 4 apostrophes render as bold + a literal '
                bold = not bold
            else:
                italic = not italic
                bold = not bold

        elif tok == "(":
            paren += 1
        elif tok == ")":
            if paren:
                paren -= 1

        elif tok == "\n":
            # Deliberate: MediaWiki closes unterminated italics at end of line,
            # and resetting parens stops one stray '(' from poisoning every
            # link in the rest of the article. Anything that legitimately spans
            # lines is inside a template or a table and is already flagged.
            paren = 0
            italic = bold = False
            h = _HEADING_RE.match(text, pos + 1)
            if h and not tmpl_stack and table == 0:
                section = h.group(2).strip() or None

    return links


def extract_page_links(page_id: int, wikitext: str | None) -> list[dict]:
    rows = extract_links(wikitext)
    for r in rows:
        r["src_page_id"] = page_id
    return rows
```

**17b. Understand the seven flags you're producing.** The design doc names six. This emits seven.

| Flag              | True when                                                  | Why it's needed                                      |
| ----------------- | ---------------------------------------------------------- | ---------------------------------------------------- |
| `in_parens`       | inside unclosed `(` on the same line                       | Pronunciation guides, etymologies, birth/death dates |
| `in_italics`      | inside `''...''`                                           | Taxonomic names, book and film titles                |
| `in_template`     | inside any `{{...}}`                                       | Hatnotes, navboxes, citations, conversions           |
| `in_table`        | inside `{                                                  | ...                                                  |
| `in_ref`          | inside `<ref>...</ref>`                                    | Citations                                            |
| `in_infobox`      | inside a template whose name starts with an infobox prefix | The single biggest source of false first links       |
| `in_file_caption` | inside `[[File:...                                         | ...]]`                                               |

`in_file_caption` is the addition, and it isn't optional. The design doc's §4.3 exclusion list *says* "image captions" but the six flags it defines don't cover them, and a caption link is one of the most common false first links there is — the lead image sits before the first paragraph in the wikitext, and its caption is prose. Note it in the design doc as a seventh flag rather than leaving the discrepancy for future-you to trip over.

**17c. Add `numpy` to the package.** You need it in Step 25, and it's easier to install it now than to remember later.

In `pyproject.toml`, add to `dependencies`:

```toml
    "numpy>=2.0",
```

and add two new optional groups:

```toml
[project.optional-dependencies]
dev = ["pytest>=8", "ruff>=0.5", "pylint>=3.2", "mwparserfromhell>=0.7"]
viz = ["networkx>=3.3", "matplotlib>=3.9", "pyvis>=0.3.2"]
```

`mwparserfromhell` is a **dev** dependency — it's the differential-test oracle in Step 18, not part of the pipeline. While you're there, delete it from `airflow-docker/docker/requirements.txt`: the container never uses it, and an unused pin is a thing that can break your image build for no benefit.

`viz` is separate because networkx and matplotlib have no business inside the Airflow image either. Plotting is analysis you do on Windows.

```powershell
pip install -e ".[dev,viz]"
```

### Verify

A three-line sanity check before you write any tests. From the repo root with the venv active:

```powershell
python -c @"
from wikigraph.links import extract_links
wt = '''{{Infobox animal|genus=[[Canis]]}}
The '''dog''' (from [[Latin]] ''canis'') is a [[domestication|domesticated]] [[gray wolf]].<ref>[[Cited]]</ref>'''
for l in extract_links(wt):
    flags = ','.join(k[3:] for k in
        ('in_parens','in_italics','in_template','in_table','in_ref','in_infobox','in_file_caption') if l[k])
    print(f'{l[\"ordinal\"]:>2}  {l[\"target_raw\"]:<24} {flags}')
"@
```

Expected:

```
 1  Canis                    template,infobox
 2  Latin                    parens
 3  domestication
 4  gray wolf
 5  Cited                    ref
```

The link with no flags at the lowest ordinal is `domestication` — and that is the correct first link for that lead. If you get `Canis`, your template counter isn't running. If you get `Latin`, your paren counter isn't.

### If it breaks

- **Every link comes back with `in_template: True`** — the `}}` branch isn't popping. Most likely `{|`/`|}` are matching where they shouldn't; confirm both alternatives in `_TOKEN_RE` carry the `^[ \t]*` anchor and that you compiled with `re.MULTILINE`.
- **Links inside `[[Mercury (planet)]]`-style titles show `in_parens`** — `skip_until = cut` is missing or set after the `continue` guards. It has to run before the scanner reaches the target's own characters.
- **`display_text` is set on every link** — the trail branch is running unconditionally. It belongs in the `else` of the `f["pipe"]` check, and only when a trail actually matched.
- **Nothing is extracted from a page you can see links in** — check the pre-pass. A malformed `<ref>` early in a page with a `<!--` that never closes will blank the remainder of the article. That's correct behaviour for genuinely broken wikitext, but confirm it's not your regex.

Commit:

```powershell
git add src/wikigraph/links.py pyproject.toml airflow-docker/docker/requirements.txt
git commit -m "Wikilink extractor: single-pass scanner with seven context flags"
```

---

## Step 18 — Testing the extractor properly

### Why

The design doc says: *"Validate against 20 hand-checked articles before trusting it — an hour of manual checking saves a week."* That's right, and it's also not enough on its own. Twenty hand-checked articles catch the bugs you can see. They don't catch the ones that only appear on the 400,000th article, and they can't tell you whether a change you made last Tuesday broke something.

So this step builds **five layers**, each catching a class of bug the others miss:

| Layer                                 | Catches                                                       | Runs in |
| ------------------------------------- | ------------------------------------------------------------- | ------- |
| 1. Table-driven unit tests            | Wrong flag on a construct you thought about                   | 0.1 s   |
| 2. Invariants                         | Structural corruption — bad ordinals, overlapping offsets     | 0.5 s   |
| 3. Differential vs `mwparserfromhell` | Links you're missing or inventing                             | 20 s    |
| 4. Hand-checked gold set              | Wrong *definition* — the thing no automated test can find     | 5 s     |
| 5. Robustness and throughput          | Crashes and quadratic blowups on the tail of the distribution | 30 s    |

Layer 3 is worth dwelling on. A differential test compares your implementation against an independent one and reports where they disagree. It cannot tell you which is *right* — that's your job — but it is extremely good at finding the cases you never thought to write a test for, because the disagreements are exactly the constructs where your mental model and someone else's diverge. `mwparserfromhell` has a decade of real-world wikitext behind it. Use it as a foil, not as an authority.

Layer 4 is the one people skip and the one that matters most, because it's the only layer testing whether your *rules* are right rather than whether your *code* matches your rules. Everything else is self-referential.

### Do

**18a. Layer 1 — the table.** Create `tests/test_links.py`:

```python
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
    ("paren closes",   "Dog (from [[Latin]]) and [[Cat]].",
     [("Latin", {"in_parens"}), ("Cat", set())]),
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
```

Four of those deserve a comment, because they are the ones that fail if you write this from scratch without knowing about them:

- **`paren inside title`** — `[[Mercury (planet)]]` contains a `(` that must not increment the counter. If it does, every link for the rest of the line reads `in_parens: True`.
- **`template pipe is not a table close`** — `{{cite|a|}}` ends in `|}}`. Without the line-start anchor on the table-close token, the scanner reads `|}` and is left holding a single `}`, so the template stack never unwinds and *the rest of the article* is `in_template`.
- **`apostrophes in prose`** — English is full of apostrophes. Only runs of two or more toggle formatting; a single one is a character.
- **`broken outer brackets`** — the outer `[[` is garbage and the real link is inside it. Naively you'd extract `Broken [[Good` as a target.

**18b. Layer 2 — the invariants.** These are properties that must hold for *every* input, so you assert them over real articles rather than hand-written cases. Append to `tests/test_links.py`:

```python
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
```

`sample_wikitext` is a fixture backed by real articles. Create `tests/conftest.py`:

```python
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
```

**18c. Build the corpus.** Create `scripts/sample_articles.py`:

```python
"""Sample real articles out of the warehouse into a test fixture.

Committing a few hundred real articles turns "it worked when I ran it" into a
regression suite. Deterministic by page_id so the corpus is stable across runs
and a diff means something.

Usage:  python scripts/sample_articles.py --n 300
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib

import psycopg

OUT = pathlib.Path("tests/fixtures/articles.jsonl")

# Stratified: a uniform sample is 90% short stubs and would never exercise the
# template-heavy, table-heavy tail where the bugs live.
QUERY = """
WITH banded AS (
  SELECT page_id, title, wikitext,
         ntile(4) OVER (ORDER BY text_bytes) AS band
  FROM raw.page
  WHERE shard_name = %(shard)s AND NOT is_redirect AND wikitext IS NOT NULL
)
SELECT page_id, title, wikitext FROM (
  SELECT *, row_number() OVER (PARTITION BY band ORDER BY page_id) AS rn
  FROM banded
) x
WHERE rn <= %(per_band)s
ORDER BY page_id
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--shard", default="p10p1400054")
    args = ap.parse_args()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(os.environ["WH_DSN"]) as conn:
        rows = conn.execute(
            QUERY, {"shard": args.shard, "per_band": args.n // 4}
        ).fetchall()

    with OUT.open("w", encoding="utf-8", newline="\n") as fh:
        for page_id, title, wikitext in rows:
            fh.write(json.dumps(
                {"page_id": page_id, "title": title, "wikitext": wikitext},
                ensure_ascii=False) + "\n")
    mb = OUT.stat().st_size / 1e6
    print(f"wrote {len(rows)} articles to {OUT} ({mb:.1f} MB)")


if __name__ == "__main__":
    main()
```

Run it (the `$env:WH_DSN` line is the one from Part 1's Step 9):

```powershell
python scripts\sample_articles.py --n 300
```

> **Is this committed?** Yes. 300 stratified articles is roughly 10 MB of text — large for a repo but not unreasonable, and the value of a stable regression corpus is high. If you'd rather not, add it to `.gitignore` and accept that CI runs layers 1 and 5 only. Wikipedia text is CC BY-SA; if you publish this repo, note that in the fixture directory's README.

**18d. Layer 3 — differential testing against `mwparserfromhell`.** Create `tests/test_links_differential.py`:

```python
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
```

**Expect divergence, and expect most of it to be yours-is-right.** The categories you'll see:

- **We skip, they keep: links inside HTML comments and `<nowiki>`.** `mwparserfromhell` parses them as links because it models the wikitext tree, not the rendered page. You're right; the reader never sees these.
- **We skip, they keep: intra-page anchors** (`[[#History]]`). You're right — that isn't an edge.
- **We keep, they skip, or vice versa: deeply broken markup.** Judgment calls on both sides. Read a few and pick.
- **We skip, they keep: links past the 20,000-per-page cap.** Only on list articles. Fine.

If you see a category that is *your* bug — a construct you genuinely mishandle — add it to the Layer 1 table with the correct expectation, fix it, and tighten `MAX_DISAGREEMENT`.

**18e. Layer 4 — the twenty hand-checked articles.** This is the hour that saves the week.

Create `scripts/review_first_links.py`:

```python
"""Print each article's computed first link WITH ITS CONTEXT, for manual review.

The context window is the whole point. A bare list of "Dog -> domestication"
tells you nothing; seeing the 200 characters the link sits in tells you
immediately whether the rule fired correctly.

Usage:
  python scripts/review_first_links.py --n 20 > review.txt
  # read review.txt, then record your verdicts in
  # tests/fixtures/first_link_gold.tsv  as  page_id <TAB> expected_target
  # (use  -  for "no valid first link")
"""
from __future__ import annotations

import argparse
import json
import pathlib

from wikigraph.links import extract_links

FLAGS = ("in_parens", "in_italics", "in_template", "in_table",
         "in_ref", "in_infobox", "in_file_caption")
CORPUS = pathlib.Path("tests/fixtures/articles.jsonl")


def first_body_link(wikitext):
    """The first-link rule, in Python, so review matches what SQL will do.

    Kept deliberately in sync with mart/fct_first_link.sql. If you change one,
    change the other -- and bump RULE_VERSION.
    """
    for l in extract_links(wikitext):
        if l["target_prefix"] in {"file", "image", "category", "media"}:
            continue
        if any(l[f] for f in FLAGS):
            continue
        return l
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--window", type=int, default=200)
    args = ap.parse_args()

    arts = [json.loads(l) for l in CORPUS.open(encoding="utf-8")]
    step = max(1, len(arts) // args.n)

    for a in arts[::step][:args.n]:
        link = first_body_link(a["wikitext"])
        print("=" * 78)
        print(f"{a['page_id']}  {a['title']}")
        if link is None:
            print("  FIRST LINK: (none)")
            print("  LEAD: " + a["wikitext"][:args.window].replace("\n", " "))
            continue
        o = link["char_offset"]
        lo, hi = max(0, o - args.window // 2), o + args.window // 2
        print(f"  FIRST LINK: {link['target_raw']}   (ordinal {link['ordinal']}, "
              f"offset {o}, section {link['section_name']})")
        print("  CONTEXT: ..." + a["wikitext"][lo:hi].replace("\n", " ") + "...")


if __name__ == "__main__":
    main()
```

```powershell
python scripts\review_first_links.py --n 20 | Out-File -Encoding utf8 review.txt
notepad review.txt
```

**Now do the actual work.** For each of the twenty, open the article on Wikipedia, look at the first link in the first paragraph that a reader would actually click, and compare. Record your verdict in `tests/fixtures/first_link_gold.tsv`:

```
# page_id    expected first-link target (raw, as written in the wikitext)
# '-' means: this article correctly has no first link
4269    domestication
1234    Physical quantity
5678    -
```

You are looking for four failure shapes, and you will find at least one of each in twenty articles:

| What you see                                  | What it means                                                                               |
| --------------------------------------------- | ------------------------------------------------------------------------------------------- |
| First link is an infobox field                | `in_infobox` isn't catching that template's name — add it to `INFOBOX_PREFIXES`             |
| First link is a pronunciation or an etymology | It's inside `{{IPAc-en}}` or parentheses; check whether the paren opened on a previous line |
| First link is a `File:` or `Category:`        | The namespace filter in the first-link rule isn't running                                   |
| First link is *later* than it should be       | You over-flagged — an unbalanced construct earlier in the article never closed              |

That last one is the dangerous one, because it looks like nothing is wrong. It's why the gold set records the *expected* answer rather than just "no crash."

Then wire it up as a test. Append to `tests/test_links.py`:

```python
import pathlib

GOLD = pathlib.Path("tests/fixtures/first_link_gold.tsv")


def test_first_link_gold_set(articles):
    """The only test here that checks the RULES rather than the CODE."""
    if not GOLD.exists():
        pytest.skip("gold set not yet recorded -- run scripts/review_first_links.py")
    from scripts.review_first_links import first_body_link

    expected = {}
    for line in GOLD.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#"):
            pid, target = line.split("\t")
            expected[int(pid)] = target.strip()

    by_id = {a["page_id"]: a for a in articles}
    wrong = []
    for pid, want in expected.items():
        if pid not in by_id:
            continue
        got = first_body_link(by_id[pid]["wikitext"])
        got_t = got["target_raw"] if got else "-"
        if got_t != want:
            wrong.append(f"{pid} {by_id[pid]['title']!r}: want {want!r}, got {got_t!r}")

    assert not wrong, "first-link rule regressions:\n  " + "\n  ".join(wrong)
```

> `from scripts.review_first_links import ...` needs `scripts/` to be importable. Add an empty `scripts/__init__.py`, or move `first_body_link` into `src/wikigraph/links.py` — the latter is cleaner and you'll want it there in Step 24 anyway.

**18f. Layer 5 — robustness and throughput.** Create `tests/test_links_robustness.py`:

```python
"""Malformed input must not crash, hang, or go quadratic.

Wikipedia has 7 million articles. Whatever can be malformed, is.
"""
import time

import pytest

from wikigraph.links import extract_links

PATHOLOGICAL = [
    "",
    None,
    "[[",
    "]]",
    "[[[[[[[[",
    "{{" * 5000,
    "((((((((((" * 1000,
    "''" * 10000,
    "<ref>" * 2000,
    "{|\n" * 3000,
    "[[A|" * 2000,
    "[[" + "x" * 100_000 + "]]",
    "<!--" + "y" * 100_000,
    "\n" * 50_000,
    "[[Dog]]" * 30_000,        # exceeds MAX_LINKS_PER_PAGE
]


@pytest.mark.parametrize("wt", PATHOLOGICAL, ids=range(len(PATHOLOGICAL)))
def test_survives(wt):
    t0 = time.perf_counter()
    rows = extract_links(wt)
    assert time.perf_counter() - t0 < 5.0, "possible quadratic blowup"
    assert [r["ordinal"] for r in rows] == list(range(1, len(rows) + 1))


def test_scales_linearly():
    """Doubling the input must not quadruple the time.

    The scanner is single-pass, so this should hold. The failure mode it
    guards against is someone adding a text.find() that rescans from 0.
    """
    unit = "The [[dog]] (from [[Latin]] ''canis'') is a [[pet]].<ref>[[x]]</ref>\n\n"
    small, big = unit * 500, unit * 2000

    def timed(t):
        t0 = time.perf_counter()
        extract_links(t)
        return time.perf_counter() - t0

    timed(small)                                    # warm up
    ratio = timed(big) / max(timed(small), 1e-6)
    assert ratio < 6.0, f"4x input took {ratio:.1f}x time"


@pytest.mark.slow
def test_throughput(sample_wikitext):
    total = sum(len(w) for w in sample_wikitext)
    t0 = time.perf_counter()
    links = sum(len(extract_links(w)) for w in sample_wikitext)
    el = time.perf_counter() - t0
    print(f"\n{len(sample_wikitext)} articles | {total/1e6:.1f} MB | "
          f"{links:,} links | {el:.2f}s | {total/el/1e6:.1f} MB/s | "
          f"{len(sample_wikitext)/el:,.0f} articles/s | "
          f"{links/len(sample_wikitext):.1f} links/article")
```

Register the marker in `pyproject.toml` so pytest doesn't warn:

```toml
markers = [
    "integration: requires a running Postgres warehouse",
    "slow: benchmarks; run with -m slow",
]
```

**18g. Run everything.**

```powershell
.\tasks.ps1 test
python -m pytest -m slow -s -q
```

### Verify

Layer 1 and 2 must be **all green**. Layers 3 and 5 are as much measurements as assertions — record what they print.

The throughput line is the number you need for Step 26's planning. On synthetic text spanning realistic link densities I measured **5 MB/s** on very dense markup (one link per 28 characters) and **23 MB/s** on prose-heavy text at 35 links per 12 KB article. Real articles sit between those. Whatever you get, do this arithmetic and write it down:

```
articles in shard          387,504     (717,051 ns=0 pages minus 329,547 redirects)
mean text_bytes             ~31 KB     (from your EDA)
wikitext to scan            ~12 GB
at your measured MB/s   =   ______ minutes per shard, single core
x 19 shards / pool of 3 =   ______ hours wall clock
```

Append to `NOTES.md`:

```markdown
## Link extraction (Steps 17-18)
- Extractor throughput: ____ MB/s, ____ articles/s (Layer 5, on 300-article corpus)
- Links per article: mean ____   (design doc assumed 35, +/- 40%)
- Differential vs mwparserfromhell: ____% disagreement on ____ occurrences
  Top divergence categories: ____
- Gold set: ____ / 20 correct on first attempt
  Rules changed as a result: ____
- Projected: ____ min/shard single-core, ____ h for 19 shards at pool=3
```

**The links-per-article number settles the design doc's first open question.** It assumed 35 with ±40% uncertainty across a 25–45 range, which swings the total edge count between 175M and 315M. You now have a real number for one shard. Put it in the design doc's §6 sizing table and note which shard it came from.

### If it breaks

- **Differential rate is 20%, not 2%** — sort the `MISSING` output by count. One high-count entry usually means one systematic bug: a namespace prefix you skip that they don't, or `<gallery>` (which you should *not* blank — its `File:` lines are real links).
- **`test_scales_linearly` fails** — something rescans. Every `text.find()` inside the `[[` branch needs an upper bound, or it scans to the end of the document once per link; on a 300 KB list article with no blank lines that is genuinely quadratic. The `close` argument on the `\n\n` lookup in 17a is there for exactly this reason. If you add another `find()`, bound it too.
- **`test_survives` times out on `"{{" * 5000`** — the template stack grows to 5,000 entries and every `[[` calls `any(f["is_file"] for f in frames)`. Cap `tmpl_stack` at a few hundred; deeper than that is not real wikitext.
- **The gold-set test fails on articles you're sure about** — check you're comparing against the wikitext in your fixture, not the current live article. Wikipedia changed since your 2026-07-01 dump.
- **`pytest.importorskip` silently skips layer 3** — `pip install -e ".[dev]"` again; `mwparserfromhell` is in the dev extra.

Commit:

```powershell
git add tests/ scripts/sample_articles.py scripts/review_first_links.py pyproject.toml
git commit -m "Five-layer test suite for the link extractor, incl. gold set and differential"
```

---

## Step 19 — `raw.pagelink` and the link loader

### Why

**Where does the wikitext come from?** Two options: re-read the shard's Parquet from the staging volume, or stream it out of `raw.page`. Read from Postgres, for three reasons:

1. **You will re-extract.** Rule 4 exists because the link rules change. Re-extraction is a normal event, and it shouldn't depend on a scratch file surviving.
2. **Parquet is disposable.** Step 26 deletes each shard's Parquet as soon as it loads, to keep 55 GB of staging from accumulating. If extraction read from Parquet, it would have to run inside the ingest window.
3. **It decouples the DAGs.** Link extraction becomes a thing you run against whatever is already in the warehouse, on its own schedule, without touching ingest.

The cost is streaming ~12 GB of wikitext out of Postgres per shard. Use a **server-side cursor** so Postgres holds the result set and hands you rows in batches — a client-side cursor would try to materialize 387,504 rows averaging 31 KB each into Python memory, which is 12 GB and an instant OOM.

**Why Parquet in the middle again?** Same argument as Part 1's Step 7: CPU-bound extraction and I/O-bound loading fail for different reasons and should retry independently. A failed COPY shouldn't cost you 40 minutes of re-scanning.

**Why `raw.pagelink` is partitioned by `shard_name` and not by `HASH(src_page_id)`.** The design doc specifies hash partitioning for `stg.pagelink`, and at 245M rows that's the right call *for the staging table*. But `raw.pagelink` has a different job: it's the unit of reprocessing (Rule 2). Rerunning one shard must truncate one partition. Hash partitioning would scatter that shard's rows across all 16 partitions and make idempotent reload impossible. Landing tables partition by the thing you reload; analytical tables partition by the thing you query.

### Do

**19a. The migration.** Create `db/migrations/V004__pagelink.sql`:

```sql
SET ROLE etl;

-- One row per wikilink OCCURRENCE, unresolved, in document order.
-- Grain is deliberately not one-row-per-distinct-target: duplicates carry the
-- ordinal, and the ordinal is the whole basis of the first-link rule.
--
-- Titles are NOT normalized here. Python emits target_raw exactly as written;
-- public.norm_title() canonicalizes it in stg. See Rule 3.
CREATE TABLE IF NOT EXISTS raw.pagelink (
    src_page_id     integer  NOT NULL,
    shard_name      text     NOT NULL,
    dump_date       date     NOT NULL,
    ordinal         integer  NOT NULL,   -- 1-based, document order, no gaps
    target_raw      text     NOT NULL,   -- as written inside [[...]], anchor removed
    target_prefix   text,                -- candidate namespace prefix, lowercased
    anchor          text,                -- the #section part
    display_text    text,                -- the |piped part, or the link trail
    section_name    text,                -- nearest preceding == heading ==
    leading_colon   boolean  NOT NULL,   -- [[:Category:X]] links, [[Category:X]] categorizes
    in_parens       boolean  NOT NULL,
    in_italics      boolean  NOT NULL,
    in_template     boolean  NOT NULL,
    in_table        boolean  NOT NULL,
    in_ref          boolean  NOT NULL,
    in_infobox      boolean  NOT NULL,
    in_file_caption boolean  NOT NULL,   -- 7th flag; see Step 17b
    char_offset     integer  NOT NULL,   -- index into raw.page.wikitext
    rule_version    smallint NOT NULL,   -- links.RULE_VERSION at extraction time
    extracted_at    timestamptz NOT NULL DEFAULT now()
) PARTITION BY LIST (shard_name);

-- No indexes. This table is bulk-loaded and then read once per dbt build;
-- indexes would cost ~5x on load and buy nothing. Same reasoning as raw.page.

-- Extend the manifest rather than adding a second bookkeeping table: one place
-- to answer "what is in the warehouse and when did it get there".
ALTER TABLE raw.ingest_manifest
    ADD COLUMN IF NOT EXISTS links_extracted     bigint,
    ADD COLUMN IF NOT EXISTS articles_scanned    bigint,
    ADD COLUMN IF NOT EXISTS link_rule_version   smallint,
    ADD COLUMN IF NOT EXISTS link_extract_ended  timestamptz;

RESET ROLE;
```

```powershell
.\tasks.ps1 migrate
```

**19b. The extraction driver.** Add to `src/wikigraph/links.py`:

```python
# --------------------------------------------------------------------------
# Shard-level driver: raw.page -> Parquet
# --------------------------------------------------------------------------
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg

from .config import PARQUET_ROW_GROUP

LINK_SCHEMA = pa.schema([
    ("src_page_id",     pa.int32()),
    ("shard_name",      pa.string()),
    ("dump_date",       pa.date32()),
    ("ordinal",         pa.int32()),
    ("target_raw",      pa.string()),
    ("target_prefix",   pa.string()),
    ("anchor",          pa.string()),
    ("display_text",    pa.string()),
    ("section_name",    pa.string()),
    ("leading_colon",   pa.bool_()),
    ("in_parens",       pa.bool_()),
    ("in_italics",      pa.bool_()),
    ("in_template",     pa.bool_()),
    ("in_table",        pa.bool_()),
    ("in_ref",          pa.bool_()),
    ("in_infobox",      pa.bool_()),
    ("in_file_caption", pa.bool_()),
    ("char_offset",     pa.int32()),
    ("rule_version",    pa.int16()),
])

_PAGE_SQL = """
    SELECT page_id, wikitext
    FROM raw.page
    WHERE shard_name = %s AND NOT is_redirect AND wikitext IS NOT NULL
    ORDER BY page_id
"""


def extract_shard_links(
    dsn: str,
    shard_name: str,
    dump_date,
    out_path,
    limit: int | None = None,
    itersize: int = 200,
) -> dict:
    """Extract every wikilink in one shard's articles into a Parquet file.

    Redirects are skipped: their wikitext is `#REDIRECT [[X]]` and that single
    link is already captured as redirect_target on raw.page. Including them
    would put a spurious edge on every one of the 329,547 redirects in shard 0.
    """
    from pathlib import Path

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    buf: list[dict] = []
    articles = links_written = zero_link_pages = 0
    max_links = 0
    writer = pq.ParquetWriter(out_path, LINK_SCHEMA, compression="zstd")

    def flush() -> None:
        nonlocal buf
        if not buf:
            return
        cols = {f.name: [r[f.name] for r in buf] for f in LINK_SCHEMA}
        writer.write_table(pa.Table.from_pydict(cols, schema=LINK_SCHEMA))
        buf = []

    try:
        with psycopg.connect(dsn) as conn:
            # Named cursor == server-side cursor. Postgres keeps the result set
            # and streams `itersize` rows at a time. itersize is small because
            # each row carries ~31 KB of wikitext: 200 rows is ~6 MB per fetch.
            with conn.cursor(name="wg_pages") as cur:
                cur.itersize = itersize
                cur.execute(_PAGE_SQL, (shard_name,))
                for page_id, wikitext in cur:
                    articles += 1
                    rows = extract_links(wikitext)
                    if not rows:
                        zero_link_pages += 1
                    max_links = max(max_links, len(rows))
                    for r in rows:
                        r["src_page_id"] = page_id
                        r["shard_name"] = shard_name
                        r["dump_date"] = dump_date
                        r["rule_version"] = RULE_VERSION
                        buf.append(r)
                        links_written += 1
                    if len(buf) >= PARQUET_ROW_GROUP:
                        flush()
                    if limit and articles >= limit:
                        break
        flush()
    finally:
        writer.close()

    return {
        "shard_name":       shard_name,
        "dump_date":        str(dump_date),
        "parquet_path":     str(out_path),
        "articles_scanned": articles,
        "links_written":    links_written,
        "zero_link_pages":  zero_link_pages,
        "max_links_on_page": max_links,
        "rule_version":     RULE_VERSION,
        "bytes_out":        out_path.stat().st_size,
    }
```

**19c. The loader.** The COPY logic is identical to Part 1's `load_parquet` apart from the column list, so factor it out rather than copying it. Add to `src/wikigraph/load.py`:

```python
def copy_parquet_to_partition(
    dsn: str,
    parquet_path: str | Path,
    parent: str,               # 'pagelink'
    partition: str,            # 'pagelink_p10p1400054'
    partition_value: str,      # 'p10p1400054'
    columns: list[str],
    pg_types: list[str],
    batch_size: int = 20_000,
) -> int:
    """Create-if-absent, TRUNCATE, binary COPY, ANALYZE. Returns rows written.

    Everything Part 1's Step 9 said about FORMAT BINARY, partition replacement
    and ANALYZE applies unchanged -- this is that function with the table names
    lifted out. When you next touch load_parquet(), migrate it onto this too.
    """
    pf = pq.ParquetFile(parquet_path)
    rows = 0
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS raw.{part} "
                        "PARTITION OF raw.{parent} FOR VALUES IN ({val})").format(
                    part=sql.Identifier(partition),
                    parent=sql.Identifier(parent),
                    val=sql.Literal(partition_value),
                )
            )
            cur.execute(sql.SQL("TRUNCATE raw.{}").format(sql.Identifier(partition)))
            stmt = sql.SQL("COPY raw.{} ({}) FROM STDIN (FORMAT BINARY)").format(
                sql.Identifier(partition),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
            )
            with cur.copy(stmt) as cp:
                cp.set_types(pg_types)
                for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
                    cols = [c.to_pylist() for c in batch.columns]
                    for row in zip(*cols):
                        cp.write_row(row)
                        rows += 1
        conn.commit()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("ANALYZE raw.{}").format(sql.Identifier(partition)))
    return rows


# Order must match LINK_SCHEMA in links.py, position by position.
LINK_COLUMNS = [
    "src_page_id", "shard_name", "dump_date", "ordinal", "target_raw",
    "target_prefix", "anchor", "display_text", "section_name", "leading_colon",
    "in_parens", "in_italics", "in_template", "in_table", "in_ref",
    "in_infobox", "in_file_caption", "char_offset", "rule_version",
]
LINK_PG_TYPES = [
    "integer", "text", "date", "integer", "text",
    "text", "text", "text", "text", "boolean",
    "boolean", "boolean", "boolean", "boolean", "boolean",
    "boolean", "boolean", "integer", "smallint",
]


def load_links_parquet(dsn: str, parquet_path, shard_name: str) -> dict:
    part = "pagelink_" + re.sub(r"[^0-9a-zA-Z_]", "_", shard_name)
    rows = copy_parquet_to_partition(
        dsn, parquet_path, "pagelink", part, shard_name,
        LINK_COLUMNS, LINK_PG_TYPES,
    )
    return {"shard_name": shard_name, "rows_loaded": rows, "partition": part}
```

**19d. Smoke it on 2,000 articles first.** Never start with the full run.

```powershell
python -c @"
import datetime, os, time
from wikigraph.links import extract_shard_links
t0 = time.time()
s = extract_shard_links(os.environ['WH_DSN'], 'p10p1400054',
                        datetime.date(2026,7,1),
                        r'staging\p10p1400054.links.smoke.parquet', limit=2000)
print(s)
print(f'{time.time()-t0:.1f}s  |  {s[\"links_written\"]/max(s[\"articles_scanned\"],1):.1f} links/article')
"@
```

**19e. The full shard.**

```powershell
python -c @"
import datetime, os, time
from wikigraph.links import extract_shard_links
from wikigraph.load import load_links_parquet
t0 = time.time()
s = extract_shard_links(os.environ['WH_DSN'], 'p10p1400054',
                        datetime.date(2026,7,1), r'staging\p10p1400054.links.parquet')
t1 = time.time()
print(s); print(f'extract {(t1-t0)/60:.1f} min')
r = load_links_parquet(os.environ['WH_DSN'], r'staging\p10p1400054.links.parquet', 'p10p1400054')
print(r); print(f'load {(time.time()-t1)/60:.1f} min')
assert r['rows_loaded'] == s['links_written'], 'reconciliation failed'
"@
```

### Verify

**1. Row counts reconcile exactly.** The assert above is the gate; the query is the proof.

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c @"
SELECT count(*)                                       AS link_rows,
       count(DISTINCT src_page_id)                    AS articles,
       round(count(*)::numeric / count(DISTINCT src_page_id), 1) AS links_per_article,
       pg_size_pretty(pg_total_relation_size('raw.pagelink_p10p1400054')) AS size
FROM raw.pagelink WHERE shard_name = 'p10p1400054';
"@
```

`articles` should be very close to 387,504 — that's your 717,051 ns=0 pages minus 329,547 redirects. It will be slightly lower, because pages with `wikitext IS NULL` are excluded and articles with genuinely zero links contribute no rows.

**2. Idempotency.** Run `load_links_parquet` a second time and re-run the count. Identical, or your `TRUNCATE` isn't firing.

**3. The flags are actually firing.** This is the query that tells you whether Step 17 works on real data rather than on your fixtures:

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c @"
SELECT count(*) AS total,
       round(100.0*count(*) FILTER (WHERE in_template)     /count(*),1) AS pct_template,
       round(100.0*count(*) FILTER (WHERE in_infobox)      /count(*),1) AS pct_infobox,
       round(100.0*count(*) FILTER (WHERE in_ref)          /count(*),1) AS pct_ref,
       round(100.0*count(*) FILTER (WHERE in_table)        /count(*),1) AS pct_table,
       round(100.0*count(*) FILTER (WHERE in_parens)       /count(*),1) AS pct_parens,
       round(100.0*count(*) FILTER (WHERE in_italics)      /count(*),1) AS pct_italics,
       round(100.0*count(*) FILTER (WHERE in_file_caption) /count(*),1) AS pct_caption,
       round(100.0*count(*) FILTER (WHERE NOT (in_template OR in_infobox OR in_ref
             OR in_table OR in_parens OR in_italics OR in_file_caption))/count(*),1) AS pct_clean
FROM raw.pagelink;
"@
```

**Sanity-check the shape, not the exact numbers.** `pct_template` should be substantial — a large minority to a majority of all wikilinks on Wikipedia live inside templates, mostly navboxes. `pct_clean` in the low tens of percent is normal and healthy. Two results mean you have a bug:

- **`pct_clean` above ~80%** — your counters aren't incrementing. Go back to Step 17's Verify.
- **`pct_clean` below ~5%** — a counter isn't *decrementing*. Almost always the `{|`/`|}` line-anchor problem: one template with a trailing `|}}` unbalances the stack and everything after it in the article reads as `in_template`. Find the worst offenders:
  
  ```sql
  SELECT src_page_id,
         count(*) AS links,
         count(*) FILTER (WHERE in_template) AS in_tmpl
  FROM raw.pagelink GROUP BY 1
  HAVING count(*) > 50 AND count(*) FILTER (WHERE in_template) = count(*)
  ORDER BY 2 DESC LIMIT 10;
  ```
  
  Then pull that page's wikitext and run the extractor on it directly.

**4. Prefix coverage.** You'll need this in Step 21 to build the namespace seed, so collect it now:

```powershell
docker exec -it wikigraph-warehouse psql -U etl -d wikigraph -c @"
SELECT target_prefix, count(*) AS n
FROM raw.pagelink WHERE target_prefix IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 40;
"@
```

Keep this output. Anything at the top of that list that is a real MediaWiki namespace or interwiki prefix needs a row in the seed, or its links will be treated as ns=0 article links and pollute the graph.

Record in `NOTES.md`:

```markdown
## Shard p10p1400054 link extraction (Step 19)
- Articles scanned: ____   (expected ~387,504)
- Links written: ____      -> ____ links/article
- Extract wall time: ____ min   (____ MB/s of wikitext)
- Load wall time: ____ min
- Parquet: ____ MB | raw.pagelink partition: ____ GB
- Bytes per row in Postgres: ____   <- design doc §6 assumed ~41; check it
- Zero-link articles: ____ | max links on one page: ____
- Flag shares: template __% infobox __% ref __% table __% parens __% italics __% caption __%
- Unflagged (candidate body links): __%
```

**That bytes-per-row number matters for Step 26.** The design doc budgets `stg.pagelink` at 10 GB for 245M rows, which works out to 41 bytes per row. That estimate ignores Postgres's 24-byte tuple header and the four text columns. Measure yours; if it's ~130 bytes, the real figure at 19 shards is closer to 33 GB and the plan in Step 26 changes.

### If it breaks

- **`OperationalError: cursor "wg_pages" does not exist`** — server-side cursors need a transaction. Don't pass `autocommit=True` to `psycopg.connect` here.
- **Extraction runs, then the connection dies at ~10 minutes** — an idle-in-transaction timeout or a network-level idle timeout on the container. The cursor holds a transaction open for the whole scan. Raise `idle_in_transaction_session_timeout`, or chunk by `page_id` range and use a fresh connection per chunk.
- **Memory climbs steadily** — `itersize` is too large, or `flush()` isn't being called because `PARQUET_ROW_GROUP` is bigger than the whole shard's link count. Watch the Parquet file size grow during the run; if it stays at 0 bytes until the end, buffering is broken.
- **`insufficient data left in message`** on COPY — `LINK_COLUMNS`, `LINK_PG_TYPES` and `LINK_SCHEMA` are out of sync. All three lists must correspond position by position. This is the same failure Part 1's Step 9 warned about and it will get you again.
- **`could not create unique index` / `duplicate key`** — you added a PK to `raw.pagelink`. Don't; the reconciliation in Step 21's dbt tests is where uniqueness gets asserted, and a PK on a bulk-load target costs ~5× on load.

Commit:

```powershell
git add db/migrations/V004__pagelink.sql src/wikigraph/links.py src/wikigraph/load.py NOTES.md
git commit -m "raw.pagelink, extraction driver, and binary COPY link loader"
```

---

## Step 20 — The links DAG

### Why

Same reasoning as Part 1's Step 12: the code is already proven standalone, so this is pure orchestration. Two things are genuinely new.

**It's a third DAG, not two more tasks on `wikigraph_ingest`.** Link extraction reads `raw.page`, not the shard files, so it has no dependency on ingest beyond "that shard is loaded." Keeping it separate means you can re-extract all 19 shards after a rule change without re-parsing a single byte of XML — which, given Rule 4, you will do.

**It discovers its own work from the manifest.** Rather than being told which shards to process, it asks the warehouse which shards are `status = 'loaded'`. That's a small thing that turns into a large thing during the backfill: you can run this DAG repeatedly while ingest is still working through the other shards, and it picks up whatever is ready.

### Do

Create `airflow-docker/dags/wikigraph_links.py`:

```python
"""Links: raw.page.wikitext -> Parquet -> raw.pagelink.

Orchestration only. All logic lives in the wikigraph package under src/.
"""
from __future__ import annotations

import datetime as dt
import os

import pendulum
from airflow.sdk import Param, dag, task

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
}


def _warehouse_dsn() -> str:
    return os.environ["AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE"].replace(
        "postgres://", "postgresql://", 1
    )


@dag(
    dag_id="wikigraph_links",
    description="Extract wikilinks with context flags into raw.pagelink",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["wikigraph", "links"],
    params={
        "shards": Param([], type="array",
                        description="Shard names. Empty = every loaded shard."),
        "force": Param(False, type="boolean",
                       description="Re-extract shards that already have links."),
    },
)
def wikigraph_links():

    @task
    def discover_loaded(**context) -> list[dict]:
        """Ask the warehouse what is ready, rather than being told."""
        import psycopg

        wanted = list(context["params"]["shards"] or [])
        force = context["params"]["force"]

        sql = """
            SELECT shard_name, dump_date::text
            FROM raw.ingest_manifest
            WHERE status = 'loaded'
              AND (%(force)s OR links_extracted IS NULL
                   OR link_rule_version IS DISTINCT FROM %(rv)s)
              AND (cardinality(%(wanted)s::text[]) = 0
                   OR shard_name = ANY(%(wanted)s::text[]))
            ORDER BY shard_name
        """
        from wikigraph.links import RULE_VERSION

        with psycopg.connect(_warehouse_dsn()) as conn:
            rows = conn.execute(
                sql, {"force": force, "wanted": wanted, "rv": RULE_VERSION}
            ).fetchall()

        out = [{"name": n, "dump_date": d} for n, d in rows]
        if not out:
            raise ValueError(
                "no shards need link extraction — either none are loaded, or "
                "all are already at rule_version "
                f"{RULE_VERSION}. Pass force=true to re-extract."
            )
        print(f"selected {len(out)}: {[s['name'] for s in out]}")
        return out

    @task(
        pool="shard_parse",                   # CPU-bound, same resource as parsing
        execution_timeout=pendulum.duration(hours=3),
        map_index_template="{{ task.op_kwargs['shard']['name'] }}",
    )
    def extract_shard(shard: dict) -> dict:
        from pathlib import Path

        from wikigraph.config import STAGING_DIR
        from wikigraph.links import extract_shard_links
        from wikigraph.load import upsert_manifest

        dsn = _warehouse_dsn()
        dump_date = dt.date.fromisoformat(shard["dump_date"])
        out = Path(STAGING_DIR) / shard["dump_date"] / f"{shard['name']}.links.parquet"

        stats = extract_shard_links(dsn, shard["name"], dump_date, out)

        # --- GATE 1: zero links from a whole shard means the scanner broke. ---
        if stats["links_written"] == 0:
            upsert_manifest(dsn, shard["name"], dump_date, status="failed",
                            error_detail="link extraction produced zero rows")
            raise ValueError(f"{shard['name']}: zero links extracted")

        # --- GATE 2: implausible link density. ---
        # The design doc's range is 25-45 links/article. This is deliberately
        # wider: it is a tripwire for a broken scanner, not a quality bar.
        per = stats["links_written"] / max(stats["articles_scanned"], 1)
        if not 5 <= per <= 200:
            upsert_manifest(dsn, shard["name"], dump_date, status="failed",
                            error_detail=f"implausible link density {per:.1f}/article")
            raise ValueError(
                f"{shard['name']}: {per:.1f} links/article is outside [5, 200] — "
                "the extractor is probably wrong, not the data"
            )
        return stats

    @task(
        pool="warehouse_load",
        execution_timeout=pendulum.duration(hours=2),
        map_index_template="{{ task.op_kwargs['stats']['shard_name'] }}",
    )
    def load_shard_links(stats: dict) -> dict:
        from pathlib import Path

        from wikigraph.load import load_links_parquet, upsert_manifest

        dsn = _warehouse_dsn()
        dump_date = dt.date.fromisoformat(stats["dump_date"])
        result = load_links_parquet(dsn, stats["parquet_path"], stats["shard_name"])

        # --- GATE 3: reconciliation across stages. ---
        if result["rows_loaded"] != stats["links_written"]:
            upsert_manifest(dsn, stats["shard_name"], dump_date, status="failed",
                            error_detail="row count mismatch extract vs load")
            raise ValueError(
                f"{stats['shard_name']}: extracted {stats['links_written']:,} "
                f"but loaded {result['rows_loaded']:,}"
            )

        upsert_manifest(
            dsn, stats["shard_name"], dump_date,
            links_extracted=result["rows_loaded"],
            articles_scanned=stats["articles_scanned"],
            link_rule_version=stats["rule_version"],
            link_extract_ended=pendulum.now("UTC"),
        )
        # The link Parquet has served its purpose the moment the COPY commits.
        # 19 shards' worth would be tens of GB of scratch nobody reads again.
        Path(stats["parquet_path"]).unlink(missing_ok=True)
        return result

    @task
    def summarize(results: list[dict]) -> None:
        total = sum(r["rows_loaded"] for r in results)
        print(f"loaded {total:,} link rows across {len(results)} shard(s)")
        for r in sorted(results, key=lambda x: x["shard_name"]):
            print(f"  {r['shard_name']:>16}  {r['rows_loaded']:>12,} links")

    shards = discover_loaded()
    extracted = extract_shard.expand(shard=shards)
    loaded = load_shard_links.expand(stats=extracted)
    summarize(loaded)


wikigraph_links()
```

Trigger it:

```powershell
.\tasks.ps1 af dags trigger wikigraph_links --conf '{\"shards\": [\"p10p1400054\"], \"force\": true}'
```

### Verify

1. **Green on one shard**, and `summarize` logs a count matching your Step 19 measurement.
2. **The manifest records it:**
   
   ```sql
   SELECT shard_name, status, pages_loaded, articles_scanned, links_extracted,
          link_rule_version,
          round(links_extracted::numeric / articles_scanned, 1) AS per_article
   FROM raw.ingest_manifest ORDER BY shard_name;
   ```
3. **Re-triggering with `force: false` fails the discovery task** with your "no shards need link extraction" message. That's correct — the DAG has noticed the work is already done at the current rule version. Confirm the error text is the one you wrote; a bare `IndexError` here means the guard didn't fire and you'd silently re-do 40 minutes of work on every trigger.
4. **Bump `RULE_VERSION` to 2 in `links.py`, re-trigger with `force: false`.** It should now select the shard. Set it back to 1.

That fourth check is the one worth doing, because it's Rule 4 working as designed: the pipeline notices that your rules changed and re-derives what depends on them.

### If it breaks

- **`cardinality(%(wanted)s::text[])` errors** — psycopg adapts a Python list to a Postgres array, but an *empty* list needs the explicit cast that's already in the SQL. If you rewrote the query, keep the cast.
- **`extract_shard` times out at 3 hours** — measure first, then raise. If one shard genuinely needs more than 3 hours, the extractor has a pathological case; find it with the per-page timing rather than raising the ceiling.
- **Tasks queue forever** — `shard_parse` has 3 slots and ingest may be holding them. `.\tasks.ps1 af pools list`. If you'll routinely run both DAGs together, give links its own pool rather than raising `shard_parse`.
- **The Parquet unlink fails on Windows paths** — it shouldn't; `STAGING_DIR` inside the container is `/opt/airflow/staging`, a named volume. If you see a `WinError`, `WIKIGRAPH_STAGING_DIR` isn't set in the compose override.

Commit:

```powershell
git add airflow-docker/dags/wikigraph_links.py
git commit -m "Links DAG: manifest-driven fan-out with density and reconciliation gates"
```

---

## Step 21 — `stg_pagelink`, redirect resolution, and `fct_article_link`

### Why

Three things happen here, and only the middle one is hard.

**Namespace inference.** The extractor emitted a *candidate* prefix — the text before the first colon, lowercased. `category` is a namespace. `dog` in `[[Dog: A Story]]` is not. Deciding which is which is a lookup against a fixed list, which is a set operation, which means SQL. You'll do it with a **dbt seed**: a CSV committed to the repo that dbt loads into a table. Seeds are for small, static, human-maintained reference data, and this is the textbook case.

**Redirect resolution.** 46% of the pages in this shard are redirects, and links point at them constantly. An unresolved edge list has a large fraction of edges terminating at stubs that aren't nodes in your graph. So every link target gets checked against the redirect table and rewritten to its final destination before the edge is created.

The interesting question is **how many hops to follow**. MediaWiki follows exactly one: click a link to a double redirect and you land on the intermediate redirect page, looking at a "Redirect to:" line. Double redirects are considered a defect and bots fix them, so they're rare — but they exist, and a graph that stops at one hop drops those edges entirely. This runbook follows up to three hops with a cycle guard, behind a dbt var, because the intent of the encyclopedia is more useful for graph analysis than the literal click behaviour. Measure how much it changes and record the number; if it's under a tenth of a percent, the choice doesn't matter and you can stop thinking about it.

**Aggregation to `(src, dst)`.** `stg_pagelink` is one row per *occurrence*. `fct_article_link` is one row per *pair*, with `link_count` and `min_ordinal` carried along. Occurrence-level detail stays in `stg` for anything that needs the ordinal.

**A note on what you should expect.** On one shard, most link targets don't resolve — they point at articles in other page-ID ranges that aren't loaded yet. That's not a bug and the numbers will look alarming. Shard `p10p1400054` is the lowest ID range, so it holds the oldest and most-linked articles, which means your resolution rate will be much better than the 3% a uniform sample would give — but it will not be good. The number to watch is that it *climbs steeply* in Step 27.

### Do

**21a. The namespace seed.** Create `dbt/wikigraph/seeds/ns_alias.csv`:

```csv
prefix,namespace,note
media,-2,
special,-1,
talk,1,
user,2,
user talk,3,
wikipedia,4,
project,4,alias
wp,4,alias
wikipedia talk,5,
wt,5,alias
file,6,
image,6,alias
file talk,7,
image talk,7,alias
mediawiki,8,
mediawiki talk,9,
template,10,
t,10,alias
template talk,11,
help,12,
help talk,13,
category,14,
cat,14,alias
category talk,15,
portal,100,
p,100,alias
portal talk,101,
draft,118,
draft talk,119,
timedtext,710,
module,828,
commons,-99,interwiki
c,-99,interwiki
wikt,-99,interwiki
wiktionary,-99,interwiki
s,-99,interwiki
wikisource,-99,interwiki
q,-99,interwiki
wikiquote,-99,interwiki
b,-99,interwiki
v,-99,interwiki
n,-99,interwiki
d,-99,interwiki
wikidata,-99,interwiki
m,-99,interwiki
meta,-99,interwiki
mw,-99,interwiki
phab,-99,interwiki
doi,-99,interwiki
arxiv,-99,interwiki
de,-99,interwiki
fr,-99,interwiki
es,-99,interwiki
it,-99,interwiki
ja,-99,interwiki
nl,-99,interwiki
pl,-99,interwiki
pt,-99,interwiki
ru,-99,interwiki
sv,-99,interwiki
zh,-99,interwiki
ar,-99,interwiki
he,-99,interwiki
ko,-99,interwiki
fa,-99,interwiki
tr,-99,interwiki
uk,-99,interwiki
```

`-99` means "resolves outside this wiki." It isn't a real MediaWiki namespace number; it's a sentinel that keeps interwiki links out of the ns=0 graph without needing a second column.

**This list is deliberately incomplete.** The prefix-frequency query from Step 19's Verify is how you finish it: anything high in that list that belongs here and isn't gets added. Enwiki has ~300 interwiki prefixes and about 250 of them appear a handful of times.

Register it in `dbt/wikigraph/dbt_project.yml`:

```yaml
seeds:
  wikigraph:
    +schema: stg
    ns_alias:
      +column_types:
        namespace: smallint
```

```powershell
.\tasks.ps1 dbt seed
```

**21b. Declare the new source.** Add to `dbt/wikigraph/models/sources.yml`, under the existing `raw` source's `tables:`:

```yaml
      - name: pagelink
        description: >
          One row per wikilink occurrence, unresolved and unnormalized.
          Partitioned by shard_name. Written by wikigraph_links.
        columns:
          - name: src_page_id
            data_tests: [not_null]
          - name: ordinal
            data_tests: [not_null]
          - name: target_raw
            data_tests: [not_null]
```

**21c. `dbt/wikigraph/models/staging/stg_pagelink.sql`:**

```sql
{{ config(
    materialized='table',
    schema='stg',
    indexes=[
      {'columns': ['src_page_id', 'ordinal'], 'unique': True},
      {'columns': ['target_norm']},
    ]
) }}

-- Typed, normalized link occurrences. Deliberately NARROWER than raw.pagelink:
-- target_prefix, anchor, char_offset and rule_version stay in raw for ad-hoc
-- work. At 19 shards this table is ~257M rows and every column you carry costs
-- gigabytes -- see Step 26.
--
-- NOT PARTITIONED. The design doc specifies HASH(src_page_id) partitioning, and
-- at full scale that is right, but dbt's `table` materialization produces an
-- unpartitioned table and fighting that is a distraction from getting the link
-- logic correct. Revisit in Step 26. This is a choice, not an oversight.

select
    l.src_page_id,
    l.ordinal,
    l.target_raw,

    -- THE join key. One implementation, in SQL. See Rule 3.
    public.norm_title(l.target_raw)          as target_norm,

    -- Unknown prefix => it wasn't a namespace, it was part of the title.
    -- `[[Dog: A Story]]` has candidate prefix 'dog', which isn't in the seed,
    -- so it stays ns=0 and the full string is the title. That is correct.
    coalesce(n.namespace, 0)::smallint       as target_ns,

    l.display_text,
    l.section_name,
    l.leading_colon,

    l.in_parens,
    l.in_italics,
    l.in_template,
    l.in_table,
    l.in_ref,
    l.in_infobox,
    l.in_file_caption,

    l.shard_name

from {{ source('raw', 'pagelink') }} l
left join {{ ref('ns_alias') }} n
       on n.prefix = l.target_prefix
```

**21d. `dbt/wikigraph/models/staging/int_redirect_resolved.sql`:**

```sql
{{ config(
    materialized='table',
    schema='stg',
    indexes=[{'columns': ['from_norm'], 'unique': True}]
) }}

-- Redirect chains, collapsed. from_norm is any redirect title; final_norm is
-- where a reader following the chain would end up.
--
-- MediaWiki itself follows exactly ONE hop -- a double redirect leaves the
-- reader on the intermediate page. We follow up to redirect_max_hops because
-- the editors' intent is more useful for graph analysis than the literal click
-- behaviour, and because double redirects are a known maintenance backlog
-- rather than a deliberate structure. Set the var to 1 for click fidelity.
--
-- The `seen` array is a cycle guard. A -> B -> A is a real pathology that
-- exists in the wild (design doc §5.4) and without the guard this CTE does not
-- terminate.

with recursive chain as (
    select
        p.norm_title          as from_norm,
        p.redirect_to_norm    as to_norm,
        1                     as hops,
        array[p.norm_title]   as seen
    from {{ ref('stg_page') }} p
    where p.is_redirect
      and p.redirect_to_norm is not null

    union all

    select
        c.from_norm,
        n.redirect_to_norm,
        c.hops + 1,
        c.seen || n.norm_title
    from chain c
    join {{ ref('stg_page') }} n
      on n.norm_title = c.to_norm
    where n.is_redirect
      and n.redirect_to_norm is not null
      and c.hops < {{ var('redirect_max_hops', 3) }}
      and not (n.norm_title = any(c.seen))
)

select distinct on (from_norm)
    from_norm,
    to_norm as final_norm,
    hops
from chain
order by from_norm, hops desc     -- deepest resolution wins
```

**21e. `dbt/wikigraph/models/marts/fct_article_link.sql`:**

```sql
{{ config(
    materialized='table',
    schema='mart',
    indexes=[
      {'columns': ['src_article_id', 'dst_article_id'], 'unique': True},
      {'columns': ['dst_article_id']},
    ],
    pre_hook="set local work_mem = '256MB'"
) }}

-- Resolved article-to-article edges. Redirects collapsed, self-loops dropped,
-- red links dropped. Grain: one row per (src, dst) pair.
--
-- The pre_hook matters: the join below hashes all of dim_article. At default
-- work_mem (64MB) Postgres spills to disk in batches and the build takes
-- several times longer. SET LOCAL scopes it to this model's transaction, so
-- you are not raising work_mem globally for every session.

with occurrence as (
    select
        l.src_page_id,
        l.ordinal,
        coalesce(r.final_norm, l.target_norm)  as dst_norm,
        r.final_norm is not null               as via_redirect
    from {{ ref('stg_pagelink') }} l
    left join {{ ref('int_redirect_resolved') }} r
           on r.from_norm = l.target_norm
    where l.target_ns = 0
)

select
    o.src_page_id                       as src_article_id,
    d.article_id                        as dst_article_id,

    -- smallint per the design doc. A handful of list articles genuinely link
    -- to the same target more than 32,767 times; clamp rather than overflow.
    least(count(*), 32767)::smallint    as link_count,

    min(o.ordinal)                      as min_ordinal,
    bool_or(o.via_redirect)             as via_redirect

from occurrence o
-- Inner join = red links and cross-shard targets are dropped. Before the
-- backfill that is most of them.
join {{ ref('dim_article') }} d on d.norm_title = o.dst_norm
-- src must also be a real article: guards against a redirect's own
-- `#REDIRECT [[X]]` sneaking in if extraction ever stops filtering them.
join {{ ref('dim_article') }} s on s.article_id = o.src_page_id
where o.src_page_id <> d.article_id       -- no self-loops
group by 1, 2
```

**21f. Degrees, and a dbt lesson.** `mart.dim_article` has `out_degree` and `in_degree` columns sitting NULL. You cannot fill them in `dim_article` itself, because `fct_article_link` already `ref()`s `dim_article` and dbt's DAG must be acyclic — adding the reverse reference produces `Found a cycle`. The standard fix is a downstream metrics model, not a self-referencing update.

`dbt/wikigraph/models/marts/article_degree.sql`:

```sql
{{ config(materialized='table', schema='mart',
          indexes=[{'columns': ['article_id'], 'unique': True}]) }}

select
    a.article_id,
    coalesce(o.n, 0) as out_degree,
    coalesce(i.n, 0) as in_degree
from {{ ref('dim_article') }} a
left join (select src_article_id, count(*) n
           from {{ ref('fct_article_link') }} group by 1) o
       on o.src_article_id = a.article_id
left join (select dst_article_id, count(*) n
           from {{ ref('fct_article_link') }} group by 1) i
       on i.dst_article_id = a.article_id
```

**21g. Tests.** Add to `dbt/wikigraph/models/marts/schema.yml`:

```yaml
  - name: fct_article_link
    description: "Resolved, deduplicated article->article edges."
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [src_article_id, dst_article_id]
    columns:
      - name: src_article_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_article')
              field: article_id
      - name: dst_article_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_article')
              field: article_id
      - name: link_count
        data_tests: [not_null]
```

That needs `dbt_utils`. Create `dbt/wikigraph/packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
```

```powershell
.\tasks.ps1 dbt deps
```

> If you add packages, set `"install_deps": True` in the Cosmos `operator_args` in `wikigraph_transform.py`, or Cosmos will run models against a project whose `dbt_packages/` it never populated.

And a singular test — a plain SQL file where **returning rows means failure**. `dbt/wikigraph/tests/assert_link_resolution_floor.sql`:

```sql
{{ config(severity = 'warn') }}

-- On one shard, most link targets live in other page-ID ranges, so a low
-- resolution rate is expected and this is a WARN. After the backfill (Step 27),
-- change severity to 'error' and raise min_link_resolution to ~0.85 -- at that
-- point a drop really does mean something broke.

with occ as (
    select count(*)::numeric as n
    from {{ ref('stg_pagelink') }} where target_ns = 0
),
res as (
    select coalesce(sum(link_count), 0)::numeric as n
    from {{ ref('fct_article_link') }}
)
select occ.n as ns0_occurrences,
       res.n as resolved_occurrences,
       round(res.n / nullif(occ.n, 0), 4) as rate
from occ, res
where res.n / nullif(occ.n, 0) < {{ var('min_link_resolution', 0.15) }}
```

**21h. Build.**

```powershell
.\tasks.ps1 dbt build
```

### Verify

**1. Namespace inference is sane.**

```sql
SELECT target_ns, count(*) AS n,
       round(100.0*count(*)/sum(count(*)) OVER (), 1) AS pct
FROM stg.stg_pagelink GROUP BY 1 ORDER BY 2 DESC;
```

You should see ns=0 dominant, then 14 (Category) and 6 (File) as the big minorities, then a long tail. **If ns=0 is over 95%, your seed didn't load** — check `\dt stg.ns_alias` and that `dbt seed` ran.

**2. Redirect resolution, and how much the multi-hop choice actually bought you.**

```sql
SELECT hops, count(*) AS n FROM stg.int_redirect_resolved GROUP BY 1 ORDER BY 1;
```

`hops = 1` will be almost everything. The `hops >= 2` count divided by the total is exactly how much your three-hop choice differs from MediaWiki's behaviour. Write the number down; if it's tiny, stop worrying about it.

Also count the pathologies the design doc's §5.4 predicts:

```sql
-- Redirects whose chain never reaches an article: loops, or dangling targets.
SELECT count(*) AS unresolvable
FROM stg.stg_page p
LEFT JOIN stg.int_redirect_resolved r ON r.from_norm = p.norm_title
LEFT JOIN mart.dim_article a ON a.norm_title = coalesce(r.final_norm, p.redirect_to_norm)
WHERE p.is_redirect AND a.article_id IS NULL;
```

Before the backfill this is dominated by cross-shard targets — Part 1 measured 78,272 dangling redirects on this shard. It should collapse in Step 27, which makes it a good progress metric.

**3. The edge table.**

```sql
SELECT count(*) AS edges,
       count(DISTINCT src_article_id) AS srcs,
       count(DISTINCT dst_article_id) AS dsts,
       round(avg(link_count), 2) AS avg_link_count,
       round(100.0*count(*) FILTER (WHERE via_redirect)/count(*), 1) AS pct_via_redirect,
       pg_size_pretty(pg_total_relation_size('mart.fct_article_link')) AS size
FROM mart.fct_article_link;
```

`pct_via_redirect` is one of the more interesting numbers in the project: it's the fraction of Wikipedia's link graph that would be *wrong* if you hadn't resolved redirects. Expect it to be substantial — the design doc's whole argument for doing resolution in the transform layer rests on it.

**4. The hubs — the first output that looks like an answer.**

```sql
SELECT a.title, d.in_degree, d.out_degree
FROM mart.article_degree d
JOIN mart.dim_article a USING (article_id)
ORDER BY d.in_degree DESC LIMIT 25;
```

**These should be recognizable.** Expect things like *United States*, *World War II*, *France*, *Latin*, *Association football*. If the top of that list is disambiguation pages, list articles, or nonsense, something upstream is wrong.

**5. The resolution rate.** The singular test prints it; get it directly too:

```sql
SELECT (SELECT count(*) FROM stg.stg_pagelink WHERE target_ns = 0) AS ns0_occurrences,
       (SELECT sum(link_count) FROM mart.fct_article_link)          AS resolved,
       (SELECT count(*) FROM mart.dim_article)                      AS articles_available;
```

Anywhere in the 20–50% range is normal for this shard. Record it — the delta after the backfill is the single clearest demonstration in this project of why the backfill matters.

Append to `NOTES.md`:

```markdown
## Graph build, single shard (Step 21)
- stg_pagelink rows: ____ | ns=0: ____% | ns=14: ____% | ns=6: ____%
- Redirect chains: 1 hop ____ | 2 hops ____ | 3 hops ____  (multi-hop = ____%)
- Unresolvable redirects: ____   (Part 1 measured 78,272 dangling)
- fct_article_link edges: ____ | via_redirect: ____%
- Link resolution rate: ____%   <- compare after backfill
- Top 5 by in-degree: ____
- Table sizes: stg_pagelink ____ | fct_article_link ____
```

### If it breaks

- **`Found a cycle: model.wikigraph.dim_article --> ...`** — you tried to fill the degree columns inside `dim_article`. Use the downstream `article_degree` model.
- **`relation "ns_alias" does not exist`** — `dbt seed` hasn't run, or the seed landed in the default schema. `dbt seed` is not part of `dbt build`'s default selection in every version; run it explicitly after adding or editing a CSV.
- **The `unique` test on `(src_article_id, dst_article_id)` fails** — impossible from the `group by` above, so it means an older version of the table is still there. `dbt build --full-refresh --select fct_article_link`.
- **`fct_article_link` builds but is empty** — almost always the join key. Compare a sample by hand:
  
  ```sql
  SELECT l.target_raw, l.target_norm,
         EXISTS (SELECT 1 FROM mart.dim_article a WHERE a.norm_title = l.target_norm) AS hits
  FROM stg.stg_pagelink l WHERE l.target_ns = 0 LIMIT 20;
  ```
  
  If `hits` is false for targets you can see in `dim_article`, `norm_title()` is being applied to one side and not the other — which is Rule 3 being violated somewhere.
- **The build takes 20+ minutes on one shard** — check the `pre_hook` applied (`explain` the model's query and look for `Batches: 1`, not `Batches: 16`), and that `ANALYZE` ran on the `raw.pagelink` partition. Both were in Step 19.
- **`could not create unique index ... key is duplicated`** on `int_redirect_resolved` — two rows for one `from_norm` means the `distinct on` lost its `order by`. They have to match.

Commit:

```powershell
git add dbt/ NOTES.md
git commit -m "Namespace seed, redirect resolution, and the resolved edge table"
```

---

## Step 22 — Your first graph plot

### Why

**Goal #1.** This is the step where the project stops being a pipeline and starts being a thing you can show someone.

The constraint the design doc names is real: you cannot plot 7M nodes and 245M edges. Nothing can. Any renderer you point at that will either die or produce a grey rectangle. Every useful graph visualization is a *sampling decision* made before the plot, and the two that work here are ego networks and category-induced subgraphs.

**The design doc gives the ego network as a recursive CTE.** That's the correct general form, but it has no degree cap, and without one a 2-hop ego network around a hub is not a graph, it's the encyclopedia. A hub with 500 out-edges whose neighbours each have 500 gives you 250,000 nodes at hop 2. The version below caps fan-out at each hop with `LATERAL ... LIMIT`, which is both bounded and easy to reason about — and at two hops you don't need recursion at all.

**Plot the induced subgraph, not the traversal tree.** Once you've chosen your node set, you want *every* edge between those nodes, not just the ones you walked. That's what produces the clustering structure that makes these plots worth looking at. It's the last query in the script and it's the difference between a starburst and a map.

### Do

**22a. Categories, for better plots.** The design doc is right that category-induced subgraphs beat ego networks, and you already have the data — you just haven't projected it. Five lines. `dbt/wikigraph/models/staging/stg_category_link.sql`:

```sql
{{ config(materialized='table', schema='stg',
          indexes=[{'columns': ['category_norm']}, {'columns': ['src_page_id']}]) }}

-- [[Category:X]] categorizes the page. [[:Category:X]] is an ordinary link TO
-- the category page and is not membership -- hence the leading_colon filter.
-- Everything after 'Category:' is the category name; the sort key (the piped
-- part) is kept because it is occasionally the only clue to a person's surname.

select distinct
    src_page_id,
    public.norm_title(substr(target_raw, position(':' in target_raw) + 1)) as category_norm,
    display_text as sort_key
from {{ ref('stg_pagelink') }}
where target_ns = 14
  and not leading_colon
```

**22b. The export script.** Create `scripts/ego.py`:

```python
"""Export a bounded subgraph around a seed article, and optionally plot it.

Two sampling strategies:
  --seed "Dog"                 ego network, n hops, degree-capped
  --category "Jazz musicians"  every article in a category, plus edges among them

Both then take the INDUCED subgraph: all edges between the chosen nodes, not
just the ones the traversal walked. That is what makes the clusters visible.

Usage:
  python scripts/ego.py --seed "Dog" --hops 2 --fanout 40 --max-nodes 400 --plot
  python scripts/ego.py --category "American jazz musicians" --max-nodes 600 --plot
"""
from __future__ import annotations

import argparse
import csv
import os
import pathlib
import re

import psycopg

OUT = pathlib.Path("exports")

RESOLVE_TITLE = """
    SELECT a.article_id, a.title
    FROM mart.dim_article a
    WHERE a.norm_title = public.norm_title(%s)
"""

EGO = """
WITH seed AS (SELECT %(seed)s::int AS id),
h1 AS (
    SELECT x.dst_article_id AS id
    FROM seed s CROSS JOIN LATERAL (
        SELECT l.dst_article_id FROM mart.fct_article_link l
        WHERE l.src_article_id = s.id
        ORDER BY l.link_count DESC, l.min_ordinal
        LIMIT %(fanout)s
    ) x
),
h2 AS (
    SELECT y.dst_article_id AS id
    FROM h1 CROSS JOIN LATERAL (
        SELECT l.dst_article_id FROM mart.fct_article_link l
        WHERE l.src_article_id = h1.id
        ORDER BY l.link_count DESC, l.min_ordinal
        LIMIT %(fanout2)s
    ) y
),
candidates AS (
    SELECT id FROM seed UNION SELECT id FROM h1 UNION SELECT id FROM h2
),
nodes AS (
    (SELECT id FROM seed)
    UNION
    (SELECT c.id FROM candidates c
     JOIN mart.article_degree d ON d.article_id = c.id
     ORDER BY d.in_degree DESC
     LIMIT %(max_nodes)s)
)
SELECT l.src_article_id, l.dst_article_id, l.link_count
FROM mart.fct_article_link l
JOIN nodes a ON a.id = l.src_article_id
JOIN nodes b ON b.id = l.dst_article_id
"""

CATEGORY = """
WITH members AS (
    SELECT cl.src_page_id AS id
    FROM stg.stg_category_link cl
    JOIN mart.dim_article a ON a.article_id = cl.src_page_id
    WHERE cl.category_norm = public.norm_title(%(category)s)
),
nodes AS (
    SELECT m.id FROM members m
    JOIN mart.article_degree d ON d.article_id = m.id
    ORDER BY d.in_degree DESC
    LIMIT %(max_nodes)s
)
SELECT l.src_article_id, l.dst_article_id, l.link_count
FROM mart.fct_article_link l
JOIN nodes a ON a.id = l.src_article_id
JOIN nodes b ON b.id = l.dst_article_id
"""

LABELS = """
    SELECT a.article_id, a.title, d.in_degree, d.out_degree
    FROM mart.dim_article a
    JOIN mart.article_degree d USING (article_id)
    WHERE a.article_id = ANY(%s)
"""


def slug(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_").lower()[:60]


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--seed", help="article title")
    g.add_argument("--category", help="category name, without the Category: prefix")
    ap.add_argument("--hops", type=int, default=2)
    ap.add_argument("--fanout", type=int, default=40, help="max out-edges per node")
    ap.add_argument("--max-nodes", type=int, default=400)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--html", action="store_true", help="also write an interactive pyvis page")
    args = ap.parse_args()

    OUT.mkdir(exist_ok=True)
    name = slug(args.seed or args.category)

    with psycopg.connect(os.environ["WH_DSN"]) as conn:
        if args.seed:
            row = conn.execute(RESOLVE_TITLE, (args.seed,)).fetchone()
            if not row:
                raise SystemExit(
                    f"no article titled {args.seed!r}. It may be a redirect, or in "
                    "a shard you have not loaded yet."
                )
            seed_id, seed_title = row
            print(f"seed: {seed_title} (article_id {seed_id})")
            edges = conn.execute(EGO, {
                "seed": seed_id,
                "fanout": args.fanout,
                "fanout2": args.fanout if args.hops >= 2 else 0,
                "max_nodes": args.max_nodes,
            }).fetchall()
        else:
            seed_id = None
            edges = conn.execute(
                CATEGORY, {"category": args.category, "max_nodes": args.max_nodes}
            ).fetchall()

        ids = sorted({e[0] for e in edges} | {e[1] for e in edges})
        if not ids:
            raise SystemExit("empty subgraph — try a more connected seed, or "
                             "raise --fanout")
        labels = {r[0]: r for r in conn.execute(LABELS, (ids,)).fetchall()}

    with (OUT / f"{name}_edges.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["src", "dst", "link_count"])
        w.writerows(edges)
    with (OUT / f"{name}_nodes.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["article_id", "title", "in_degree", "out_degree"])
        w.writerows(labels[i] for i in ids)

    print(f"{len(ids)} nodes, {len(edges)} edges -> exports/{name}_*.csv")

    if args.plot or args.html:
        plot(name, ids, edges, labels, seed_id, args.html)


def plot(name, ids, edges, labels, seed_id, html) -> None:
    import matplotlib
    matplotlib.use("Agg")            # no GUI backend; write straight to a file
    import matplotlib.pyplot as plt
    import networkx as nx

    G = nx.DiGraph()
    for i in ids:
        G.add_node(i, title=labels[i][1])
    for s, d, c in edges:
        G.add_edge(s, d, weight=c)

    # Degree WITHIN the subgraph, not globally -- the point of the picture is
    # which nodes are central HERE.
    deg = dict(G.degree())
    sizes = [40 + 12 * deg[i] for i in G.nodes()]
    colors = ["#c1440e" if i == seed_id else "#3b6ea5" for i in G.nodes()]

    pos = nx.spring_layout(G, k=1.6 / max(len(G) ** 0.5, 1), iterations=60, seed=42)

    fig, ax = plt.subplots(figsize=(16, 16))
    # arrows=False is not cosmetic: arrowheads are drawn as one FancyArrowPatch
    # per edge, which is minutes rather than seconds past a few thousand edges.
    nx.draw_networkx_edges(G, pos, ax=ax, alpha=0.12, width=0.5, arrows=False)
    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=sizes, node_color=colors,
                           linewidths=0)
    top = sorted(G.nodes(), key=lambda i: deg[i], reverse=True)[:35]
    nx.draw_networkx_labels(
        G, pos, ax=ax,
        labels={i: labels[i][1] for i in top},
        font_size=9,
    )
    ax.set_title(f"{name}  —  {len(G)} nodes, {G.number_of_edges()} edges")
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(f"exports/{name}.png", dpi=170)
    print(f"wrote exports/{name}.png")

    if html:
        from pyvis.network import Network
        net = Network(height="900px", width="100%", directed=True,
                      bgcolor="#ffffff", notebook=False)
        for i in G.nodes():
            net.add_node(i, label=labels[i][1],
                         value=deg[i], title=f"in={labels[i][2]} out={labels[i][3]}")
        for s, d in G.edges():
            net.add_edge(s, d)
        net.write_html(f"exports/{name}.html", notebook=False)
        print(f"wrote exports/{name}.html")


if __name__ == "__main__":
    main()
```

**22c. Run it.**

```powershell
python scripts\ego.py --seed "Dog" --hops 2 --fanout 40 --max-nodes 400 --plot --html
Start-Process .\exports\dog.png
```

Then try a category, which will look better:

```powershell
python scripts\ego.py --category "American jazz musicians" --max-nodes 600 --plot
```

**22d. Pick seeds that are actually in your shard.** Before the backfill, most titles won't resolve. Find good ones:

```sql
SELECT a.title, d.out_degree, d.in_degree
FROM mart.article_degree d
JOIN mart.dim_article a USING (article_id)
WHERE d.out_degree BETWEEN 30 AND 200
  AND NOT a.is_list_page AND NOT a.is_disambig
ORDER BY d.in_degree DESC LIMIT 40;
```

Articles with a *moderate* out-degree and a high in-degree make the best ego networks. Very high out-degree gives you a hairball; very low gives you a star with no structure.

**22e. Add `exports/` to `.gitignore`, except the good ones.**

```gitignore
exports/*
!exports/*.png
```

The CSVs and the pyvis HTML are regenerable and large. A PNG you're proud of is a portfolio artifact — commit it.

### Verify

Open the PNG. Three things tell you it worked:

1. **The node labels are recognizable Wikipedia articles**, and they're topically related to your seed. If they're random, your join keys are wrong.
2. **There is visible clustering** — dense clumps connected by thinner bridges. If it's a uniform ball, you're plotting the traversal tree instead of the induced subgraph; check the final `JOIN nodes a ... JOIN nodes b` is on *both* endpoints.
3. **Edge count is meaningfully higher than node count.** A 400-node ego network should have well over 400 edges. If edges ≈ nodes, the induced-subgraph step found nothing extra and your neighbourhood isn't interconnected — usually a sign you're at the sparse edge of the loaded shard.

Sanity-check the export against the database directly:

```sql
SELECT count(*) FROM mart.fct_article_link
WHERE src_article_id = (SELECT article_id FROM mart.dim_article
                        WHERE norm_title = public.norm_title('Dog'));
```

That number should be ≥ the seed's out-degree in your `_nodes.csv`, and equal to it when it's below `--fanout`.

Append to `NOTES.md`:

```markdown
## First graph plot (Step 22)  ** GOAL #1 **
- Seed: ____ | hops ____ | fanout ____ | nodes ____ | edges ____
- Export + layout wall time: ____ s
- Largest usable subgraph before spring_layout got slow: ____ nodes
- Artifact: exports/____.png
```

### If it breaks

- **`no article titled 'X'`** — three possible reasons, in order of likelihood: it's in another shard; it's a redirect (redirects aren't in `dim_article` by design — resolve it first with `SELECT final_norm FROM stg.int_redirect_resolved WHERE from_norm = public.norm_title('X')`); or your normalization differs. Use the 22d query to pick a seed you know exists.
- **The plot takes minutes and the image is a black smear** — too many edges. Drop `--fanout` to 15 and `--max-nodes` to 200. Also confirm `arrows=False`; that flag alone is often the whole problem.
- **`ValueError: Unrecognized backend` / no window appears** — `matplotlib.use("Agg")` must be called *before* `import matplotlib.pyplot`. On Windows without a display this is required, not optional.
- **pyvis writes a blank page** — older pyvis versions use `show()` and need `notebook=False`; newer ones want `write_html`. If neither works, drop the `--html` flag; the PNG is the deliverable.
- **`spring_layout` never finishes** — it's O(n²) per iteration. Above ~2,000 nodes use `nx.kamada_kawai_layout` on a sample, or export the GraphML and lay it out in Gephi or Cytoscape, which use ForceAtlas2 and handle 100k nodes.

Commit:

```powershell
git add scripts/ego.py dbt/wikigraph/models/staging/stg_category_link.sql .gitignore exports/*.png
git commit -m "Ego and category subgraph export, plus the first plotted graph"
```

**Goal #1 is done.** You have a directed graph of Wikipedia, resolved through redirects, exported and plotted.

---

## Step 23 — `article_alias` and the search cascade

### Why

**Goal #2.** The design doc's key insight is worth restating because it changes what you build: *the redirect population is the alias dictionary*. "JFK" → *John F. Kennedy*, "The Big Apple" → *New York City*. Wikipedia editors built you a synonym table over two decades, and 329,547 of them are already sitting in `stg_page` for this shard alone. You are not building a search engine; you are exposing a dictionary that already exists.

Anchor text is the second source. If 4,000 articles link to *Barack Obama* with the display text "44th President", that is a phrase people type. You extracted `display_text` in Step 17 precisely for this.

**Three tiers, in cost order.** Exact match on a normalized key is sub-millisecond and answers most real queries. Full-text search handles multi-word queries and stemming. Trigram similarity catches typos and partial names and is the expensive one. Run them in that order and stop when you have enough results — don't run all three for every query.

**Two Postgres traps live in this step**, and both produce confusing errors:

**`unaccent()` is not `IMMUTABLE`.** Its dictionary can be redefined with `ALTER TEXT SEARCH DICTIONARY`, so Postgres marks it `STABLE` and refuses to let you use it in an index expression. You get `functions in index expression must be marked IMMUTABLE`, which reads like a Postgres bug and isn't. The fix is a thin wrapper that names the dictionary explicitly and asserts immutability. You are making a promise you must keep: if you ever alter that dictionary, your indexes are silently wrong and need rebuilding.

**dbt drops and recreates tables on every run.** Any index you create by hand in `psql` disappears the next time the model builds, and you find out when a query that took 3 ms takes 40 seconds. Indexes on dbt models must live in the model's `config()` or a `post_hook`. Never anywhere else.

### Do

**23a. The immutable unaccent wrapper.** Create `db/migrations/V005__search_helpers.sql`:

```sql
-- unaccent() is declared STABLE, not IMMUTABLE, because ALTER TEXT SEARCH
-- DICTIONARY can change its behaviour. Postgres therefore rejects it in index
-- expressions with "functions in index expression must be marked IMMUTABLE".
--
-- This wrapper pins the dictionary by name and asserts immutability. That is a
-- promise: if anyone ever alters public.unaccent, every index built on this
-- function is silently wrong and must be REINDEXed. Nobody will. But write it
-- down, because "silently wrong index" is a bad thing to rediscover.
CREATE OR REPLACE FUNCTION public.f_unaccent(text)
RETURNS text
LANGUAGE sql
IMMUTABLE PARALLEL SAFE STRICT
RETURN public.unaccent('public.unaccent', $1);

COMMENT ON FUNCTION public.f_unaccent(text) IS
  'IMMUTABLE unaccent, safe in index expressions. Do not ALTER the underlying dictionary.';
```

```powershell
.\tasks.ps1 migrate
```

**23b. `dbt/wikigraph/models/marts/article_alias.sql`:**

```sql
{{ config(
    materialized='table',
    schema='mart',
    indexes=[
      {'columns': ['alias_norm']},
      {'columns': ['article_id']},
    ],
    post_hook=[
      "create index if not exists ix_alias_trgm on {{ this }} "
      "using gin (public.f_unaccent(alias_text) gin_trgm_ops)"
    ]
) }}

-- Every string a human might type, mapped to a canonical article.
--
-- The GIN index is in a post_hook rather than the indexes config because it
-- needs an operator class on a function expression, and post_hook is raw SQL
-- with no rendering in between. `{{ this }}` resolves to the model's relation.
-- Either way it MUST be declared here: dbt drops and recreates this table on
-- every run, so an index you added by hand in psql lives until the next build.
--
-- NOT YET IMPLEMENTED: alias_type 'bold_intro' (the '''bolded''' names in an
-- article's first sentence, per design doc §3.3). It needs a wikitext scan,
-- which under Rule 3 belongs in links.py, not here -- so it is a small
-- extractor change plus a raw.page_alias table, not a SQL change. The three
-- types below cover the overwhelming majority of the value; the design doc's
-- own §4.2 argument is that redirects ARE the dictionary.

with canonical as (
    select
        a.norm_title  as alias_norm,
        a.article_id,
        a.title       as alias_text,
        'canonical'   as alias_type,
        1.0::real     as weight
    from {{ ref('dim_article') }} a
),

redirect as (
    select
        p.norm_title  as alias_norm,
        a.article_id,
        p.title       as alias_text,
        'redirect'    as alias_type,
        0.9::real     as weight
    from {{ ref('stg_page') }} p
    left join {{ ref('int_redirect_resolved') }} r
           on r.from_norm = p.norm_title
    join {{ ref('dim_article') }} a
      on a.norm_title = coalesce(r.final_norm, p.redirect_to_norm)
    where p.is_redirect
),

-- Anchor text, aggregated. The frequency filter is doing real work: link
-- display text includes an enormous amount of one-off phrasing ("the same
-- year", "his father") that would be noise in a search index. Requiring the
-- phrase to have been used at least twice for the same target removes most of
-- it at almost no cost in recall.
anchor as (
    select
        public.norm_title(l.display_text)  as alias_norm,
        d.article_id,
        min(l.display_text)                as alias_text,
        'anchor_text'                      as alias_type,
        count(*)                           as freq
    from {{ ref('stg_pagelink') }} l
    left join {{ ref('int_redirect_resolved') }} r
           on r.from_norm = l.target_norm
    join {{ ref('dim_article') }} d
      on d.norm_title = coalesce(r.final_norm, l.target_norm)
    where l.target_ns = 0
      and l.display_text is not null
      and length(l.display_text) between 2 and 120
      and l.display_text !~ '[\[\]{}|]'      -- leftover markup, not a phrase
    group by 1, 2
    having count(*) >= 2
),

unioned as (
    select alias_norm, article_id, alias_text, alias_type, weight from canonical
    union all
    select alias_norm, article_id, alias_text, alias_type, weight from redirect
    union all
    select alias_norm, article_id, alias_text, alias_type,
           -- log-scaled so a 40,000-use anchor does not swamp everything;
           -- capped below the canonical weight so the real title always wins.
           least(0.8, 0.2 + 0.1 * ln(freq))::real as weight
    from anchor
)

-- The PK is (alias_norm, article_id, alias_type) and the CTEs can each produce
-- duplicates within a type -- two redirects that normalize identically, for
-- instance. distinct on collapses them, keeping the heaviest.
select distinct on (alias_norm, article_id, alias_type)
    alias_norm, article_id, alias_text, alias_type, weight
from unioned
where alias_norm <> ''
order by alias_norm, article_id, alias_type, weight desc
```

**23c. `dbt/wikigraph/models/marts/article_search.sql`:**

```sql
{{ config(
    materialized='table',
    schema='mart',
    indexes=[{'columns': ['article_id'], 'unique': True}],
    post_hook=[
      "create index if not exists ix_search_tsv on {{ this }} using gin (tsv)",
      "create index if not exists ix_search_title_trgm on {{ this }} "
      "using gin (public.f_unaccent(title) gin_trgm_ops)"
    ]
) }}

-- One row per article, with everything the ranking function needs so a search
-- never has to join. pagerank is denormalized here on purpose (design doc
-- §3.4) and stays NULL until you compute it.
--
-- to_tsvector('english', x) -- the TWO-argument form -- is IMMUTABLE. The
-- one-argument form depends on the default_text_search_config GUC and is only
-- STABLE, so it cannot be used in a generated column or an index expression.
-- Getting this wrong is a very confusing error message.

with aliases as (
    select article_id, string_agg(distinct alias_text, ' ') as alias_blob
    from {{ ref('article_alias') }}
    where alias_type <> 'canonical'
    group by 1
)

select
    a.article_id,
    a.title,
    a.title || ' ' || coalesce(al.alias_blob, '')       as search_text,
    to_tsvector('english',
        a.title || ' ' || coalesce(al.alias_blob, ''))   as tsv,
    a.pagerank
from {{ ref('dim_article') }} a
left join aliases al on al.article_id = a.article_id
```

**23d. Tests.** Add to `dbt/wikigraph/models/marts/schema.yml`:

```yaml
  - name: article_alias
    description: "Every string a human might type -> a canonical article."
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [alias_norm, article_id, alias_type]
    columns:
      - name: alias_norm
        data_tests: [not_null]
      - name: article_id
        data_tests:
          - not_null
          - relationships: {to: ref('dim_article'), field: article_id}
      - name: alias_type
        data_tests:
          - accepted_values:
              arguments:
                values: ['canonical', 'redirect', 'anchor_text', 'bold_intro']

  - name: article_search
    columns:
      - name: article_id
        data_tests: [unique, not_null]
      - name: tsv
        data_tests: [not_null]
```

```powershell
.\tasks.ps1 dbt build
```

**23e. The search cascade.** Create `scripts/search.py`:

```python
"""Three-tier fuzzy article search.

Tier 1 exact  -- normalized alias equality. Sub-millisecond, index-only.
Tier 2 FTS    -- websearch_to_tsquery against the GIN tsvector index.
Tier 3 trigram-- pg_trgm similarity. Catches typos. The expensive one.

Run in order, stop when you have enough. Running all three on every query is
how a search endpoint that felt fine in development falls over in use.

Usage:  python scripts/search.py "jfk"
"""
from __future__ import annotations

import os
import sys
import time

import psycopg

EXACT = """
SELECT DISTINCT a.article_id, a.title, al.alias_type, al.weight::float AS score
FROM mart.article_alias al
JOIN mart.dim_article a USING (article_id)
WHERE al.alias_norm = public.norm_title(%(q)s)
ORDER BY score DESC
LIMIT %(k)s
"""

FTS = """
SELECT a.article_id, a.title, 'fts' AS alias_type,
       ts_rank_cd(s.tsv, websearch_to_tsquery('english', %(q)s))::float AS score
FROM mart.article_search s
JOIN mart.dim_article a USING (article_id)
WHERE s.tsv @@ websearch_to_tsquery('english', %(q)s)
ORDER BY score DESC, a.title
LIMIT %(k)s
"""

# `%` is the pg_trgm similarity operator and IS the index condition -- it uses
# ix_alias_trgm. Putting similarity() in the WHERE clause instead would force a
# sequential scan over every alias, which is the classic pg_trgm mistake.
TRGM = """
SELECT DISTINCT ON (a.article_id)
       a.article_id, a.title, 'trigram' AS alias_type,
       similarity(public.f_unaccent(al.alias_text), public.f_unaccent(%(q)s))::float AS score
FROM mart.article_alias al
JOIN mart.dim_article a USING (article_id)
WHERE public.f_unaccent(al.alias_text) %% public.f_unaccent(%(q)s)
ORDER BY a.article_id, score DESC
LIMIT %(k)s
"""


def search(conn, q: str, k: int = 10):
    for label, sql in (("exact", EXACT), ("fts", FTS), ("trigram", TRGM)):
        t0 = time.perf_counter()
        rows = conn.execute(sql, {"q": q, "k": k}).fetchall()
        ms = (time.perf_counter() - t0) * 1000
        print(f"-- tier {label}: {len(rows)} hits in {ms:.1f} ms")
        for aid, title, kind, score in rows:
            print(f"   {score:6.3f}  {title:<55} [{kind}]  id={aid}")
        if len(rows) >= 3:
            return
    print("   (nothing found in any tier)")


if __name__ == "__main__":
    with psycopg.connect(os.environ["WH_DSN"]) as conn:
        conn.execute("SET pg_trgm.similarity_threshold = 0.3")
        search(conn, " ".join(sys.argv[1:]) or "dog")
```

```powershell
python scripts\search.py "united states"
python scripts\search.py "jfk"
python scripts\search.py "beyonce"
python scripts\search.py "wrold war"
```

### Verify

**1. The alias table has the shape you expect.**

```sql
SELECT alias_type, count(*) AS n, count(DISTINCT article_id) AS articles,
       round(avg(weight)::numeric, 3) AS avg_weight
FROM mart.article_alias GROUP BY 1 ORDER BY 2 DESC;
```

`canonical` equals your `dim_article` count exactly. `redirect` should be a large fraction of your 329,547 redirects — not all, because some resolve to articles in other shards. `anchor_text` is usually the biggest of the three.

**2. Aliases point somewhere sensible.** Pick a well-known article and look at every name it answers to:

```sql
SELECT alias_text, alias_type, round(weight::numeric, 2) AS w
FROM mart.article_alias
WHERE article_id = (SELECT article_id FROM mart.dim_article
                    WHERE norm_title = public.norm_title('United States'))
ORDER BY weight DESC, alias_type LIMIT 30;
```

This is the most satisfying query in the step. You should see abbreviations, former names, and colloquialisms you never wrote down.

**3. The indexes exist AND are used.** Both halves matter.

```sql
\d+ mart.article_alias
\d+ mart.article_search
```

Then:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT a.title FROM mart.article_alias al JOIN mart.dim_article a USING (article_id)
WHERE public.f_unaccent(al.alias_text) % public.f_unaccent('beyonce');
```

**Look for `Bitmap Index Scan on ix_alias_trgm`.** If you see `Seq Scan`, either the index didn't build or the query isn't using the `%` operator — `WHERE similarity(x, y) > 0.3` cannot use a trigram index, and that is the single most common pg_trgm mistake.

**4. Re-run `dbt build` and re-check the indexes.** They must still be there. This is the actual test of whether you put them in the right place — a hand-created index would be gone now.

**5. Search behaves.** Typo tolerance ("wrold war"), accent insensitivity ("beyonce"), and abbreviation resolution ("jfk") should each work, subject to the article existing in your shard. Time each tier and record it.

Append to `NOTES.md`:

```markdown
## Search layer (Step 23)  ** GOAL #2 **
- article_alias rows: ____  (canonical ____ / redirect ____ / anchor_text ____)
- article_search rows: ____ | table + GIN size: ____
- Index build time (post_hook): ____ s
- Query latency: exact ____ ms | fts ____ ms | trigram ____ ms
- pg_trgm.similarity_threshold used: ____
- Aliases per article: mean ____ , max ____
```

### If it breaks

- **`functions in index expression must be marked IMMUTABLE`** — you used bare `unaccent()`. That's what `f_unaccent` in V005 is for. Same error on `to_tsvector(x)`: use the two-argument form.
- **`operator does not exist: text % text`** — `pg_trgm` isn't installed in the search path. V001 created it; confirm with `\dx`.
- **Trigram search returns nothing** — the default `pg_trgm.similarity_threshold` is 0.3 and short queries rarely clear it. Lower it per session, as the script does. Below ~0.2 you get noise.
- **The `%%` in `scripts/search.py` looks like a typo** — it isn't. psycopg uses `%` for parameter placeholders, so a literal `%` operator must be doubled. Getting this wrong yields `IndexError: tuple index out of range`, which points nowhere near the real cause.
- **`article_alias` build is slow or spills** — the `anchor` CTE aggregates every ns=0 link occurrence. Add `pre_hook="set local work_mem = '256MB'"` as in Step 21.
- **`accepted_values` test fails on `alias_type`** — you added a type and didn't add it to the list. Keep the list and the model in sync; that's what the test is for.
- **`string_agg(distinct ...)` errors** — it needs the `distinct` inside, before the expression, and cannot be combined with `order by` on a different column. If you want ordering, aggregate a sorted subquery instead.

Commit:

```powershell
git add db/migrations/V005__search_helpers.sql dbt/ scripts/search.py NOTES.md
git commit -m "article_alias, article_search, GIN indexes, and the search cascade"
```

**Goal #2 is done** — as much as one shard allows. Come back to the queries above after Step 26; the difference is dramatic.

---

## Step 24 — `mart.fct_first_link`

### Why

**Goal #3, part one.** The design doc says this is the goal most likely to go wrong, and that the failure is in the *definition*, not the query. It's right, and Steps 17–18 already did most of the hard part: with the seven flags on `stg_pagelink`, the model itself is about fifteen lines.

What's left is one structural decision and one hazard.

**The decision: every article gets a row, including dead ends.** A `LEFT JOIN` from `dim_article`, not an inner join from `stg_pagelink`. If an article has no valid first link, it gets a row with `dst_article_id IS NULL`. That matters because Step 25 needs a complete node list — an article missing from `fct_first_link` and an article with a NULL first link are different facts, and the walk has to distinguish them.

**The hazard: you now have the first-link rule written twice.** Once in Python, in `first_body_link()`, which is what you reviewed by hand in Step 18. Once in SQL, in the model below. That's exactly the situation Rule 3 exists to prevent, and here it's unavoidable — Python needs the rule to make review possible, SQL needs it to build the table at scale.

Since you can't eliminate the duplication, **test it**. The integration test in 24c runs both implementations against the same articles and fails on disagreement. That converts an invisible drift bug into a red test.

### Do

**24a. Move `first_body_link` into the package.** If you left it in `scripts/review_first_links.py` in Step 18, move it to `src/wikigraph/links.py` now and import it from the script. It's about to have two consumers.

```python
FIRST_LINK_EXCLUDED_FLAGS = (
    "in_parens", "in_italics", "in_template", "in_table",
    "in_ref", "in_infobox", "in_file_caption",
)
FIRST_LINK_EXCLUDED_PREFIXES = frozenset({"file", "image", "media", "category"})


def first_body_link(wikitext: str | None) -> dict | None:
    """The first-link rule, in Python.

    MUST stay in sync with dbt/wikigraph/models/marts/fct_first_link.sql.
    tests/test_first_link_parity.py fails if they drift. If you change either,
    change both and bump RULE_VERSION.

    Note this cannot see two exclusions the SQL applies -- red links and
    self-links -- because both need the article table. Parity is asserted only
    on articles where the SQL found a link at all.
    """
    for l in extract_links(wikitext):
        if l["target_prefix"] in FIRST_LINK_EXCLUDED_PREFIXES:
            continue
        if any(l[f] for f in FIRST_LINK_EXCLUDED_FLAGS):
            continue
        return l
    return None
```

**24b. `dbt/wikigraph/models/marts/fct_first_link.sql`:**

```sql
{{ config(
    materialized='table',
    schema='mart',
    indexes=[
      {'columns': ['src_article_id'], 'unique': True},
      {'columns': ['dst_article_id']},
    ],
    pre_hook="set local work_mem = '256MB'"
) }}

-- One row per article: its first link under the "click the first link" rules.
-- dst_article_id IS NULL means dead end -- the article HAS no valid first link.
-- Every article in dim_article gets a row; that completeness is what Step 25's
-- walk depends on.
--
-- MIRRORS wikigraph.links.first_body_link(). Change one, change the other,
-- bump rule_version. tests/test_first_link_parity.py enforces it.

with candidate as (
    select distinct on (l.src_page_id)
        l.src_page_id,
        d.article_id as dst_article_id,
        l.ordinal
    from {{ ref('stg_pagelink') }} l
    left join {{ ref('int_redirect_resolved') }} r
           on r.from_norm = l.target_norm
    join {{ ref('dim_article') }} d
      on d.norm_title = coalesce(r.final_norm, l.target_norm)
    where l.target_ns = 0                       -- excludes File:, Category:, interwiki
      and not (l.in_parens
            or l.in_italics
            or l.in_template
            or l.in_table
            or l.in_ref
            or l.in_infobox
            or l.in_file_caption)
      and l.src_page_id <> d.article_id         -- no self-links
    -- The join to dim_article is what drops red links: a target with no
    -- article is not a place a reader can land.
    order by l.src_page_id, l.ordinal
)

select
    a.article_id                              as src_article_id,
    c.dst_article_id,
    c.ordinal,
    {{ var('first_link_rule_version', 1) }}::smallint as rule_version
from {{ ref('dim_article') }} a
left join candidate c on c.src_page_id = a.article_id
```

Add to `dbt/wikigraph/models/marts/schema.yml`:

```yaml
  - name: fct_first_link
    description: >
      Each article's first body link. NULL dst means dead end.
      rule_version identifies which set of exclusion rules produced this.
    columns:
      - name: src_article_id
        data_tests:
          - unique
          - not_null
          - relationships: {to: ref('dim_article'), field: article_id}
      - name: dst_article_id
        data_tests:
          - relationships: {to: ref('dim_article'), field: article_id}
      - name: rule_version
        data_tests: [not_null]
```

**24c. The parity test.** Create `tests/test_first_link_parity.py`:

```python
"""The first-link rule exists in Python and in SQL. Prove they agree.

Marked `integration` because it needs a warehouse with fct_first_link built.
Skips itself when WH_DSN is absent, so CI stays green.
"""
import os

import pytest

psycopg = pytest.importorskip("psycopg")
from wikigraph.links import first_body_link

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def conn():
    dsn = os.environ.get("WH_DSN")
    if not dsn:
        pytest.skip("WH_DSN not set")
    with psycopg.connect(dsn) as c:
        yield c


def test_python_and_sql_agree(articles, conn):
    ids = [a["page_id"] for a in articles]
    rows = conn.execute("""
        SELECT f.src_article_id, a.title
        FROM mart.fct_first_link f
        JOIN mart.dim_article a ON a.article_id = f.dst_article_id
        WHERE f.src_article_id = ANY(%s)
    """, (ids,)).fetchall()
    sql_answer = dict(rows)

    # norm_title, from SQL, so the comparison uses ONE normalization. Rule 3
    # applies to test code too -- reimplementing it here would defeat the point.
    disagree = []
    for a in articles:
        if a["page_id"] not in sql_answer:
            continue          # SQL found a red link / cross-shard target; Python
                              # cannot know that, so there is nothing to compare
        py = first_body_link(a["wikitext"])
        if py is None:
            disagree.append(f"{a['title']!r}: SQL found "
                            f"{sql_answer[a['page_id']]!r}, Python found nothing")
            continue
        same = conn.execute(
            "SELECT public.norm_title(%s) = public.norm_title(%s)",
            (py["target_raw"], sql_answer[a["page_id"]]),
        ).fetchone()[0]
        if not same:
            disagree.append(f"{a['title']!r}: SQL {sql_answer[a['page_id']]!r} "
                            f"vs Python {py['target_raw']!r}")

    assert not disagree, ("first-link rule drift between SQL and Python:\n  "
                          + "\n  ".join(disagree[:20]))
```

> **Expect a handful of legitimate disagreements**, and read them before deciding they're bugs. The commonest is redirects: Python reports `target_raw` as written, SQL reports the *resolved* article title. The `norm_title` comparison above doesn't handle that — if the disagreements are all "Python said `Dogs`, SQL said `Dog`", resolve Python's answer through `int_redirect_resolved` in the test rather than loosening the assertion.

**24d. Build.**

```powershell
.\tasks.ps1 dbt build --select +fct_first_link
$env:WH_DSN = "postgresql://$($env:PG_ETL_USER):$($env:PG_ETL_PASSWORD)@localhost:$($env:PG_HOST_PORT)/$($env:PG_DB)"
python -m pytest tests/test_first_link_parity.py -q
```

### Verify

**1. Coverage — and brace yourself.**

```sql
SELECT count(*) AS articles,
       count(dst_article_id) AS with_first_link,
       round(100.0 * count(dst_article_id) / count(*), 1) AS pct_linked
FROM mart.fct_first_link;
```

**On one shard this will look bad, and that is expected.** Most articles have a perfectly good first link in their wikitext that points at an article in a page-ID range you haven't loaded. The join to `dim_article` drops it, and the article records as a dead end. `pct_linked` in the 30–60% range here is normal. In Step 27 it should exceed 90%; if it doesn't, *then* you have a rules problem.

**2. The most-linked-to first-link targets.** This is the first hint of the attractor structure:

```sql
SELECT a.title, count(*) AS incoming_first_links
FROM mart.fct_first_link f
JOIN mart.dim_article a ON a.article_id = f.dst_article_id
GROUP BY 1 ORDER BY 2 DESC LIMIT 25;
```

Expect abstract, definitional articles at the top — the kind of thing lead sentences reach for. If the top of this list is *United States* or a year article, the rule is picking up something it shouldn't.

**3. Spot-check ten by hand.** Not optional — this is a rule, not a fact.

```sql
SELECT a.title AS article, t.title AS first_link, f.ordinal
FROM mart.fct_first_link f
JOIN mart.dim_article a ON a.article_id = f.src_article_id
JOIN mart.dim_article t ON t.article_id = f.dst_article_id
WHERE NOT a.is_list_page AND NOT a.is_disambig
ORDER BY random() LIMIT 10;
```

Open each on Wikipedia. If more than one or two look wrong, fix `INFOBOX_PREFIXES` or the flags, bump `RULE_VERSION` and `first_link_rule_version`, rebuild, and compare the numbers from check 2 between versions. That comparison is the entire reason Rule 4 exists.

**4. High ordinals are a smell.**

```sql
SELECT width_bucket(ordinal, 1, 100, 10) AS bucket,
       min(ordinal), max(ordinal), count(*)
FROM mart.fct_first_link WHERE dst_article_id IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

Most articles' first body link should be within the first 5–20 link occurrences. A long tail at ordinal 200+ means those articles have a large template or infobox block ahead of the prose — normal for some — but a *fat* tail means a flag is over-firing and swallowing the real first link.

Append to `NOTES.md`:

```markdown
## fct_first_link (Step 24), rule_version ____
- Articles: ____ | with a first link: ____ (____%)   <- expect low pre-backfill
- Median ordinal of the chosen link: ____ | p95: ____
- Top 5 first-link targets: ____
- Python/SQL parity test: ____ disagreements on ____ articles
- Manual spot-check: ____ / 10 correct
```

### If it breaks

- **`pct_linked` is near zero** — the flags are over-firing; check the `pct_clean` number from Step 19's Verify. If that was healthy, the problem is the join, not the rule.
- **Every `ordinal` is 1** — the flag conditions aren't in the `WHERE`. Check the `not (...)` block survived your editing; a misplaced paren makes it always true.
- **The `unique` test on `src_article_id` fails** — `distinct on` lost its matching `order by`, or you inner-joined `candidate` instead of left-joining from `dim_article`.
- **The parity test reports disagreements on every article** — you're comparing raw strings without normalizing. Use `public.norm_title()` on both sides, via SQL.
- **`relationships` test on `dst_article_id` fails** — impossible from an inner join to `dim_article`, so it means a stale table. `--full-refresh`.

Commit:

```powershell
git add dbt/ src/wikigraph/links.py tests/test_first_link_parity.py NOTES.md
git commit -m "fct_first_link with rule versioning, plus a Python/SQL parity test"
```

---

## Step 25 — The in-memory functional-graph walk

### Why

**Goal #3, part two.** Every article now has exactly one outgoing first link, or none. That makes this a **functional graph**: out-degree is 0 or 1 everywhere. Functional graphs have a property that makes the whole problem easy — *every* path provably terminates, in either a cycle or a dead end, and you can find the terminal for all n nodes in a single O(n) pass.

**Do not do this in SQL.** A recursive CTE per article is 7 million recursive queries. Even a clever set-based formulation fights the shape of the problem: the answer for a node depends on the answer for its successor, which is inherently sequential.

**Do it in memory.** The whole functional graph is one integer array: 7M × 4 bytes = 28 MB. I measured the walk below on a synthetic 7M-node functional graph — **11.4 seconds**, single-threaded, in Python. The design doc says "runtime: seconds" and that turns out to be right.

**The algorithm is three-colour marking**, the same one used for cycle detection in a depth-first search, made iterative:

- **White** — not visited.
- **Grey** — on the path currently being walked.
- **Black** — solved; its terminal and distance are known.

Walk forward from an unvisited node, painting grey, until you hit one of three things:

| You hit           | Meaning                             | What to do                                       |
| ----------------- | ----------------------------------- | ------------------------------------------------ |
| A dead end (`-1`) | The path ends here                  | Last node is the terminal, distance 0            |
| A **black** node  | You've joined a solved path         | Inherit its terminal, distance + 1               |
| A **grey** node   | You've closed a loop *on this path* | Everything from it onward is a newly found cycle |

Then unwind the path backwards, assigning distances. Each node is painted grey once and black once, so the total work is linear no matter how the paths interleave.

**Two amendments to the design doc, both deliberate.**

**`fct_first_link_path.path` — don't store it.** The design doc specifies a `path integer[]` capped at ~50. At 7M articles averaging 15–25 hops, that's 100M+ integers, and it's the largest thing in the mart for information you can regenerate in microseconds: given `fct_first_link` in memory, reconstructing any single path is a loop of a dozen array lookups. Store the terminal, the distance, and the cycle id; reconstruct paths on demand. The column stays in the schema with a comment, so re-adding it is a decision rather than an archaeology exercise.

**`terminal_type` has no `'max_depth'`.** The design doc lists it, but a functional-graph walk cannot hit a depth limit — every path terminates by construction. If you ever see a node that doesn't, you have a bug in the walk, not a deep chain. And `'philosophy'` isn't a value Python emits either: whether a cycle is *the* Philosophy cycle is a fact about your data, so it's derived in SQL by looking for that article in the cycle members. Python emits `'dead_end'`, `'cycle_member'`, `'basin'`.

**Where the output lives.** Python computes it, so dbt can't own it — but dbt has to read it. That's what the `derived` schema is for.

### Do

**25a. The migration.** Create `db/migrations/V006__derived_schema.sql`:

```sql
-- Artifacts computed in Python that dbt must read.
--
-- Not `raw`: nothing about these mirrors the source. Not `mart`: dbt owns mart
-- exclusively and two writers to one schema is how ownership models rot.
-- A third schema costs one migration and keeps the boundary honest.
CREATE SCHEMA IF NOT EXISTS derived AUTHORIZATION etl;

GRANT USAGE ON SCHEMA derived TO dbt;
GRANT SELECT ON ALL TABLES IN SCHEMA derived TO dbt;
ALTER DEFAULT PRIVILEGES FOR ROLE etl IN SCHEMA derived GRANT SELECT ON TABLES TO dbt;

SET ROLE etl;

CREATE TABLE IF NOT EXISTS derived.first_link_walk (
    src_article_id    integer  PRIMARY KEY,
    steps_to_terminal integer  NOT NULL,   -- 0 for cycle members and dead ends
    terminal_type     text     NOT NULL,   -- dead_end | cycle_member | basin
    cycle_id          integer,             -- NULL when the chain ends in a dead end
    rule_version      smallint NOT NULL,
    computed_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS derived.first_link_cycle (
    cycle_id     integer PRIMARY KEY,
    members      integer[] NOT NULL,
    cycle_length smallint  NOT NULL,
    basin_size   integer   NOT NULL,
    rule_version smallint  NOT NULL
);

RESET ROLE;
```

```powershell
.\tasks.ps1 migrate
```

**25b. `src/wikigraph/walk.py`:**

```python
"""Walk the first-link functional graph.

Out-degree is 0 or 1 everywhere, so every path terminates in a cycle or a dead
end and all terminals can be found in one O(n) pass with three-colour marking.

Iterative, not recursive: a chain of a few thousand articles would exceed
Python's recursion limit, and the deep ones are exactly the interesting ones.
"""
from __future__ import annotations

import numpy as np

DEAD_END, CYCLE, IN_BASIN = 0, 1, 2
WHITE, GREY, BLACK = 0, 1, 2
TERMINAL_NAME = {DEAD_END: "dead_end", CYCLE: "cycle_member", IN_BASIN: "basin"}


def walk(nxt: np.ndarray) -> dict:
    """nxt[i] = dense index of i's first link, or -1 for a dead end.

    Returns, all length n:
      steps  int32  hops to the terminal (0 for cycle members and dead ends)
      kind   int8   DEAD_END | CYCLE | IN_BASIN
      cycle  int32  id of the cycle this node drains into, or -1
    plus `cycles` (membership lists) and `basin` (size per cycle).
    """
    n = len(nxt)
    colour = np.zeros(n, dtype=np.int8)
    onpath = np.full(n, -1, dtype=np.int32)   # position on the CURRENT path
    steps = np.zeros(n, dtype=np.int32)
    kind = np.zeros(n, dtype=np.int8)
    cycle = np.full(n, -1, dtype=np.int32)
    cycles: list[list[int]] = []

    path: list[int] = []
    for start in range(n):
        if colour[start] != WHITE:
            continue

        path.clear()
        v = start
        while v != -1 and colour[v] == WHITE:
            colour[v] = GREY
            onpath[v] = len(path)
            path.append(v)
            v = nxt[v]

        if v == -1:
            # Walked off the end. The last node on the path IS the terminal.
            tail_steps, tail_kind, tail_cycle = 0, DEAD_END, -1
        elif colour[v] == BLACK:
            # Joined a path solved on an earlier iteration. Inherit its answer.
            tail_steps, tail_kind, tail_cycle = steps[v] + 1, IN_BASIN, cycle[v]
        else:
            # GREY: v is on the path we just laid down, so everything from v to
            # the end of the path is a cycle nobody has seen before.
            at = onpath[v]
            members = path[at:]
            cid = len(cycles)
            cycles.append(members)
            for m in members:
                colour[m] = BLACK
                steps[m] = 0
                kind[m] = CYCLE
                cycle[m] = cid
            del path[at:]                      # the rest of the path drains INTO it
            tail_steps, tail_kind, tail_cycle = 1, IN_BASIN, cid

        # Unwind. The node nearest the terminal was resolved first.
        d = tail_steps
        for i in range(len(path) - 1, -1, -1):
            node = path[i]
            colour[node] = BLACK
            steps[node] = d
            kind[node] = IN_BASIN if d > 0 else tail_kind
            cycle[node] = tail_cycle
            d += 1

    basin = np.zeros(len(cycles), dtype=np.int64)
    if cycles:
        has = cycle >= 0
        np.add.at(basin, cycle[has], 1)

    return {"steps": steps, "kind": kind, "cycle": cycle,
            "cycles": cycles, "basin": basin}


def build_next(article_ids: np.ndarray,
               src: np.ndarray, dst: np.ndarray) -> np.ndarray:
    """Map sparse article_ids onto dense 0..n-1 indices and build nxt.

    article_ids must be sorted -- searchsorted is what makes this O(n log n)
    instead of a 7-million-entry Python dict.
    """
    n = len(article_ids)
    nxt = np.full(n, -1, dtype=np.int32)
    if len(src):
        nxt[np.searchsorted(article_ids, src)] = np.searchsorted(article_ids, dst)
    return nxt


def reconstruct_path(nxt: np.ndarray, start: int, cap: int = 200) -> list[int]:
    """One article's full chain, on demand. This is why we don't store paths."""
    out, v, seen = [], start, set()
    while v != -1 and v not in seen and len(out) < cap:
        out.append(v)
        seen.add(v)
        v = nxt[v]
    return out
```

**25c. Test it before you trust it.** Create `tests/test_walk.py`:

```python
"""The walk is clever, so it gets a stupid reference implementation to check.

Random small functional graphs, brute-forced. If the fast version and the
obvious version ever disagree, the fast one is wrong.
"""
import random

import numpy as np

from wikigraph.walk import CYCLE, DEAD_END, IN_BASIN, walk


def reference(nxt):
    """Follow every chain one node at a time. O(n^2) and obviously correct."""
    out = []
    for i in range(len(nxt)):
        seen, v, k = {}, i, 0
        while v != -1 and v not in seen:
            seen[v] = k
            v = nxt[v]
            k += 1
        if v == -1:
            out.append((k - 1, DEAD_END if k - 1 == 0 else IN_BASIN, None))
        else:
            entry = seen[v]
            members = frozenset(m for m, idx in seen.items() if idx >= entry)
            out.append((entry, CYCLE if entry == 0 else IN_BASIN, members))
    return out


def test_matches_reference():
    random.seed(7)
    for _ in range(300):
        n = random.randint(1, 25)
        nxt = np.array([random.choice([-1] + list(range(n))) for _ in range(n)],
                       dtype=np.int32)
        r = walk(nxt)
        for i, (s, k, members) in enumerate(reference(nxt)):
            assert r["steps"][i] == s, nxt.tolist()
            assert r["kind"][i] == k, nxt.tolist()
            cid = r["cycle"][i]
            if members is None:
                assert cid == -1, nxt.tolist()
            else:
                assert cid >= 0 and frozenset(r["cycles"][cid]) == members, nxt.tolist()


def test_self_loop():
    r = walk(np.array([0], dtype=np.int32))
    assert r["kind"][0] == CYCLE and r["cycles"] == [[0]] and r["basin"][0] == 1


def test_two_cycle_with_tail():
    # 0 -> 1 -> 2 -> 1
    r = walk(np.array([1, 2, 1], dtype=np.int32))
    assert list(r["steps"]) == [1, 0, 0]
    assert list(r["kind"]) == [IN_BASIN, CYCLE, CYCLE]
    assert r["basin"][0] == 3
```

```powershell
python -m pytest tests/test_walk.py -q
```

**25d. The driver.** Create `scripts/first_link_walk.py`:

```python
"""Run the functional-graph walk and write results to the derived schema.

Usage:  python scripts/first_link_walk.py
"""
from __future__ import annotations

import os
import time

import numpy as np
import psycopg
from psycopg import sql

from wikigraph.walk import TERMINAL_NAME, build_next, walk


def main() -> None:
    dsn = os.environ["WH_DSN"]
    t0 = time.perf_counter()

    with psycopg.connect(dsn) as conn:
        rule_version = conn.execute(
            "SELECT max(rule_version) FROM mart.fct_first_link"
        ).fetchone()[0]

        # Sorted, so searchsorted works. This is the dense index.
        ids = np.array(
            [r[0] for r in conn.execute(
                "SELECT article_id FROM mart.dim_article ORDER BY article_id")],
            dtype=np.int64,
        )
        edges = conn.execute("""
            SELECT src_article_id, dst_article_id
            FROM mart.fct_first_link
            WHERE dst_article_id IS NOT NULL
        """).fetchall()

    src = np.fromiter((e[0] for e in edges), dtype=np.int64, count=len(edges))
    dst = np.fromiter((e[1] for e in edges), dtype=np.int64, count=len(edges))
    print(f"loaded {len(ids):,} articles, {len(edges):,} first links "
          f"in {time.perf_counter()-t0:.1f}s "
          f"({(ids.nbytes + src.nbytes + dst.nbytes)/1e6:.0f} MB)")

    nxt = build_next(ids, src, dst)
    t1 = time.perf_counter()
    r = walk(nxt)
    print(f"walk: {time.perf_counter()-t1:.1f}s | {len(r['cycles']):,} cycles")

    kinds = r["kind"]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE derived.first_link_walk")
            with cur.copy(
                "COPY derived.first_link_walk "
                "(src_article_id, steps_to_terminal, terminal_type, cycle_id, "
                " rule_version) FROM STDIN (FORMAT BINARY)"
            ) as cp:
                cp.set_types(["integer", "integer", "text", "integer", "smallint"])
                for i in range(len(ids)):
                    cid = int(r["cycle"][i])
                    cp.write_row((int(ids[i]), int(r["steps"][i]),
                                  TERMINAL_NAME[int(kinds[i])],
                                  cid if cid >= 0 else None, rule_version))

            # Thousands of rows, not millions: executemany is the right tool
            # here, and binary COPY of int[] is more fiddle than it is worth.
            cur.execute("TRUNCATE derived.first_link_cycle")
            cur.executemany(
                "INSERT INTO derived.first_link_cycle "
                "(cycle_id, members, cycle_length, basin_size, rule_version) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(cid, [int(ids[m]) for m in members], len(members),
                  int(r["basin"][cid]), rule_version)
                 for cid, members in enumerate(r["cycles"])],
            )
        conn.commit()
        conn.autocommit = True
        for t in ("first_link_walk", "first_link_cycle"):
            conn.execute(sql.SQL("ANALYZE derived.{}").format(sql.Identifier(t)))

    print(f"total {time.perf_counter()-t0:.1f}s")


if __name__ == "__main__":
    main()
```

```powershell
python scripts\first_link_walk.py
```

**25e. Expose it through dbt.** Add to `dbt/wikigraph/models/sources.yml`:

```yaml
  - name: derived
    description: "Python-computed artifacts. dbt reads, never writes."
    schema: derived
    tables:
      - name: first_link_walk
      - name: first_link_cycle
```

`dbt/wikigraph/models/marts/fct_first_link_path.sql`:

```sql
{{ config(materialized='table', schema='mart', tags=['postwalk'],
          indexes=[{'columns': ['src_article_id'], 'unique': True},
                   {'columns': ['terminal_type']}]) }}

-- NOTE: the design doc specifies a `path integer[]` column here. Deliberately
-- omitted -- see Step 25's Why. At 7M articles and ~20 hops it is the largest
-- object in the mart, for something wikigraph.walk.reconstruct_path() rebuilds
-- in microseconds. Add it back only with a measured reason.

select
    w.src_article_id,
    w.steps_to_terminal::smallint as steps_to_terminal,
    case
        when w.cycle_id is not null and phi.cycle_id is not null
             and w.cycle_id = phi.cycle_id then 'philosophy'
        when w.terminal_type = 'dead_end'                    then 'dead_end'
        when w.terminal_type = 'cycle_member'                then 'other_cycle'
        when w.cycle_id is not null                          then 'other_cycle'
        else 'dead_end'
    end                           as terminal_type,
    w.cycle_id,
    w.rule_version
from {{ source('derived', 'first_link_walk') }} w
-- "Is this THE Philosophy cycle" is a fact about the data, not about the
-- algorithm, so it is derived here rather than emitted by Python.
left join (
    select c.cycle_id
    from {{ source('derived', 'first_link_cycle') }} c
    join {{ ref('dim_article') }} a
      on a.article_id = any(c.members)
    where a.norm_title = public.norm_title('Philosophy')
    limit 1
) phi on true
```

`dbt/wikigraph/models/marts/fct_first_link_cycle.sql`:

```sql
{{ config(materialized='table', schema='mart', tags=['postwalk'],
          indexes=[{'columns': ['cycle_id'], 'unique': True}]) }}

select
    c.cycle_id,
    c.members,
    c.cycle_length,
    c.basin_size,
    c.rule_version,
    (select array_agg(a.title order by a.title)
     from {{ ref('dim_article') }} a
     where a.article_id = any(c.members)) as member_titles
from {{ source('derived', 'first_link_cycle') }} c
```

**25f. Orchestrate it.** The two models above depend on a Python step that runs *between* dbt models, which `DbtDag` can't express — it renders one dbt project as one DAG. The fix is two pieces:

Exclude them from the main transform DAG. In `airflow-docker/dags/wikigraph_transform.py`:

```python
from cosmos import RenderConfig

wikigraph_transform = DbtDag(
    ...
    render_config=RenderConfig(exclude=["tag:postwalk"]),
)
```

Then create `airflow-docker/dags/wikigraph_analytics.py`:

```python
"""Analytics: the functional-graph walk, then the models that depend on it.

Uses DbtTaskGroup rather than DbtDag: a task GROUP can be wired downstream of a
plain Python task, which is exactly what "run dbt, then Python, then more dbt"
needs. DbtDag renders a whole project as its own DAG and cannot interleave.
"""
from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

DBT_PROJECT = "/opt/airflow/dbt/wikigraph"

profile_config = ProfileConfig(
    profile_name="wikigraph",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT}/profiles.yml",
)


@dag(
    dag_id="wikigraph_analytics",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["wikigraph", "analytics"],
)
def wikigraph_analytics():

    @task(execution_timeout=pendulum.duration(minutes=30))
    def first_link_walk() -> dict:
        import numpy as np

        from wikigraph.walk import build_next, walk
        # Same body as scripts/first_link_walk.py. In practice, move that
        # script's main() into src/wikigraph/walk.py as run_walk(dsn) and call
        # it from both places -- Rule 1 applies to scripts too.
        from wikigraph.walk import run_walk

        dsn = os.environ["AIRFLOW_CONN_WIKIGRAPH_WAREHOUSE"].replace(
            "postgres://", "postgresql://", 1)
        stats = run_walk(dsn)
        if stats["articles"] == 0:
            raise ValueError("no articles — is fct_first_link built?")
        return stats

    postwalk = DbtTaskGroup(
        group_id="postwalk_models",
        project_config=ProjectConfig(DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(select=["tag:postwalk"]),
        execution_config=ExecutionConfig(
            dbt_executable_path=os.environ.get(
                "DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt")),
        operator_args={"install_deps": True},
    )

    first_link_walk() >> postwalk


wikigraph_analytics()
```

### Verify

**1. The walk terminates and the numbers add up.**

```sql
SELECT terminal_type, count(*) AS n,
       round(100.0*count(*)/sum(count(*)) OVER (), 1) AS pct,
       round(avg(steps_to_terminal), 1) AS avg_steps,
       max(steps_to_terminal) AS max_steps
FROM mart.fct_first_link_path GROUP BY 1 ORDER BY 2 DESC;
```

**On one shard this is dominated by `dead_end`, and that is the expected result.** Every chain that leaves the loaded page-ID range dies. *Philosophy* is `page_id` 13,692,155 and lives in shard `p9093926p14370073`, so `terminal_type = 'philosophy'` will be **zero rows** here. What you are verifying is that the machinery works: cycles found, distances assigned, every article accounted for.

**2. Every article has exactly one row.**

```sql
SELECT (SELECT count(*) FROM mart.dim_article)          AS articles,
       (SELECT count(*) FROM mart.fct_first_link_path)  AS walked;
```

Equal. Not approximately.

**3. The cycles are real.**

```sql
SELECT cycle_id, cycle_length, basin_size, member_titles
FROM mart.fct_first_link_cycle ORDER BY basin_size DESC LIMIT 10;
```

Read the member titles. A 2-cycle between two closely related abstract nouns is completely normal and is the shape the Philosophy attractor takes. A 1-cycle (an article whose first link is itself) means the self-link exclusion in Step 24 didn't fire.

**4. Follow one chain by hand** — the payoff query:

```sql
WITH RECURSIVE chain AS (
    SELECT article_id, title, 0 AS step
    FROM mart.dim_article WHERE norm_title = public.norm_title('Dog')
  UNION ALL
    SELECT a.article_id, a.title, c.step + 1
    FROM chain c
    JOIN mart.fct_first_link f ON f.src_article_id = c.article_id
    JOIN mart.dim_article a ON a.article_id = f.dst_article_id
    WHERE c.step < 30
)
SELECT step, title FROM chain ORDER BY step;
```

(A recursive CTE is exactly right for *one* article. It's doing it 7 million times that isn't.)

**5. Timing.** The walk should be seconds. On a synthetic 7M-node functional graph I measured **11.4 s** for the walk itself; loading the edges out of Postgres and writing the results back dominates the total. If the walk alone takes minutes, `build_next` is falling back to a Python loop — check `article_ids` is sorted and is a numpy array, not a list.

Append to `NOTES.md`:

```markdown
## First-link walk (Step 25)  ** GOAL #3, single shard **
- Articles walked: ____ | first links present: ____
- Load from PG: ____ s | walk: ____ s | write back: ____ s
- Cycles found: ____ | largest basin: ____ | longest cycle: ____
- Terminal mix: dead_end ____% | other_cycle ____% | philosophy ____%
- Mean steps to terminal: ____ | max: ____
- NOTE: dominated by dead ends pre-backfill. Re-run after Step 26.
```

### If it breaks

- **`IndexError` in `build_next`** — a `dst_article_id` isn't in `article_ids`. The FK makes that impossible unless the two queries ran against different builds. Re-run both in one transaction, or add `assert np.isin(dst, article_ids).all()`.
- **The walk never finishes** — you have recursion, or `onpath` isn't being set. Every node must be painted grey exactly once; add a counter and assert it equals n.
- **A node's `steps_to_terminal` is huge and wrong** — the unwind loop's `d` isn't reset per path, or `del path[at:]` is missing after a cycle is found. `test_matches_reference` catches both; run it.
- **Memory spikes to several GB while loading edges** — `fetchall()` on 7M rows builds 7M Python tuples. That's ~500 MB of objects and it's survivable, but at full scale prefer `fromiter` over a server-side cursor, or `COPY ... TO STDOUT (FORMAT BINARY)` straight into numpy.
- **Cosmos renders the postwalk models in `wikigraph_transform` anyway** — `RenderConfig(exclude=...)` needs the tag on the model's `config()` block, not in `dbt_project.yml`'s `models:` tree. Confirm with `dbt ls --select tag:postwalk`.
- **`ImportError: cannot import name 'run_walk'`** — you haven't moved `main()` out of the script yet. Do it; Rule 1 applies to scripts as much as to DAGs.

Commit:

```powershell
git add db/migrations/V006__derived_schema.sql src/wikigraph/walk.py scripts/first_link_walk.py `
        tests/test_walk.py dbt/ airflow-docker/dags/wikigraph_analytics.py NOTES.md
git commit -m "Functional-graph walk: derived schema, O(n) three-colour marking, analytics DAG"
```

**Goal #3's machinery is done.** The answer arrives in Step 27.

---

## Step 26 — Backfilling the remaining 18 shards

### Why

Nothing here is new logic. Every DAG is idempotent per shard, every gate has fired at least once, and the marts rebuild from scratch on every dbt run. The backfill is a *scheduling and capacity* problem, and the reason it gets its own step is that at 19× the volume, three things that were free become expensive:

1. **Scratch space.** 19 shards of page Parquet is ~55 GB that nobody reads after the COPY commits.
2. **Postgres configuration.** Part 1's tuning assumed the Docker daemon had ~8 GB. Your `NOTES.md` says the WSL VM has **45.59 GB and 24 logical processors**. You have been running a warehouse tuned for a fifth of the machine you own.
3. **The `stg_pagelink` materialization.** ~13.5M rows unpartitioned is fine. ~257M is a different conversation, and the design doc's 10 GB estimate for it is optimistic in a way that's worth understanding before you find out.

Do the pre-flight. It's twenty minutes and it's the difference between a run you start and forget and a run that dies at 3 a.m. with a half-loaded partition.

### Do

**26a. Pre-flight: the arithmetic.** Fill this in from your own `NOTES.md` before you start anything.

| Quantity                    | Shard 0, measured | × 19                           |
| --------------------------- | ----------------- | ------------------------------ |
| Input XML                   | 9.9 GB            | ~190–230 GB (already on disk)  |
| Page Parquet                | 2,970 MB          | 55 GB **if you keep them all** |
| `raw.page` partition        | ____ GB           | ____ GB                        |
| `raw.pagelink` partition    | ____ GB           | ____ GB                        |
| Peak staging (with cleanup) | ~3 GB             | ~3 GB × pool size              |

Then the derived layers, which don't scale per-shard but per-total:

| Table                   | Est. rows at 19 shards | Est. size             |
| ----------------------- | ---------------------- | --------------------- |
| `stg.stg_pagelink`      | ~257M                  | your bytes/row × 257M |
| `mart.fct_article_link` | ~180M                  | ~8 GB + ~6 GB indexes |
| `mart.article_alias`    | ~25M                   | ~2 GB + ~3 GB GIN     |
| `mart.article_search`   | ~14M                   | ~4 GB with GIN        |

**About the design doc's `stg.pagelink` estimate.** It budgets 10 GB for 245M rows — 41 bytes per row. That figure ignores Postgres's 24-byte per-tuple header and the four text columns. If your Step 19 measurement came out near 130 bytes/row in `raw.pagelink`, the honest number for the narrowed `stg_pagelink` is 80–95 bytes/row, or **20–25 GB plus indexes**. Correct the design doc's §6 table with your measured value.

```powershell
docker system df
wsl --system -d docker-desktop df -h /
Get-PSDrive C | Select-Object Used, Free
```

**Your `NOTES.md` says 3,072.9 GB free.** Disk is not your constraint; time is. But check `docker system df` anyway — the WSL2 VHDX only grows, and while you enabled sparse mode in Part 1's Step 3c, freed blocks are returned lazily. If the VHDX is already large, `wsl --manage docker-desktop --set-sparse true` won't shrink it retroactively.

**26b. Retune Postgres for the machine you actually have.** In `docker/warehouse/docker-compose.yml`, with 45.59 GB in the VM:

```yaml
      - -c
      - shared_buffers=10GB               # ~25% of the VM
      - -c
      - effective_cache_size=32GB         # ~70%
      - -c
      - work_mem=64MB                     # per operation, x concurrent ops: leave it
      - -c
      - maintenance_work_mem=4GB          # CREATE INDEX and VACUUM. Biggest single win.
      - -c
      - max_wal_size=16GB                 # 19 shards of COPY is a lot of WAL
      - -c
      - max_parallel_maintenance_workers=6
      - -c
      - max_parallel_workers=16
      - -c
      - max_worker_processes=16
```

```powershell
.\tasks.ps1 db-down
.\tasks.ps1 db-up
docker exec -it wikigraph-warehouse psql -U postgres -d wikigraph -c "SELECT name, setting, unit FROM pg_settings WHERE name IN ('shared_buffers','maintenance_work_mem','max_wal_size','effective_cache_size');"
```

`db-down`, not `db-nuke` — you are keeping the data.

> Leave `work_mem` at 64MB. It is allocated *per sort or hash operation*, and a parallel query with several hash joins can use many multiples of it. Raise it per-model with `SET LOCAL` in a `pre_hook`, as `fct_article_link` already does. Raising it globally is how a warehouse OOMs under concurrency.

**26c. Resize the pools for 24 cores.**

```powershell
.\tasks.ps1 af pools set shard_parse 6 "Parallel shard XML parses and link extraction"
.\tasks.ps1 af pools set warehouse_load 3 "Concurrent COPY streams into the warehouse"
```

Six, not twenty-four. Parse and extraction are single-threaded per task but they compete with Postgres for the same memory and the same disk, and the Airflow scheduler needs headroom or it starts marking healthy tasks as zombies. Start at 6, watch `docker stats`, raise it if the CPU is genuinely idle.

**26d. Delete page Parquet after a successful load.** Link extraction reads from `raw.page` now (Step 19), so nothing needs the page Parquet once the COPY commits. Without this, 19 shards leave 55 GB of scratch behind. In `airflow-docker/dags/wikigraph_ingest.py`, at the end of `load_shard`, after the manifest update:

```python
        # The Parquet has served its purpose. Link extraction reads raw.page,
        # so nothing downstream needs it -- and 19 shards is ~55 GB of scratch.
        # Re-parsing one shard is 1.4 minutes if you ever want it back.
        Path(stats["parquet_path"]).unlink(missing_ok=True)
```

with `from pathlib import Path` inside the task body.

**26e. Decide `stg_pagelink`'s materialization.** Two workable answers, and the choice is yours once you've done the arithmetic in 26a.

**Option A — keep it a table (recommended first).** ~22 GB, builds once per dbt run, and every downstream model reads it with indexes. Add one config change so the build doesn't generate 22 GB of WAL:

```sql
{{ config(materialized='table', schema='stg', unlogged=true, ...) }}
```

`unlogged` skips WAL entirely. The tradeoff is that an unlogged table is **truncated on an unclean shutdown** — which is precisely the right trade for a table dbt rebuilds from `raw.pagelink` on demand. Don't use it on anything you can't regenerate.

**Option B — make it a view.** Change `materialized='table'` to `materialized='view'` and delete the `indexes` config (views can't have them). Saves the 22 GB entirely. The cost: `norm_title()` is recomputed over 257M rows on every downstream model that reads it — three of them — instead of once. That's roughly 10–15 minutes per full build against 22 GB of disk and a faster build.

**What about the design doc's HASH partitioning?** It's the right answer and dbt is the wrong tool for it. dbt's `table` materialization emits `create table ... as select`, which cannot be partitioned. Getting there means either a custom materialization, or creating the partitioned parent in a migration and switching the model to `incremental` with `delete+insert`. Both are real work for a benefit you cannot yet measure. **Do the backfill unpartitioned, measure your query times, and partition only if they're bad.** Leave the note in the model file — it's already there from Step 21 — so the choice stays visible.

**26f. Run the ingest.** All 19; the 18 that aren't loaded will process, and the one that is will reprocess identically (Rule 2).

```powershell
.\tasks.ps1 af dags trigger wikigraph_ingest --conf '{\"dump_date\": \"2026-07-01\"}'
```

Empty `shards` means all. Expect roughly `19 × 1.4 min ÷ 6` for parsing — under ten minutes of wall clock — plus load time, which is the slower half.

**26g. Run the links.** Only after ingest is green, so the manifest is complete.

```powershell
.\tasks.ps1 af dags trigger wikigraph_links
```

No config needed: `discover_loaded` picks up every shard with `status = 'loaded'` and no links at the current rule version. **This is the long one.** Use your Step 18 throughput measurement:

```
12 GB of wikitext per shard ÷ your MB/s = ____ min/shard
× 19 shards ÷ pool of 6                 = ____ hours
```

**26h. Watch it.** In one PowerShell window:

```powershell
while ($true) {
  docker exec wikigraph-warehouse psql -U etl -d wikigraph -t -c @"
SELECT to_char(now(),'HH24:MI') || '  loaded=' ||
       count(*) FILTER (WHERE status='loaded') || '/19  links=' ||
       count(*) FILTER (WHERE links_extracted IS NOT NULL) || '/19  rows=' ||
       coalesce(sum(links_extracted),0)
FROM raw.ingest_manifest;
"@
  Start-Sleep -Seconds 120
}
```

And in another, keep an eye on the disk:

```powershell
while ($true) { docker system df --format '{{.Type}} {{.Size}}'; Start-Sleep 300 }
```

**26i. Rebuild the marts, once, at the end.**

```powershell
.\tasks.ps1 dbt build --full-refresh
python scripts\first_link_walk.py
.\tasks.ps1 dbt build --select tag:postwalk
```

**Rebuild everything, not incrementally.** `fct_article_link` resolves link targets against `dim_article`, and `dim_article` only became complete when the last shard landed. Every edge computed against a partial article table needs recomputing. The aggregation itself is shard-local — `src_article_id` determines the shard — but the *resolution* is global, so a full refresh is not optional.

Set `--threads 8` in `profiles.yml` for this run if you like, but remember `work_mem` is per-operation per-thread.

### Verify

**1. The manifest is complete and consistent.**

```sql
SELECT count(*) AS shards,
       count(*) FILTER (WHERE status = 'loaded')            AS loaded,
       count(*) FILTER (WHERE links_extracted IS NOT NULL)  AS with_links,
       sum(pages_seen)     AS pages_seen,
       sum(pages_loaded)   AS ns0_pages,
       sum(links_extracted) AS links,
       round(sum(links_extracted)::numeric / sum(articles_scanned), 1) AS links_per_article
FROM raw.ingest_manifest;
```

19, 19, 19. Anything else, find the gap:

```sql
SELECT shard_name, status, pages_loaded, links_extracted, error_detail
FROM raw.ingest_manifest
WHERE status <> 'loaded' OR links_extracted IS NULL ORDER BY shard_name;
```

**2. Every partition exists and none is empty.**

```sql
SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(oid))
FROM pg_class WHERE relname LIKE 'pagelink_p%' ORDER BY relname;
```

19 rows. A zero-row partition is a shard that silently produced nothing.

**3. Reconciliation across every layer.**

```sql
WITH layers AS (
  SELECT 'manifest.pages_loaded' AS layer, sum(pages_loaded)::bigint AS n
    FROM raw.ingest_manifest WHERE status = 'loaded'
  UNION ALL SELECT 'raw.page',              count(*) FROM raw.page
  UNION ALL SELECT 'stg.stg_page',          count(*) FROM stg.stg_page
  UNION ALL SELECT 'mart.dim_article',      count(*) FROM mart.dim_article
  UNION ALL SELECT 'manifest.links',        sum(links_extracted)::bigint
    FROM raw.ingest_manifest
  UNION ALL SELECT 'raw.pagelink',          count(*) FROM raw.pagelink
  UNION ALL SELECT 'stg.stg_pagelink',      count(*) FROM stg.stg_pagelink
)
SELECT * FROM layers;
```

`manifest.pages_loaded` = `raw.page` = `stg.stg_page`, exactly. `manifest.links` = `raw.pagelink` = `stg.stg_pagelink`, exactly. `mart.dim_article` = `stg.stg_page` minus redirects.

**4. `page_id` is globally unique across shards.** The design doc asserts shards are disjoint page-ID ranges. Now you can check rather than trust:

```sql
SELECT count(*) AS duplicate_page_ids FROM (
  SELECT page_id FROM raw.page GROUP BY 1 HAVING count(*) > 1
) x;
```

Zero. If not, two shards overlap and every downstream count is inflated. (The `unique` test on `stg_page.page_id` catches this too — this query tells you *which*.)

### If it breaks

- **One `parse_shard` or `extract_shard` instance fails** — select just that mapped instance in the UI and **Clear**. It re-runs alone. Do not clear the whole DAG run; you'd redo 18 successful shards.
- **`No space left on device` mid-run** — 26d didn't take effect, or the DAG didn't reload. `docker exec` into a worker and `ls -la /opt/airflow/staging/2026-07-01/`. Delete stale Parquet by hand and restart the failed shard.
- **Everything slows down after a few shards** — check `pg_stat_activity` for `wait_event_type = 'IO'` and `checkpoint` entries in the Postgres log. Raise `max_wal_size` further; checkpoints during sustained COPY are the usual culprit.
- **Zombie tasks / "task exited with return code -9"** — the OOM killer. `-9` is `SIGKILL`, which no Python traceback survives. Lower `shard_parse`, or lower `shared_buffers`: they're competing for the same VM memory.
- **`dbt build --full-refresh` runs for hours** — check that `ANALYZE` ran on every new partition (the loader does it, but a partition created outside the loader wouldn't have it): `SELECT relname, last_analyze FROM pg_stat_user_tables WHERE schemaname='raw';` Any NULL there will produce a catastrophic plan.
- **`fct_article_link` fails with `out of memory` or spills hard** — raise the `pre_hook` to `set local work_mem = '1GB'` for that model only.
- **A shard's link count is wildly out of line with the others** — that's Gate 2 in the links DAG doing its job if it failed, or worth investigating if it passed. Shards are page-ID ranges, so later shards hold newer, shorter articles and genuinely have fewer links each. A 2× spread across shards is expected; 20× is a bug.

Update `NOTES.md`:

```markdown
## Backfill (Step 26)
- Started ____ , finished ____
- Ingest wall time: ____ (pool=6) | link extraction: ____ (pool=6)
- Postgres retuned: shared_buffers ____ , maintenance_work_mem ____
- stg_pagelink materialization chosen: table (unlogged) / view — because ____
- Total: pages_seen ____ | ns0 ____ | articles ____ | redirects ____ (__%)
- raw.page ____ GB | raw.pagelink ____ GB (____ rows, ____ bytes/row)
- stg_pagelink ____ GB | fct_article_link ____ GB | alias+search ____ GB
- Warehouse total: ____ GB   (design doc budgeted 120 GB)
- Full dbt --full-refresh: ____ min
- Failures needing a manual clear: ____
```

Commit:

```powershell
git add docker/warehouse/docker-compose.yml airflow-docker/dags/wikigraph_ingest.py dbt/ NOTES.md
git commit -m "Backfill: retuned warehouse, staging cleanup, all 19 shards"
```

---

## Step 27 — Full-scale verification, and the real answers

### Why

Everything you built on one shard now has a complete encyclopedia underneath it. This step is partly verification and mostly **the payoff**: the questions the whole project was for, which were unanswerable an hour ago.

### Do

**27a. Tighten the tests you deliberately loosened.** Two of them were sized for one shard and are now too permissive to be useful.

In `dbt/wikigraph/tests/assert_link_resolution_floor.sql`, change `severity = 'warn'` to `severity = 'error'`, and in `dbt_project.yml`:

```yaml
vars:
  min_link_resolution: 0.85
  redirect_max_hops: 3
  first_link_rule_version: 1
```

A test that can't fail isn't a test. Now that cross-shard misses are gone, a resolution rate below ~85% means something genuinely broke.

**27b. Re-run the walk and the whole suite.**

```powershell
.\tasks.ps1 dbt build
python scripts\first_link_walk.py
.\tasks.ps1 dbt build --select tag:postwalk
.\tasks.ps1 test
```

**27c. The comparison table.** This is the artifact worth keeping — every number from your single-shard `NOTES.md`, beside its full-scale twin.

| Metric                     | Shard 0 | All 19 | Where it came from              |
| -------------------------- | ------- | ------ | ------------------------------- |
| ns=0 pages                 | 717,051 | ____   | Step 26 verify                  |
| Redirect share             | 46.0%   | ____%  | design doc guessed 26.9% vs 61% |
| Articles                   | 387,504 | ____   | `dim_article`                   |
| Dangling redirects         | 78,272  | ____   | Step 21 verify                  |
| Link occurrences           | ____    | ____   | `stg_pagelink`                  |
| Links per article          | ____    | ____   | design doc assumed 35 ±40%      |
| Resolved edges             | ____    | ____   | `fct_article_link`              |
| **Link resolution rate**   | ____%   | ____%  | **the headline number**         |
| Articles with a first link | ____%   | ____%  | `fct_first_link`                |
| Philosophy basin           | 0       | ____   | `fct_first_link_path`           |

**27d. Answer the questions.**

*Is Philosophy still the attractor in a 2026 dump?*

```sql
SELECT terminal_type, count(*) AS articles,
       round(100.0*count(*)/sum(count(*)) OVER (), 2) AS pct,
       round(avg(steps_to_terminal), 1) AS avg_steps
FROM mart.fct_first_link_path GROUP BY 1 ORDER BY 2 DESC;

SELECT cycle_id, cycle_length, basin_size, member_titles
FROM mart.fct_first_link_cycle ORDER BY basin_size DESC LIMIT 15;
```

*How long is the walk?*

```sql
SELECT steps_to_terminal, count(*) FROM mart.fct_first_link_path
WHERE terminal_type = 'philosophy' GROUP BY 1 ORDER BY 1;
```

*Which articles are the highways — the ones most paths pass through?* This is the one that needs the in-memory graph rather than SQL; every path visits many nodes and counting that in Postgres is the recursive-CTE trap again:

```python
# scripts/first_link_highways.py
import collections, os, numpy as np, psycopg
from wikigraph.walk import build_next

with psycopg.connect(os.environ["WH_DSN"]) as c:
    ids = np.array([r[0] for r in c.execute(
        "SELECT article_id FROM mart.dim_article ORDER BY article_id")], dtype=np.int64)
    e = c.execute("SELECT src_article_id, dst_article_id FROM mart.fct_first_link "
                  "WHERE dst_article_id IS NOT NULL").fetchall()
    titles = dict(c.execute("SELECT article_id, title FROM mart.dim_article"))

nxt = build_next(ids,
                 np.fromiter((x[0] for x in e), np.int64, len(e)),
                 np.fromiter((x[1] for x in e), np.int64, len(e)))

# Every node's path contributes to its successors' counts. Walking all n paths
# is O(sum of path lengths) -- a few hundred million steps, about a minute.
hits = np.zeros(len(ids), dtype=np.int64)
for start in range(len(ids)):
    v, seen = nxt[start], 0
    while v != -1 and seen < 200:
        hits[v] += 1
        v = nxt[v]
        seen += 1

for i in np.argsort(-hits)[:30]:
    print(f"{hits[i]:>9,}  {titles[int(ids[i])]}")
```

*What else the data supports.* The design doc's §5 is now entirely unblocked — PageRank over `fct_article_link`, orphans and dead ends from `article_degree`, bot-vs-human authorship from `contributor_name`, staleness from `revision_ts` against in-degree.

**27e. Update the design doc.** Its §6 sizing table still contains estimates. Replace every one with a measured number and say which shard or which full run it came from. Specifically resolve:

- **Link density.** The ±40% uncertainty on 35 links/article: measured.
- **Redirect share.** 26.9% vs 61%: measured at 46% on shard 0; now measured across all 19.
- **`stg.pagelink` at 10 GB.** Almost certainly wrong; see Step 26a.
- **The seven-flag correction.** `in_file_caption` is not in the design doc's list of six.
- **`fct_first_link_path.path`.** Documented as omitted, with the reason.
- **`terminal_type = 'max_depth'`.** Cannot occur in a functional graph; remove it.

**27f. Regenerate the lineage docs.**

```powershell
.\tasks.ps1 dbt docs generate
Start-Process .\dbt\wikigraph\target\index.html
```

### Definition of done

- [ ] All 19 shards `loaded` with `links_extracted` set, at one `rule_version`
- [ ] Row counts reconcile exactly: manifest = raw = stg, at both the page and link grain
- [ ] `page_id` is unique across every shard
- [ ] `dbt build` green with `min_link_resolution` raised to 0.85 and `severity: error`
- [ ] `pytest` green, including the gold set, the parity test, and the walk reference
- [ ] `scripts\search.py "jfk"` returns *John F. Kennedy* from the exact tier
- [ ] A category-induced subgraph plot you'd put in a portfolio
- [ ] `fct_first_link_path` has a dominant attractor and you know what it is
- [ ] `NOTES.md` has the shard-0-vs-full comparison table
- [ ] `wikigraph_design.md` §6 contains measured numbers, not estimates
- [ ] Everything committed; `raw_data/`, `staging/`, `exports/*.csv`, `.env` are not

---

## What comes next

Ordered by value ÷ effort, as the design doc's §5 has them. Each is now a small amount of work on top of what exists.

1. **PageRank.** `fct_article_link` is the whole input. 180M edges is comfortable for `scipy.sparse` in memory — the same dense-index trick as `build_next`, then twenty power iterations. Backfill the `pagerank` column on `dim_article` and `article_search`, and your search ranking gets meaningfully better for free.
2. **`stg.template_use`, `stg.external_link`, `stg.infobox_field`.** The extractor already walks every `{{...}}` and knows the template names; it just discards them. Emitting them is a second output list from the same scan, so it costs one more Parquet file and one more partitioned table each. `external_link` answers "what does Wikipedia actually cite," which is a genuinely publishable question.
3. **Redirect pathologies.** You have `int_redirect_resolved` and its cycle guard. The rows the guard rejected are loops, and the ones whose `final_norm` matches no article are broken redirects. Both are real defects you could report upstream — an actual contribution back to Wikipedia rather than a portfolio piece.
4. **`rule_version` 2.** Run your gold-set review again on a random 20 from the full corpus. Change what's wrong, bump the version, and compare the two terminal distributions. This is the payoff of Rule 4 and it takes an evening.
5. **A second dump.** Every table carries `dump_date`. Ingesting 2026-08-01 gives you growth, link churn, deletion tracking, and staleness — for free, because you designed for it in Part 1.

Two things to fix before any of that, if the numbers in Step 26 said so:

- **Partition `stg.pagelink`** if your query times are bad. dbt won't do it; create the partitioned parent in a migration and switch the model to `incremental` with `delete+insert` on `shard_name`.
- **Reconsider keeping `wikitext`.** It's the dominant storage cost. The design doc says keep it and it's right — every idea above needs re-parsing — but if disk becomes tight, dropping it for articles below some length percentile is the cheapest large win available.

---

## Pitfalls, ranked by how much time they'll cost you

Extends Part 1's table; these are the ones specific to this half.

| Pitfall                                        | Symptom                                                                    | Fix                                             |
| ---------------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------- |
| Normalizing titles in Python as well as SQL    | 3% of edges silently vanish; nothing errors                                | Rule 3: emit `target_raw`, let SQL normalize    |
| `{{tmpl\|}}` read as a table close             | Everything after it in the article is `in_template`; `pct_clean` collapses | Anchor `{\|` and `\|}` to line start            |
| Paren inside a link target                     | Every link after it on the line reads `in_parens`                          | `skip_until` past the target before scanning    |
| Unbounded `find()` inside the link branch      | Fine on articles, quadratic on 300 KB list pages                           | Bound every `find()` by the closing bracket     |
| Indexes created by hand on a dbt model         | 3 ms query becomes 40 s after the next build                               | Declare in `config(indexes=...)` or `post_hook` |
| `unaccent()` in an index expression            | `functions in index expression must be marked IMMUTABLE`                   | The `f_unaccent` wrapper in V005                |
| `similarity(a,b) > 0.3` in a WHERE clause      | Seq scan over every alias                                                  | Use the `%` operator; it's the index condition  |
| Recursive CTE for first-link chains            | 7M recursive queries                                                       | One int array in memory, O(n) three-colour walk |
| Recursion in the graph walk                    | `RecursionError` on the deep chains, which are the interesting ones        | Iterative with an explicit path list            |
| Hash-partitioning the landing table            | A shard reload can't truncate one partition                                | Landing tables partition by what you reload     |
| `fetchall()` on wikitext                       | Instant OOM: 387k rows × 31 KB                                             | Server-side cursor with a small `itersize`      |
| Incremental mart rebuild after backfill        | Edges resolved against a partial `dim_article`                             | `--full-refresh` once, at the end               |
| `work_mem` raised globally                     | OOM under dbt's thread concurrency                                         | `SET LOCAL` in a model `pre_hook`               |
| Clearing the whole DAG run to retry one shard  | 18 successful shards reprocessed                                           | Clear the single mapped task instance           |
| First-link rule changed without a version bump | Two incomparable path distributions, no way to tell which is better        | Rule 4: bump `RULE_VERSION`                     |

---

## Appendix: command reference additions

```powershell
# ---- dbt (new) ----
.\tasks.ps1 dbt deps                       # install packages.yml (dbt_utils)
.\tasks.ps1 dbt seed                       # load seeds/ns_alias.csv
.\tasks.ps1 dbt build --full-refresh       # rebuild every model from scratch
.\tasks.ps1 dbt build --select +fct_first_link   # a model and its ancestors
.\tasks.ps1 dbt build --select tag:postwalk      # the models that need the walk
.\tasks.ps1 dbt ls --select tag:postwalk         # confirm tag selection works

# ---- airflow (new DAGs) ----
.\tasks.ps1 af dags trigger wikigraph_links --conf '{\"shards\": [\"p10p1400054\"], \"force\": true}'
.\tasks.ps1 af dags trigger wikigraph_analytics
.\tasks.ps1 af pools set shard_parse 6 "Parallel parses and link extraction"

# ---- analysis (needs $env:WH_DSN and the venv) ----
python scripts\sample_articles.py --n 300           # build the test corpus
python scripts\review_first_links.py --n 20         # manual review output
python scripts\ego.py --seed "Dog" --hops 2 --plot --html
python scripts\ego.py --category "American jazz musicians" --max-nodes 600 --plot
python scripts\search.py "jfk"
python scripts\first_link_walk.py

# ---- tests ----
.\tasks.ps1 test                            # everything except slow + integration
python -m pytest -m slow -s -q              # throughput benchmarks, with output
python -m pytest -m integration -q          # needs WH_DSN and a built warehouse
python -m pytest tests/test_links.py -q     # the fast loop you live in
```

**The `$env:WH_DSN` line, since you'll need it every session:**

```powershell
Get-Content .env | Where-Object { $_ -match '^\w+=' } | ForEach-Object {
    $k,$v = $_.Split('=',2); Set-Item -Path "env:$($k.Trim())" -Value $v.Trim()
}
$env:WH_DSN = "postgresql://$($env:PG_ETL_USER):$($env:PG_ETL_PASSWORD)@localhost:$($env:PG_HOST_PORT)/$($env:PG_DB)"
```

---

## Notes on what was and wasn't verified before writing this

In the spirit of Part 1's note about the parser fixture:

- **The extractor** in Step 17 was run against every case in Step 18a's table plus a realistic article lead before being written down. All 30 behave as asserted, and the realistic lead yields the correct first link. The throughput figures (5 MB/s on very link-dense markup, 23 MB/s on prose-density text) are measured on synthetic articles, not real ones — treat them as a range to check yourself against, not a prediction.
- **The walk** in Step 25 was checked against the brute-force reference in `tests/test_walk.py` on 300 random functional graphs, plus the self-loop and 2-cycle-with-tail cases, and timed on a synthetic 7M-node graph: **11.4 seconds** for the walk itself.
- **The SQL was not executed.** No Postgres was available while writing this, so every model, migration and index in Steps 21–27 is written from the documented semantics rather than from a run. The dbt config forms (`indexes`, `unlogged`, `post_hook`, `pre_hook`, singular tests, seeds, `RenderConfig`) are all real features, but check them against your installed versions rather than trusting the syntax here. Where something is version-sensitive, the **If it breaks** section says so.
- **The sizing arithmetic** uses your measured shard-0 numbers from `NOTES.md` where they exist and clearly-marked estimates where they don't. Everything with a blank in it is a blank on purpose.
