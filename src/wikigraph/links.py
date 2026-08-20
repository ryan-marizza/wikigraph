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

# Bump when you change what the module considers a link or how it flags one.
# Stored alongside every extraction so you can tell two runs apart.
RULE_VERSION = 1

# Templates whose contents count as "infobox" for first link purposes. Navboxes
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
    """Replace a region with whitespace, preserving newlines and total length."""
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
    """Return one dict per wikilink occurance, in document order."""
    if not wikitext:
        return []

    text = _BLANK_RE.sub(_blank, wikitext)
    links: list[dict] = []

    tmpl_stack: list[str] = []
    frames: list[dict] = []
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
                break
            pipe = text.find("|", pos +2)
            cut = pipe if 0 <= pipe < close else close
            # Bounded on purpose: an unbounded find("\n\n") scans to the end of
            # the document on every link, which is genuinely quadratic on a
            # 300 KB list article with no blank lines.
            gap = text.find("\n\n", pos + 2, close)
            if 0 <= gap < cut:
                continue
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
            if text[gt - 1] != "/":
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
            elif n in (3, 4):
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