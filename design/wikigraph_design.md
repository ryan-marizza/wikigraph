# WikiGraph — Pipeline & Data Model Design

**Source:** 19 MediaWiki 0.11 export shards, 228.3 GB decompressed, `enwiki-2026-07-01`
**Scope:** namespace 0 only (articles + redirects)
**Warehouse:** PostgreSQL
**Orchestration:** Airflow (Docker)

---

## 1. What the EDA already tells us

Five facts from `wikidata_eda.html` that drive every decision below.

| Observation | Consequence for the design |
|---|---|
| **26.9% of pages are redirects** | Redirect resolution is not a nice-to-have. Wikilinks point at redirect titles constantly, so an unresolved edge list will have a large fraction of edges terminating at stubs. Resolution must happen in the transform layer, once, not at query time. |
| **Median article is 12K chars, max 338K, mean 31K** | Heavy right skew. Any per-row parse that isn't streaming will blow up on the tail. Also means wikitext storage is the dominant cost (~62 GB TOAST-compressed for 7M articles). |
| **Shards are 10–13 GB each, 19 of them** | Natural unit of parallelism. Airflow dynamic task mapping over shards, not over pages. Shards are page-ID ranges (`p10p1400054`), so they're disjoint — no dedup needed across shards. |
| **`ns` values 0–5 present in sample, 88.5% ns=0** | Filter at parse time, not load time. Never write ns≠0 to disk. |
| **One `<revision>` per page; timestamps span 2002–2026** | These are current-revision dumps, not full history. The data model should *not* include a revision-history grain — but leave `revision_id`/`timestamp` on the page row so you can diff future dumps. |

**One thing the EDA flags but doesn't resolve:** whether any shard carries multiple `<revision>` blocks. The parser only keeps the first. Worth a cheap assertion task in the DAG (count `<revision>` per `<page>` on a sample) before you trust the grain.

---

## 2. Pipeline architecture

### Layering

```
raw_data/*.xml  ──parse──▶  landing Parquet  ──COPY──▶  raw.*  ──▶  stg.*  ──▶  mart.*
   (228 GB)                  (on volume)                (typed)    (clean)   (analytics)
```

**Do not** parse XML into pandas and `to_sql()`. At this volume that's the difference between hours and days. The pattern that works:

1. **Parse task** (one per shard, `.expand()`): `iterparse` streams `<page>`, filters `ns==0`, extracts fields + wikitext, writes Parquet in row groups of ~50K to a mounted volume. Memory stays flat. Your existing `iter_pages` is already 95% of this — it just needs the `ns` filter and a writer.
2. **Load task**: `COPY ... FROM STDIN (FORMAT csv)` or `pg_bulkload` into `raw.page`. Parquet → COPY via `psycopg.copy()` streaming.
3. **Link extraction task**: reads wikitext, regex/mwparserfromhell over `[[...]]`, emits edge rows with ordinal + context flags. This is the expensive step — it's CPU-bound and independent per page, so run it shard-parallel too.
4. **Transform tasks**: pure SQL from `raw` → `stg` → `mart`. This is where dbt earns its keep if you want it; if not, `SQLExecuteQueryOperator` with versioned `.sql` files is fine.

### Idempotency

Every task keys on `(dump_date, shard_name)`. Reruns delete-then-insert that partition rather than appending. `raw.page` is partitioned by `shard_name` (LIST partitioning) so a failed shard is a `TRUNCATE` of one partition, not a full reload.

### DAG shape

```
                  ┌─ parse_shard[0..18] ─┬─ load_raw[0..18] ─┐
discover_shards ──┤                      │                   ├─ build_stg ─┬─ build_marts ─ compute_metrics ─ validate
                  └─ assert_grain (1 shard sample) ──────────┘             └─ build_search_index
```

Two DAGs, actually: an `ingest` DAG that runs when a new dump lands (monthly-ish), and a `transform` DAG triggered on its completion. Keeps the 6+ hour parse from blocking iteration on the SQL layer.

---

## 3. Data model

Three schemas. `raw` is append-only and mirrors the XML. `stg` is normalized and typed. `mart` is denormalized for the three end goals.

### 3.1 Raw layer

```sql
CREATE SCHEMA raw;

-- Landing table. Mirrors <page> 1:1 for ns=0. Partitioned by shard for idempotent reloads.
CREATE TABLE raw.page (
    page_id           integer        NOT NULL,
    dump_date         date           NOT NULL,
    shard_name        text           NOT NULL,
    title             text           NOT NULL,   -- raw, as it appears in XML (spaces, not underscores)
    namespace         smallint       NOT NULL,
    is_redirect       boolean        NOT NULL,
    redirect_target   text,                      -- raw title string from <redirect title="...">
    revision_id       bigint,
    revision_ts       timestamptz,
    contributor_name  text,
    contributor_id    bigint,
    text_bytes        integer,
    wikitext          text,                      -- TOASTed; ~62 GB compressed at 7M articles
    ingested_at       timestamptz    NOT NULL DEFAULT now()
) PARTITION BY LIST (shard_name);

-- Manifest: what has been ingested, for idempotency + observability.
CREATE TABLE raw.ingest_manifest (
    shard_name    text        NOT NULL,
    dump_date     date        NOT NULL,
    file_bytes    bigint,
    pages_seen    bigint,     -- all namespaces
    pages_loaded  bigint,     -- ns=0 only
    parse_started timestamptz,
    parse_ended   timestamptz,
    status        text        NOT NULL,  -- pending | parsing | loaded | failed
    error_detail  text,
    PRIMARY KEY (shard_name, dump_date)
);
```

**Note on `wikitext`:** keep it. Every downstream idea in §5 needs re-parsing, and re-reading 228 GB of XML to answer a new question is a bad trade against 62 GB of TOAST. If storage is tight, drop it after `stg` is built and keep only articles above some length percentile.

### 3.2 Staging layer

The hard problem here is **title normalization**. MediaWiki titles have rules that will silently break your joins if you ignore them:

- Underscores and spaces are equivalent (`New_York` = `New York`)
- First letter is case-insensitive, the rest are not (`iPhone` ≠ `IPhone`, but `apple` = `Apple`)
- Leading/trailing whitespace stripped, internal whitespace collapsed
- Links carry anchors (`[[Dog#Behavior]]`), display text (`[[Dog|dogs]]`), and namespace prefixes
- HTML entities appear in titles (`&amp;`)

Every title gets a `norm_title` computed by one shared function. Join on `norm_title`, never on the display title.

```sql
CREATE SCHEMA stg;

-- One row per ns=0 page, including redirects.
CREATE TABLE stg.page (
    page_id          integer PRIMARY KEY,
    title            text    NOT NULL,          -- display form
    norm_title       text    NOT NULL,          -- canonical join key
    is_redirect      boolean NOT NULL,
    redirect_to_norm text,                      -- normalized target title
    revision_id      bigint,
    revision_ts      timestamptz,
    text_bytes       integer,
    dump_date        date    NOT NULL
);
CREATE UNIQUE INDEX ux_stg_page_norm ON stg.page (norm_title);
CREATE INDEX ix_stg_page_redirect ON stg.page (redirect_to_norm) WHERE is_redirect;

-- Every wikilink occurrence, unresolved, in document order.
-- Grain: one row per link occurrence (NOT per distinct target — duplicates are meaningful for ordinal).
CREATE TABLE stg.pagelink (
    src_page_id     integer  NOT NULL,
    ordinal         integer  NOT NULL,   -- 1-based position in the rendered article
    target_raw      text     NOT NULL,   -- exactly as written inside [[...]]
    target_norm     text     NOT NULL,
    target_ns       smallint NOT NULL,   -- inferred from prefix; 0, 14 (Category), 6 (File), ...
    anchor          text,                -- the #section part
    display_text    text,                -- the |piped part
    section_name    text,                -- nearest preceding == heading ==
    -- context flags: everything the first-link rules need
    in_parens       boolean  NOT NULL,
    in_italics      boolean  NOT NULL,
    in_template     boolean  NOT NULL,   -- inside {{...}}
    in_table        boolean  NOT NULL,   -- inside {|...|}
    in_ref          boolean  NOT NULL,   -- inside <ref>
    in_infobox      boolean  NOT NULL,
    char_offset     integer  NOT NULL,
    PRIMARY KEY (src_page_id, ordinal)
) PARTITION BY HASH (src_page_id);       -- ~245M rows total

-- Partitioned parents hold no data; the children must be created explicitly.
DO $$ BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format(
      'CREATE TABLE stg.pagelink_p%s PARTITION OF stg.pagelink
         FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
  END LOOP;
END $$;
```

Two Postgres constraints worth internalizing, because they shape the schema:
the partition key **must** be part of the primary key (hence `src_page_id` leading
both PKs above), and a `UNIQUE` constraint on a partitioned table can only be
enforced if it includes the partition key — which is why `stg.pagelink` is keyed
on `(src_page_id, ordinal)` rather than on a surrogate `link_id`.

Those six boolean flags are the single most important thing in this schema. Extracting them costs you nothing extra at parse time (you're already walking the wikitext), and without them **goal #3 is not implementable** — see §4.3.

```sql
-- Category membership, extracted from [[Category:...]] in wikitext.
CREATE TABLE stg.category_link (
    src_page_id   integer NOT NULL,
    category_norm text    NOT NULL,
    sort_key      text,
    PRIMARY KEY (src_page_id, category_norm)
);

-- Template transclusions, {{...}}. Cheap to extract, unlocks several ideas in §5.
CREATE TABLE stg.template_use (
    src_page_id     integer NOT NULL,
    template_norm   text    NOT NULL,
    occurrence      smallint NOT NULL,
    PRIMARY KEY (src_page_id, template_norm, occurrence)
);

-- Infobox key/value pairs. Optional but high-leverage (see §5.2).
CREATE TABLE stg.infobox_field (
    src_page_id   integer NOT NULL,
    infobox_type  text    NOT NULL,   -- 'Infobox person', 'Infobox settlement', ...
    field_name    text    NOT NULL,
    field_value   text,
    PRIMARY KEY (src_page_id, infobox_type, field_name)
);

-- External links, for the citation/source analysis idea.
CREATE TABLE stg.external_link (
    src_page_id integer NOT NULL,
    url         text    NOT NULL,
    domain      text    NOT NULL,
    in_ref      boolean NOT NULL,
    occurrence  smallint NOT NULL,
    PRIMARY KEY (src_page_id, url, occurrence)
);
```

### 3.3 Mart layer

```sql
CREATE SCHEMA mart;

-- Canonical articles only. Redirects are NOT nodes in the graph.
CREATE TABLE mart.dim_article (
    article_id     integer PRIMARY KEY,   -- = page_id of the canonical (non-redirect) page
    title          text    NOT NULL,
    norm_title     text    NOT NULL UNIQUE,
    text_bytes     integer,
    revision_ts    timestamptz,
    is_disambig    boolean NOT NULL DEFAULT false,
    is_list_page   boolean NOT NULL DEFAULT false,
    is_stub        boolean NOT NULL DEFAULT false,
    out_degree     integer,
    in_degree      integer,
    pagerank       double precision,
    dump_date      date    NOT NULL
);

-- THE key table for fuzzy search. Every string a human might type -> a canonical article.
-- Sources: canonical titles, redirect titles, piped display texts, bold intro aliases.
CREATE TABLE mart.article_alias (
    alias_norm  text    NOT NULL,
    article_id  integer NOT NULL REFERENCES mart.dim_article,
    alias_text  text    NOT NULL,
    alias_type  text    NOT NULL,   -- 'canonical' | 'redirect' | 'anchor_text' | 'bold_intro'
    weight      real    NOT NULL DEFAULT 1.0,  -- anchor_text weighted by frequency
    PRIMARY KEY (alias_norm, article_id, alias_type)
);

-- Resolved article-to-article edges. Redirects collapsed, self-loops and red links dropped.
-- Grain: one row per (src, dst) pair — deduplicated. Use stg.pagelink for occurrence-level detail.
CREATE TABLE mart.fct_article_link (
    src_article_id  integer  NOT NULL,
    dst_article_id  integer  NOT NULL,
    link_count      smallint NOT NULL,   -- how many times src links to dst
    min_ordinal     integer  NOT NULL,   -- earliest position; useful as a relevance proxy
    via_redirect    boolean  NOT NULL,   -- did any occurrence route through a redirect
    PRIMARY KEY (src_article_id, dst_article_id)
) PARTITION BY HASH (src_article_id);
CREATE INDEX ix_link_dst ON mart.fct_article_link (dst_article_id) ;  -- reverse traversal

-- One row per article: its first link, precomputed under the standard rules.
CREATE TABLE mart.fct_first_link (
    src_article_id   integer PRIMARY KEY REFERENCES mart.dim_article,
    dst_article_id   integer REFERENCES mart.dim_article,  -- NULL = dead end
    ordinal          integer,
    rule_version     smallint NOT NULL   -- bump when you change the exclusion rules
);

-- Precomputed terminal state of each article's first-link walk.
-- Rebuilt as a batch job, not queried recursively at request time.
CREATE TABLE mart.fct_first_link_path (
    src_article_id     integer PRIMARY KEY REFERENCES mart.dim_article,
    steps_to_terminal  smallint,          -- NULL if never terminates
    terminal_type      text NOT NULL,     -- 'philosophy' | 'other_cycle' | 'dead_end' | 'max_depth'
    cycle_id           integer,           -- FK into fct_first_link_cycle
    path               integer[]          -- full node sequence; capped at ~50
);

-- Distinct attractors discovered by the first-link walk.
CREATE TABLE mart.fct_first_link_cycle (
    cycle_id     integer PRIMARY KEY,
    members      integer[] NOT NULL,
    cycle_length smallint  NOT NULL,
    basin_size   integer   NOT NULL   -- how many articles drain into this cycle
);

CREATE TABLE mart.dim_category (
    category_id   integer PRIMARY KEY,
    norm_title    text NOT NULL UNIQUE,
    title         text NOT NULL,
    member_count  integer
);

CREATE TABLE mart.bridge_article_category (
    article_id  integer NOT NULL REFERENCES mart.dim_article,
    category_id integer NOT NULL REFERENCES mart.dim_category,
    PRIMARY KEY (article_id, category_id)
);
CREATE INDEX ix_bac_cat ON mart.bridge_article_category (category_id);
```

### 3.4 Search layer

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE mart.article_search (
    article_id   integer PRIMARY KEY REFERENCES mart.dim_article,
    title        text NOT NULL,
    search_text  text NOT NULL,     -- title + aliases + first paragraph, concatenated
    tsv          tsvector,
    pagerank     double precision   -- denormalized for ranking without a join
);

CREATE INDEX ix_search_tsv  ON mart.article_search USING gin (tsv);
CREATE INDEX ix_search_trgm ON mart.article_search USING gin (title gin_trgm_ops);
CREATE INDEX ix_alias_trgm  ON mart.article_alias  USING gin (alias_text gin_trgm_ops);
```

---

## 4. How each goal lands on this model

### 4.1 Directed graph visualization

You cannot plot 7M nodes and 245M edges. The model supports the two viable approaches:

**Ego networks** — pick a seed, expand *n* hops, cap by degree:

```sql
WITH RECURSIVE ego AS (
    SELECT :seed_id AS article_id, 0 AS depth
  UNION
    SELECT l.dst_article_id, e.depth + 1
    FROM ego e
    JOIN mart.fct_article_link l ON l.src_article_id = e.article_id
    WHERE e.depth < 2
)
SELECT DISTINCT l.src_article_id, l.dst_article_id
FROM mart.fct_article_link l
WHERE l.src_article_id IN (SELECT article_id FROM ego)
  AND l.dst_article_id IN (SELECT article_id FROM ego);
```

**Category-induced subgraphs** — all articles in "Category:Jazz musicians" and the edges among them. This is what `bridge_article_category` is for, and it produces far more interesting plots than random ego nets.

Prune by `pagerank` and `link_count` to keep node counts in the low thousands. Export edge lists to the viz layer (Cytoscape.js / sigma.js / networkx+graphviz) rather than rendering from SQL.

### 4.2 Fuzzy search

Three-tier cascade, all backed by `article_alias`:

1. **Exact** — `alias_norm = normalize(:q)`. Sub-millisecond, catches most real queries.
2. **Full-text** — `tsv @@ websearch_to_tsquery(:q)`, ranked by `ts_rank_cd * log(pagerank)`.
3. **Trigram** — `title % :q ORDER BY similarity(title, :q)`, catches typos and partial names.

```sql
SELECT a.article_id, a.title,
       similarity(a.title, :q) AS trgm_score,
       ts_rank_cd(s.tsv, websearch_to_tsquery('english', :q)) AS fts_score,
       s.pagerank
FROM mart.article_search s
JOIN mart.dim_article a USING (article_id)
WHERE s.tsv @@ websearch_to_tsquery('english', :q)
   OR a.title % :q
ORDER BY (0.4 * similarity(a.title, :q)
        + 0.4 * ts_rank_cd(s.tsv, websearch_to_tsquery('english', :q))
        + 0.2 * COALESCE(s.pagerank, 0) * 1000) DESC
LIMIT 20;
```

**Why aliases matter more than the fuzzy matching itself:** the 27% redirect population *is* the alias dictionary. "JFK" → "John F. Kennedy", "The Big Apple" → "New York City". Wikipedia editors built you a synonym table over decades. Anchor text (`display_text` in `stg.pagelink`) is the second-best source — if 4,000 articles link to *Barack Obama* with the text "44th President", that's a query people will type.

If pg_trgm proves too slow at 7M rows, the escape hatch is embeddings in `pgvector` over the same `search_text` column — the schema doesn't change, you just add a `vector(384)` column.

### 4.3 First-link paths

This is the goal most likely to go wrong, and the failure is in the *definition*, not the query.

The "click the first link" convention (the Getting-to-Philosophy game) means the first link **in the article body**, excluding:

- links inside parentheses
- italicized links
- links in infoboxes, navboxes, tables, image captions, footnotes
- links to non-article namespaces (File:, Category:, Help:)
- red links (targets that don't exist)
- self-links
- pronunciation guides and disambiguation hatnotes

Every one of those is a flag on `stg.pagelink`. `mart.fct_first_link` is then just:

```sql
INSERT INTO mart.fct_first_link (src_article_id, dst_article_id, ordinal, rule_version)
SELECT DISTINCT ON (p.src_page_id)
       p.src_page_id, a.article_id, p.ordinal, 1
FROM stg.pagelink p
JOIN mart.dim_article a ON a.norm_title = p.target_norm
WHERE p.target_ns = 0
  AND NOT (p.in_parens OR p.in_italics OR p.in_template
        OR p.in_table  OR p.in_ref    OR p.in_infobox)
  AND p.src_page_id <> a.article_id
ORDER BY p.src_page_id, p.ordinal;
```

`rule_version` is there because you *will* revise these rules two or three times, and you want to compare the resulting path distributions rather than silently overwrite.

**Walk the chains in Python, not SQL.** A recursive CTE per article is 7M recursive queries. Instead: pull `fct_first_link` into memory as a single int array (7M × 4 bytes = 28 MB — the whole functional graph fits in L3-adjacent memory), then iterate. Each node has out-degree exactly 1, so this is a *functional graph* — every path provably terminates in either a cycle or a dead end, and you can compute all terminals in O(n) with iterative path compression. Runtime: seconds.

Then the interesting questions become one-liners: basin sizes per cycle, the depth distribution, which articles are the biggest "highways" (appearing in the most paths), and whether Philosophy is still the dominant attractor in a 2026 dump.

---

## 5. Other things this data supports

Ordered roughly by (value ÷ effort).

### 5.1 Structural graph analysis — nearly free once §4.1 exists
- **PageRank / HITS** over the full 245M-edge graph. Compare Wikipedia's internal notion of importance against actual pageview data (available as a separate Wikimedia dump) — the articles where these disagree most are the interesting ones.
- **Shortest paths / six-degrees** — the Wikispeedia-style "connect any two articles" game. Bidirectional BFS over the edge list, precomputed diameter and eccentricity.
- **Community detection** (Louvain/Leiden) on the link graph, then compare the discovered communities against the human-assigned category tree. Where they diverge, either the categories are wrong or the topic is genuinely interdisciplinary.
- **Bridge and articulation-point detection** — which articles, if removed, disconnect whole regions of the encyclopedia.

### 5.2 Content and semantics
- **Infobox mining** → a structured entity table. `stg.infobox_field` gives you birth dates, populations, coordinates, chemical formulas across millions of articles. Effectively a free knowledge graph; you can reconstruct a large slice of Wikidata from it and diff the two.
- **Geospatial layer** — coordinates appear in `{{coord}}` templates on ~1.5M articles. Join to the link graph and you get "which places link to which places," mappable directly.
- **Temporal extraction** — dates in text and infoboxes → an event timeline. Cross-reference with the link graph to find events that cluster.
- **Article quality modeling** — predict stub/start/B/GA/FA class from features you already have (length, link density, refs per KB, template usage, section count). The assessment labels live in Talk-namespace templates, so this one needs ns=1 too.

### 5.3 Editorial and meta analysis
- **Bot vs. human authorship.** Your EDA already surfaced this: of the top 15 contributors in a 2,000-page sample, at least 6 are bots (`CitationCleanerBot`, `JJMC89 bot III`, `Xqbot`, `EmausBot`, `Cewbot`, `SchlurcherBot`), and `Tom.Reding` alone touched 10% of the sample. Quantifying what fraction of the encyclopedia's last-touch belongs to automation is a genuinely good finding.
- **Staleness map** — `revision_ts` per article vs. its in-degree. Highly-linked articles that haven't been edited in years are the encyclopedia's weak points.
- **Citation/source concentration** — `stg.external_link` aggregated by domain answers "what does Wikipedia actually cite?" Top domains, dead-link rates, over-reliance on single sources per topic area.
- **Orphans and dead ends** — articles with in-degree 0 (unreachable by browsing) or out-degree 0. Both are maintenance backlogs, and both are easy wins from `dim_article`.

### 5.4 Redirect-specific
- **Redirect chains and loops.** A → B → C is legal; A → B → A is a bug. At 27% redirect share there will be thousands of pathologies, and finding them is a genuine contribution back to Wikipedia.
- **Alias taxonomy** — classify redirects into abbreviation / alternate spelling / former name / misspelling / subtopic. Feeds §4.2 directly, and the "former name" class alone gives you a rename-history dataset.

### 5.5 Interactive builds
- **Wiki Race game** — pick two articles, race the shortest path. Needs only `fct_article_link` plus a BFS endpoint.
- **First-link visualizer** — type an article, watch the chain animate toward its attractor. Straight off `fct_first_link_path`.
- **Dump-over-dump diffing** — once you ingest a second month, `dump_date` on every table gives you growth, link churn, and deletion tracking for free. Worth designing for now even if you only have one dump.

---

## 6. Sizing and sequencing

| Table | Est. rows | Est. size |
|---|---|---|
| `raw.page` (with wikitext) | ~18M (7M articles + 11M redirects) | ~65 GB |
| `stg.pagelink` | ~245M | ~10 GB + ~8 GB indexes |
| `mart.fct_article_link` | ~180M (deduped) | ~7 GB + ~6 GB indexes |
| `mart.dim_article` | ~7M | < 1 GB |
| `mart.article_alias` | ~25M | ~2 GB |
| `mart.article_search` + GIN | ~7M | ~4 GB |

Budget ~120 GB for Postgres.

**Two assumptions in that table you should verify before trusting it:**

- **Link density = 35 ns=0 wikilinks per article.** Swings the edge count by ±40% across a plausible 25–45 range. Cheap to check on shard 0.
- **The 7M / 11M article-to-redirect split.** This is the published enwiki ballpark, but it implies ~61% of ns=0 pages are redirects, whereas your EDA sample showed 26.9%. That sample was the first 2,000 pages of the *lowest page-ID shard* — the oldest pages on the wiki, which are not representative. The honest position is that neither number is established yet. Counting `is_redirect` across one full shard settles it, and it matters: it's the difference between ~65 GB and ~45 GB in `raw.page`.

The `mediawiki_page` table in the official SQL dumps has exact counts if you want to skip the estimation entirely.

**Suggested order of work.** Each step is independently useful, and none blocks on the next.

1. Parse + load one shard end to end. Prove the Parquet→COPY path and confirm the revision grain.
2. Title normalization function + `stg.page` + redirect resolution. Everything downstream depends on this being right.
3. Link extraction with all six context flags. Validate on 20 hand-checked articles.
4. `fct_article_link` → ego-network export → first graph plot. **Goal #1 done.**
5. `article_alias` + search indexes. **Goal #2 done.**
6. `fct_first_link` + the in-memory functional-graph walk. **Goal #3 done.**
7. Backfill the remaining 18 shards.
8. PageRank, categories, and whatever from §5 looks good.

Steps 1–6 on a single shard give you a working end-to-end system in a fraction of the time, and every bug you'd hit at 228 GB you'll hit at 12 GB first.
