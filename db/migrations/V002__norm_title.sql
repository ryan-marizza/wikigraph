-- ONE implementation of MediaWiki title normalization, in SQL, used by everything.
--
-- WHY SQL AND NOT PYTHON: the single hardest-to-find bug in this entire project
-- would be two implementations of this rule drifting apart. Python normalizes a
-- link target one way, SQL normalizes the page title another way, the join silently
-- drops 3% of edges, and nothing errors. Every graph statistic is then quietly wrong.
-- One implementation, in the layer where the join happens. The Python parser stays
-- dumb: it emits target_raw exactly as written and lets SQL compute target_norm.
--
-- MediaWiki title rules implemented here:
--   * underscores and spaces are equivalent      New_York = New York
--   * runs of whitespace collapse to one space
--   * leading/trailing whitespace is stripped
--   * the FIRST letter is case-insensitive       apple = Apple
--     but the rest are case-SENSITIVE            iPhone != IPhone
CREATE OR REPLACE FUNCTION public.norm_title(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE PARALLEL SAFE STRICT
AS $$
    SELECT CASE
             WHEN s = '' THEN s
             ELSE upper(left(s, 1)) || substr(s, 2)
           END
    FROM (
        SELECT regexp_replace(btrim(replace(t, '_', ' ')), '\s+', ' ', 'g') AS s
    ) x;
$$;

-- KNOWN GAP: does not unescape HTML entities. '&' should become '&'.
-- This affects a small but nonzero fraction of titles. Measure the count once
-- shard 0 is loaded (query in Step 16) before deciding whether to fix it.
--   TODO(V00x): decide on entity handling based on measured impact.

COMMENT ON FUNCTION public.norm_title(text) IS
  'Canonical MediaWiki title normalization. Join on this, never on the display title.';