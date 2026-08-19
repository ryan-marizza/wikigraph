{{ config(materialized='table', schema='mart')}}

-- Canonical articles only. Redirects are NOT nodes in the graph — an edge
-- pointing at a redirect gets resolved to its target during link processing,
-- so a redirect never appears as a vertex.
--
-- Degree and pagerank columns are declared but NULL until link extraction
-- exists. Declaring them now keeps the contract stable for consumers.

select 
    page_id as article_id,
    title,
    norm_title,
    text_bytes,
    revision_ts,

    -- Cheap heuristic classifications. Refine later; they're useful immediately
    -- for excluding noise from graph stats.

    title ilike '%(disambiguation)' as is_disambig,
    title ilike 'List of %' as is_list_page,
    coalesce(text_bytes, 0) < 1500 as is_stub,

    cast(null as integer) as out_degree,
    cast(null as integer) as in_degree,
    cast(null as double precision) as pagerank,

    dump_date
from {{ ref('stg_page')}}
where not is_redirect