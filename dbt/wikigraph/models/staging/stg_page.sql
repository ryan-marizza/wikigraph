{{ config(materialized='table', schema='stg')}}

-- One row per ns=0 page, INCLUDING redirects.
-- Redirects stay here because they are the alias dictionary (design doc §4.2) —
-- 27% of pages, built by Wikipedia editors over two decades. They are excluded
-- from the graph as NODES later, in dim_article, not here.

select 
    page_id,
    title,
    public.norm_title(title) as norm_title, --the join key
    is_redirect,
    -- Redirect targets can carry a section anchor: 'Dog#Behavior'. Strip it —
    -- the anchor is a position within the target page, not a different page.
    case
        when is_redirect
        then public.norm_title(split_part(redirect_target, '#', 1))
    end as redirect_to_norm,
    
    revision_id,
    revision_ts,
    text_bytes,
    dump_date,
    shard_name

from {{ source('raw', 'page')}}
where namespace = 0