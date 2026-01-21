{{ config(
    schema=resolve_schema_for('staging')
) }}

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['guid']) }}    as uid,
    title       as title,
    author      as username,
    guid        as url,
    link        as redirect_url,
    published   as published_at
from
    {{ source('hackernews_raw', 'newest_items') }} gt    
