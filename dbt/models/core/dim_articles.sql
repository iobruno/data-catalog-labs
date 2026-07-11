{{ config(
    schema=resolve_schema_for('core')
) }}

with combined_articles as (
    select
        h.uid,
        h.title,
        h.username,
        h.url,
        h.redirect_url,
        h.published_at,
        'hot' as source_feed
    from 
        {{ ref('stg_hot_articles') }} h

    union all

    select
        n.uid,
        n.title,
        n.username,
        n.url,
        n.redirect_url,
        n.published_at,
        'newest' as source_feed
    from 
        {{ ref('stg_newest_articles') }} n
)

select * from combined_articles
qualify row_number() over (
    partition by 
        uid 
    order by
        case source_feed
            when 'hot' then 1
            else 2
        end
) = 1
