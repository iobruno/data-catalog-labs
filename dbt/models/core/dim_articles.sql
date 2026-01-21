{{ config(
    schema=resolve_schema_for('core')
) }}

select h.* 
from {{ ref('stg_hot_articles') }} h

union distinct

select n.* 
from {{ ref('stg_newest_articles') }} n
