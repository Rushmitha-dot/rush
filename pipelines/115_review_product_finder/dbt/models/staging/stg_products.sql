-- Domain from title keywords. Hair words win over skin words, so "hair oil" is hair.
with matches as (
    select
        p.parent_asin,
        k.domain,
        row_number() over (partition by p.parent_asin order by k.priority, k.keyword) as rn
    from {{ source('raw', 'products') }} p
    join {{ ref('product_domain_keywords') }} k
      on lower(p.title) like '%' || k.keyword || '%'
)
select
    p.parent_asin,
    p.title,
    p.store,
    p.average_rating,
    p.rating_number,
    p.price,
    coalesce(m.domain, 'other') as domain
from {{ source('raw', 'products') }} p
left join matches m on m.parent_asin = p.parent_asin and m.rn = 1
