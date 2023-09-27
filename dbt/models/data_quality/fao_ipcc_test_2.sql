with categories_union as (

    select
        area_code,
        year,
        value

    from
        {{ ref('stg_fao_ipcc_level_1') }}

    union all

    select
        area_code,
        year,
        value
    
    from
        {{ source('ghg', 'fao_data') }}

    where
        item_code = 6819 -- Other category

),

items_summarized as (

    select
        area_code,
        year,
        sum(value) as level_1_items_sum
    
    from
        categories_union
    
    group by
        area_code,
        year
)

select
    t.area_code,
    t.year,
    t.value as total_value,
    s.level_1_items_sum as summarized_value,
    round(t.value - s.level_1_items_sum, 2) as values_difference,
    if(round(abs(t.value - s.level_1_items_sum), 2) <= 0.02, 'OK', 'NOT OK') as status 

from
    {{ source('ghg', 'fao_data') }} as t

join
    items_summarized as s
 on t.area_code = s.area_code
and t.year = s.year

where
    t.item_code = 6825 -- All sectors with LULUCF
