with other_category as (

    select
        c.ISO_alpha3_Code,
        d.year,
        d.value
    
    from
        {{ source('ghg', 'fao_data') }} d
    
    join
        {{ source('ghg', 'countries') }} c
     on d.area_code = c.M49_Code

    where
        item_code = 6819 -- Other category

),

categories_union as (

    select
        ISO_alpha3_Code,
        year,
        value

    from
        {{ ref('stg_fao_ipcc_level_1') }}

    union all

    select
        *
    
    from
        other_category

),

items_summarized as (

    select
        ISO_alpha3_Code,
        year,
        sum(value) as level_1_items_sum
    
    from
        categories_union
    
    group by
        ISO_alpha3_Code,
        year
)

select
    c.ISO_alpha3_Code as country_code,
    t.year,
    t.value as total_value,
    s.level_1_items_sum as summarized_value,
    round(t.value - s.level_1_items_sum, 2) as values_difference,
    if(round(abs(t.value - s.level_1_items_sum), 2) <= 0.02, 'OK', 'NOT OK') as status 

from
    {{ source('ghg', 'fao_data') }} as t

join
    {{ source('ghg', 'countries') }} c
 on t.area_code = c.M49_Code

join
    items_summarized as s
 on c.ISO_alpha3_Code = s.ISO_alpha3_Code
and t.year = s.year

where
    t.item_code = 6825 -- All sectors with LULUCF
