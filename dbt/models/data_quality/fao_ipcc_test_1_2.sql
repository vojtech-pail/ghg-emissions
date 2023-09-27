{% set cat_values = 'A' %}

with items_summarized as (

    select
        d.area_code,
        d.year,
        i.ipcc_category_code,
        sum(d.value) as level_2_items_sum

    from
        {{ source('ghg', 'fao_data') }} d

    join
        {{ source('ghg', 'fao_items') }} i
     on d.item_code = i.item_code

    where
        i.ipcc_category_code = '{{cat_values}}'
    and i.ipcc_category_level = 2

    group by
        d.area_code,
        d.year,
        i.ipcc_category_code

)

select
    -- t.area_code as country_code,
    c.Country_or_Area as country_name,
    t.year,
    --i.ipcc_category_code,
    g.ipcc_category,
    t.value as total_value,
    s.level_2_items_sum,
    round(t.value - s.level_2_items_sum, 2) as values_difference,
    if(round(abs(t.value - s.level_2_items_sum), 2) <= 0.02, 'OK', 'NOT OK') as status 

from
    {{ source('ghg', 'fao_data') }} t

join
    {{ source('ghg', 'fao_items') }} i
 on t.item_code = i.item_code

join
    {{ source('ghg', 'ipcc_categories') }} g
 on i.ipcc_category_code = g.ipcc_category_code

left join
    {{ source('ghg', 'countries') }} c
 on t.area_code = c.M49_Code

full outer join
    items_summarized s
 on t.area_code = s.area_code
and t.year = s.year
and i.ipcc_category_code = s.ipcc_category_code

where
    i.ipcc_category_code = '{{cat_values}}'
and i.ipcc_category_level = 1