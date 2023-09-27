with level_2_summarized as (

    select
        d.area_code,
        d.year,
        i.fao_category_code,
        sum(d.value) as level_2_emissions

    from
        {{ source('ghg', 'fao_data') }} d

    join
        {{ source('ghg', 'fao_items') }} i
     on d.item_code = i.item_code

    join
        {{ source('ghg', 'fao_categories') }} c
     on i.fao_category_code = c.fao_category_code

    where
        i.fao_category_level = 2
    and c.top_level_category_code = 'A' -- aggregate only agrifood systems categories

    group by
        d.area_code,
        d.year,
        i.fao_category_code

)

select
    c.ISO_alpha3_Code as country_code,
    t.year,
    i.fao_category_code,
    t.value as total_value,
    s.level_2_emissions as summarized_value,
    round(t.value - s.level_2_emissions, 2) as values_difference,
    if(round(abs(t.value - s.level_2_emissions), 2) <= 0.02, 'OK', 'NOT OK') as status 

from
    {{ source('ghg', 'fao_data') }} t

join
    {{ source('ghg', 'fao_items') }} i
 on t.item_code = i.item_code

join
    {{ source('ghg', 'countries') }} c
 on t.area_code = c.M49_Code

join
    level_2_summarized s
 on t.area_code = s.area_code
and t.year = s.year
and i.fao_category_code = s.fao_category_code

where
    i.fao_category_level = 1 -- only level 1 fao validation categories
