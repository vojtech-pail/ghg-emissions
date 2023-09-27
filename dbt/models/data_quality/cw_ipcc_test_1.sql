with items_summarized as (

    select
        d.iso_code3,
        d.year,
        s.ipcc_category_code,
        sum(d.value) as level_2_items_sum
    
    from
        {{ source('ghg', 'cw_data') }} as d
    
    join
        {{ source('ghg', 'cw_items') }} s
     on d.sector = s.sector
    
    where
        s.ipcc_category_level = 2
    
    group by
        d.iso_code3,
        d.year,
        s.ipcc_category_code

)

select
    t.iso_code3 as country_code,
    t.year,
    i.ipcc_category_code,
    t.value as total_value,
    s.level_2_items_sum as summarized_value,
    round(t.value - s.level_2_items_sum, 2) as values_difference,
    if(round(abs(t.value - s.level_2_items_sum), 2) <= 0.02, 'OK', 'NOT OK') as status 

from
    {{ source('ghg', 'cw_data') }} as t

join
    {{ source('ghg', 'cw_items') }} i
 on t.sector = i.sector

right outer join
    items_summarized as s
 on t.iso_code3 = s.iso_code3
and t.year = s.year
and i.ipcc_category_code = s.ipcc_category_code

where
    i.ipcc_category_level = 1