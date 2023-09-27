with items_summarized as (

    select
        iso_code3,
        year,
        sum(value) as level_1_emissions
    
    from
        {{ ref('stg_cw_ipcc_level_1') }}
    
    group by
        iso_code3,
        year
)

select
    t.iso_code3 as country_code,
    t.year,
    t.value as total_value,
    s.level_1_emissions as summarized_value,
    round(abs(t.value - s.level_1_emissions), 2) as absolute_difference,
    if(round(abs(t.value - s.level_1_emissions), 2) <= 0.02, 'OK', 'NOT OK') as status 

from
    {{ source('ghg', 'cw_data') }} as t

join
    items_summarized as s
 on t.iso_code3 = s.iso_code3
and t.year = s.year

where
    t.sector = 'Total including LUCF'