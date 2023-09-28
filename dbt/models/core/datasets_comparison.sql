with totals_comparison as (

    select
        *,
        cw_value - fao_value as values_difference, -- cw difference than reported in FAO (in Mt CO2)
        round(SAFE_DIVIDE((cw_value - fao_value), fao_value), 4) as relative_difference -- cw difference than reported in FAO (%)

    from
        {{ ref('stg_datasets_comparison_aggregated') }}

)

select
    *,
    sum(values_difference) over(
        partition by
            country_code
        order by
            year asc
    ) as running_total_difference,
    avg(values_difference) over(
        partition by    
            country_code
        order by
            year
        rows between 2 preceding and 2 following 
    ) as ma5_values_difference,
    cw_value - lag(cw_value) over(
        partition by
            country_code
        order by
            year
    ) as cw_yoy_difference,
    fao_value - lag(fao_value) over(
        partition by
            country_code
        order by
            year
    ) as fao_yoy_difference,

from
    totals_comparison