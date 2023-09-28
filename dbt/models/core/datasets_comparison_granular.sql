with totals_comparison as (

    select
        *,
        cw_value - fao_value as values_difference, -- real difference than reported (Mt CO2)
        round(SAFE_DIVIDE((cw_value - fao_value), fao_value), 4) as relative_difference -- real difference than reported (%)

    from
        {{ ref('stg_datasets_comparison_granular') }}

)

select
    *,
    sum(values_difference) over(
        partition by
            country_code,
            ipcc_category_code
        
        order by
            year asc
    ) as running_total

from
    totals_comparison