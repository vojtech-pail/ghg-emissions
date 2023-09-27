select
    country_code,
    year,
    sum(cw_value) as cw_value,
    sum(fao_value) as fao_value

from
    {{ ref('stg_datasets_comparison_granular') }}

group by
    country_code,
    year