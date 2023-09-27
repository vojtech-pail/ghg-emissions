select
    cw.iso_code3 as country_code,
    cw.ipcc_category_code,
    cw.year,
    cw.value as cw_value, -- climate watch values are in megatons
    fao.value/1000 as fao_value -- fao values are in kilotons

from
    {{ ref('stg_cw_ipcc_level_1') }} cw

join
    {{ ref('stg_fao_ipcc_level_1') }} fao
on cw.iso_code3 = fao.ISO_alpha3_Code
and cw.year = fao.year
and cw.ipcc_category_code = fao.ipcc_category_code