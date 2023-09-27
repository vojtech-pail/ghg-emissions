select
    c.ISO_alpha3_Code as country_code,
    g.ipcc_category_code,
    cw.year,
    cw.value as cw_value, -- climate watch values are in megatons
    fao.value/1000 as fao_value -- fao values are in kilotons

from
    {{ ref('stg_cw_ipcc_level_1') }} cw

join
    {{ source('ghg', 'ipcc_categories') }} g
on cw.ipcc_category_code = g.ipcc_category_code

join
    {{ source('ghg', 'countries') }} c
on cw.iso_code3 = c.ISO_alpha3_Code

join
    {{ ref('stg_fao_ipcc_level_1') }} fao
on c.M49_Code = fao.area_code
and cw.year = fao.year
and cw.ipcc_category_code = fao.ipcc_category_code