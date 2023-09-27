select
    c.ISO_alpha3_Code,
    d.year,
    i.ipcc_category_code,
    d.value

from
    {{ source('ghg', 'fao_data') }} d

join
    {{ source('ghg', 'fao_items') }} i
 on d.item_code = i.item_code

join
    {{ source('ghg', 'countries') }} c
 on d.area_code = c.M49_Code

where
    i.ipcc_category_level = 1

