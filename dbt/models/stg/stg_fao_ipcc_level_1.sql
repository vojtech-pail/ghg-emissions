select
    d.area_code,
    i.item,
    i.ipcc_category_code,
    d.year,
    d.value

from
    {{ source('ghg', 'fao_data') }} d

join
    {{ source('ghg', 'fao_items') }} i
 on d.item_code = i.item_code

where
    i.ipcc_category_level = 1

