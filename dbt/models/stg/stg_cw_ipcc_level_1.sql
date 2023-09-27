select
    d.iso_code3,
    d.year,
    i.ipcc_category_code,
    d.value

from
    {{ source('ghg', 'cw_data') }} d

join
    {{ source('ghg', 'cw_items') }} i
 on d.sector = i.sector

where
    i.ipcc_category_level = 1
