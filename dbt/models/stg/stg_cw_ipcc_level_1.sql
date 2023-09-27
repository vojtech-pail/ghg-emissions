select
    d.iso_code3,
    d.sector,
    i.ipcc_category_code,
    d.year,
    d.value

from
    {{ source('ghg', 'cw_data') }} d

join
    {{ source('ghg', 'cw_items') }} i
 on d.sector = i.sector

where
    i.ipcc_category_level = 1