select
    if(count(distinct unit) > 1, 'NOT OK', 'OK') as status
from {{ source('ghg', 'fao_data') }}
