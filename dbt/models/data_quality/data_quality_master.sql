select
    "Result of fao_ipcc_test_1_2 model" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('fao_ipcc_test_1_2') }}
where
    status = 'NOT OK'

union all

select
    "Result of fao_ipcc_test_2 model" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('fao_ipcc_test_2') }}
where
    status = 'NOT OK'

union all

select
    "Result of fao_fao_test_1 model" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('fao_fao_test_1') }}
where
    status = 'NOT OK'

union all

select
    "Result of fao_fao_test_2 model" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('fao_fao_test_2') }}
where
    status = 'NOT OK'

union all

select
    "Result of cw_ipcc_test_1 model" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('cw_ipcc_test_1') }}
where
    status = 'NOT OK'

union all

select
    "Result of cw_ipcc_test_2 model" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('cw_ipcc_test_2') }}
where
    status = 'NOT OK'

union all

select
    "Result of fao_source_unit_test model (1 = very bad)" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('fao_source_unit_test') }}
where
    status = 'NOT OK'

union all

select
    "Result of cw_source_unit_test model (1 = very bad)" as test_name,
    count(*) as not_ok_statuses
from
    {{ ref('cw_source_unit_test') }}
where
    status = 'NOT OK'