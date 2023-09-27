with calculated_metrics as (

    select
        country_code,
        sum(values_difference) as total_difference,
        avg(values_difference) as avg_difference,
        avg(relative_difference) as avg_relative_difference

    from
        {{ ref('datasets_comparison') }}

    group by
        country_code

)

select
    *,
    dense_rank() over(
        order by
            avg_relative_difference desc
    ) as rank_by_relative_difference

from
    calculated_metrics