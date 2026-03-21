-- 이상치 확인
with unioned as (
    select *
    from {{ ref('int_trips_unioned') }}
)
select 'negative_distance' as check_type, count(*) as cnt
from unioned
where trip_distance < 0

union all

select 'invalid_passenger' as check_type, count(*) as cnt
from unioned
where passenger_count <= 0