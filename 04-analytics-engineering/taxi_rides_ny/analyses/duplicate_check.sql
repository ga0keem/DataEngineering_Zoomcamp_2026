-- 중복 데이터 확인
with unioned as (
    select *
    from {{ ref('int_trips_unioned') }}
)
select
    vendor_id,
    pickup_datetime,
    pickup_location_id,
    service_type,
    count(*) as trip_count
from unioned
group by vendor_id, pickup_datetime, pickup_location_id, service_type
having count(*) > 1
order by trip_count desc;
