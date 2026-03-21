-- null 확인
with unioned as (
    select *
    from {{ ref('int_trips_unioned') }}
)
select
    sum(case when vendor_id is null then 1 else 0 end) as vendor_id_nulls,
    sum(case when pickup_datetime is null then 1 else 0 end) as pickup_datetime_nulls,
    sum(case when dropoff_datetime is null then 1 else 0 end) as dropoff_datetime_nulls,
    sum(case when pickup_location_id is null then 1 else 0 end) as pickup_location_nulls,
    sum(case when dropoff_location_id is null then 1 else 0 end) as dropoff_location_nulls,
    sum(case when passenger_count is null then 1 else 0 end) as passenger_count_nulls,
    sum(case when trip_distance is null then 1 else 0 end) as trip_distance_nulls,
    sum(case when payment_type is null then 1 else 0 end) as payment_type_nulls
from unioned