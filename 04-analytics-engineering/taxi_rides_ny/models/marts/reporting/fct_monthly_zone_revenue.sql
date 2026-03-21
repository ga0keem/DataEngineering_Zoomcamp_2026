-- Data mart for monthly revenue analysis by pickup zone and service type
-- This aggregation is optimized for business reporting and dashboards
-- Enables analysis of revenue trends across different zones and taxi types

select
    pickup_zone,
    cast(date_trunc(pickup_datetime, month) as date) as revenue_month,
    service_type,

    -- Revenue
    coalesce(sum(fare_amount), 0) as revenue_monthly_fare,
    coalesce(sum(extra), 0) as revenue_monthly_extra,
    coalesce(sum(mta_tax), 0) as revenue_monthly_mta_tax,
    coalesce(sum(tip_amount), 0) as revenue_monthly_tip_amount,
    coalesce(sum(tolls_amount), 0) as revenue_monthly_tolls_amount,
    coalesce(sum(ehail_fee), 0) as revenue_monthly_ehail_fee,
    coalesce(sum(improvement_surcharge), 0) as revenue_monthly_improvement_surcharge,
    coalesce(sum(total_amount), 0) as revenue_monthly_total_amount,

    coalesce(count(trip_id), 0) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

from {{ ref('fct_trips') }}

where pickup_zone != 'Unknown'
  and service_type in ('Green', 'Yellow')
  and pickup_datetime is not null

group by pickup_zone, revenue_month, service_type