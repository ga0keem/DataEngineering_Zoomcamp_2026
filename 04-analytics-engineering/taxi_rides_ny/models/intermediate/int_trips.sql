-- Enrich and deduplicate trip data
-- Demonstrates enrichment and surrogate key generation
-- Note: Data quality analysis available in analyses/trips_data_quality.sql

with unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

payment_types as (
    select * from {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched as (
    select
        -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} as trip_id,
        
        -- Identifiers
        coalesce(u.vendor_id, -1) as vendor_id,
        coalesce(u.service_type, 'Unknown') as service_type,
        coalesce(u.rate_code_id, 0) as rate_code_id,

        -- Location IDs
        coalesce(u.pickup_location_id, -1) as pickup_location_id,
        coalesce(u.dropoff_location_id, -1) as dropoff_location_id,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        coalesce(u.store_and_fwd_flag, 'N') as store_and_fwd_flag,
        coalesce(u.passenger_count, 0) as passenger_count,
        coalesce(u.trip_distance, 0) as trip_distance,
        coalesce(u.trip_type, 0) as trip_type,

        -- Payment breakdown
        coalesce(u.fare_amount, 0) as fare_amount,
        coalesce(u.extra, 0) as extra,
        coalesce(u.mta_tax, 0) as mta_tax,
        coalesce(u.tip_amount, 0) as tip_amount,
        coalesce(u.tolls_amount, 0) as tolls_amount,
        coalesce(u.ehail_fee, 0) as ehail_fee,
        coalesce(u.improvement_surcharge, 0) as improvement_surcharge,
        coalesce(u.total_amount, 0) as total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description

    from unioned u
    left join payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
    where not (
        u.fare_amount < 0
        and coalesce(pt.description,'Unknown') not in ('Dispute','Voided')
    )
    -- Optionally remove rows with critical nulls (like pickup_datetime)
    and u.pickup_datetime is not null
    and u.pickup_location_id is not null
)

select * from cleaned_and_enriched

-- Deduplicate: if multiple trips match (same vendor, second, location, service), keep first
qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1