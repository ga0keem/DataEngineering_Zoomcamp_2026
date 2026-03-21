select
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    trips.pickup_location_id,
    coalesce(pz.borough, 'Unknown') as pickup_borough,
    coalesce(pz.zone, 'Unknown') as pickup_zone,

    trips.dropoff_location_id,
    coalesce(dz.borough, 'Unknown') as dropoff_borough,
    coalesce(dz.zone, 'Unknown') as dropoff_zone,

    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,

    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from {{ ref('int_trips') }} as trips

left join {{ ref('dim_zones') }} as pz
    on trips.pickup_location_id = pz.location_id

left join {{ ref('dim_zones') }} as dz
    on trips.dropoff_location_id = dz.location_id

where trips.service_type in ('Green', 'Yellow')
  and trips.pickup_datetime is not null

{% if is_incremental() %}
  and trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}