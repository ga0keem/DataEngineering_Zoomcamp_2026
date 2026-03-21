select
    locationid as location_id,
    coalesce(nullif(trim(borough), ''), 'Unknown') as borough,
    coalesce(nullif(trim(zone), ''), 'Unknown') as zone,
    coalesce(nullif(trim(service_zone), ''), 'Unknown') as service_zone
from {{ ref('taxi_zone_lookup') }}