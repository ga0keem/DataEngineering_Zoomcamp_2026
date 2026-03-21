select
    vendor_id,
    {{ get_vendor_data('vendor_id') }} as vendor_name
from {{ ref('int_trips') }}
group by vendor_id