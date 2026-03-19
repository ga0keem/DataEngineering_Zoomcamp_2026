-- payment_type 분포 확인
with unioned as (
    select *
    from {{ ref('int_trips_unioned') }}
)
select
    coalesce(payment_type, 0) as payment_type,
    count(*) as cnt,
    round(100*count(*)/sum(count(*)) over(), 2) as pct
from unioned
group by payment_type
order by cnt desc