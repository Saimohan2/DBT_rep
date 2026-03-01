{{config (materialized='table')}}

with days as(select dayname(initial) as initial, initial_status,
        dayname(fup1) as fup1, fup1_status,
        dayname(fup2) as fup2, fup2_status
from {{ref('s3load')}}
where comments not like 'bo%'),

segregated as(select day, status, responses,
row_number() over(order by responses desc, status) as position
from(select day, status, count(*) as responses
from(select initial as day, initial_status as status
from days
where initial_status!='NR'
union all
select fup1, fup1_status
from days
where fup1_status!='NR'
union all
select fup2, fup2_status
from days
where fup2_status!='NR')t
group by day, status)r)

select status, day, responses,
    row_number() over(partition by status order by responses desc, day) as pos
from segregated