{{config (materialized='table')}}

with initials as(select mail_type, initial_day as day, responses
from(select mail_type, initial_day, responses,
    dense_rank() over(order by responses desc) as pos
    from(select 'Initial' as mail_type, initial_day, count(*) as responses
        from {{ref('stg_working_data')}}
        where status not in ('NR','bounced')
        group by initial_day)t)r
where pos=1),

fup1 as(select mail_type, fup1_day as day, responses
from(select mail_type, fup1_day, responses,
    dense_rank() over(order by responses desc) as pos
    from(select 'Fup1' as mail_type, fup1_day, count(*) as responses
        from {{ref('stg_working_data')}}
        where status not in ('NR','bounced')
        group by fup1_day)t)r
where pos=1),

fup2 as(select mail_type, fup2_day as day, responses
from(select mail_type, fup2_day, responses,
    dense_rank() over(order by responses desc) as pos
    from(select 'Fup2' as mail_type, fup2_day, count(*) as responses
        from {{ref('stg_working_data')}}
        where status not in ('NR','bounced')
        group by fup2_day)t)r
where pos=1),

overall as(select initial_day as day, count(*) as responses
from {{ref('stg_working_data')}}
where status not in ('NR','bounced')
group by initial_day
union all
select fup1_day as day, count(*) as responses
from {{ref('stg_working_data')}}
where status not in ('NR','bounced')
group by fup1_day
union all
select fup2_day as day, count(*) as responses
from {{ref('stg_working_data')}}
where status not in ('NR','bounced')
group by fup2_day),

hotday as(select mail_type, day, responses
from(select 'hotday' as mail_type, day, responses,
    dense_rank() over(order by responses desc) as pos
from(select day, sum(responses) as responses
from overall
group by day)t)r
where pos=1)

select * from initials
union
select * from fup1
union
select * from fup2
union
select * from hotday
order by mail_type