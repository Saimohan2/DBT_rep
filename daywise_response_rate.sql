with daywise as(select dayname(initial) as day,
    sum(case when initial_status in ('WARM','POSITIVE','NEGATIVE') then 1 else 0 end) as responses, count(*) as total
from netflix_db.dbt_smohan.bulkload
group by dayname(initial)
union all
select dayname(fup1) as day,
    sum(case when fup1_status in ('WARM','POSITIVE','NEGATIVE') then 1 else 0 end) as responses, count(*) as total
from netflix_db.dbt_smohan.bulkload
group by dayname(fup1)
union all
select dayname(fup2) as day,
    sum(case when fup2_status in ('WARM','POSITIVE','NEGATIVE') then 1 else 0 end) as responses, count(*) as total
from netflix_db.dbt_smohan.bulkload
where fup2 is not null
group by dayname(fup2)),

ranked as(select day, responses, mails_sent, response_rate,
    row_number() over(order by response_rate desc, mails_sent) as response_rank
from(select day, sum(responses) as responses, sum(total) as mails_sent,
    100.0*sum(responses)/sum(total) as response_rate
from daywise
group by day)t),

overall as(select sum(responses) as responses, sum(total) as mails_sent, 100.0*sum(responses)/sum(total) as response_rate
from daywise),

positive_days as(select dayname(initial) as initial_day, initial_status, dayname(fup1) as fup1_day, fup1_status,
    dayname(fup2) as fup2_day, fup2_status, status
from netflix_db.dbt_smohan.bulkload
where status='POSITIVE'),

daywise_pos as(select day, count(*) as positives
from(select initial_day as day
from positive_days
where initial_status!='NR'
union all
select fup1_day
from positive_days
where fup1_status!='NR'
union all
select fup2_day
from positive_days
where fup2_status!='NR')t
group by day)

select r.day, r.responses, r.mails_sent, round(r.response_rate,2) as response_rate,
    round(100.0*d.positives/r.mails_sent,2) as positive_rate,
    round(100.0*d.positives/r.responses,2) as positive_share, r.response_rank,
    case when r.response_rate>(select response_rate from overall) then 'HIGH'
        when r.response_rate<(select response_rate from overall) then 'LOW'
        else 'NEUTRAL'
        end as response_probability
from ranked r
join daywise_pos d
on r.day=d.day
order by positive_share desc, response_rank, mails_sent;