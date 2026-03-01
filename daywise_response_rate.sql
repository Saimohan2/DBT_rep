with daywise as(select dayname(initial) as day,
    sum(case when initial_status in ('WARM','POSITIVE','NEGATIVE')then 1 else 0 end) as responses, count(*) as total
from netflix_db.dbt_smohan.bulkload
group by dayname(initial)
union all
select dayname(fup1) as day,
    sum(case when fup1_status in ('WARM','POSITIVE','NEGATIVE')then 1 else 0 end) as responses, count(*) as total
from netflix_db.dbt_smohan.bulkload
group by dayname(fup1)
union all
select dayname(fup2) as day,
    sum(case when fup2_status in ('WARM','POSITIVE','NEGATIVE')then 1 else 0 end) as responses, count(*) as total
from netflix_db.dbt_smohan.bulkload
where fup2 is not null
group by dayname(fup2)),

ranked as(select day, responses, mails_sent, response_rate,
    row_number()
        over(order by response_rate desc, mails_sent) as productive_rank
from(select day, sum(responses) as responses, sum(total) as mails_sent,
    100.0*sum(responses)/sum(total) as response_rate
    from daywise
    group by day)t
union all
select 'Overall', sum(responses), sum(total),
    100.0*sum(responses)/sum(total), null
from daywise)

select day, responses,
    round(response_rate,2) as response_rate, productive_rank
from ranked
where response_rate>(select response_rate
                     from ranked
                     where day='Overall')
order by productive_rank;