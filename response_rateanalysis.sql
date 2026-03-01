select day, count(*) as responses
from(select dayname(initial) as day, initial_status
from netflix_db.dbt_smohan.bulkload
where initial_status!='NR'
union all
select dayname(fup1) as day, fup1_status
from netflix_db.dbt_smohan.bulkload
where fup1_status!='NR'
union all
select dayname(fup2) as day, fup2_status
from netflix_db.dbt_smohan.bulkload
where fup2_status!='NR')t
group by day
order by responses desc;