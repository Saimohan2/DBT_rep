{{ config(materialized='table') }}


select
   type,
   count(*) as total_titles,
   count(case when country = 'India' then 1 end) as india_titles,
   count(case when rating = 'PG' then 1 end) as pg_titles
from {{ ref('stg_netflix_titles') }}
group by type