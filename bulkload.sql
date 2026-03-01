with transformed as(select upper(account_name) as account_name,
        upper(concat(first_name,' ',coalesce(last_name,''))) as contact_name,
        coalesce(designation, 'NA') as title,
        email_id,
        upper(practice) as practice,
        try_to_date(initial, 'DD-MM-YYYY') as initial,
        upper(coalesce(initial_status,'NR')) as initial_status,
        try_to_date(fup1, 'DD-MM-YYYY') as fup1,
        upper(coalesce(fup1_status,'NR')) as fup1_status,
        try_to_date(fup2, 'DD-MM-YYYY') as fup2,
        upper(coalesce(fup2_status,'NR')) as fup2_status,
        upper(coalesce(comments,'Delivered')) as comments,
        row_number() over(partition by email_id order by try_to_date(initial, 'DD-MM-YYYY')) as rn
from {{source('bulkload_132026','historicalraw')}}),

dups_handled as(select *
from transformed
where rn=1)

select account_name, contact_name, title, email_id, practice, initial, initial_status,
    fup1, fup1_status, fup2, fup2_status, coalesce(status,'NO RESPONSE') as status
from(select *,
    case when initial_status='NEGATIVE' or fup1_status='NEGATIVE' or fup2_status='NEGATIVE'
        then 'NEGATIVE'
        when initial_status='WARM' or fup1_status='WARM' or fup2_status='WARM'
        then 'WARM'
        when initial_status='POSITIVE' or fup1_status='POSITIVE' or fup2_status='POSITIVE'
        then 'POSITIVE'
        when comments='BOUNCED' then 'BOUNCED'
        end as status
from dups_handled)t
order by initial