select coalesce(account_name, 'No Name') as account_name,
        trim(concat(first_name,' ',coalesce(last_name, ''))) as contact_name,
        coalesce(designation, 'Not Available') as title,
        email_id,
        upper(practice) as practice,
        try_to_date(initial, 'DD-MM-YYYY') as initial,
        coalesce(lower(initial_status), 'NR') as initial_status,
        try_to_date(fup1, 'DD-MM-YYYY') as fup1,
        coalesce(lower(fup1_status), 'NR') as fup1_status,
        try_to_date(fup2, 'DD-MM-YYYY') as fup2,
        coalesce(lower(fup2_status), 'NR') as fup2_status,
        coalesce(lower(comments), 'delivered') as comments,
        case when initial_status='Negative' or fup1_status='Negative' or
                    fup2_status='Negative' then 'negative'
            when initial_status='Warm' or fup1_status='Warm' or fup2_status='Warm'
                    then 'warm'
            when initial_status='Positive' or fup1_status='Positive' or
                    fup2_status='Positive' then 'positive' 
            when comments='Bounced' then 'bounced'
            else 'no response'
            end as final_status
from {{source('s3rawsrc','mailed_data')}}