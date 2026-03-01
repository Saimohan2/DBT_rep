select account_name,
        trim(concat(first_name,' ',coalesce(last_name,''))) as contact_name,
        designation as title,
        email_id,
        practice,
        try_to_date(initial, 'DD-MM-YYYY') as initial,
        dayname(try_to_date(initial, 'DD-MM-YYYY')) as initial_day,
        try_to_date(fup1, 'DD-MM-YYYY') as fup1,
        dayname(try_to_date(fup1, 'DD-MM-YYYY')) as fup1_day,
        try_to_date(fup2, 'DD-MM-YYYY') as fup2,
        dayname(try_to_date(fup2, 'DD-MM-YYYY')) as fup2_day,
        coalesce(lower(status),'NR') as status,
        current_date as created_at
from {{source('my_raw_source', 'working_data')}}