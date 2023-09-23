Select date_trunc('week',diag_date)       as week,
       date_part('week',diag_date)        as week_number,
       diag1,
       diag_desc,
       count(member_id)                   as member_count,
       round(avg(days_since_last_diag),1) as avg_days_since_recurrance,
       {{ dbt_utils.surrogate_key(["date_trunc('week',diag_date)","diag1"]) }} as agg_diagnoses_pk
from {{ref('fct_claims')}}
group by 1 ,2, 3, 4