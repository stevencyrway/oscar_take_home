with base as (select *,
                     ROW_NUMBER() OVER (PARTITION BY member_id, diag1 ORDER BY date_svc desc) AS dedupe_row
              from defaultdb.public.claims)

Select *
from base
where dedupe_row = 1