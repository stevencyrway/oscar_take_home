{{ config(
    materialized="table"
) }}
WITH OrderedData AS (
    SELECT
    *,
        CAST(date_svc AS DATE) as diag_date,
        LAG(CAST(date_svc AS DATE)) OVER(PARTITION BY member_id, diag1 ORDER BY CAST(date_svc AS DATE)) as previous_diag_date,
        MIN(CAST(date_svc AS DATE)) OVER(PARTITION BY member_id, diag1) as first_diag_date

    FROM {{ref('stg_claims')}}
)

SELECT
    *,
    diag_date - previous_diag_date as days_since_last_diag,
    ROW_NUMBER() OVER(PARTITION BY member_id, diag1 ORDER BY diag_date) as cumulative_occurrences_per_member,
    {{ dbt_utils.surrogate_key(['diag_date','member_id','diag1']) }} as fct_claims_pk
FROM
    OrderedData
ORDER BY
    member_id, diag_date, diag1