{{ config(
    materialized="table"
) }}
WITH OrderedData AS (
    SELECT
    *,
        CAST(date_svc AS DATE) as fill_date,
        LAG(CAST(date_svc AS DATE)) OVER(PARTITION BY member_id, ndc ORDER BY CAST(date_svc AS DATE)) as previous_fill_date,
        MIN(CAST(date_svc AS DATE)) OVER(PARTITION BY member_id, ndc) as first_fill_date

    FROM
        defaultdb.public.prescriptions
)

SELECT
    *,
    fill_date - previous_fill_date as days_since_last_fill,
    ROW_NUMBER() OVER(PARTITION BY member_id, ndc ORDER BY fill_date) as cumulative_occurrences_per_member,
    {{ dbt_utils.surrogate_key(['fill_date', 'member_id','ndc']) }} as fct_prescriptions_pk
FROM
    OrderedData
ORDER BY
    member_id, fill_date, ndc