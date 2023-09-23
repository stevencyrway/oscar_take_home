with member_claims_stats as (select member_id,
                                    round(avg(days_since_last_diag), 1)              as avg_days_between_diag,
                                    round(avg(cumulative_occurrences_per_member), 1) as avg_total_member_same_diags
                             from {{ref('fct_claims')}}
                             group by 1),
     member_prescription_stats as (select member_id,
                                          avg(days_since_last_fill)              as avg_days_between_fills,
                                          avg(cumulative_occurrences_per_member) as avg_total_member_fills
                                   from {{ref('fct_prescriptions')}}
                                   group by 1)
Select mp.member_id,
       earliest_claim,
       latest_claim_date,
       earliest_prescription,
       latest_prescription,
       number_of_claims,
       number_of_prescriptions,
       avg_days_between_diag,
       avg_total_member_same_diags,
       avg_days_between_fills,
       avg_total_member_fills
from member_profile mp
         left join member_claims_stats mcs on mcs.member_id = mp.member_id
         left join member_prescription_stats mps on mps.member_id = mp.member_id