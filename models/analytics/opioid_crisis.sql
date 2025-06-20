with county_names as(
    select
        opioid_id,
        state_name,
        case when county_name is null then 'Unknown' else county_name end as county_name,
        row_number() over(partition by opioid_id order by opioid_id desc) as row_id
    from {{ ref('stg_opioid_county_state') }}
)

select
    o.opioid_id,
    cn.state_name,
    cn.county_name,
    o.year,
    o.geo_level,
    o.geo_code,
    o.plan_type,
    o.opioid_claim_count,
    o.overall_claim_count,
    o.opioid_presc_rate_1y,
    o.opioid_presc_rate_5y,
    o.long_acting_opioid_claims,
    o.long_acting_opioid_presc_rate,
    o.long_acting_opioid_presc_rate_1y,
    o.long_acting_opioid_presc_rate_5y
from {{ ref('stg_opioids') }} as o 
left join 
    county_names as cn 
on o.opioid_id = cn.opioid_id
and cn.row_id = 1