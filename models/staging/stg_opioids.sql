select 
    Year as year,
    Geo_Lvl as geo_level,
    Geo_Cd as geo_code,
    Geo_Desc as state,
    Plan_Type as plan_type,
    Tot_Opioid_Clms as opioid_claim_count,
    Tot_Clms as overall_claim_count,
    Opioid_Prscrbng_Rate as opioid_presc_rate,
    Opioid_Prscrbng_Rate_5Y_Chg as opioid_presc_rate_5y,
    Opioid_Prscrbng_Rate_1Y_Chg as opioid_presc_rate_1y,
    LA_Tot_Opioid_Clms as long_acting_opioid_claims,
    LA_Opioid_Prscrbng_Rate as long_acting_opioid_presc_rate,
    LA_Opioid_Prscrbng_Rate_5Y_Chg as long_acting_opioid_presc_rate_5y,
    LA_Opioid_Prscrbng_Rate_1Y_Chg as long_acting_opioid_presc_rate_1y
from 
    {{ source('my_datasets', 'raw_opioids') }}
where Geo_Desc not in ('National')