select
    concat(Plan_Type, Tot_Opioid_Clms) as opioid_id,
    split(Geo_Desc, ':')[safe_offset(0)] as state_name,
    split(Geo_Desc, ':')[safe_offset(1)] as county_name
from {{ source('my_datasets', 'raw_opioids') }}
where Geo_Desc not in ('National')