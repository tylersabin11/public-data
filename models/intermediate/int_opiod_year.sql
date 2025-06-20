--placeholder file
select
    year
from {{ ref('stg_opioids') }}