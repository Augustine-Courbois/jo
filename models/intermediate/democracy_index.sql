select 
r.country,
g.code,
r.year,
r.electoral_democracy_index
from {{ ref('stg_raw_data__democracy_index') }} as r
LEFT JOIN {{ ref('stg_raw_data__gender_inequality_index') }} as g
USING (country)