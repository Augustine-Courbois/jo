with source as (
select * 
from {{ source('raw_data', 'democracy_index') }}
)

, final AS (
select
    entity as country,
    year,
    `electoral democracy index` as electoral_democracy_index
from source
WHERE year >= 1990
)

SELECT *
FROM final
WHERE country = "Australia"
ORDER BY year DESC




