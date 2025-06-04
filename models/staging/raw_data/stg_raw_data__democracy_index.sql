with source as (
select * 
from {{ source('raw_data', 'democracy_index') }}
)

, final AS (
select
    entity as country,
    CASE 
      WHEN year = 2023 THEN 2024 --on remplace les index 2023 en 2024 car nous n'avons pas encore accès aux données 2024
      ELSE year
    END AS year,
    `electoral democracy index` as electoral_democracy_index
from source
WHERE year >= 1990
)

SELECT *
FROM final
ORDER BY year DESC





