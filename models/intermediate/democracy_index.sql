WITH tab AS (
select 
r.country,
g.code,
r.year,
r.electoral_democracy_index
from {{ ref('stg_raw_data__democracy_index') }} as r
LEFT JOIN {{ ref('stg_raw_data__gender_inequality_index') }} as g
USING (country)
)

-- créer les clés code-year pour le merge--
, final AS (
SELECT
CONCAT(code,"_",year) as code_year, 
country, 
code, 
CASE 
    WHEN year = 2023 THEN 2024 --on remplace les index 2023 en 2024 car nous n'avons pas encore accès aux données 2024
    ELSE year
END AS year,
electoral_democracy_index
FROM tab
)
--check remplacement année
SELECT *
FROM final
ORDER BY year DESC


