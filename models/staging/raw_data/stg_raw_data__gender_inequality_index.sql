with 

source as (
    select * from {{ source('raw_data', 'gender_inequality_index') }}
),

renamed as (
    select
        entity as country,
        code,
        year,
        `gender inequality index` as gender_inequality_index

    from source

)

-- remplacer les index 2023 en 2024 car nous n'avons pas encore accès aux données 2024 --
, date_change AS (
SELECT
country, 
code, 
CASE 
      WHEN year = 2023 THEN 2024
      ELSE year
END AS year,
gender_inequality_index
FROM renamed
)

--créer les code_year--
SELECT
CONCAT(code,"_",CAST(year AS STRING)) as code_year, 
country, 
code, 
year,
gender_inequality_index
from date_change