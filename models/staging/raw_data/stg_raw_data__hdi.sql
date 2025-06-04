with 

source as (

    select * from {{ source('raw_data', 'hdi') }}

),

renamed as (
    select
        entity,
        code,
        year,
        `human development index` as human_development_index
    from source
    WHERE year >=1990
)

, convert AS (
--déjà clean, on transforme la date float en int64--
SELECT 
Entity as country,
Code as code, 
CAST(Year AS INT64) as year,
human_development_index as human_development_index
FROM renamed
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
human_development_index
FROM convert
)

--créer les code_year--
SELECT
CONCAT(code,"_",CAST(year AS STRING)) as code_year, 
country, 
code, 
year,
human_development_index
from date_change