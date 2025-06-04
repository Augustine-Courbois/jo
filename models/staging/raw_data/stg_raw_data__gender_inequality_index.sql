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

-- créer les clés code-year pour le merge--
SELECT
CONCAT(code,"_",year) as code_year, 
country, 
code, 
CASE 
      WHEN year = 2023 THEN 2024 --on remplace les index 2023 en 2024 car nous n'avons pas encore accès aux données 2024
      ELSE year
    END AS year,
gender_inequality_index
FROM renamed
