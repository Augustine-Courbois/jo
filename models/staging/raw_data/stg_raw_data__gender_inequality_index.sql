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
year,
gender_inequality_index
FROM renamed
