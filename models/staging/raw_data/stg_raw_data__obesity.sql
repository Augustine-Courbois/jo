with 
source as (
    select * from {{ source('raw_data', 'obesity') }}
),

renamed as (
    select
        entity as country,
        code,
        year,
        prevalence_obesity_adults
    from source
)

-- créer les clés code-year pour le merge--
SELECT
CONCAT(code,"_",year) as code_year, 
country, 
code, 
year,
prevalence_obesity_adults
FROM renamed
