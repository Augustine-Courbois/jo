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

--déjà clean, on transforme la date float en int64--
SELECT 
Entity as country,
Code as code, 
CAST(Year AS INT64) as year,
human_development_index as human_development_index
FROM renamed
