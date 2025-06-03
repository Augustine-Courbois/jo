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

select * from renamed
