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

)

select * from renamed
