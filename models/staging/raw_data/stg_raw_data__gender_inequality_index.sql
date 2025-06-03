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

select * from renamed
