with 

source as (

    select * from {{ source('raw_data', 'regions') }}

),

renamed as (

    select
        string_field_0,
        string_field_1,
        string_field_2,
        string_field_3,
        string_field_4

    from source

)

select * from renamed
