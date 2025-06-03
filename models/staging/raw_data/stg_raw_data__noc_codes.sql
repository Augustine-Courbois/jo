with 

source as (

    select * from {{ source('raw_data', 'noc_codes') }}

),

renamed as (

    select
        index,
        noc,
        region,
        notes

    from source

)

select * from renamed
