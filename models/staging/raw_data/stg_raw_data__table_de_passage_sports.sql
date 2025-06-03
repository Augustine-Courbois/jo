with 

source as (

    select * from {{ source('raw_data', 'table_de_passage_sports') }}

),

renamed as (

    select
        primary_key,
        sport

    from source

)

select * from renamed
