with 

source as (

    select * from {{ source('raw_data', 'gdp') }}

),

renamed as (

    select
        string_field_0,
        string_field_1,
        string_field_2,
        string_field_3,
        double_field_4,
        double_field_5,
        double_field_6,
        double_field_7,
        double_field_8,
        double_field_9,
        double_field_10,
        double_field_11,
        double_field_12,
        double_field_13,
        double_field_14,
        double_field_15,
        double_field_16,
        double_field_17,
        double_field_18,
        double_field_19,
        double_field_20,
        double_field_21,
        double_field_22,
        double_field_23,
        double_field_24,
        double_field_25,
        double_field_26,
        double_field_27,
        double_field_28,
        double_field_29,
        double_field_30,
        double_field_31,
        double_field_32,
        double_field_33,
        double_field_34,
        double_field_35,
        double_field_36,
        double_field_37,
        double_field_38,
        double_field_39,
        double_field_40,
        double_field_41,
        double_field_42,
        double_field_43,
        double_field_44,
        double_field_45,
        double_field_46,
        double_field_47,
        double_field_48,
        double_field_49,
        double_field_50,
        double_field_51,
        double_field_52,
        double_field_53,
        double_field_54,
        double_field_55,
        double_field_56,
        double_field_57,
        double_field_58,
        double_field_59,
        double_field_60,
        double_field_61,
        double_field_62,
        double_field_63,
        double_field_64,
        double_field_65,
        double_field_66,
        double_field_67,
        int64_field_68,
        string_field_69

    from source

)

select * from renamed
