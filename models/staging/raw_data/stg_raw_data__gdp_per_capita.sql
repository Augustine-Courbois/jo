with 

source as (

    select * from {{ source('raw_data', 'gdp_per_capita') }}

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
        string_field_68,
        string_field_69

    from source

)

--Step1: supprimer les colonnes indésirables--
, supp AS (
SELECT *
EXCEPT
(string_field_2, 
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
string_field_68,
string_field_69)
FROM renamed
)

--step2:renommer les colonnes--
, rename AS (
SELECT 
string_field_0 as country,
string_field_1 as code_wb,
double_field_34 as `1990`,
double_field_35 as `1991`,
double_field_36 as `1992`,
double_field_37 as `1993`,
double_field_38 as `1994`,
double_field_39 as `1995`,
double_field_40 as `1996`,
double_field_41 as `1997`,
double_field_42 as `1998`,
double_field_43 as `1999`,
double_field_44 as `2000`,
double_field_45 as `2001`,
double_field_46 as `2002`,
double_field_47 as `2003`,
double_field_48 as `2004`,
double_field_49 as `2005`,
double_field_50 as `2006`,
double_field_51 as `2007`,
double_field_52 as `2008`,
double_field_53 as `2009`,
double_field_54 as `2010`,
double_field_55 as `2011`,
double_field_56 as `2012`,
double_field_57 as `2013`,
double_field_58 as `2014`,
double_field_59 as `2015`,
double_field_60 as `2016`,
double_field_61 as `2017`,
double_field_62 as `2018`,
double_field_63 as `2019`,
double_field_64 as `2020`,
double_field_65 as `2021`,
double_field_66 as `2022`,
double_field_67 as `2023`,
FROM supp
)

--Step3: passage des colonnes en lignes--
, pivot AS (
SELECT 
  country,
  code_wb as code,
  CAST(annee_col AS INT64) AS year,
  gdp_per_capita
FROM rename
UNPIVOT (
  gdp_per_capita FOR annee_col IN (
    `1990`,
`1991`,
`1992`,
`1993`,
`1994`,
`1995`,
`1996`,
`1997`,
`1998`,
`1999`,
`2000`,
`2001`,
`2002`,
`2003`,
`2004`,
`2005`,
`2006`,
`2007`,
`2008`,
`2009`,
`2010`,
`2011`,
`2012`,
`2013`,
`2014`,
`2015`,
`2016`,
`2017`,
`2018`,
`2019`,
`2020`,
`2021`,
`2022`,
`2023`
  )
)
)
--Step4: rajout d'une colonne d'income category--
, category AS (
SELECT *,
  CASE 
    -- 1990
    WHEN year = 1990 AND gdp_per_capita <= 610 THEN 'Low income'
    WHEN year = 1990 AND gdp_per_capita <= 2465 THEN 'Lower-middle income'
    WHEN year = 1990 AND gdp_per_capita <= 7620 THEN 'Upper-middle income'
    WHEN year = 1990 THEN 'High income'

    -- 1991
    WHEN year = 1991 AND gdp_per_capita <= 675 THEN 'Low income'
    WHEN year = 1991 AND gdp_per_capita <= 2695 THEN 'Lower-middle income'
    WHEN year = 1991 AND gdp_per_capita <= 7910 THEN 'Upper-middle income'
    WHEN year = 1991 THEN 'High income'

    -- 1992
    WHEN year = 1992 AND gdp_per_capita <= 755 THEN 'Low income'
    WHEN year = 1992 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1992 AND gdp_per_capita <= 8355 THEN 'Upper-middle income'
    WHEN year = 1992 THEN 'High income'

    -- 1993
    WHEN year = 1993 AND gdp_per_capita <= 765 THEN 'Low income'
    WHEN year = 1993 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1993 AND gdp_per_capita <= 8625 THEN 'Upper-middle income'
    WHEN year = 1993 THEN 'High income'

    -- 1994
    WHEN year = 1994 AND gdp_per_capita <= 765 THEN 'Low income'
    WHEN year = 1994 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1994 AND gdp_per_capita <= 8955 THEN 'Upper-middle income'
    WHEN year = 1994 THEN 'High income'

    -- 1995
    WHEN year = 1995 AND gdp_per_capita <= 765 THEN 'Low income'
    WHEN year = 1995 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1995 AND gdp_per_capita <= 9385 THEN 'Upper-middle income'
    WHEN year = 1995 THEN 'High income'

    -- 1996
    WHEN year = 1996 AND gdp_per_capita <= 765 THEN 'Low Income'
    WHEN year = 1996 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1996 AND gdp_per_capita <= 9645 THEN 'Upper-middle income'
    WHEN year = 1996 THEN 'High income'

    -- 1997
    WHEN year = 1997 AND gdp_per_capita <= 765 THEN 'Low Income'
    WHEN year = 1997 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1997 AND gdp_per_capita <= 9655 THEN 'Upper-middle income'
    WHEN year = 1998 THEN 'High income'

     -- 1998
    WHEN year = 1998 AND gdp_per_capita <= 765 THEN 'Low income'
    WHEN year = 1998 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 1998 AND gdp_per_capita <= 9360 THEN 'Upper-middle income'
    WHEN year = 1998 THEN 'High income'

    -- 1999
    WHEN year = 1999 AND gdp_per_capita <= 755 THEN 'Low income'
    WHEN year = 1999 AND gdp_per_capita <= 2995 THEN 'Lower-middle income'
    WHEN year = 1999 AND gdp_per_capita <= 9265 THEN 'Upper-middle income'
    WHEN year = 1999 THEN 'High income'

    -- 2000
    WHEN year = 2000 AND gdp_per_capita <= 755 THEN 'Low income'
    WHEN year = 2000 AND gdp_per_capita <= 2995 THEN 'Lower-middle income'
    WHEN year = 2000 AND gdp_per_capita <= 9265 THEN 'Upper-middle income'
    WHEN year = 2000 THEN 'High income'

    -- 2001
    WHEN year = 2001 AND gdp_per_capita <= 745 THEN 'Low income'
    WHEN year = 2001 AND gdp_per_capita <= 2975 THEN 'Lower-middle income'
    WHEN year = 2001 AND gdp_per_capita <= 9205 THEN 'Upper-middle income'
    WHEN year = 2001 THEN 'High income'

    -- 2002
    WHEN year = 2002 AND gdp_per_capita <= 735 THEN 'Low income'
    WHEN year = 2002 AND gdp_per_capita <= 2935 THEN 'Lower-middle income'
    WHEN year = 2002 AND gdp_per_capita <= 9075 THEN 'Upper-middle income'
    WHEN year = 2002 THEN 'High income'

    -- 2003
    WHEN year = 2003 AND gdp_per_capita <= 735 THEN 'Low income'
    WHEN year = 2003 AND gdp_per_capita <= 2935 THEN 'Lower-middle income'
    WHEN year = 2003 AND gdp_per_capita <= 9385 THEN 'Upper-middle income'
    WHEN year = 2003 THEN 'High income'

    -- 2004
    WHEN year = 2004 AND gdp_per_capita <= 765 THEN 'Low income'
    WHEN year = 2004 AND gdp_per_capita <= 3035 THEN 'Lower-middle income'
    WHEN year = 2004 AND gdp_per_capita <= 10065 THEN 'Upper-middle income'
    WHEN year = 2004 THEN 'High income'

    -- 2005
    WHEN year = 2005 AND gdp_per_capita <= 875 THEN 'Low income'
    WHEN year = 2005 AND gdp_per_capita <= 3465 THEN 'Lower-middle income'
    WHEN year = 2005 AND gdp_per_capita <= 10725 THEN 'Upper-middle income'
    WHEN year = 2005 THEN 'High income'

    -- 2006
    WHEN year = 2006 AND gdp_per_capita <= 905 THEN 'Low income'
    WHEN year = 2006 AND gdp_per_capita <= 3595 THEN 'Lower-middle income'
    WHEN year = 2006 AND gdp_per_capita <= 11115 THEN 'Upper-middle income'
    WHEN year = 2006 THEN 'High income'

    -- 2007
    WHEN year = 2007 AND gdp_per_capita <= 935 THEN 'Low income'
    WHEN year = 2007 AND gdp_per_capita <= 3705 THEN 'Lower-middle income'
    WHEN year = 2007 AND gdp_per_capita <= 11455 THEN 'Upper-middle income'
    WHEN year = 2007 THEN 'High income'

    -- 2008
    WHEN year = 2008 AND gdp_per_capita <= 975 THEN 'Low income'
    WHEN year = 2008 AND gdp_per_capita <= 3855 THEN 'Lower-middle income'
    WHEN year = 2008 AND gdp_per_capita <= 11905 THEN 'Upper-middle income'
    WHEN year = 2008 THEN 'High income'

    -- 2009
    WHEN year = 2009 AND gdp_per_capita <= 995 THEN 'Low income'
    WHEN year = 2009 AND gdp_per_capita <= 3945 THEN 'Lower-middle income'
    WHEN year = 2009 AND gdp_per_capita <= 12195 THEN 'Upper-middle income'
    WHEN year = 2009 THEN 'High income'

    -- 2010
    WHEN year = 2010 AND gdp_per_capita <= 1005 THEN 'Low income'
    WHEN year = 2010 AND gdp_per_capita <= 3975 THEN 'Lower-middle income'
    WHEN year = 2010 AND gdp_per_capita <= 12275 THEN 'Upper-middle income'
    WHEN year = 2010 THEN 'High income'

     -- 2011
    WHEN year = 2011 AND gdp_per_capita <= 1025 THEN 'Low income'
    WHEN year = 2011 AND gdp_per_capita <= 4035 THEN 'Lower-middle income'
    WHEN year = 2011 AND gdp_per_capita <= 12475 THEN 'Upper-middle income'
    WHEN year = 2011 THEN 'High income'

    -- 2012
    WHEN year = 2012 AND gdp_per_capita <= 1025 THEN 'Low income'
    WHEN year = 2012 AND gdp_per_capita <= 4035 THEN 'Lower-middle income'
    WHEN year = 2012 AND gdp_per_capita <= 12615 THEN 'Upper-middle income'
    WHEN year = 2012 THEN 'High income'

    -- 2013
    WHEN year = 2013 AND gdp_per_capita <= 1035 THEN 'Low income'
    WHEN year = 2013 AND gdp_per_capita <= 4085 THEN 'Lower-middle income'
    WHEN year = 2013 AND gdp_per_capita <= 12745 THEN 'Upper-middle income'
    WHEN year = 2013 THEN 'High income'

    -- 2014
    WHEN year = 2014 AND gdp_per_capita <= 1045 THEN 'Low income'
    WHEN year = 2014 AND gdp_per_capita <= 4125 THEN 'Lower-middle income'
    WHEN year = 2014 AND gdp_per_capita <= 12735 THEN 'Upper-middle income'
    WHEN year = 2014 THEN 'High income'

    -- 2015
    WHEN year = 2015 AND gdp_per_capita <= 1045 THEN 'Low income'
    WHEN year = 2015 AND gdp_per_capita <= 4125 THEN 'Lower-middle income'
    WHEN year = 2015 AND gdp_per_capita <= 12475 THEN 'Upper-middle income'
    WHEN year = 2015 THEN 'High income'

    -- 2016
    WHEN year = 2016 AND gdp_per_capita <= 1025 THEN 'Low income'
    WHEN year = 2016 AND gdp_per_capita <= 4035 THEN 'Lower-middle income'
    WHEN year = 2016 AND gdp_per_capita <= 12235 THEN 'Upper-middle income'
    WHEN year = 2016 THEN 'High income'

    -- 2017
    WHEN year = 2017 AND gdp_per_capita <= 1005 THEN 'Low income'
    WHEN year = 2017 AND gdp_per_capita <= 3955 THEN 'Lower-middle income'
    WHEN year = 2017 AND gdp_per_capita <= 12055 THEN 'Upper-middle income'
    WHEN year = 2017 THEN 'High income'

    -- 2018
    WHEN year = 2018 AND gdp_per_capita <= 1025 THEN 'Low income'
    WHEN year = 2018 AND gdp_per_capita <= 3995 THEN 'Lower-middle income'
    WHEN year = 2018 AND gdp_per_capita <= 12375 THEN 'Upper-middle income'
    WHEN year = 2018 THEN 'High income'

    -- 2019
    WHEN year = 2019 AND gdp_per_capita <= 1035 THEN 'Low income'
    WHEN year = 2019 AND gdp_per_capita <= 4045 THEN 'Lower-middle income'
    WHEN year = 2019 AND gdp_per_capita <= 12535 THEN 'Upper-middle income'
    WHEN year = 2019 THEN 'High income'

    -- 2020
    WHEN year = 2020 AND gdp_per_capita <= 1045 THEN 'Low income'
    WHEN year = 2020 AND gdp_per_capita <= 4095 THEN 'Lower-middle income'
    WHEN year = 2020 AND gdp_per_capita <= 12695 THEN 'Upper-middle income'
    WHEN year = 2020 THEN 'High income'

    -- 2021
    WHEN year = 2021 AND gdp_per_capita <= 1085 THEN 'Low income'
    WHEN year = 2021 AND gdp_per_capita <= 4255 THEN 'Lower-middle income'
    WHEN year = 2021 AND gdp_per_capita <= 13205 THEN 'Upper-middle income'
    WHEN year = 2021 THEN 'High income'

    -- 2022
    WHEN year = 2022 AND gdp_per_capita <= 1135 THEN 'Low income'
    WHEN year = 2022 AND gdp_per_capita <= 4465 THEN 'Lower-middle income'
    WHEN year = 2022 AND gdp_per_capita <= 13845 THEN 'Upper-middle income'
    WHEN year = 2022 THEN 'High income'

    -- 2023
    WHEN year = 2023 AND gdp_per_capita <= 1145 THEN 'Low income'
    WHEN year = 2023 AND gdp_per_capita <= 4515 THEN 'Lower-middle income'
    WHEN year = 2023 AND gdp_per_capita <= 14005 THEN 'Upper-middle income'
    WHEN year = 2023 THEN 'High income'

    -- 2024
    WHEN year = 2024 AND gdp_per_capita <= 1145 THEN 'Low income'
    WHEN year = 2024 AND gdp_per_capita <= 4515 THEN 'Lower-middle income'
    WHEN year = 2024 AND gdp_per_capita <= 14005 THEN 'Upper-middle income'
    WHEN year = 2024 THEN 'High income'

    ELSE 'Unknown'
  END AS income_category
FROM 
pivot 
)

-- créer les clés code-year pour le merge--
SELECT
CONCAT(code,"_",year) as code_year, 
country, 
code, 
year,
gdp_per_capita, 
income_category
FROM category




