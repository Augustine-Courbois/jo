with 

source as (

    select * from {{ source('raw_data', 'population') }}

),

renamed as (

    select
        string_field_0,
        string_field_1,
        string_field_2,
        string_field_3,
        int64_field_4,
        int64_field_5,
        int64_field_6,
        int64_field_7,
        int64_field_8,
        int64_field_9,
        int64_field_10,
        int64_field_11,
        int64_field_12,
        int64_field_13,
        int64_field_14,
        int64_field_15,
        int64_field_16,
        int64_field_17,
        int64_field_18,
        int64_field_19,
        int64_field_20,
        int64_field_21,
        int64_field_22,
        int64_field_23,
        int64_field_24,
        int64_field_25,
        int64_field_26,
        int64_field_27,
        int64_field_28,
        int64_field_29,
        int64_field_30,
        int64_field_31,
        int64_field_32,
        int64_field_33,
        int64_field_34,
        int64_field_35,
        double_field_36,
        int64_field_37,
        double_field_38,
        int64_field_39,
        double_field_40,
        double_field_41,
        double_field_42,
        int64_field_43,
        double_field_44,
        double_field_45,
        int64_field_46,
        int64_field_47,
        double_field_48,
        double_field_49,
        int64_field_50,
        double_field_51,
        int64_field_52,
        int64_field_53,
        int64_field_54,
        double_field_55,
        int64_field_56,
        int64_field_57,
        double_field_58,
        double_field_59,
        double_field_60,
        double_field_61,
        double_field_62,
        int64_field_63,
        double_field_64,
        int64_field_65,
        double_field_66,
        int64_field_67,
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
int64_field_4,
int64_field_5,
int64_field_6,
int64_field_7,
int64_field_8,
int64_field_9,
int64_field_10,
int64_field_11,
int64_field_12,
int64_field_13,
int64_field_14,
int64_field_15,
int64_field_16,
int64_field_17,
int64_field_18,
int64_field_19,
int64_field_20,
int64_field_21,
int64_field_22,
int64_field_23,
int64_field_24,
int64_field_25,
int64_field_26,
int64_field_27,
int64_field_28,
int64_field_29,
int64_field_30,
int64_field_31,
int64_field_32,
int64_field_33,
string_field_68,
string_field_69)
FROM renamed
)

--step2:renommer les colonnes--
, rename AS (
SELECT 
string_field_0 as country,
string_field_1 as code,
int64_field_34 as `1990`,
int64_field_35 as `1991`,
double_field_36 as `1992`,
int64_field_37 as `1993`,
double_field_38 as `1994`,
int64_field_39 as `1995`,
double_field_40 as `1996`,
double_field_41 as `1997`,
double_field_42 as `1998`,
int64_field_43 as `1999`,
double_field_44 as `2000`,
double_field_45 as `2001`,
int64_field_46 as `2002`,
int64_field_47 as `2003`,
double_field_48 as `2004`,
double_field_49 as `2005`,
int64_field_50 as `2006`,
double_field_51 as `2007`,
int64_field_52 as `2008`,
int64_field_53 as `2009`,
int64_field_54 as `2010`,
double_field_55 as `2011`,
int64_field_56 as `2012`,
int64_field_57 as `2013`,
double_field_58 as `2014`,
double_field_59 as `2015`,
double_field_60 as `2016`,
double_field_61 as `2017`,
double_field_62 as `2018`,
int64_field_63 as `2019`,
double_field_64 as `2020`,
int64_field_65 as `2021`,
double_field_66 as `2022`,
int64_field_67 as `2023`
FROM supp
)

--Step3: passage des colonnes en lignes--
, pivot AS (
SELECT 
  country,
  code,
  CAST(annee_col AS INT64) AS year,
  CAST(population AS INT64) AS population
FROM (
  SELECT 
  country, 
  code,
 CAST (`1990` AS FLOAT64) AS `1990`,
CAST (`1991` AS FLOAT64) AS `1991`,
CAST (`1992` AS FLOAT64) AS `1992`,
CAST (`1993` AS FLOAT64) AS `1993`,
CAST (`1994` AS FLOAT64) AS `1994`,
CAST (`1995` AS FLOAT64) AS `1995`,
CAST (`1996` AS FLOAT64) AS `1996`,
CAST (`1997` AS FLOAT64) AS `1997`,
CAST (`1998` AS FLOAT64) AS `1998`,
CAST (`1999` AS FLOAT64) AS `1999`,
CAST (`2000` AS FLOAT64) AS `2000`,
CAST (`2001` AS FLOAT64) AS `2001`,
CAST (`2002` AS FLOAT64) AS `2002`,
CAST (`2003` AS FLOAT64) AS `2003`,
CAST (`2004` AS FLOAT64) AS `2004`,
CAST (`2005` AS FLOAT64) AS `2005`,
CAST (`2006` AS FLOAT64) AS `2006`,
CAST (`2007` AS FLOAT64) AS `2007`,
CAST (`2008` AS FLOAT64) AS `2008`,
CAST (`2009` AS FLOAT64) AS `2009`,
CAST (`2010` AS FLOAT64) AS `2010`,
CAST (`2011` AS FLOAT64) AS `2011`,
CAST (`2012` AS FLOAT64) AS `2012`,
CAST (`2013` AS FLOAT64) AS `2013`,
CAST (`2014` AS FLOAT64) AS `2014`,
CAST (`2015` AS FLOAT64) AS `2015`,
CAST (`2016` AS FLOAT64) AS `2016`,
CAST (`2017` AS FLOAT64) AS `2017`,
CAST (`2018` AS FLOAT64) AS `2018`,
CAST (`2019` AS FLOAT64) AS `2019`,
CAST (`2020` AS FLOAT64) AS `2020`,
CAST (`2021` AS FLOAT64) AS `2021`,
CAST (`2022` AS FLOAT64) AS `2022`,
CAST (`2023` AS FLOAT64) AS `2023`,
FROM rename)

UNPIVOT (
  population FOR annee_col IN (
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

--remplacer les index 2023 en 2024 car nous n'avons pas encore accès aux données 2024--
, date_change AS (
SELECT
country, 
code, 
CASE 
      WHEN year = 2023 THEN 2024
      ELSE year
END AS year,
population
FROM pivot
)

--créer les code_year--
SELECT
CONCAT(code,"_",CAST(year AS STRING)) as code_year, 
country, 
code, 
year,
population
from date_change