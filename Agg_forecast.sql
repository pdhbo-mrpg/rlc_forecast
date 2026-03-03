--****************************************************************************
--Aggregate latest market forecast to a retail month level
--****************************************************************************
CREATE OR REPLACE TABLE DATA_VAULT_DEV.DBT_MBOTHA.RLC_MARKET_FCST_MTH AS
WITH CAL AS
     (
             SELECT DISTINCT
                     calendardate ,
                     retail_year_MONTH
             FROM
                     analytics_vault.dv_info_marts.DIM_CALENDAR
             WHERE
                     shortdayname = 'Sun' ),
    segment_prod as (
    select distinct SEGMENT AS rlc_segment, "rlc product category code"  AS RLC_PRODUCT_LEVEL3_CODE 
    from  DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL
    )

     SELECT
                     s.rlc_segment,
                     c.retail_year_MONTH                    ,
                     f.region_l1 AS RLC_REGION_CODE         ,
                     f.prod_l1   AS RLC_PRODUCT_LEVEL3_CODE ,
                     SUM(fcst)   AS FORECAST
             FROM
                     DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST F
             JOIN
                     CAL c
             ON
                     c.calendardate = f.retail_week
             JOIN SEGMENT_PROD AS s on f.prod_l1 = s.RLC_PRODUCT_LEVEL3_CODE
             GROUP BY
                     1 ,
                     2 ,
                     3 ,
                     4;

--****************************************************************************
--Aggregate latest rom forecast to a retail month level
--****************************************************************************
CREATE OR REPLACE TABLE DATA_VAULT_DEV.DBT_MBOTHA.RLC_ROM_FCST_MTH AS
WITH CAL AS
     (
             SELECT DISTINCT
                     calendardate ,
                     retail_year_MONTH
             FROM
                     analytics_vault.dv_info_marts.DIM_CALENDAR
             WHERE
                     shortdayname = 'Sun' ) ,
                      segment_prod as (
    select distinct SEGMENT AS rlc_segment, "rlc product category code"  AS RLC_PRODUCT_LEVEL3_CODE 
    from  DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL
    ),
     divf AS
     (
             SELECT
                     c.retail_year_MONTH                    ,
                     f.region_l1 AS RLC_REGION_CODE         ,
                     f.prod_l1   AS RLC_PRODUCT_LEVEL3_CODE ,
                     SUM(fcst)   AS DIVISIONAL_FCST
             FROM
                     DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST F
             JOIN
                     CAL c
             ON
                     c.calendardate = f.retail_week
             WHERE
                     ENTITY_L1 IN ( 'MRP' ,
                                    'MIL' ,
                                    'POW' ,
                                    'HOM' ,
                                    'SST' ,
                                    'YCH' )
             GROUP BY
                     1 ,
                     2 ,
                     3 
                      ) ,
     mktf AS
     (
             SELECT
                     c.retail_year_MONTH                    ,
                     f.region_l1 AS RLC_REGION_CODE         ,
                     f.prod_l1   AS RLC_PRODUCT_LEVEL3_CODE ,
                     SUM(fcst)   AS MARKET_FCST
             FROM
                     DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST F
             JOIN
                     CAL c
             ON
                     c.calendardate = f.retail_week
             GROUP BY
                     1 ,
                     2 ,
                     3 )
SELECT
        s.rlc_segment,
        d.retail_year_month       ,
        d.RLC_REGION_CODE         ,
        d.RLC_PRODUCT_LEVEL3_CODE ,
        m.MARKET_FCST - d.DIVISIONAL_FCST AS FORECAST
FROM
        divf d
JOIN segment_prod as s on d.RLC_PRODUCT_LEVEL3_CODE =s.RLC_PRODUCT_LEVEL3_CODE 
JOIN
        mktf m
   ON d.retail_year_MONTH = m.retail_year_MONTH and d.RLC_REGION_CODE = m.RLC_REGION_CODE AND d.RLC_PRODUCT_LEVEL3_CODE = m.RLC_PRODUCT_LEVEL3_CODE;

--****************************************************************************
--Aggregate latest weekly DIVISIONAL forecast to a retail month level
--****************************************************************************
CREATE OR REPLACE TABLE DATA_VAULT_DEV.DBT_MBOTHA.RLC_DIVISION_FCST_MTH AS
WITH CAL AS
     (
             SELECT DISTINCT
                     calendardate ,
                     retail_year_MONTH
             FROM
                     analytics_vault.dv_info_marts.DIM_CALENDAR
             WHERE
                     shortdayname = 'Sun' )


             SELECT
                    
                     c.retail_year_MONTH                    ,
                     f.region_l1 AS RLC_REGION_CODE         ,
                     f.prod_l1   AS RLC_PRODUCT_LEVEL3_CODE ,
                     CASE
                     WHEN
                             ENTITY_L1 = 'MRP'
                     THEN
                             1
                     WHEN
                             ENTITY_L1 = 'MIL'
                     THEN
                             4
                     WHEN
                             ENTITY_L1 = 'POW'
                     THEN
                             12
                     WHEN
                             ENTITY_L1 = 'HOM'
                     THEN
                             2
                     WHEN
                             ENTITY_L1 = 'SST'
                     THEN
                             3
                     WHEN
                             ENTITY_L1 = 'YCH'
                     THEN
                             13
                     ELSE
                             99
                     END         AS DIVISIONID              ,
                     SUM(fcst)   AS FORECAST
             FROM
                     DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST F
             JOIN
                     CAL c
             ON
                     c.calendardate = f.retail_week
             
             WHERE
                     ENTITY_L1 IN ( 'MRP' ,
                                    'MIL' ,
                                    'POW' ,
                                    'HOM' ,
                                    'SST' ,
                                    'YCH' )
             GROUP BY
                     1 ,
                     2 ,
                     3 ,
                     4;