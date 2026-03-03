--****************************************************************************
--Create Weekly history table for input into Multidimensional Prophet Forecast model
--****************************************************************************
CREATE OR REPLACE TABLE DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_HIST_WEEK AS
WITH wkdate AS
     (
             SELECT DISTINCT
                     calendardate ,
                     retail_year_week
             FROM
                     data_vault.dv_info_marts.DIM_CALENDAR
             WHERE
                     shortdayname = 'Sun' ) ,
     --Only include where there is at least 2 years worth of sales data
     --TODO: substitute moving average as forecast for these records excluded
     qual1 AS
     (
             SELECT
                     "rlc region code"           ,
                     "COUNTRY"                   ,
                     "rlc product category code" ,
                     "rlc level 1.5"             ,
                     "rlc level 1"               ,
                     "sk division"               ,
                     count(DISTINCT "retail week") AS wkcount
             FROM
                     DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL
             WHERE
                     "sk division" IN ( 1 ,
                                        2 ,
                                        3 ,
                                        4 ,
                                        12 ,
                                        13 )
             AND     "sales"       > 0
             AND     "rlc exclude" = 'False'
             GROUP BY
                     1 ,
                     2 ,
                     3 ,
                     4 ,
                     5 ,
                     6
             HAVING
                     count(DISTINCT "retail week") >= 104 ) ,
     --Only include history from week 40 of 2023
     qual2 AS
     (
             SELECT DISTINCT
                     "retail week"
             FROM
                    DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL
             WHERE
                     "retail week" >= 202340 ) ,
     --Include all possible combinations
     qual3 AS
     (
             SELECT
                     "retail week"               ,
                     "rlc region code"           ,
                     "COUNTRY"                   ,
                     "rlc product category code" ,
                     "rlc level 1.5"             ,
                     "rlc level 1"               ,
                     "sk division"
             FROM
                     qual2
             CROSS JOIN
                     qual1 ) ,
     --For apparel segment use Mr Price's hierarchy for forecasting and MinT reconciliation for all divisions
     app_h AS
     (
             SELECT DISTINCT
                     "rlc product category code" ,
                     "rlc level 1.5"             ,
                     "rlc level 1"
             FROM
                     qual3
             WHERE
                     "sk division" = 1 ) ,
     --Reformat history for apparel segment to accomodate forecasting and hierarchical reconciliation
     APP AS
     (
             SELECT
                     w.calendardate                                     ,
                     q."rlc region code"                   AS region_l1 ,
                     q.COUNTRY                             AS region_l2 ,
                     'AllRegion'                           AS region_l3 ,
                     q."rlc product category code"         AS prod_l1   ,
                     h."rlc level 1.5"                     AS prod_l2   ,
                     h."rlc level 1"                       AS prod_l3   ,
                     'AllProd'                             AS prod_l4   ,
                     CASE
                     WHEN
                             q."sk division" = 1
                     THEN
                             'MRP'
                     WHEN
                             q."sk division" = 4
                     THEN
                             'MIL'
                     WHEN
                             q."sk division" = 12
                     THEN
                             'POW'
                     ELSE
                             ''
                     END                                   AS entity_l1 ,
                     'GRP'                                 AS entity_l2 ,
                     'MKT'                                 AS entity_l3 ,
                     greatest(0, sum(NVL("act sales", 0))) AS sales
             FROM
                     qual3 q
             LEFT OUTER JOIN
                    DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL D
             using
                     ( "retail week" ,"sk division" ,"rlc region code" ,"rlc product category code" )
             JOIN
                     wkdate w
             ON
                     w.retail_year_week = q."retail week"
             JOIN
                     app_h h
             ON
                     h."rlc product category code" = q."rlc product category code"
             WHERE
                     q."sk division" IN ( 1 ,
                                          4 ,
                                          12 ) --only MRP, MIL and PWF
             AND     q."retail week" >= 202340
             GROUP BY
                     1  ,
                     2  ,
                     3  ,
                     4  ,
                     5  ,
                     6  ,
                     7  ,
                     8  ,
                     9  ,
                     10 ,
                     11 ) ,
     --For homeware segment use MRP Home's hiearchy for all divisions
     hom_h AS
     (
             SELECT DISTINCT
                     "rlc product category code" ,
                     "rlc level 1.5"             ,
                     "rlc level 1"
             FROM
                     qual3
             WHERE
                     "sk division" = 2 ) ,
     --Reformat history for homeware segment to accomodate forecasting and hierarchical reconciliation
     HOMW AS
     (
             SELECT
                     w.calendardate                                     ,
                     q."rlc region code"                   AS region_l1 ,
                     q.COUNTRY                             AS region_l2 ,
                     'AllRegion'                           AS region_l3 ,
                     q."rlc product category code"         AS prod_l1   ,
                     h."rlc level 1.5"                     AS prod_l2   ,
                     h."rlc level 1"                       AS prod_l3   ,
                     'AllProd'                             AS prod_l4   ,
                     CASE
                     WHEN
                             q."sk division" = 2
                     THEN
                             'HOM'
                     WHEN
                             q."sk division" = 3
                     THEN
                             'SST'
                     WHEN
                             q."sk division" = 13
                     THEN
                             'YCH'
                     ELSE
                             ''
                     END                                   AS entity_l1 ,
                     'GRP'                                 AS entity_l2 ,
                     'MKT'                                 AS entity_l3 ,
                     greatest(0, sum(NVL("act sales", 0))) AS sales
             FROM
                     qual3 q
             LEFT OUTER JOIN
                     DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL D
             using
                     ( "retail week" ,"sk division" ,"rlc region code" ,"rlc product category code" )
             JOIN
                     wkdate w
             ON
                     w.retail_year_week = q."retail week"
             JOIN
                     hom_h h
             ON
                     h."rlc product category code" = q."rlc product category code"
             WHERE
                     q."sk division" IN ( 2 ,
                                          3 ,
                                          13 ) --only for HOM,SST and YCH
             GROUP BY
                     1  ,
                     2  ,
                     3  ,
                     4  ,
                     5  ,
                     6  ,
                     7  ,
                     8  ,
                     9  ,
                     10 ,
                     11 ) ,
     --Use MRP and HOM hierarchies for market data
     cons_h AS
     (
             SELECT DISTINCT
                     "rlc product category code" ,
                     "rlc level 1.5"             ,
                     "rlc level 1"
             FROM
                     qual3
             WHERE
                     "sk division" = 1
             
             UNION ALL
             
             SELECT DISTINCT
                     "rlc product category code" ,
                     "rlc level 1.5"             ,
                     "rlc level 1"
             FROM
                     qual3
             WHERE
                     "sk division" = 2 ) ,
     --Reformat history for market to accomodate forecasting and hierarchical reconciliation
     MKT AS
     (
             SELECT
                     w.calendardate                                        ,
                     q."rlc region code"                      AS region_l1 ,
                     q.COUNTRY                                AS region_l2 ,
                     'AllRegion'                              AS region_l3 ,
                     q."rlc product category code"            AS prod_l1   ,
                     h."rlc level 1.5"                        AS prod_l2   ,
                     h."rlc level 1"                          AS prod_l3   ,
                     'AllProd'                                AS prod_l4   ,
                     'MKT'                                    AS entity_l1 ,
                     max(MARKET_RAND_SALES) AS sales
                     --greatest(0, MAX(nvl("market sales", 0))) AS sales --take largest value across all divisions
             FROM
                     qual3 q
             inner JOIN
             DATA_VAULT_DEV.DBT_MBOTHA.RLC_MARKET_FACT_WEEK D
             ON q."retail week"=d.RETAIL_WEEK and q."rlc region code"=d.RLC_REGION_CODE and q."rlc product category code" = d.RLC_PRODUCT_LEVEL3_CODE
                     --ANALYTICS_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL D
             -- using
             --         ( "retail week" ,"sk division" ,"rlc region code" ,"rlc product category code" )
             JOIN
                     wkdate w
             ON
                     w.retail_year_week = q."retail week"
             JOIN
                     cons_h h
             ON
                     h."rlc product category code" = q."rlc product category code"
             WHERE
                     q."sk division" IN ( 1 ,
                                          2 ,
                                          3 ,
                                          4 ,
                                          12 ,
                                          13 ) --all divisions
             GROUP BY
                     1 ,
                     2 ,
                     3 ,
                     4 ,
                     5 ,
                     6 ,
                     7 ,
                     8 ,
                     9
                     ) ,
     --Reformat history for the group to accomodate forecasting and hierarchical reconciliation
     GRP AS
     (
             SELECT
                     w.calendardate                                     ,
                     q."rlc region code"                   AS region_l1 ,
                     q.COUNTRY                             AS region_l2 ,
                     q."rlc product category code"         AS prod_l1   ,
                     h."rlc level 1.5"                     AS prod_l2   ,
                     h."rlc level 1"                       AS prod_l3   ,
                     'GRP'                                 AS entity_l1 ,
                     greatest(0, sum(nvl("act sales", 0))) AS sales
             FROM
                     qual3 q
             LEFT OUTER JOIN
                     DATA_VAULT.DV_INFO_MARTS.REPORT_RLC_DIVISIONAL D
             using
                     ( "retail week" ,"sk division" ,"rlc region code" ,"rlc product category code" )
             JOIN
                     wkdate w
             ON
                     w.retail_year_week = q."retail week"
             JOIN
                     cons_h h
             ON
                     h."rlc product category code" = q."rlc product category code"
             WHERE
                     q."sk division" IN ( 1 ,
                                          2 ,
                                          3 ,
                                          4 ,
                                          12 ,
                                          13 ) --all divisions
             GROUP BY
                     1 ,
                     2 ,
                     3 ,
                     4 ,
                     5 ,
                     6 ,
                     7 ) ,
     --Reformat history for the rest of the market to accomodate forecasting and hierarchical reconciliation
     ROM AS
     (
             SELECT
                     M.calendardate                                       ,
                     M.region_l1                                          ,
                     M.region_l2                                          ,
                     M.region_l3                                          ,
                     M.prod_l1                                            ,
                     M.prod_l2                                            ,
                     M.prod_l3                                            ,
                     M.prod_l4                                            ,
                     'ROM'                                   AS entity_l1 ,
                     'ROM'                                   AS entity_l2 ,
                     'MKT'                                   AS entity_l3 ,
                     greatest(0, G.sales, M.sales - G.sales) AS sales
             FROM
                     MKT M
             JOIN
                     GRP G
             USING
                     ( calendardate ,region_l1 ,prod_l1 ) )
SELECT
        *
FROM
        APP

UNION ALL

SELECT
        *
FROM
        HOMW

UNION ALL

SELECT
        *
FROM
        ROM;


--**************************************************************************************
--Interpolate missing/zero values
--*************************************************************
MERGE INTO
        DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_HIST_WEEK t
USING
        ( WITH RECURSIVE src AS
        (
                SELECT
                        prod_l1      ,
                        region_l1    ,
                        entity_l1    ,
                        calendardate ,
                        NULLIF(sales, 0) AS sales_nz
                FROM
                        DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_HIST_WEEK ) ,ord AS
        (
                SELECT
                        * ,
                        ROW_NUMBER() OVER
                                (
                                        PARTITION BY
                                                prod_l1   ,
                                                region_l1 ,
                                                entity_l1
                                        ORDER BY
                                                calendardate
                                )
                        AS rn
                FROM
                        src ) ,prep AS
        (
                SELECT
                        * ,
                        FIRST_VALUE(sales_nz) IGNORE NULLS OVER
                                (
                                        PARTITION BY
                                                prod_l1   ,
                                                region_l1 ,
                                                entity_l1
                                        ORDER BY
                                                rn
                                        ROWS BETWEEN
                                                CURRENT ROW AND UNBOUNDED FOLLOWING
                                )
                        AS next_valid_val
                FROM
                        ord ) ,rec AS
        (
                SELECT
                        prod_l1        ,
                        region_l1      ,
                        entity_l1      ,
                        calendardate   ,
                        rn             ,
                        sales_nz       ,
                        next_valid_val ,
                        CASE
                        WHEN
                                sales_nz IS NOT NULL
                        THEN
                                sales_nz
                        ELSE
                                next_valid_val
                        END AS sales_filled
                FROM
                        prep
                WHERE
                        rn = 1
                
                UNION ALL
                
                SELECT
                        p.prod_l1        ,
                        p.region_l1      ,
                        p.entity_l1      ,
                        p.calendardate   ,
                        p.rn             ,
                        p.sales_nz       ,
                        p.next_valid_val ,
                        CASE
                        WHEN
                                p.sales_nz IS NOT NULL
                        THEN
                                p.sales_nz
                        WHEN
                                p.next_valid_val IS NULL
                        THEN
                                r.sales_filled
                        ELSE
                                (r.sales_filled + p.next_valid_val) / 2.0
                        END AS sales_filled
                FROM
                        rec r
                JOIN
                        prep p
                ON
                        p.prod_l1   = r.prod_l1
                AND     p.region_l1 = r.region_l1
                AND     p.entity_l1 = r.entity_l1
                AND     p.rn        = r.rn + 1 )
        SELECT
                prod_l1      ,
                region_l1    ,
                entity_l1    ,
                calendardate ,
                sales_filled
        FROM
                rec ) s ON t.prod_l1 = s.prod_l1
        AND t.region_l1              = s.region_l1
        AND t.entity_l1              = s.entity_l1
        AND t.calendardate           = s.calendardate
WHEN MATCHED
AND     (
                t.sales IS NULL OR t.sales = 0 ) THEN
        UPDATE
        SET
                t.sales = s.sales_filled;

--****************************************************************************
--Substitute missing or small value data with lowest value in history greater than 9
--****************************************************************************
UPDATE
        DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_HIST_WEEK t
SET
        sales = sub.min_val
FROM
        (
                SELECT
                        s.region_l1 ,
                        s.prod_l1   ,
                        s.entity_l1 ,
                        MIN(s.sales) AS min_val
                FROM
                        DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_HIST_WEEK s
                WHERE
                        s.sales > 9 -- Ensure we get a meaningful minimum
                GROUP BY
                        s.region_l1 ,
                        s.prod_l1   ,
                        s.entity_l1 ) AS sub
WHERE
        t.region_l1 = sub.region_l1
AND     t.prod_l1   = sub.prod_l1
AND     t.entity_l1 = sub.entity_l1
AND     t.sales     < 10;