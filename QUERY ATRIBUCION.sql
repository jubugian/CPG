CREATE OR REPLACE TABLE `calm-mariner-105612.CPG.ATRIBUCION_CPG`
PARTITION BY eventDate
CLUSTER BY site, platform
OPTIONS(
  description = "Tabla temporal con data de Atribución de los últimos 7 dias"
) AS
with  
dateFrom AS (SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 25 DAY))
, dateTo AS (SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

, orders    AS (
                SELECT
                       ORD_CREATED_DT           AS ord_DateCreated
                     , TRIM(UPPER(SIT_SITE_ID)) AS ord_Site
                     , ORD_ORDER_ID             AS ord_OrderId
                     , ORD_PACK_ID              AS ord_PackId
                     , ORD_ITEM.ID              AS ord_ItemId
                     , ORD_ITEM.QTY             AS ord_ItemQty
                     , ORD_ITEM.UNIT_PRICE      AS ord_unitPrice --
                     , ORD_TGMV_FLG             AS TGMV_FLG --
                     , CC_USD_RATIO             AS CC_USD_RATIO --
                     , ORD_BUYER.ID             AS ord_BuyerId
                     , ORD_CATEGORY.ID_L1       AS ID_L1
                     , ORD_CATEGORY.NAME_L1     AS NAME_L1
                     , ORD_CATEGORY.ID_L2       AS ID_L2
                     , ORD_CATEGORY.NAME_L2     AS NAME_L2
                     , ORD_CATEGORY.ID_L3       AS ID_L3
                     , ORD_CATEGORY.NAME_L3     AS NAME_L3
                     , ORD_ITEM.DEALS           AS ITEM_DEALS
                FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS`
                WHERE ORD_CREATED_DT >= (SELECT * FROM dateFrom)
                  AND ORD_CREATED_DT <= (SELECT * FROM dateTo)
                  AND ORD_ITEM.SUPERMARKET_FLG = true
                  AND ORD_STATUS <> 'cancelled'
                  AND TRIM(UPPER(SIT_SITE_ID)) IN ('MLA','MLM','MLB','MCO') --= 'MLB'
               )
-- Transational Purchase (cart)
, purchase  AS (
                SELECT DISTINCT
                       PCK_PACK_ID          AS pack_PackId
                     , PCK_PACK_PURCHASE_ID AS pack_PurchaseId
                FROM `meli-bi-data.WHOWNER.BT_CRT_PACKS`
                WHERE DATE(PCK_PACK_DATE_CREATED_DTTM) >= (SELECT * FROM dateFrom)
                  AND DATE(PCK_PACK_DATE_CREATED_DTTM) <= (SELECT * FROM dateTo)
                  AND PCK_PACK_ID IN (SELECT DISTINCT ord_PackId FROM orders)
               )
, trx       AS (
                SELECT 
                       o.ord_DateCreated  
                     , CAST(COALESCE(p.pack_PurchaseId, o.ord_OrderId) AS STRING) AS ord_trxId
                     , o.ord_Site
                     , o.ord_OrderId
                     , o.ord_PackId as ord_PackId
                     , CONCAT(o.ord_Site, CAST(o.ord_ItemId AS STRING)) AS ord_ItemId
                     , o.ord_ItemQty
                     , o.ord_unitPrice        AS ord_unitPrice --
                     , o.TGMV_FLG             AS TGMV_FLG --
                     , o.CC_USD_RATIO         AS CC_USD_RATIO --
                     , o.ord_BuyerId
                     , o.ID_L1
                     , o.NAME_L1
                     , o.ID_L2
                     , o.NAME_L2
                     , o.ID_L3
                     , o.NAME_L3
                     , o.ITEM_DEALS
                FROM orders o
                LEFT JOIN purchase p ON o.ord_PackId = p.pack_PackId
               )
, gaEcomm   AS (
                SELECT DISTINCT
                       date                AS ge_Date
                     , TRIM(UPPER(site))   AS ge_Site
                     , fullVisitorId       AS ge_FullVisitorId
                     , sessionId           AS ge_sessionId
                     , trackTime           AS ge_trackTime
                     , transactionId       AS ge_TransactionId
                     , checkoutType        AS ge_CheckoutType
                     , productQuantity     AS ge_ProductQuantity
                     , productRevenue      AS ge_ProductRevenue
                     , TRIM(UPPER(itemId)) AS ge_ItemId
                FROM `calm-mariner-105612.ALL_GA360.LK_Ecommerce_ML`
                WHERE date >= (SELECT * FROM dateFrom)
                  AND date <= (SELECT * FROM dateTo)
                  AND businessUnit = 'ML'  
                  AND site IN ('MLA','MLM','MLB','MCO') --= 'MLB'
               )
-- Genera tabla temporal: BT_SiteMerch_EcommerceTmp
, sqlFinalOrders  AS (
                      SELECT
                             g.ge_date                              AS date
                           , g.ge_site                              AS site
                           , g.ge_fullVisitorId                     AS fullVisitorId
                           , g.ge_sessionId                         AS sessionId
                           , g.ge_trackTime                         AS trackTime
                           , g.ge_transactionId                     AS transactionId
                           , g.ge_checkoutType                      AS checkoutType
                           , g.ge_productQuantity                   AS productQuantity
                           , ROUND(g.ge_productRevenue/1000000, 2)  AS productRevenue
                           , t.ord_DateCreated                      AS ord_DateCreated
                           , t.ord_BuyerId                          AS buyerId
                           , t.ord_OrderId                          AS orderId
                           , t.ord_ItemId                           AS itemId
                           , t.ord_PackId                           AS PackId
                           , t.ID_L1
                           , t.NAME_L1
                           , t.ID_L2
                           , t.NAME_L2
                           , t.ID_L3
                           , t.NAME_L3
                           , t.ITEM_DEALS
                           , t.TGMV_FLG                            AS tgmvFlg
                           , t.ord_unitPrice                       AS unitPrice
                           , t.ord_ItemQty                         AS ord_ItemQty 
                           , t.CC_USD_RATIO                        AS CC_USD_RATIO
                           , RANK() OVER (PARTITION BY t.ord_OrderId ORDER BY g.ge_trackTime desc ) AS rnkNbr
                      FROM gaEcomm g
                      INNER JOIN trx t
                      ON    g.ge_site            = t.ord_Site    AND
                            g.ge_transactionId   = t.ord_trxId   AND
                            g.ge_itemId          = t.ord_ItemId  AND
                            g.ge_productQuantity = t.ord_ItemQty
                     )
, ordersFinal AS (
                  SELECT
                         date
                       , site
                       , fullVisitorId
                       , sessionId
                       , trackTime
                       , transactionId
                       , checkoutType
                       --, productQuantity
                       , productRevenue
                       , tgmvFlg --
                       , unitPrice --
                       , ord_ItemQty --
                       , CC_USD_RATIO
                       , ord_DateCreated
                       , buyerId
                       , orderId
                       , itemId
                       , PackId
                       , ID_L1
                       , NAME_L1
                       , ID_L2
                       , NAME_L2
                       , ID_L3
                       , NAME_L3
                       , ITEM_DEALS
                  FROM  sqlFinalOrders
                  WHERE rnkNbr = 1
                 )
, ga_sessions AS (
                  SELECT
                         date
                       , site
                       , fullVisitorId
                       , visitStartTime
                       , hits
                  FROM `calm-mariner-105612.ALL_GA360.ga_sessions` ga
                  WHERE date >= (SELECT * FROM dateFrom)
                   and date <= (SELECT * FROM dateTo)
                   AND businessUnit = 'ML'
                   AND site IN ('MLA','MLM','MLB','MCO')
                   --AND site = 'MLB'
                   
                 )
, sessions    AS (
                  SELECT
                         date AS ga_sessions_date
                       , CONCAT(fullVisitorId,visitStartTime) as sessionID
                       , CASE
                          WHEN ga_sessions.site = 'MLA' THEN (SELECT EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(CAST(((visitStartTime*1000)+hits.time) AS INT64)) AT TIME ZONE "America/Argentina/Buenos_Aires"))
                          WHEN ga_sessions.site = 'MLB' THEN (SELECT EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(CAST(((visitStartTime*1000)+hits.time) AS INT64)) AT TIME ZONE "America/Sao_Paulo"))
                          WHEN ga_sessions.site = 'MLM' THEN (SELECT EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(CAST(((visitStartTime*1000)+hits.time) AS INT64)) AT TIME ZONE "America/Mexico_City"))
                          WHEN ga_sessions.site = 'MCO' THEN (SELECT EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(CAST(((visitStartTime*1000)+hits.time) AS INT64)) AT TIME ZONE "America/Bogota"))
                         END AS trackTimeLocal
                       , Site AS gaSite
                       , ga_sessions.fullVisitorId AS fullvisitorid
                       , hits.eventInfo.eventLabel  AS eventlabel
                       , CASE
                          WHEN hits.appInfo.appName='MercadoLibre iOS' THEN 'iOS'
                          WHEN hits.appInfo.appName= 'MercadoLibre Android' THEN 'Android'
                          WHEN ( SELECT value FROM hits.customDimensions WHERE index=1 ) IN ('desktop', 'tablet', 'UNKNOWN', 'forced_desktop') THEN 'Web Desktop'
                          WHEN ( SELECT value FROM hits.customDimensions WHERE index=1 ) = 'mobile' THEN 'Web Mobile'
                         END AS Platform
                         , CASE
                           WHEN (hits.eventInfo.eventLabel ) LIKE '%L_=%'    THEN SUBSTR((hits.eventInfo.eventLabel ),STRPOS(s.eventLabel,'=')+1)
                           WHEN (hits.eventInfo.eventLabel ) LIKE '%DEAL%=%' THEN SUBSTR((hits.eventInfo.eventLabel ),STRPOS(s.eventLabel,'=')+4)
                          END AS IDcategoryOrDeal
                        , CASE
                           WHEN (hits.eventInfo.eventLabel ) LIKE '%L_=%'    THEN 'L'|| SUBSTR((hits.eventInfo.eventLabel ),STRPOS(s.eventLabel,'=')-1,1)
                           WHEN (hits.eventInfo.eventLabel ) LIKE '%DEAL%=%' THEN 'DEAL'
                          END AS categoryOrDeal
                  FROM ga_sessions, UNNEST(ga_sessions.hits) as hits
                  WHERE (hits.eventInfo.eventAction ) LIKE '%SUPERMERCADO%'
                    AND (hits.eventInfo.eventCategory ) LIKE '%LANDINGS%'
                    AND (
                         (hits.eventInfo.eventLabel ) LIKE '%DEAL%'
                      OR (hits.eventInfo.eventLabel ) LIKE '%\\_L1=%'
                      OR (hits.eventInfo.eventLabel ) LIKE '%\\_L2=%'
                      OR (hits.eventInfo.eventLabel ) LIKE '%\\_L3=%'
                        )
                  GROUP BY 1, 2, 3, 4, 5, 6, 7
                 )
/*, sqlFinalHits AS (
                   SELECT
                          s.ga_sessions_date
                        , s.trackTimeLocal
                        , s.ga_sessions_site AS gaSite
                        , s.fullvisitorid
                        , s.eventlabel
                        , s.Platform
                        , s.sessionID 
                        , CASE
                           WHEN s.eventLabel LIKE '%L_=%'    THEN SUBSTR(s.eventLabel,STRPOS(s.eventLabel,'=')+1)
                           WHEN s.eventLabel LIKE '%DEAL%=%' THEN SUBSTR(s.eventLabel,STRPOS(s.eventLabel,'=')+4)
                          END AS IDcategoryOrDeal
                        , CASE
                           WHEN s.eventLabel LIKE '%L_=%'    THEN 'L'|| SUBSTR(s.eventLabel,STRPOS(s.eventLabel,'=')-1,1)
                           WHEN s.eventLabel LIKE '%DEAL%=%' THEN 'DEAL'
                          END AS categoryOrDeal
                   FROM sessions AS s
                  )
, finalHits AS    (
                   SELECT
                          ga_sessions_date
                        , trackTimeLocal
                        , gaSite
                        , fullvisitorid
                        , eventlabel
                        , Platform
                        , sessionID
                        , IDcategoryOrDeal
                        , categoryOrDeal
                FROM sqlFinalHits
               )*/
, sqlfinal AS (
               SELECT DISTINCT
                      h.ga_sessions_date AS ga_sessions_date
                    , h.trackTimeLocal   AS trackTimeLocal
                    , h.gaSite           AS gaSite
                    , h.fullvisitorid    AS fullvisitorid
                    , h.eventlabel       AS eventlabel
                    , h.Platform         AS Platform
                    , h.sessionId        AS sessionID
                    , h.IDcategoryOrDeal AS IDcategoryOrDeal
                    , h.categoryOrDeal   AS categoryOrDeal
                    , o.date             AS ord_date
                    , o.site             AS ord_site
                    , o.fullVisitorId    AS ord_fullVisitorId
                    , o.sessionId        AS ord_sessionId
                    , o.trackTime        AS ord_trackTime
                    , o.transactionId    AS ord_transactionId
                    , o.checkoutType     AS ord_checkoutType
                    --, o.productQuantity  AS ord_productQuantity
                    , o.productRevenue   AS ord_productRevenue
                    , o.unitPrice        AS unitPrice
                    , o.tgmvFlg          AS tgmvFlg
                    , o.ord_ItemQty      AS ord_ItemQty
                    , o.CC_USD_RATIO     AS CC_USD_RATIO
                    , o.ord_DateCreated  AS ord_ord_DateCreated
                    , o.buyerId          AS ord_buyerId
                    , o.orderId          AS ord_orderId
                    , o.itemId           AS ord_itemId
                    , o.PackId           AS ord_PackId
                    , o.ID_L1            AS ord_ID_L1
                    , o.NAME_L1          AS ord_NAME_L1
                    , o.ID_L2            AS ord_ID_L2
                    , o.NAME_L2          AS ord_NAME_L2
                    , o.ID_L3            AS ord_ID_L3
                    , o.NAME_L3          AS ord_NAME_L3
                    , RANK() OVER (PARTITION BY o.orderId ORDER BY h.trackTimeLocal desc ) AS RnkAttr
               FROM sessions h --finalHits h
               INNER JOIN ordersFinal o
                    ON h.fullVisitorId   = o.fullVisitorId
                   AND h.trackTimeLocal <= o.trackTime
                   AND h.gaSite          = o.site  
               WHERE (h.categoryOrDeal = 'DEAL' AND SAFE_CAST(h.IDcategoryOrDeal AS INTEGER) IN UNNEST(o.ITEM_DEALS))
                  OR (h.categoryOrDeal = 'L1'   AND SAFE_CAST(h.IDcategoryOrDeal AS INTEGER) = o.ID_L1)
                  OR (h.categoryOrDeal = 'L2'   AND SAFE_CAST(h.IDcategoryOrDeal AS INTEGER) = o.ID_L2)
                  OR (h.categoryOrDeal = 'L3'   AND SAFE_CAST(h.IDcategoryOrDeal AS INTEGER) = o.ID_L3)
               )
, sqlAttr  AS (
               SELECT * EXCEPT (RnkAttr)
               FROM sqlFinal
               WHERE RnkAttr = 1
              )

, AttrFinal AS (
    SELECT DISTINCT
      h.ga_sessions_date AS ga_sessions_date
    , h.trackTimeLocal   AS trackTimeLocal
    , h.gaSite           AS gaSite
    , h.fullvisitorid    AS fullvisitorid
    , h.sessionId        AS sessionId
    , h.eventlabel       AS eventlabel
    , h.Platform         AS Platform
    , h.IDcategoryOrDeal AS IDcategoryOrDeal
    , h.categoryOrDeal   AS categoryOrDeal
    , a.ord_date AS ord_date
    , a.ord_site AS ord_site
    , a.ord_fullVisitorId AS ord_fullVisitorId
    , a.ord_sessionId AS ord_sessionId
    , a.ord_trackTime AS ord_trackTime
    , a.ord_transactionId AS ord_transactionId
    , a.ord_checkoutType AS ord_checkoutType
    --, a.ord_productQuantity AS ord_productQuantity
    , a.unitPrice           AS unitPrice 
    , a.tgmvFlg             AS tgmvFlg 
    , a.ord_ItemQty         AS ord_ItemQty
    , a.CC_USD_RATIO        AS CC_USD_RATIO
    , a.ord_productRevenue AS ord_productRevenue
    , a.ord_ord_DateCreated AS ord_ord_DateCreated
    , a.ord_buyerId AS ord_buyerId
    , a.ord_orderId AS ord_orderId
    , a.ord_itemId AS ord_itemId
    , a.ord_PackId AS ord_PackId
    , a.ord_ID_L1 AS ord_ID_L1
    , a.ord_NAME_L1 AS ord_NAME_L1
    , a.ord_ID_L2  AS ord_ID_L2
    , a.ord_NAME_L2  AS ord_NAME_L2
    , a.ord_ID_L3  AS ord_ID_L3
    , a.ord_NAME_L3  AS ord_NAME_L3
    , ROUND(
             CASE
                  WHEN tgmvFlg is TRUE THEN ord_ItemQty * unitPrice * CC_USD_RATIO
                  ELSE 0
                 END
             , 2
            ) AS TGMV
    , TIMESTAMP_DIFF(TIMESTAMP(a.ord_trackTime), TIMESTAMP(h.trackTimeLocal), HOUR) <= 24 AS attr24h
    , TIMESTAMP_DIFF(TIMESTAMP(a.ord_trackTime), TIMESTAMP(h.trackTimeLocal), HOUR) <= 48 AS attr48h
    , TIMESTAMP_DIFF(TIMESTAMP(a.ord_trackTime), TIMESTAMP(h.trackTimeLocal), HOUR) <= 72 AS attr72h
    , TIMESTAMP_DIFF(TIMESTAMP(a.ord_trackTime), TIMESTAMP(h.trackTimeLocal), HOUR) <= 168 AS attr168h
FROM finalHits h
LEFT JOIN sqlAttr  a
   ON h.ga_sessions_date  = a.ga_sessions_date
  AND h.trackTimeLocal    = a.trackTimeLocal
  AND h.gaSite            = a.gaSite
  AND h.fullvisitorid     = a.fullvisitorid
  AND h.eventlabel        = a.eventlabel
  AND h.Platform          = a.Platform
  AND h.IDcategoryOrDeal  = a.IDcategoryOrDeal
  AND h.categoryOrDeal    = a.categoryOrDeal
  --GROUP BY 1, 2, 3, 4, 5,6, 7, 8,9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
  --, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
    )


SELECT
       s.eventLabel                 AS eventLabel
     , s.Platform                   AS Platform
     , s.gaSite                     AS Site
     , s.ga_sessions_date           AS eventDate
     , COUNT(DISTINCT s.sessionID ) AS totalEvents

     , COUNT(DISTINCT IF(s.attr24h,ord_transactionId, NULL)) AS totalTransactions24
     , COUNT(DISTINCT IF(s.attr24h,ord_buyerId, NULL)) AS totalBuyers24
     , COUNT(DISTINCT IF(s.attr24h,ord_itemId, NULL)) AS totalItems24
     , ROUND(
             SUM(
                 CASE
                  WHEN s.attr24h
                     THEN tgmv
                     ELSE 0
                 END
                )
             , 2
            ) AS TGMV24

     , COUNT(DISTINCT IF(s.attr48h,ord_transactionId, NULL)) AS totalTransactions48
     , COUNT(DISTINCT IF(s.attr48h,ord_buyerId, NULL)) AS totalBuyers48
     , COUNT(DISTINCT IF(s.attr48h,ord_itemId, NULL)) AS totalItems48
     , ROUND(
             SUM(
                 CASE
                  WHEN s.attr48h
                     THEN tgmv
                     ELSE 0
                 END
                )
             , 2
            ) AS TGMV48

     , COUNT(DISTINCT IF(s.attr72h,ord_transactionId, NULL)) AS totalTransactions72
     , COUNT(DISTINCT IF(s.attr72h,ord_buyerId, NULL)) AS totalBuyers72
     , COUNT(DISTINCT IF(s.attr72h,ord_itemId, NULL)) AS totalItems72
     , ROUND(
             SUM(
                 CASE
                  WHEN s.attr72h
                     THEN tgmv
                     ELSE 0
                 END
                )
             , 2
            ) AS TGMV72

     , COUNT(DISTINCT IF(s.attr168h,ord_transactionId, NULL)) AS totalTransactions168
     , COUNT(DISTINCT IF(s.attr168h,ord_buyerId, NULL)) AS totalBuyers168
     , COUNT(DISTINCT IF(s.attr168h,ord_itemId, NULL)) AS totalItems168
     , ROUND(
             SUM(
                 CASE
                  WHEN s.attr168h
                     THEN tgmv
                     ELSE 0
                 END
                )
             , 2
            ) AS TGMV168

    FROM AttrFinal AS s
    GROUP BY 1, 2, 3, 4