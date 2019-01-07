-- Created by Linus Larsson
-- 2019-01-07
-- https://lynuhs.com

SELECT
  date as date,
  pagePath as page,
  count(distinct session_id) as uniquePageviews,
  count(distinct transactionId) as customerPageviews,
  round(count(distinct transactionId)/count(distinct session_id),4) as conversionRate
  
FROM 
  (SELECT
    hits.transaction.transactionId as transactionId,
    max(CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) AS session_id_key
    
   FROM
    `my-project-name.my-dataset-name.ga_sessions_20*` ga, -- Change to your own table
    unnest(ga.hits) hits
   WHERE
    hits.transaction.transactionId is not null and
    parse_date('%y%m%d', _TABLE_SUFFIX) between
    date_sub(current_date(), interval 1 day) and date_sub(current_date(), interval 1 day)
    
    GROUP BY
    hits.transaction.transactionId
  ) trans right join
  (SELECT
    date,
    hits.page.pagePath as pagePath,
    CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id
   FROM
    `my-project-name.my-dataset-name.ga_sessions_20*` ga,
    unnest(ga.hits) hits
   WHERE
    hits.type = 'PAGE' and
    parse_date('%y%m%d', _TABLE_SUFFIX) between
    date_sub(current_date(), interval 1 day) and date_sub(current_date(), interval 1 day)
   ) page on trans.session_id_key = page.session_id

GROUP BY
  pagePath, date
ORDER BY
 uniquePageviews desc
