

#Calculate total visits, pagesviews, and transactions for Jan, Feb, and March 2017 (order by month)
      SELECT *
      FROM (
      SELECT  DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month  --k cần distinct, vì sau khi sum mình cũng sẽ group lại thành 1 dòng
            , COUNT(totals.visits) AS visits
            , SUM(totals.pageviews) AS pageviews
            , COUNT(totals.transactions) AS transactions
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      WHERE _table_suffix between '0101'and '0331'
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS ga_session1
      ORDER BY ga_session1.month;

#Bounce rate per traffic source in July 2017
      SELECT trafficSource.source
            , COUNT(totals.visits) AS total_visits
            , COUNT(totals.bounces)	AS total_no_of_bounces,
            (100*COUNT(totals.bounces))/COUNT(totals.visits) AS bounce_rate
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      GROUP BY trafficSource.source
      ORDER BY COUNT(totals.visits) DESC;

#Revenue by traffic source by week, by month in June 2017
SELECT 'Month' AS timetype,month AS time, source, revenue
FROM
(SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,trafficSource.source, SUM(productRevenue/1000000) AS revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE  productRevenue IS NOT NULL AND (_table_suffix between '0601'and '0630')
GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)), source) AS revenue1
UNION ALL

SELECT 'Week' AS timetype,week AS time, source, revenue
FROM
(SELECT DISTINCT FORMAT_TIMESTAMP('%Y%W', PARSE_DATE('%Y%m%d', date)) AS week,trafficSource.source, SUM(productRevenue/1000000) AS revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE  productRevenue IS NOT NULL AND (_table_suffix between '0601'and '0630')
GROUP BY FORMAT_TIMESTAMP('%Y%W', PARSE_DATE('%Y%m%d', date)), source) AS revenue2;


With month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(p.productRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  unnest(hits) hits,
  unnest(product) p
WHERE p.productRevenue is not null
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(p.productRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  unnest(hits) hits,
  unnest(product) p
WHERE p.productRevenue is not null
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data;


#Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, and July 2017
WITH purchaser AS 
      (SELECT month, AVG(total_pageviews/num_purchasers) AS avg_pageviews_purchase
      FROM (
      SELECT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(totals.pageviews) AS total_pageviews, COUNT(DISTINCT (fullVisitorId)) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0601'and '0731'AND (totals.transactions >=1 AND productRevenue IS NOT NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS purchaser1
      GROUP BY month),
non_purchaser AS 
      (SELECT month, AVG(total_pageviews/num_purchasers) AS avg_pageviews_non_purchase
      FROM (
      SELECT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(totals.pageviews) AS total_pageviews, COUNT(DISTINCT (fullVisitorId)) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0601'and '0731'AND (totals.transactions IS NULL AND productRevenue IS NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS non_purchaser1
      GROUP BY month)
SELECT p.month,p.avg_pageviews_purchase,n.avg_pageviews_non_purchase
FROM purchaser p
LEFT JOIN non_purchaser n ON p.month=n.month;  --k nên inner join ở chỗ này


#Average number of transactions per user that made a purchase in July 2017*/
SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d", date)) as month,
      SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE totals.transactions>=1
AND totals.totalTransactionRevenue IS NOT NULL
AND product.productRevenue IS NOT NULL
GROUP BY month;


#Average amount of money spent per session. Only include purchaser data in July 2017*/
SELECT month, AVG(total_revenue/num_purchasers) AS Avg_total_transactions_per_user
      FROM (
      SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(productRevenue/1000000) AS total_revenue, COUNT(fullVisitorId) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0701'and '0731'AND (totals.transactions IS NOT NULL AND productRevenue IS NOT NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS purchaser1
      GROUP BY month;


#Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
SELECT v2ProductName AS other_purchased_products, SUM (productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
WHERE fullVisitorId IN
      (SELECT DISTINCT fullVisitorID 
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE v2ProductName="YouTube Men's Vintage Henley"AND _table_suffix between '0701'and '0731'AND productRevenue IS NOT NULL) AND v2ProductName!="YouTube Men's Vintage Henley" AND productRevenue IS NOT NULL    --e trình bay điều kiện như thế này hơi khó nhìn
GROUP BY v2ProductName
ORDER BY quantity DESC;



#Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.The output should be calculated in product level.
WITH
product_view AS (
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) AS month,
  COUNT(product.productSKU) AS num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart AS(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) AS month,
  COUNT(product.productSKU) AS num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase AS(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) AS month,
  COUNT(product.productSKU) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

SELECT
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
FROM product_view pv
LEFT JOIN add_to_cart a ON pv.month = a.month
LEFT JOIN purchase p ON pv.month = p.month
ORDER BY  pv.month;

