--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Thụt dòng cho từng đoạn, từng phần để dễ nhìn hơn

--Q1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
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
--correct
--k cần select * from (), nó k giúp câu lệnh trong gọn hơn


--Q2: Bounce rate per traffic source in July 2017
      SELECT trafficSource.source
            , COUNT(totals.visits) AS total_visits
            , COUNT(totals.bounces)	AS total_no_of_bounces,
            (100*COUNT(totals.bounces))/COUNT(totals.visits) AS bounce_rate
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      GROUP BY trafficSource.source
      ORDER BY COUNT(totals.visits) DESC;
--thiếu ràng điều kiện time range


--Q3: Revenue by traffic source by week, by month in June 2017
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

--câu 3 này e trình bày k xuống hàng ở từng field, ghi thẳng băng từ trên xuống hơi khó nhìn
-->
with month_data as(
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


--Q4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
WITH purchaser AS 
      (SELECT month, AVG(total_pageviews/num_purchasers) AS avg_pageviews_purchase
      FROM (
      SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(totals.pageviews) AS total_pageviews, COUNT(DISTINCT (fullVisitorId)) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0601'and '0731'AND (totals.transactions >=1 AND productRevenue IS NOT NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS purchaser1
      GROUP BY month),
non_purchaser AS 
      (SELECT month, AVG(total_pageviews/num_purchasers) AS avg_pageviews_non_purchase
      FROM (
      SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(totals.pageviews) AS total_pageviews, COUNT(DISTINCT (fullVisitorId)) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0601'and '0731'AND (totals.transactions IS NULL AND productRevenue IS NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS non_purchaser1
      GROUP BY month)
SELECT p.month,p.avg_pageviews_purchase,n.avg_pageviews_non_purchase
FROM purchaser p
JOIN non_purchaser n ON p.month=n.month;  --k nên inner join ở chỗ này

--cách ghi hơi dài, --k cần distinct, vì sau khi sum mình cũng sẽ group lại thành 1 dòng
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  and product.productRevenue is not null
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;


--câu 4 này lưu ý là mình nên dùng left join hoặc full join, bởi vì trong câu này, phạm vi chỉ từ tháng 6-7, nên chắc chắc sẽ có pur và nonpur của cả 2 tháng
--mình inner join thì vô tình nó sẽ ra đúng. nhưng nếu đề bài là 1 khoảng thời gian dài hơn, 2-3 năm chẳng hạn, nó cũng tháng chỉ có nonpur mà k có pur
--thì khi đó inner join nó sẽ làm mình bị mất data, thay vì hiện số của nonpur và pur thì nó để trống

--Q5: Average number of transactions per user that made a purchase in July 2017*/
SELECT month, AVG(total_transactions/num_purchasers) AS avg_total_transactions_per_user
      FROM (
      SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(totals.transactions) AS total_transactions, COUNT(DISTINCT (fullVisitorId)) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0701'and '0731'AND (totals.transactions >=1 AND productRevenue IS NOT NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS non_purchaser1
      GROUP BY month;

--cách ghi ngắn hơn
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
where  totals.transactions>=1
and totals.totalTransactionRevenue is not null
and product.productRevenue is not null
group by month;

--Q6: Average amount of money spent per session. Only include purchaser data in July 2017*/
SELECT month, AVG(total_revenue/num_purchasers) AS Avg_total_transactions_per_user
      FROM (
      SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,SUM(productRevenue/1000000) AS total_revenue, COUNT(fullVisitorId) AS num_purchasers
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE  _table_suffix between '0701'and '0731'AND (totals.transactions IS NOT NULL AND productRevenue IS NOT NULL)
      GROUP BY FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date))) AS purchaser1
      GROUP BY month;

--cách ghi ngắn hơn
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
group by month;

--Q7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
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

-->
--subquery:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null)
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc;

--CTE:

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC;


--Q8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.The output should be calculated in product level.

WITH pview AS 
      (SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month, COUNT(eCommerceAction.action_type) AS num_product_view
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE _table_suffix BETWEEN '0101' AND '0331' AND eCommerceAction.action_type='2'
      GROUP BY month),
atocard AS 
      (SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month, COUNT(eCommerceAction.action_type) AS num_addtocart
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE _table_suffix BETWEEN '0101' AND '0331' AND eCommerceAction.action_type='3'
      GROUP BY month),
purchase AS 
      (SELECT DISTINCT FORMAT_TIMESTAMP('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month, COUNT(eCommerceAction.action_type) AS num_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE _table_suffix BETWEEN '0101' AND '0331' AND eCommerceAction.action_type='6' AND product.productRevenue IS NOT NULL
      GROUP BY month)

SELECT v.month,v.num_product_view, a.num_addtocart, p.num_purchase, ROUND(SUM(a.num_addtocart*100/v.num_product_view),2) AS add_to_cart_rate,
ROUND(SUM(p.num_purchase*100/v.num_product_view),2) AS purchase_rate
FROM pview v
JOIN atocard a USING (month)
JOIN purchase p USING (month)
GROUP BY month, v.num_product_view, a.num_addtocart, p.num_purchase
ORDER BY v.month;

--bài yêu cầu tính số sản phầm, mình nên count productName hay productSKU thì sẽ hợp lý hơn là count action_type
--k nên xài inner join, nếu table1 có 10 record,table2 có 5 record,table3 có 1 record, thì sau khi inner join, output chỉ ra 1 record

--Cách 1:dùng CTE
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;

--bài này k nên inner join, vì nếu như bảng purchase k có data thì sẽ k mapping đc vs bảng productview, từ đó kết quả sẽ k có luôn, mình nên dùng left join

--Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;

                                                            ---good