create database rpc_18;
select * from dim_customer;
select count(*) from menu_item;

create table orders (
	order_id varchar(20),
    customer_id varchar(20),
    restaurant_id varchar(20),
    delivery_partner_id varchar(20),
    order_timestamp datetime, 
    subtotal_amount decimal(10,2), 
    discount_amount decimal(10,2), 
    delivery_fee decimal(10,2),
    total_amount decimal(10,2),
    is_cod char(2),
    is_cancelled char(2));
 

SET GLOBAL local_infile = 1;


LOAD DATA LOCAL INFILE "C:\\Users\\Hp\\Desktop\\Crisis Recovery in an Online Food Delivery Startup\\rpc_18_inputs_for_participants\\RPC_18_Datasets\\fact_orders.csv" 
INTO TABLE orders 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS 
(
    order_id,
    customer_id,
    restaurant_id,
    delivery_partner_id,
    order_timestamp,     
    subtotal_amount,
    discount_amount,
    delivery_fee,
    total_amount,
    is_cod,
    is_cancelled
);

# order_id	customer_id	restaurant_id	rating	review_text	review_timestamp	sentiment_score
CREATE TABLE ratings (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    rating DECIMAL(3,1),  -- Allows values like 4.5
    review_text TEXT,
    review_timestamp DATETIME,
    sentiment_score DECIMAL(5,4) -- Allows values like 0.75 or -0.45
);
    
    LOAD DATA LOCAL INFILE "C:\\Users\\Hp\\Desktop\\Crisis Recovery in an Online Food Delivery Startup\\rpc_18_inputs_for_participants\\RPC_18_Datasets\\fact_ratings.csv"
INTO TABLE ratings  -- Make sure your table name matches (e.g., 'ratings' or 'fact_ratings')
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' -- This file uses Windows line endings
IGNORE 1 ROWS 
(
    order_id,
    customer_id,
    restaurant_id,
    rating,
    review_text,
    @review_timestamp,  -- Load into a temporary variable first
    sentiment_score
)
SET review_timestamp = STR_TO_DATE(@review_timestamp, '%d-%m-%Y %H:%i'); -- Convert Day-Month-Year to SQL format

-- 1. Monthly Orders: Compare total orders across pre-crisis (Jan–May 2025) vs crisis (Jun–Sep 2025). How severe is the decline? 
select count(*) as number_of_orders,
case 
	when month(order_timestamp) <= 5 then "Pre-Crisis"
    when month(order_timestamp) > 5 then "Crisis"
    end as is_crisis
    from orders
    group by is_crisis;
    
-- 2. Which top 5 city groups experienced the highest percentage decline in orders during the crisis period compared to the pre-crisis period? 
    select * from orders;
    select * from customer;
    
    select c.city,
sum(case 
	when month(order_timestamp) <= 5 then 1
    else 0
    end) as pre_crisis_orders,
sum(case 
	when month(order_timestamp) > 5 then 1
    else 0
    end) as crisis_orders,
round((sum(case 
	when month(order_timestamp) <= 5 then 1
    else 0
    end) - sum(case 
	when month(order_timestamp) > 5 then 1
    else 0
    end)) /  sum(case 
	when month(order_timestamp) <= 5 then 1
    else 0
    end) * 100,2) as percentage_decline
    from orders o
    inner join customer c
    on o.customer_id = c.customer_id
    group by c.city
    order by percentage_decline desc; 
    
/* 3. Among restaurants with at least 50 pre-crisis orders, which top 10 high-volume 
restaurants experienced the largest percentage decline in order counts during 
the crisis period? 
*/
select restaurant_name, 
 sum(case 
	when month(order_timestamp) <= 5 then 1
    else 0
    end) as pre_crisis_orders,
  sum(case 
	when month(order_timestamp) > 5 then 1
    else 0
    end) as crisis_orders,
    round((sum(case 
	when month(order_timestamp) <= 5 then 1
    else 0
    end) - sum(case 
	when month(order_timestamp) > 5 then 1
    else 0
    end)) /  sum(case 
	when month(order_timestamp) <= 5 then 1
    else 0
    end) * 100,2) as percentage_decline
from orders o
left join restaurant r
on o.restaurant_id = r.restaurant_id
group by restaurant_name
having pre_crisis_orders >= 50
order by percentage_decline desc
limit 10;

-- 4. Cancellation Analysis: What is the cancellation rate trend pre-crisis vs crisis, and which cities are most affected?

select city,
Round((sum(case when month(order_timestamp) <= 5 AND is_cancelled = "Y" then 1 else 0 
end)) / (sum(case when month(order_timestamp) <= 5  then 1 else 0 
end))* 100,2) as pre_crisis_cancellation_rate,
Round((sum(case when month(order_timestamp) > 5 AND is_cancelled = "Y" then 1 else 0 
end)) / (sum(case when month(order_timestamp) > 5  then 1 else 0 
end))* 100,2) as in_crisis_cancellation_rate, 
Round((sum(case when month(order_timestamp) > 5 AND is_cancelled = "Y" then 1 else 0 
end)) / (sum(case when month(order_timestamp) > 5  then 1 else 0 
end))* 100 - 
(sum(case when month(order_timestamp) <= 5 AND is_cancelled = "Y" then 1 else 0 
end)) / (sum(case when month(order_timestamp) <= 5  then 1 else 0 
end))* 100,2) as Cancellation_trend_changes 
from orders o
inner join customer c
on o.customer_id = c.customer_id
group by city
order by Cancellation_trend_changes desc;

-- 5. Delivery SLA: Measure average delivery time across phases. Did SLA compliance worsen significantly in the crisis period?
WITH OrderPerformance AS (
    -- Join orders with delivery performance data
    SELECT 
        o.order_id,
        o.order_timestamp,
        dp.actual_delivery_time_mins,
        dp.expected_delivery_time_mins,
        -- Define Periods: Normal (Jan-May) vs Crisis (Jun-Sep)
        CASE 
            WHEN month(o.order_timestamp) BETWEEN '01' AND '05' THEN 'Normal Period (Jan-May)'
            WHEN month(o.order_timestamp) BETWEEN '06' AND '09' THEN 'Crisis Period (Jun-Sep)'
        END AS period_type,
        -- SLA Compliance (1 if Actual <= Expected, else 0)
        CASE 
            WHEN dp.actual_delivery_time_mins <= dp.expected_delivery_time_mins THEN 1 
            ELSE 0 
        END AS is_compliant
    FROM orders o
    JOIN delivery_performance dp ON o.order_id = dp.order_id
)
SELECT 
    period_type,
    COUNT(order_id) AS total_orders,
    ROUND(AVG(expected_delivery_time_mins), 2) AS avg_expected_time_mins,
    ROUND(AVG(actual_delivery_time_mins), 2) AS avg_actual_time_mins,
    -- Calculate Delay (Actual - Expected)
    ROUND(AVG(actual_delivery_time_mins - expected_delivery_time_mins), 2) AS avg_delay_mins,
    -- Calculate SLA Compliance Rate (%)
    ROUND(SUM(is_compliant) * 100.0 / COUNT(order_id), 2) AS sla_compliance_pct
FROM OrderPerformance
WHERE period_type != 'Other'
GROUP BY period_type
ORDER BY period_type DESC;

-- 6. Ratings Fluctuation: Track average customer rating month-by-month. Which months saw the sharpest drop?
with temp_cte as (SELECT monthname(review_timestamp) as month_name,
round(avg(rating),2) as avg_rating 
from ratings
group by month_name) 
select month_name, avg_rating, 
lag(avg_rating) over() as previous_month_rating,
round(avg_rating - lag(avg_rating) over(),2) as difference
from temp_cte
order by difference;

/* 7. Sentiment Insights: During the crisis period, identify the most frequently occurring negative keywords in customer review texts.*/
 
select review_text, count(*) from ratings
where month(review_timestamp) between 6 and 9 
group by review_text
order by count(*) desc;
 
 -- 8. Revenue Impact: Estimate revenue loss from pre-crisis vs crisis (based on subtotal, discount, and delivery fee)

select
round(sum(case when month(order_timestamp) <= 5 then total_amount else 0 end) / 5,2)  as pre_crisis_avg_monthly_revenue,
round(sum(case when month(order_timestamp) > 5 then total_amount else 0 end) / 4,2)  as crisis_avg_monthly_revenue,

round(sum(case when month(order_timestamp) <= 5 then total_amount else 0 end) / 5,2) -
round(sum(case when month(order_timestamp) > 5 then total_amount else 0 end) / 4,2) as avg_monthly_revenue_loss,

(round(sum(case when month(order_timestamp) <= 5 then total_amount else 0 end) / 5,2) -
round(sum(case when month(order_timestamp) > 5 then total_amount else 0 end) / 4,2)) * 4 as estimated_revenue_loss
from orders;

/* 9 Loyalty Impact: Among customers who placed five or more orders before the 
crisis, determine how many stopped ordering during the crisis, and out of those, 
how many had an average rating above 4.5? */

WITH PreCrisisLoyals AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS pre_crisis_order_count
    FROM orders
    WHERE order_timestamp < '2025-06-01'
    GROUP BY customer_id
    HAVING COUNT(order_id) >= 5
),
CrisisActivity AS (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE order_timestamp >= '2025-06-01'
),
ChurnedLoyals AS (
    SELECT p.customer_id
    FROM PreCrisisLoyals p
    LEFT JOIN CrisisActivity c ON p.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
),
CustomerAvgRatings AS (
    SELECT 
        customer_id,
        AVG(rating) AS avg_rating
    FROM ratings
    GROUP BY customer_id
)
SELECT 
    COUNT(cl.customer_id) AS total_churned_loyal_customers,
    SUM(CASE WHEN r.avg_rating > 4.5 THEN 1 ELSE 0 END) AS high_rated_churned_loyal_customers
FROM ChurnedLoyals cl
LEFT JOIN CustomerAvgRatings r ON cl.customer_id = r.customer_id;


