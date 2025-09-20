-- ==========================================================
-- 🧾 UPI Transaction Analytics — SQL Query Collection
-- ----------------------------------------------------------
-- Dataset : upi (custom UPI transaction table)
-- Schema  : transaction_id, sender_age_group, amount, fraud_flag, 
--           transaction_type, transaction_status, timestamp, etc.
-- Author  : Siddharth Srivastava
-- Updated : 13-August-2025
-- Use     : Portfolio Project (Data Analytics)
-- DB Used : MySQL
-- ==========================================================

USE UPI;

-- A)Top Merchant & Bank Patterns  =================================================================================================================

-- 1) Top 3 Highest Revenue Merchant Categories by State
WITH CTE AS (
	SELECT 
		sender_state, 
        merchant_category, 
        SUM(amount) AS revenue,
		RANK() OVER(PARTITION BY sender_state ORDER BY SUM(amount) DESC) AS rn
	FROM upi
	GROUP BY sender_state, merchant_category
)
SELECT 
	rn AS `rank`, 
    sender_state, 
    merchant_category,
    revenue
FROM CTE
WHERE rn <= 3;

-- 2) Top 3 Merchant Categories by Avg Spending per Age Group
WITH CTE AS (
	SELECT 
		sender_age_group, 
        merchant_category, 
        ROUND(AVG(amount),2) AS avg_spent,
		RANK() OVER(PARTITION BY sender_age_group ORDER BY AVG(amount) DESC) AS rn
	FROM upi
	GROUP BY sender_age_group, merchant_category
)
SELECT 
	rn AS `rank`, 
    sender_age_group, 
    merchant_category, 
    avg_spent
FROM CTE
WHERE rn <= 3;

-- 3) Preferred Devices per Age Group (Top 2 per group)
WITH CTE AS (
  SELECT 
	sender_age_group, 
    device_type, 
    COUNT(*) AS txn_count
  FROM upi
  GROUP BY sender_age_group, device_type
),
CTE2 AS (
  SELECT *,
	RANK() OVER (PARTITION BY sender_age_group ORDER BY txn_count DESC) AS device_rank
  FROM CTE
)
SELECT 
	sender_age_group, 
    device_type, 
    txn_count,
	CASE WHEN device_rank = 1 THEN 'Preferred Device' ELSE 'Secondary' END AS device_tag
FROM CTE2
WHERE device_rank <= 2;


-- B) Fraud & Risk Analysis =================================================================================================================


-- 1) Top 10 Fraud Counts by Bank Pair
SELECT 
	sender_bank, 
    receiver_bank, 
    COUNT(*) AS frauds
FROM upi
WHERE fraud_flag = 1
GROUP BY sender_bank, receiver_bank
ORDER BY frauds DESC
LIMIT 10;

-- 2) Fraud % by State
SELECT 
	sender_state,
	ROUND(COUNT(CASE WHEN fraud_flag = 1 THEN 1 END) / COUNT(transaction_id) * 100, 2) AS fraud_percentage
FROM upi
GROUP BY sender_state
ORDER BY fraud_percentage DESC;

-- 3) Risk Categorization by Bank Based on Fraud %
WITH CTE AS (
  SELECT 
	sender_bank, 
    COUNT(*) AS total_txns, 
	SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_txns,
	ROUND(SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS fraud_rate
  FROM upi
  GROUP BY sender_bank
)
SELECT 
	RANK() OVER(ORDER BY fraud_rate DESC) AS fraud_rank, 
    CTE.*,
	CASE 
		WHEN fraud_rate > 5 THEN 'High Risk'
		WHEN fraud_rate > 1 THEN 'Medium Risk'
		ELSE 'Low Risk'
	END AS risk_category
FROM CTE;

-- 4) Fraud Analysis by Weekend vs Weekday
SELECT
  time_of_day,
  total_weekday_txns,
  fraud_weekday_txns,
  ROUND(fraud_weekday_txns * 100.0 / total_weekday_txns, 2) AS fraud_weekday_percent,
  total_weekend_txns,
  fraud_weekend_txns,
  ROUND(fraud_weekend_txns * 100.0 / total_weekend_txns, 2) AS fraud_weekend_percent
FROM (
  SELECT
    CASE 
      WHEN hour_of_day BETWEEN 5 AND 10 THEN 'Morning'
      WHEN hour_of_day BETWEEN 11 AND 15 THEN 'Afternoon'
      WHEN hour_of_day BETWEEN 16 AND 20 THEN 'Evening'
      WHEN hour_of_day BETWEEN 21 AND 23 OR hour_of_day BETWEEN 0 AND 4 THEN 'Night'
    END AS time_of_day,

    SUM(CASE WHEN is_weekend = 0 THEN 1 ELSE 0 END) AS total_weekday_txns,
    SUM(CASE WHEN is_weekend = 1 THEN 1 ELSE 0 END) AS total_weekend_txns,
    
    SUM(CASE WHEN is_weekend = 0 AND fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_weekday_txns,
    SUM(CASE WHEN is_weekend = 1 AND fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_weekend_txns

  FROM upi
  GROUP BY time_of_day
) AS sub
ORDER BY FIELD(time_of_day, 'Morning', 'Afternoon', 'Evening', 'Night');

-- 5) Fraud Counts by Age Group and Amount Range
SELECT 
    sender_age_group,
    COUNT(CASE WHEN amount BETWEEN 1000 AND 10000 THEN 1 END) AS '1000–10000',
    COUNT(CASE WHEN amount BETWEEN 10001 AND 20000 THEN 1 END) AS '10001–20000',
    COUNT(CASE WHEN amount BETWEEN 20001 AND 30000 THEN 1 END) AS '20001–30000',
    COUNT(CASE WHEN amount >= 30001 THEN 1 END) AS '30001+'
FROM upi
WHERE fraud_flag = 1
GROUP BY sender_age_group
ORDER BY sender_age_group;


-- C) Device & Network Usage Insights  =================================================================================================================


-- 1) Device vs Network Performance (Successful Transactions)
SELECT 
	device_type, 
    network_type, 
    COUNT(*) AS success
FROM upi
WHERE fraud_flag = 0
GROUP BY device_type, network_type
ORDER BY device_type, success DESC;

-- 2) Top 3 Merchant Categories per Device by Spending
WITH CTE AS (
  SELECT 
	device_type, 
    merchant_category, 
    COUNT(*) AS txn_count, 
    SUM(amount) AS total_spent
  FROM upi
  GROUP BY device_type, merchant_category
),
CTE2 AS (
  SELECT *, 
	RANK() OVER (PARTITION BY device_type ORDER BY total_spent DESC) AS rn
  FROM CTE
)
SELECT 
	device_type,
	rn AS `rank`,
    merchant_category,
    txn_count,
    total_spent
FROM CTE2
WHERE rn <= 3;


-- D) Behavioral & Time-Based Insights  =================================================================================================================

-- 1) Transaction Labels: Peak and Active Hours per Day
WITH CTE AS (
  SELECT 
	day_of_week, 
    hour_of_day, 
    COUNT(*) AS txn_count
  FROM upi
  GROUP BY day_of_week, hour_of_day
),
CTE2 AS (
  SELECT *, 
	DENSE_RANK() OVER(PARTITION BY day_of_week ORDER BY txn_count DESC) AS rn
  FROM CTE
)
SELECT 
	day_of_week, 
    hour_of_day, 
    txn_count,
	CASE WHEN rn = 1 THEN 'Peak Hour' ELSE 'Active Hour' END AS hour_label
FROM CTE2
WHERE rn <= 3
ORDER BY FIELD(day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');

-- 2) Sender-Receiver Age Group Dynamics
WITH CTE AS (
	SELECT 
		sender_age_group, 
        receiver_age_group, 
        COUNT(*) cnt
	FROM upi
	GROUP BY sender_age_group, receiver_age_group
),
CTE2 AS (
	SELECT *, ROUND((cnt / SUM(cnt) OVER()) * 100, 2) AS percent_of_total_txn
	FROM CTE
)
SELECT 
	sender_age_group, 
    receiver_age_group, 
    percent_of_total_txn,
       CASE 
         WHEN percent_of_total_txn >= 8 THEN 'High Value'
         WHEN percent_of_total_txn BETWEEN 4 AND 7 THEN 'Mid Value'
         ELSE 'Low Value'
       END AS category
FROM CTE2
ORDER BY percent_of_total_txn DESC;


-- E) Time Series & Trend Analysis  =================================================================================================================


-- 1) Moving Average of Monthly Spend per Age Group (3-month window)
WITH CTE AS (
  SELECT 
	sender_age_group, 
    DATE_FORMAT(timestamp, '%Y-%m') AS txn_month,
	SUM(amount) AS monthly_spend
  FROM upi
  GROUP BY sender_age_group, txn_month
)
SELECT *, 
       ROUND(AVG(monthly_spend) OVER(PARTITION BY sender_age_group ORDER BY txn_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 0) AS moving_avg
FROM CTE;

-- 2) Cumulative Monthly Spend per Age Group
WITH CTE AS (
  SELECT 
	sender_age_group, 
    DATE_FORMAT(timestamp, '%Y-%m') AS txn_month,
	SUM(amount) AS monthly_spend
  FROM upi
  GROUP BY sender_age_group, txn_month
)
SELECT *,
       SUM(monthly_spend) OVER (PARTITION BY sender_age_group ORDER BY txn_month) AS cumulative_spend
FROM CTE;


-- F) Transaction Type and Status  =================================================================================================================


-- 1) Success Rate by Transaction Type
SELECT
  transaction_type,
  COUNT(*) AS total_txns,
  COUNT(CASE WHEN transaction_status = 'SUCCESS' THEN 1 END) AS successful_txns,
  ROUND(COUNT(CASE WHEN transaction_status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*), 2) AS success_rate
FROM upi
GROUP BY transaction_type
ORDER BY success_rate DESC;

-- 2) Fraud Rate by Transaction Status
SELECT
  transaction_status,
  COUNT(*) AS total_txns,
  COUNT(CASE WHEN fraud_flag = 1 THEN 1 END) AS fraud_txns,
  ROUND(COUNT(CASE WHEN fraud_flag = 1 THEN 1 END) * 100.0 / COUNT(*), 2) AS fraud_percent
FROM upi
GROUP BY transaction_status
ORDER BY fraud_percent DESC;

-- 3) Age Group-wise Spending Behavior by Transaction Type
WITH CTE AS (
  SELECT 
    sender_age_group,
    transaction_type,
    ROUND(AVG(amount), 2) AS avg_spend
  FROM upi
  GROUP BY sender_age_group, transaction_type
),
CTE2 AS (
  SELECT *,
         RANK() OVER (PARTITION BY transaction_type ORDER BY avg_spend DESC) AS rn
  FROM CTE
)
SELECT 
	transaction_type,
	CASE 
		WHEN rn = 1 THEN 'Top Spender'
		ELSE '—'
	END AS spender_tag,
	sender_age_group, 
    avg_spend
FROM CTE2
ORDER BY transaction_type, rn;