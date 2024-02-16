select * FROM regions;
select * from customer_nodes;
select * from customer_transactions;

-- Customer Nodes Exploration
-- How many unique nodes are there on the Data Bank system?
SELECT distinct node_id from customer_nodes;
SELECT COUNT(*) AS total_distinct_ids
FROM (
  SELECT DISTINCT node_id
  FROM customer_nodes
) AS unique_ids;

-- There are total 5 distinct ids

-- What is the number of nodes per region?
SELECT distinct region_id as distinct_regions FROM ( select distinct region_id from customer_nodes) as unique_regions;
select count(*) region_id from customer_nodes;

SELECT region_id, COUNT(DISTINCT node_id) AS node_count
FROM customer_nodes
GROUP BY region_id;

-- How many customers are allocated to each region?
SELECT region_id, COUNT(DISTINCT customer_id) AS customer_count
FROM customer_nodes
GROUP BY region_id;

-- How many days on average are customers reallocated to a different node?
SELECT AVG(DATEDIFF(end_date, start_date)) AS avg_reallocation_days
FROM customer_nodes
WHERE end_date != '9999-12-31'; -- Exclude non-reallocation entries

-- What is the median, 80th and 95th percentile for this same reallocation days metric for each region?




-- Customer Transactions 
-- What is the unique count and total amount for each transaction type?
select  distinct txn_type from customer_transactions;
select count(txn_amount) from customer_transactions;

SELECT txn_type,
       COUNT(DISTINCT txn_type) AS unique_txns,
       SUM(txn_amount) AS total_amount
FROM customer_transactions
GROUP BY txn_type;

-- What is the average total  deposit counts and amounts for all customers?
SELECT
  AVG(total_deposit_count) AS avg_total_deposit_count,
  AVG(total_deposit_amount) AS avg_total_deposit_amount
FROM (
  SELECT
    customer_id,
    COUNT(txn_type) AS total_deposit_count,
    SUM(txn_amount) AS total_deposit_amount
  FROM
    customer_transactions
  WHERE
    txn_type = 'Deposit'
  GROUP BY
    customer_id
) AS deposit_summary;
-- For each month  how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month? 
SELECT
  YEAR(txn_date) AS transaction_year,
  MONTH(txn_date) AS transaction_month,
  COUNT(customer_id) AS customers_count
FROM
  customer_transactions
WHERE
  txn_type IN ('Deposit', 'Purchase', 'Withdrawal') AND
  customer_id IN (
    SELECT
      customer_id
    FROM
      customer_transactions
    WHERE
      txn_type = 'Deposit'
    GROUP BY
      customer_id, YEAR(txn_date), MONTH(txn_date)
    HAVING
      COUNT(DISTINCT txn_date) > 1
  )
  AND customer_id IN (
    SELECT
      customer_id
    FROM
      customer_transactions
    WHERE
      txn_type IN ('Purchase', 'Withdrawal')
    GROUP BY
      customer_id, YEAR(txn_date), MONTH(txn_date)
    HAVING
      COUNT(DISTINCT txn_date) >= 1
  )
GROUP BY
  transaction_year, transaction_month;
-- What isthe closing balance for each customer at the end of the month? 
SELECT
  customer_id,
  YEAR(txn_date) AS transaction_year,
  MONTH(txn_date) AS transaction_month,
  SUM(txn_amount) AS closing_balance
FROM
  customer_transactions
WHERE
  customer_id IS NOT NULL
GROUP BY
  customer_id, transaction_year, transaction_month;

-- What is the percentage of customers who increase their closing balance by more than 5%?
SELECT
  COUNT(DISTINCT customer_id) AS customer_count,
  COUNT(DISTINCT CASE WHEN closing_balance_increase_percentage > 5 THEN customer_id END) AS increased_customers,
  (COUNT(DISTINCT CASE WHEN closing_balance_increase_percentage > 5 THEN customer_id END) / COUNT(DISTINCT customer_id)) * 100 AS percentage_increased
FROM (
  SELECT
    customer_id,
    YEAR(txn_date) AS transaction_year,
    MONTH(txn_date) AS transaction_month,
    MAX(closing_balance) - MIN(closing_balance) AS closing_balance_increase,
    (MAX(closing_balance) - MIN(closing_balance)) / ABS(MIN(closing_balance)) * 100 AS closing_balance_increase_percentage
  FROM (
    SELECT
      customer_id,
      txn_date,
      SUM(txn_amount) OVER (PARTITION BY customer_id, YEAR(txn_date), MONTH(txn_date) ORDER BY txn_date) AS closing_balance
    FROM
      customer_transactions
    WHERE
      customer_id IS NOT NULL
  ) AS t
  GROUP BY
    customer_id, transaction_year, transaction_month
) AS closing_balance_changes;


-- Data allocation 8

WITH monthly_balances AS (
  SELECT customer_id,
         MONTH(txn_date) AS month,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS balance_change
  FROM customer_transactions
  GROUP BY customer_id, MONTH(txn_date)
)
SELECT customer_id, month,
       LAG(balance_change, 1, 0) OVER (PARTITION BY customer_id ORDER BY month) AS opening_balance,
       balance_change + LAG(balance_change, 1, 0) OVER (PARTITION BY customer_id ORDER BY month) AS closing_balance
FROM monthly_balances;

-- 2 
SELECT 
    c.customer_id,
    t.txn_date,
    t.txn_type,
    t.txn_amount,
    SUM(CASE WHEN t.txn_type = 'deposit' THEN t.txn_amount ELSE -t.txn_amount END) OVER (PARTITION BY c.customer_id ORDER BY t.txn_date) AS running_balance
FROM customer_nodes c
JOIN customer_transactions t ON c.customer_id = t.customer_id
ORDER BY c.customer_id, t.txn_date;

-- 3
SELECT 
    customer_id,
    txn_date,
    txn_amount,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions;
-- 2. Customer balance at the end of each month
WITH RunningBalances AS (
    SELECT 
        c.customer_id,
        t.txn_date,
        t.txn_type,
        t.txn_amount,
        SUM(CASE WHEN t.txn_type = 'deposit' THEN t.txn_amount ELSE -t.txn_amount END) OVER (PARTITION BY c.customer_id ORDER BY t.txn_date) AS running_balance
    FROM customer_nodes c
    JOIN customer_transactions t ON c.customer_id = t.customer_id
)

SELECT DISTINCT
    customer_id,
    DATE_FORMAT(txn_date, '%Y-%m-01') AS month_end_date,
    LAST_VALUE(running_balance) OVER (PARTITION BY customer_id ORDER BY txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_balance_end_of_month
FROM RunningBalances;

-- 3 minimum, average and maximum values of the running balance for each customer
WITH RunningBalances AS (
    -- (Previous query for running_balance)
    -- Example: Calculating running balances for each customer
SELECT 
    customer_id,
    txn_date,
    txn_amount,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions

)

SELECT 
    customer_id,
    MIN(running_balance) AS min_balance,
    AVG(running_balance) AS avg_balance,
    MAX(running_balance) AS max_balance
FROM RunningBalances
GROUP BY customer_id;



-- 