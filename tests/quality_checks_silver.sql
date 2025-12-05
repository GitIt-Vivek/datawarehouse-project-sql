SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM
bronze.crm_sales_details


-- check for invalid values in order date
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM 
bronze.crm_sales_details
WHERE 
sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

--Checks for invalid date orders
SELECT
sls_ship_dt
FROM 
silver.crm_sales_details
WHERE sls_ship_dt > sls_due_dt

-- Checking data consistency between sales, quantity and price
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM
silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_quantity IS NULL OR sls_sales IS NULL OR sls_price IS NULL
OR sls_quantity <= 0 OR sls_sales <= 0 OR sls_price <= 0
ORDER BY sls_sales,
sls_quantity,
sls_price;

SELECT * FROM silver.crm_sales_details;
