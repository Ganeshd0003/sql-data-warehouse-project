-- Preview Bronze and Silver tables
SELECT TOP 100 * FROM bronze.crm_sales_details;
SELECT TOP 100 * FROM bronze.crm_prd_info;
SELECT TOP 100 * FROM silver.crm_prd_info;

-- Check for leading/trailing spaces in Order Number
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- Check for invalid Product Keys
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT cat_key
    FROM silver.crm_prd_info
);

-- Check for invalid Customer IDs
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
    FROM silver.crm_cust_info
);

-- Attempt 1:
-- Format YYYYMMDD as YYYY-MM-DD using string functions.
-- This only returns a formatted STRING, not a DATE datatype.
SELECT
    CONCAT(
        SUBSTRING(sls_order_dt, 1, 4), '-',
        SUBSTRING(sls_order_dt, 5, 2), '-',
        SUBSTRING(sls_order_dt, 7, 2)
    ) AS sls_order_dt
FROM bronze.crm_sales_details;

-- Attempt 2:
-- Convert the value to DATE using CASE.
-- This approach doesn't reliably work for YYYYMMDD values.
SELECT
    sls_order_dt,
    CASE
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
        ELSE CAST(sls_order_dt AS DATE)
    END AS sls_order_dt
FROM bronze.crm_sales_details;

-- Recommended approach:
-- Convert the numeric value to CHAR(8), then use TRY_CONVERT with style 112 (YYYYMMDD).
-- Returns NULL for invalid dates instead of throwing an error.
SELECT
    sls_order_dt,
    TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112) AS sls_order_dt
FROM bronze.crm_sales_details;

-- Check for NULL or negative Sales values
SELECT sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_sales < 0;

-- Check for NULL or negative Quantity values
SELECT sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity IS NULL
   OR sls_quantity < 0;

-- Check for NULL or negative Price values
SELECT sls_price
FROM bronze.crm_sales_details
WHERE sls_price IS NULL
   OR sls_price < 0;