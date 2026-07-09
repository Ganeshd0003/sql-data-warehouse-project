USE DataWarehouse;

TRUNCATE TABLE silver.crm_cust_info;

-- Insert cleaned customer data into the Silver layer

INSERT INTO silver.crm_cust_info
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gender,
    cst_create_date
)
SELECT
    cst_id,
    TRIM(cst_key) AS cst_key,
    TRIM(ISNULL(cst_firstname, 'n/a')) AS cst_firstname,
    TRIM(ISNULL(cst_lastname, 'n/a')) AS cst_lastname,
    CASE TRIM(ISNULL(cst_marital_status, ''))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE TRIM(ISNULL(cst_gender, ''))
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gender,
    cst_create_date
FROM
(
    SELECT *,
           ROW_NUMBER() OVER
           (
               PARTITION BY cst_id
               ORDER BY cst_create_date DESC
           ) AS Flag
    FROM bronze.crm_cust_info
) t
WHERE Flag = 1;