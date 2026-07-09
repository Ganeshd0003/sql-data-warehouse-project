-- Preview the ERP Customer table
SELECT *
FROM bronze.erp_cust_az12;

-- Check for NULL Customer IDs
SELECT cid
FROM bronze.erp_cust_az12
WHERE cid IS NULL;

-- Check for NULL Birth Dates
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate IS NULL;

-- Check for invalid Birth Dates (future dates)
SELECT *
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();

-- Review all distinct Gender values to identify inconsistencies
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;