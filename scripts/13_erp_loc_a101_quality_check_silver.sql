-- Preview Bronze ERP Location data
SELECT *
FROM bronze.erp_loc_a101;

-- Preview Silver Customer data for reference
SELECT *
FROM silver.crm_cust_info;

-- Check for NULL Customer IDs or leading/trailing spaces
SELECT cid
FROM bronze.erp_loc_a101
WHERE cid IS NULL
   OR TRIM(cid) <> cid;

-- Review all distinct Country values to identify inconsistencies
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;