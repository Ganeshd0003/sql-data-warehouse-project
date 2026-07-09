-- Check for duplicate or NULL product IDs
SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for leading or trailing spaces in product names
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Check for NULL or negative product costs
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Check for NULL values in the product line
SELECT prd_line
FROM bronze.crm_prd_info
WHERE prd_line IS NULL;

-- Review all distinct product line codes
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;