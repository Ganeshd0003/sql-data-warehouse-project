-- Preview the ERP Product Category table
SELECT *
FROM bronze.erp_px_cat_g1v2;

-- Check for NULL Product Category IDs or leading/trailing spaces
SELECT ID
FROM bronze.erp_px_cat_g1v2
WHERE ID IS NULL
   OR TRIM(ID) <> ID;

-- Review distinct Category values
SELECT DISTINCT CAT
FROM bronze.erp_px_cat_g1v2;

-- Review distinct Subcategory values
SELECT DISTINCT SUBCAT
FROM bronze.erp_px_cat_g1v2;

-- Review distinct Maintenance values
SELECT DISTINCT MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

-- Verify that Category IDs exist in the Silver Product table
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE ID IN (
    SELECT CAT_ID
    FROM silver.crm_prd_info
);