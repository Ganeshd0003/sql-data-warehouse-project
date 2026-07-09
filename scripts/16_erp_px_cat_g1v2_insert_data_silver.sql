USE DataWarehouse

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2
(
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
    SELECT
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
    FROM bronze.erp_px_cat_g1v2;