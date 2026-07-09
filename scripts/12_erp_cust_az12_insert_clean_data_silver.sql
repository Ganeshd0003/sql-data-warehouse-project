USE DataWarehouse;

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12
(
    CID,
    BDATE,
    GEN
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid) - 3)
        ELSE cid
    END AS cid,

    CASE
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,

    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;