CREATE OR ALTER VIEW gold.dim_products AS
	(SELECT 
		pi.prd_id AS product_id,
		pi.cat_key AS product_key,
		pi.prd_nm AS product_name,
		pi.cat_id AS category_id,
		pc.CAT AS category,
		pc.SUBCAT AS sub_category,
		pi.prd_cost AS cost,
		pi.prd_line AS product_line,
		pi.prd_start_dt AS start_date,
		pc.MAINTENANCE AS maintainance
	FROM silver.crm_prd_info AS pi
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
		ON pi.cat_id = pc.ID
);