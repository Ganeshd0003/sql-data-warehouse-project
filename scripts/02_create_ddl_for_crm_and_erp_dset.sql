-- Creating DDL for crm dataset it contains customer information, product information and sales information

-- CRM Customer Info
CREATE TABLE bronze.crm_cust_info
(
	cst_id INT,
	cst_key VARCHAR(30),
	cst_firstname VARCHAR(20),
	cst_lastname VARCHAR(20),
	cst_marital_status VARCHAR(20),
	cst_gender VARCHAR(20),
	cst_Create_date DATE
);

-- CRM Product Info
CREATE TABLE bronze.crm_prd_info
(
	prd_id INT,
	prd_key	VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost DECIMAL(10,2),
	prd_line VARCHAR(10),
	prd_start_dt DATE,
	prd_end_dt DATE
);

-- CRM Sales Info
CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num	VARCHAR(20),
	sls_prd_key	VARCHAR(20),
	sls_cust_id	INT,
	sls_order_dt VARCHAR(20),
	sls_ship_dt	VARCHAR(20),
	sls_due_dt	VARCHAR(20),
	sls_sales INT,
	sls_quantity INT,
	sls_price DECIMAL(10,2)
);


-- Creating DDL for erp dataset it contains CUST_AZ12, LOC_A101 and PX_CAT_G1V2

CREATE TABLE bronze.erp_cust_az12
(
	CID	VARCHAR(20),
	BDATE DATE,
	GEN VARCHAR(20)
);

CREATE TABLE bronze.erp_loc_a101
(
	CID	VARCHAR(20),
	CNTRY VARCHAR(20)
);

CREATE TABLE bronze.erp_px_cat_g1v2
(
	ID VARCHAR(20),CAT VARCHAR(20),
	SUBCAT VARCHAR(20),
	MAINTENANCE VARCHAR(20)

);