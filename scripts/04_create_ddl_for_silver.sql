-- Silver Layer DDL

-- Creating DDL for crm dataset it contains customer information, product information and sales information

-- CRM Customer Info
CREATE TABLE silver.crm_cust_info
(
	cst_id INT,
	cst_key VARCHAR(30),
	cst_firstname VARCHAR(20),
	cst_lastname VARCHAR(20),
	cst_marital_status VARCHAR(20),
	cst_gender VARCHAR(20),
	cst_Create_date DATE,
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);

-- CRM Product Info
CREATE TABLE silver.crm_prd_info
(
	prd_id INT,
	prd_key	VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost DECIMAL(10,2),
	prd_line VARCHAR(10),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);

-- CRM Sales Info
CREATE TABLE silver.crm_sales_details
(
	sls_ord_num	VARCHAR(20),
	sls_prd_key	VARCHAR(20),
	sls_cust_id	INT,
	sls_order_dt VARCHAR(20),
	sls_ship_dt	VARCHAR(20),
	sls_due_dt	VARCHAR(20),
	sls_sales INT,
	sls_quantity INT,
	sls_price DECIMAL(10,2),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);


-- Creating DDL for erp dataset it contains CUST_AZ12, LOC_A101 and PX_CAT_G1V2

CREATE TABLE silver.erp_cust_az12
(
	CID	VARCHAR(20),
	BDATE DATE,
	GEN VARCHAR(20),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_loc_a101
(
	CID	VARCHAR(20),
	CNTRY VARCHAR(20),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_px_cat_g1v2
(
	ID VARCHAR(20),CAT VARCHAR(20),
	SUBCAT VARCHAR(20),
	MAINTENANCE VARCHAR(20),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);