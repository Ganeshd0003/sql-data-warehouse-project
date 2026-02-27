
/* =========================================================
   PROJECT: E-Commerce Data Warehouse
   LAYER  : Bronze (Raw Zone)
   DB     : MySQL 8.0
   PURPOSE: Store immutable raw source data
========================================================= */

-- =========================================================
-- 1️ Create Databases
-- =========================================================
CREATE DATABASE IF NOT EXISTS dwh_bronze;
CREATE DATABASE IF NOT EXISTS dwh_silver;
CREATE DATABASE IF NOT EXISTS dwh_gold;

-- =========================================================
-- 2️ Use Bronze Layer
-- =========================================================
USE dwh_bronze;

-- =========================================================
-- 3️ CRM - Customer Raw
-- =========================================================
DROP TABLE IF EXISTS crm_customer_raw;

CREATE TABLE crm_customer_raw (
    cst_id              VARCHAR(50),
    cst_key             VARCHAR(100),
    cst_firstname       VARCHAR(100),
    cst_lastname        VARCHAR(100),
    cst_marital_status  VARCHAR(20),
    cst_gndr            VARCHAR(10),
    cst_create_date     VARCHAR(50),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================================================
-- 4️ CRM - Product Raw
-- =========================================================
DROP TABLE IF EXISTS crm_product_raw;

CREATE TABLE crm_product_raw (
    prd_id          VARCHAR(50),
    prd_key         VARCHAR(100),
    prd_nm          VARCHAR(200),
    prd_cost        VARCHAR(50),
    prd_line        VARCHAR(50),
    prd_start_dt    VARCHAR(50),
    prd_end_dt      VARCHAR(50),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================================================
-- 5️ CRM - Sales Raw
-- =========================================================
DROP TABLE IF EXISTS crm_sales_raw;

CREATE TABLE crm_sales_raw (
    sls_ord_num     VARCHAR(100),
    sls_prd_key     VARCHAR(100),
    sls_cust_id     VARCHAR(100),
    sls_order_dt    VARCHAR(50),
    sls_ship_dt     VARCHAR(50),
    sls_due_dt      VARCHAR(50),
    sls_sales       VARCHAR(50),
    sls_quantity    VARCHAR(50),
    sls_price       VARCHAR(50),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================================================
-- 6️ ERP - Customer Extra Raw
-- =========================================================
DROP TABLE IF EXISTS erp_customer_raw;

CREATE TABLE erp_customer_raw (
    cid             VARCHAR(100),
    bdate           DATE,
    gen             VARCHAR(20),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================================================
-- 7 ERP - Location Raw
-- =========================================================
DROP TABLE IF EXISTS erp_location_raw;

CREATE TABLE erp_location_raw (
    cid             VARCHAR(100),
    country         VARCHAR(100),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================================================
-- 8️ ERP - Product Category Raw
-- =========================================================
DROP TABLE IF EXISTS erp_product_category_raw;

CREATE TABLE erp_product_category_raw (
    id              VARCHAR(100),
    cat             VARCHAR(100),
    subcat          VARCHAR(100),
    maintenance     VARCHAR(100),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================================================
-- 9️ Load Raw Data (SERVER-SIDE SAFE VERSION)
-- Make sure files are inside secure_file_priv directory
-- =========================================================

-- CRM CUSTOMER
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
INTO TABLE crm_customer_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(cst_id,
 cst_key,
 cst_firstname,
 cst_lastname,
 cst_marital_status,
 cst_gndr,
 cst_create_date);

-- CRM PRODUCT
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
INTO TABLE crm_product_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(prd_id,
 prd_key,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt);

-- CRM SALES
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
INTO TABLE crm_sales_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(sls_ord_num,
 sls_prd_key,
 sls_cust_id,
 sls_order_dt,
 sls_ship_dt,
 sls_due_dt,
 sls_sales,
 sls_quantity,
 sls_price);

-- ERP CUSTOMER
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_az12.csv'
INTO TABLE erp_customer_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(cid,
 bdate,
 gen);

-- ERP LOCATION
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/loc_a101.csv'
INTO TABLE erp_location_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(cid,
 country);

-- ERP PRODUCT CATEGORY
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/px_cat_g1v2.csv'
INTO TABLE erp_product_category_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(id,
 cat,
 subcat,
 maintenance);
