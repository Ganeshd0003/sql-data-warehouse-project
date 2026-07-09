-- WE ARE DOING CHANGES IN TABLE AND ADD COLUMN AND MODIFY DATE COLUMN SO WE NEED TO MODIFY EXISTING TABLE'S METADATA
DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info
(
	prd_id INT,
	cat_id VARCHAR(30),
	cat_key	VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost DECIMAL(10,2),
	prd_line VARCHAR(30),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);



INSERT INTO silver.crm_prd_info 
(
	prd_id,
	cat_id,
	cat_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
	SELECT TOP 1000
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS cat_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'n/a'
		END AS prd_line,
		CAST (prd_start_dt AS DATE) AS prd_state_dt,
		CAST(
		DATEADD(
			DAY,
			-1,
			LEAD(prd_start_dt) OVER (
				PARTITION BY prd_key
				ORDER BY prd_start_dt
			)
		) AS DATE
	) AS prd_end_dt
	FROM bronze.crm_prd_info;