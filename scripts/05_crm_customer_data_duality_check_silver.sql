USE DataWarehouse;
-- TO CHECK THE TABLE STRUCTURE
SELECT TOP 50 * FROM bronze.crm_cust_info;

-- TO CHECK THE DUPLICATES, NULLS
SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- SAMPLE CHECK WHAT IS THE DIFFERNCE BETWEEN DUPLIACTE RECONDS 
SELECT * FROM bronze.crm_cust_info WHERE cst_id = 29466;

--AND FROM THIS KNOWS LATEST UPDATED RECORD HAVING COMPLENESS
SELECT * FROM
	(SELECT *,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag
	FROM bronze.crm_cust_info) t
WHERE Flag <> 1;

-- CHECKING COLUMN NO 2 LENGTH AND BLANK SPACE
SELECT TOP 1 *,LEN(cst_key) LENGTH FROM bronze.crm_cust_info;

-- GET SIZE 10 BUT CHECKING SIZE MORE THAN 10 OR NOT
SELECT * FROM bronze.crm_cust_info
WHERE LEN(cst_key) > 10;

SELECT cst_key FROM bronze.crm_cust_info WHERE cst_key <> TRIM(cst_key);


-- CHECK BLANK SPACE IS THER OR NOT IN NAME
SELECT cst_firstname,cst_lastname FROM bronze.crm_cust_info
WHERE LEN(cst_firstname) <> LEN(TRIM(cst_firstname))
OR LEN(cst_lastname) <> LEN(TRIM(cst_lastname));

-- HERE ALSO CHECK BLANK SPACE
SELECT cst_marital_status FROM bronze.crm_cust_info
WHERE LEN(cst_marital_status) <> LEN(TRIM(cst_marital_status))

-- HERE ALSO RESULT TYPE AND FIX NULL TO 'n/a'
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

-- HERE ALSO CHECK BLANK SPACE FRO GENDER
SELECT cst_gender FROM bronze.crm_cust_info
WHERE LEN(cst_gender) <> LEN(TRIM(cst_gender))

-- HERE ALSO RESULT TYPE AND FIX NULL TO 'n/a' FOR GENDER
SELECT DISTINCT cst_gender FROM bronze.crm_cust_info;