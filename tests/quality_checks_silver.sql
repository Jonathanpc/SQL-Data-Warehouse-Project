-- ========================================================================================
-- Quality Checks
-- ========================================================================================
-- Purpose: This script performs various quality checks for data consistency, accuracy,
--          and standardization across the 'silver' schema. It includes checks for:
--            - Null or duplicate primary keys.
--            - Unwanted hidden characters in string fields.
--            - Data standardization and consistency.
--            - Invalid Date ranges and orders.
--            - Data consistency between related fields.
--
-- Usage Notes:
--              Run these checks after data loading Silver Layer.
--              Investigate and resolve any discrepancies found during the checks.
-- ========================================================================================


-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Results

-- bronze.crm_cust_info -- search for any potential invalid data
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- silver.crm_cust_info -- check final result
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- bronze.crm_prd_info
SELECT prd_id, COUNT(*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- silver.crm_prd_info
SELECT prd_id, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency -- Checking prd_line for transformation edge cases
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Check for Invalid Dates
SELECT
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LENGTH(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;

-- Check for Invalid Dates -- Perfect for now but still add to clean in case of future dates
SELECT
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LENGTH(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101;

-- Check for Invalid Dates -- Perfect for now but still add to clean in case of future dates
SELECT
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LENGTH(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101;

-- Check for Invalid Dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check for Data consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be Nulls, Zero, or Negative
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity ,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Check Health of recently inserted silver.crm_sales_details
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Identify Out of Range Dates
SELECT DISTINCT
bdate
FROM
bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURDATE();

-- Run below sql code to see hidden things in the data.
-- See the raw values and their lengths
SELECT DISTINCT
    gen,
    LENGTH(gen) AS len,
    HEX(gen) AS hex_value,  -- Shows hidden characters
    CONCAT('[', gen, ']') AS bracketed  -- Shows whitespace
FROM bronze.erp_cust_az12
ORDER BY gen;

-- Data Standardization & Consistency
-- SELECT DISTINCT
-- CASE WHEN UPPER(TRIM(BOTH '\r\n ' FROM gen)) = 'F' THEN 'Female'
-- 	 WHEN UPPER(TRIM(BOTH '\r\n ' FROM gen)) = 'FEMALE' THEN 'Female'
-- 	 WHEN UPPER(TRIM(BOTH '\r\n ' FROM gen)) = 'M' THEN 'male'
--      WHEN UPPER(TRIM(BOTH '\r\n ' FROM gen)) = 'MALE' THEN 'male'
--      WHEN gen IS NULL OR TRIM(BOTH '\r\n ' FROM gen) = '' THEN NULL
--      ELSE 'n/a'
-- END AS gen2,
-- gen
-- FROM bronze.erp_cust_az12;

-- SQL code above doesn't work becuse TRIM() in sql looked at \r and \n as literal backslashe characters
-- And did not look at them as escape sequences for carriage return and newline.
-- If TRIM() is absolutely needed then you would need to use actual hex characters that were hidden in the data. (hard to read)
-- Solution: Regular Expression handles \\r as carriage return, \\n as newline, and \\s as any whitespace character (space, tabs, etc.)
-- The + means one or more which removes all consecutive whitespace/control characters.
SELECT DISTINCT
    CASE WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'F' THEN 'Female'
         WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'FEMALE' THEN 'Female'
         WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'M' THEN 'Male'
         WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'MALE' THEN 'Male'
         WHEN gen IS NULL OR REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '') = '' THEN 'n/a'
         ELSE 'n/a'
    END AS gen2,
    gen
FROM bronze.erp_cust_az12;

-- Check silver Out of Range Dates -- will still show old customer dates, but I only fixed dates in the future of current date.
SELECT DISTINCT
bdate
FROM
silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURDATE();

-- Checking values in gen
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Check bronze Data Standardization & Consistency for normalization in silver layer
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- Data Standardization & Consistency
SELECT DISTINCT cntry,
CASE WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = 'DE' THEN 'Germany'
	 WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = 'US' THEN 'United States'
     WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = 'USA' THEN 'United States'
     WHEN REGEXP_REPLACE(cntry, '[\\r\\n]+', '') = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(REGEXP_REPLACE(cntry, '[\\r\\n]+', ''))		-- Doesn't include \\s in order to preserve, but does use TRIM for spaces outside of the value.
END AS cntry2
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- Check silver Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);	

-- Data Standardization & Consistency	(All columns of this table are good to go.
SELECT DISTINCT
cat -- can check with other columns
FROM bronze.erp_px_cat_g1v2;

