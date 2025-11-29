-- ===============================================================================
-- SILVER LAYER: DATA CLEANSING AND TRANSFORMATION
-- ===============================================================================
-- Purpose: Transform and Cleanse raw Bronze Layer data into standardized Silver Layer
-- 
-- Description:
--   - Cleanses and normalizes customer, product, sales, and location data
--   - Handles data quality issues (duplicates, missing values, invalid dates)
--   - Standardizes formats and applies business rules
--   - Enriches data through derivation and calculation
--   - Tracks ETL performance metrics (duration, row counts)
--   - Logs all operations to bronze.etl_log for monitoring
--
-- Usage: Run this script after bronze layer load to create cleansed silver tables
-- ===============================================================================

-- ===============================================================================
-- SETUP: ETL LOGGING INFRASTRUCTURE
-- ===============================================================================

-- Create logging table to track ETL execution metrics and failures
CREATE TABLE IF NOT EXISTS silver.etl_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100),
    start_time DATETIME(3),              -- Millisecond precision for accurate timing
    end_time DATETIME(3),
    duration_ms DECIMAL(10,2),           -- Duration in milliseconds
    rows_loaded INT,
    status VARCHAR(20),                  -- SUCCESS or FAILED
    error_message TEXT,                  -- Stores error details if load fails
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Clear previous test runs (comment out in production to preserve history)
-- TRUNCATE TABLE bronze.etl_log;

-- Initialize overall ETL timer for end-to-end duration tracking
SET @etl_start_time = NOW(3);


-- ===============================================================================
-- CRM SYSTEM DATA CLEANSING
-- ===============================================================================
-- Source: Bronze layer CRM tables
-- Transformation Strategy: Deduplicate, normalize, standardize formats
-- ===============================================================================

-- -----------------------------------------------------------------------------
-- Clean Customer Information (CRM)
-- -----------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- Source: bronze.crm_cust_info
-- Transformations:
--   - Deduplication: Keep most recent record per customer
--   - Normalization: Standardize marital status and gender codes
--   - Data quality: Trim whitespace
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'silver.crm_cust_info';
SET @rows_loaded = 0;

START TRANSACTION;

	TRUNCATE TABLE silver.crm_cust_info;

	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,	-- Already trimmed when bronze layer ingested the data during loading
	TRIM(cst_lastname) AS cst_lastname,	-- this too (went ahead of myself and did it in bronze layer)
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'			-- Data normalization
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date
	FROM (
	SElECT *,		-- Selecting the most recent data duplicate b/c they contain more info than their duplicates
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	)t WHERE flag_last = 1;

	SELECT COUNT(*) INTO @rows_loaded FROM silver.crm_cust_info;
COMMIT;


INSERT INTO silver.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');

SELECT CONCAT(@table_name, ' cleaned successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Clean Product Information (CRM)
-- -----------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- Source: bronze.crm_prd_info
-- Transformations:
--   - Key extraction: Split product key to create category ID
--   - Normalization: Expand product line codes to full names
--   - Data enrichment: Calculate proper end dates from next start date
--   - Data quality: Handle null costs
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'silver.crm_prd_info';
SET @rows_loaded = 0;

START TRANSACTION;

	TRUNCATE TABLE silver.crm_prd_info;

	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,		-- for matching ids from 'SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2'
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	IFNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'		-- Data Normalization
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,	-- If you have it set as DATETIME you can use CAST(prd_start_dt AS DATE) AS prd_start_dt to only display year-month-day
	DATE_SUB(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),	-- Data Enrichment
		INTERVAL 1 DAY		-- Correcting the Data by making the invalid end date into the next rows start date minus 1 day so days don't overlap.
		)AS prd_end_dt
	FROM bronze.crm_prd_info;
    
    SELECT COUNT(*) INTO @rows_loaded FROM silver.crm_prd_info;
    
COMMIT;

INSERT INTO silver.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');

SELECT CONCAT(@table_name, ' cleaned successfully: ', @rows_loaded, ' rows in ',
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;
    
-- -----------------------------------------------------------------------------
-- Clean Sales Transaction Details (CRM)
-- -----------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- Source: bronze.crm_sales_details
-- Transformations:
--   - Data quality: Fix circular dependency between price and sales
--   - Data derivation: Calculate missing sales values from qty * price
--   - Date handling: Convert integer dates to proper DATE format
--   - Validation: Handle invalid dates (zeros, wrong length)
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'silver.crm_sales_details';
SET @rows_loaded = 0;

START TRANSACTION;

	TRUNCATE TABLE silver.crm_sales_details;

	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	WITH cleaned_price AS (
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price,
		CASE WHEN sls_price IS NULL OR sls_price <= 0	-- FIxing price to use before.
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE ABS(sls_price)
		END AS corrected_price
		FROM bronze.crm_sales_details
	)
	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL	-- handling invalid data - type of transformation
		 ELSE CAST(sls_order_dt AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(sls_ship_dt AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(sls_due_dt AS DATE)
	END AS sls_due_dt,
	-- Circular reference of two unperfect columns won't work and is less efficient in mysql instead fix one (price) before cleaning. Also don't forget to truncate old data when re-inserting
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * corrected_price	-- handling missing data and invalid data by deriving the data from other perfect data columns.
		 THEN sls_quantity * corrected_price
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	corrected_price AS sls_price
	FROM cleaned_price;
    
    SELECT COUNT(*) INTO @rows_loaded FROM silver.crm_sales_details;

COMMIT;

INSERT INTO silver.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');

SELECT CONCAT(@table_name, ' cleaned successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- ===============================================================================
-- ERP SYSTEM DATA CLEANSING
-- ===============================================================================
-- Source: Bronze layer ERP tables
-- Transformation Strategy: Normalize IDs, standardize codes, remove control chars
-- ===============================================================================

-- -----------------------------------------------------------------------------
-- Clean Customer Demographics (ERP)
-- -----------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- Source: bronze.erp_cust_az12
-- Transformations:
--   - ID normalization: Remove 'NAS' prefix from customer IDs
--   - Date validation: Filter out future birth dates
--   - Gender standardization: Handle hidden characters (CR/LF), normalize values
--   - Data quality: Use REGEXP_REPLACE to handle control characters
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'silver.erp_cust_az12';
SET @rows_loaded = 0;

START TRANSACTION;

	TRUNCATE TABLE silver.erp_cust_az12;

	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))		-- cleaning cid of the 3 letter unvaluable starting characters
		 ELSE cid
	END as cid,
	CASE WHEN bdate > CURDATE() THEN NULL	-- cleaning obvious future dates.
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'F' THEN 'Female'		-- UPPER(TRIM()) didn't work here because there were multiple hidden characters
		 WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'FEMALE' THEN 'Female'	-- ^Which REGEXP_REPLACE can cover. Although UPPER(TRIM()) would work in sql server.
		 WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'M' THEN 'Male'
		 WHEN UPPER(REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '')) = 'MALE' THEN 'Male'
		 WHEN gen IS NULL OR REGEXP_REPLACE(gen, '[\\r\\n\\s]+', '') = '' THEN 'n/a'
			 ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;

	SELECT COUNT(*) INTO @rows_loaded FROM silver.erp_cust_az12;

COMMIT;

INSERT INTO silver.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');

SELECT CONCAT(@table_name, ' cleaned successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Clean Customer Location Data (ERP)
-- -----------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- Source: bronze.erp_loc_a101
-- Transformations:
--   - ID normalization: Remove dashes from customer IDs for consistency
--   - Country standardization: Expand country codes, handle variations
--   - Data quality: Remove control characters while preserving internal spaces
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'silver.erp_loc_a101';
SET @rows_loaded = 0;

START TRANSACTION;

	TRUNCATE TABLE silver.erp_loc_a101;

	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT
	REPLACE(cid, '-', '') cid,	-- normalizing the column to match the key value of another table
	CASE WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = 'DE' THEN 'Germany'
		 WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = 'US' THEN 'United States'
		 WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = 'USA' THEN 'United States'
		 WHEN REGEXP_REPLACE(cntry, '[\\r\\n\\s]+', '') = '' OR cntry IS NULL THEN 'n/a'	-- Handling missing values
		 ELSE TRIM(REGEXP_REPLACE(cntry, '[\\r\\n]+', ''))		-- Doesn't include \\s in order to preserve, but does use TRIM for spaces outside of the value.
	END AS cntry
	FROM bronze.erp_loc_a101;
    
    SELECT COUNT(*) INTO @rows_loaded FROM silver.erp_loc_a101;

COMMIT;

INSERT INTO silver.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');

SELECT CONCAT(@table_name, ' cleaned successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Clean Product Category Hierarchy (ERP)
-- -----------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- Source: bronze.erp_px_cat_g1v2
-- Transformations: None - data quality verified as acceptable
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'silver.erp_px_cat_g1v2';
SET @rows_loaded = 0;

START TRANSACTION;

	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	-- This table's values have been checked and are good to go
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
    
    SELECT COUNT(*) INTO @rows_loaded FROM silver.erp_px_cat_g1v2;

COMMIT;

INSERT INTO silver.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');

SELECT CONCAT(@table_name, ' cleaned successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- ===============================================================================
-- ETL EXECUTION SUMMARY & REPORTING
-- ===============================================================================
-- Purpose: Provide comprehensive visibility into silver layer ETL performance
-- ===============================================================================

-- Calculate total end-to-end ETL duration
SET @etl_end_time = NOW(3);
SET @total_duration_ms = TIMESTAMPDIFF(MICROSECOND, @etl_start_time, @etl_end_time)/1000;

-- -----------------------------------------------------------------------------
-- Report 1: Overall ETL Completion Summary
-- -----------------------------------------------------------------------------
-- Displays: Start/end times, total duration in ms and seconds
SELECT
    '=== SILVER LAYER ETL COMPLETED ===' AS Summary,
    @etl_start_time AS etl_start_time,
    @etl_end_time AS etl_end_time,
    ROUND(@total_duration_ms, 2) AS total_duration_ms,
    CONCAT(ROUND(@total_duration_ms/1000, 2), ' seconds') AS total_duration_formatted;

-- -----------------------------------------------------------------------------
-- Report 2: Detailed Table-Level Performance Metrics
-- -----------------------------------------------------------------------------
-- Shows: Individual table clean times, row counts, and status
SELECT 
    table_name,
    start_time,
    end_time,
    ROUND(duration_ms, 2) AS duration_ms,
    rows_loaded,
    status,
    error_message
FROM silver.etl_log
WHERE start_time >= @etl_start_time
ORDER BY start_time;

-- -----------------------------------------------------------------------------
-- Report 3: Aggregate Load Statistics
-- -----------------------------------------------------------------------------
-- Displays: Total rows cleaned across all tables and cumulative processing time
SELECT
    SUM(rows_loaded) AS total_rows_cleaned,
    COUNT(*) AS tables_processed,
    ROUND(SUM(duration_ms), 2) AS total_processing_time_ms
FROM silver.etl_log
WHERE start_time >= @etl_start_time AND status = 'SUCCESS';

-- ===============================================================================
-- END OF SILVER LAYER CLEANSING
-- ===============================================================================
