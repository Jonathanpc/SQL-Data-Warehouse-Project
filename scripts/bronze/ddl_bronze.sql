-- ===============================================================================
-- BRONZE LAYER: CRM & ERP CSV FILE LOADING
-- ===============================================================================
-- Purpose: Load raw data from source systems (CRM and ERP) into bronze tables
-- Author: Data Engineering Team
-- 
-- Description:
--   - Loads customer, product, sales, and location data from CSV files
--   - Implements error handling and transaction management
--   - Tracks ETL performance metrics (duration, row counts)
--   - Logs all operations to bronze.etl_log for monitoring
--
-- Usage: Run this script to perform initial or refresh load of bronze layer
-- ===============================================================================

-- ===============================================================================
-- SETUP: ETL LOGGING INFRASTRUCTURE
-- ===============================================================================

-- Create logging table to track ETL execution metrics and failures
CREATE TABLE IF NOT EXISTS bronze.etl_log (
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
-- CRM SYSTEM DATA INGESTION
-- ===============================================================================
-- Source: CRM database exports (customer, product, sales data)
-- Load Strategy: Full truncate and reload (bronze layer raw data pattern)
-- ===============================================================================

-- -----------------------------------------------------------------------------
-- Load Customer Information (CRM)
-- -----------------------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Source: cust_info.csv
-- Description: Customer demographic and profile data from CRM system
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'bronze.crm_cust_info';
SET @rows_loaded = 0;

START TRANSACTION;
    
    -- Clear existing data (bronze layer pattern: full refresh)
    TRUNCATE TABLE bronze.crm_cust_info;
    
    -- Load CSV data with UTF-8 encoding and data transformations
    LOAD DATA LOCAL INFILE '/Users/joniperez/Desktop/Folders/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES                       -- Skip header row
    (cst_id, cst_key, @cst_firstname, @cst_lastname, cst_marital_status, cst_gndr, @cst_create_date)
    SET
        cst_firstname   = TRIM(@cst_firstname),      -- Remove leading/trailing whitespace
        cst_lastname    = TRIM(@cst_lastname),
        cst_create_date = STR_TO_DATE(@cst_create_date, '%Y-%m-%d');  -- Parse date string
    
    -- Query table count (ROW_COUNT() returns -1 for LOAD DATA)
    SELECT COUNT(*) INTO @rows_loaded FROM bronze.crm_cust_info;

COMMIT;
    
-- Log successful execution with performance metrics
INSERT INTO bronze.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');
    
-- Output load confirmation to console
SELECT CONCAT(@table_name, ' loaded successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Load Product Information (CRM)
-- -----------------------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Source: prd_info.csv
-- Description: Product catalog with pricing, categories, and lifecycle dates
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'bronze.crm_prd_info';
SET @rows_loaded = 0;

START TRANSACTION;
    
    TRUNCATE TABLE bronze.crm_prd_info;
    
    LOAD DATA LOCAL INFILE '/Users/joniperez/Desktop/Folders/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (prd_id, prd_key, @prd_nm, prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
    SET
        prd_nm        = TRIM(@prd_nm),
        prd_start_dt  = STR_TO_DATE(@prd_start_dt, '%Y-%m-%d'),
        prd_end_dt    = STR_TO_DATE(@prd_end_dt, '%Y-%m-%d');
    
    SELECT COUNT(*) INTO @rows_loaded FROM bronze.crm_prd_info;

COMMIT;
    
INSERT INTO bronze.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');
    
SELECT CONCAT(@table_name, ' loaded successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Load Sales Transaction Details (CRM)
-- -----------------------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Source: sales_details.csv
-- Description: Sales order line items with quantities, pricing, and dates
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'bronze.crm_sales_details';
SET @rows_loaded = 0;

START TRANSACTION;
    
    TRUNCATE TABLE bronze.crm_sales_details;
    
    LOAD DATA LOCAL INFILE '/Users/joniperez/Desktop/Folders/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@sls_ord_num, @sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SET
        sls_ord_num   = TRIM(@sls_ord_num),
        sls_prd_key   = TRIM(@sls_prd_key);
    
    SELECT COUNT(*) INTO @rows_loaded FROM bronze.crm_sales_details;

COMMIT;
    
INSERT INTO bronze.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');
    
SELECT CONCAT(@table_name, ' loaded successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- ===============================================================================
-- ERP SYSTEM DATA INGESTION
-- ===============================================================================
-- Source: Enterprise Resource Planning (ERP) system exports
-- Load Strategy: Full truncate and reload
-- Note: ERP uses different naming conventions and schemas than CRM
-- ===============================================================================

-- -----------------------------------------------------------------------------
-- Load Customer Demographics (ERP)
-- -----------------------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Source: CUST_AZ12.csv
-- Description: Customer demographic attributes (birthdate, gender) from ERP
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'bronze.erp_cust_az12';
SET @rows_loaded = 0;

START TRANSACTION;
    
    TRUNCATE TABLE bronze.erp_cust_az12;
    
    LOAD DATA LOCAL INFILE '/Users/joniperez/Desktop/Folders/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    INTO TABLE bronze.erp_cust_az12
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@cid, @bdate, gen)
    SET
        cid     = TRIM(@cid),
        bdate   = STR_TO_DATE(@bdate, '%Y-%m-%d');
    
    SELECT COUNT(*) INTO @rows_loaded FROM bronze.erp_cust_az12;

COMMIT;
    
INSERT INTO bronze.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');
    
SELECT CONCAT(@table_name, ' loaded successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Load Customer Location Data (ERP)
-- -----------------------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Source: LOC_A101.csv
-- Description: Customer geographic location (country) from ERP system
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'bronze.erp_loc_a101';
SET @rows_loaded = 0;

START TRANSACTION;
    
    TRUNCATE TABLE bronze.erp_loc_a101;
    
    LOAD DATA LOCAL INFILE '/Users/joniperez/Desktop/Folders/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    INTO TABLE bronze.erp_loc_a101
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@cid, @cntry)
    SET
        cid     = TRIM(@cid),
        cntry   = TRIM(@cntry);
    
    SELECT COUNT(*) INTO @rows_loaded FROM bronze.erp_loc_a101;

COMMIT;
    
INSERT INTO bronze.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');
    
SELECT CONCAT(@table_name, ' loaded successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- -----------------------------------------------------------------------------
-- Load Product Category Hierarchy (ERP)
-- -----------------------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Source: PX_CAT_G1V2.csv
-- Description: Product categorization with category, subcategory, and maintenance flags
-- -----------------------------------------------------------------------------

SET @start_time = NOW(3);
SET @table_name = 'bronze.erp_px_cat_g1v2';
SET @rows_loaded = 0;

START TRANSACTION;
    
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    
    LOAD DATA LOCAL INFILE '/Users/joniperez/Desktop/Folders/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (@id, @cat, @subcat, @maintenance)
    SET
        id              = TRIM(@id),
        cat             = TRIM(@cat),
        subcat          = TRIM(@subcat),
        maintenance     = TRIM(@maintenance);
    
    SELECT COUNT(*) INTO @rows_loaded FROM bronze.erp_px_cat_g1v2;

COMMIT;
    
INSERT INTO bronze.etl_log (table_name, start_time, end_time, duration_ms, rows_loaded, status)
VALUES (@table_name, @start_time, NOW(3), TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, @rows_loaded, 'SUCCESS');
    
SELECT CONCAT(@table_name, ' loaded successfully: ', @rows_loaded, ' rows in ', 
              ROUND(TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(3))/1000, 2), ' ms') AS result;

-- ===============================================================================
-- ETL EXECUTION SUMMARY & REPORTING
-- ===============================================================================
-- Purpose: Provide comprehensive visibility into ETL run performance
-- ===============================================================================

-- Calculate total end-to-end ETL duration
SET @etl_end_time = NOW(3);
SET @total_duration_ms = TIMESTAMPDIFF(MICROSECOND, @etl_start_time, @etl_end_time)/1000;

-- -----------------------------------------------------------------------------
-- Report 1: Overall ETL Completion Summary
-- -----------------------------------------------------------------------------
-- Displays: Start/end times, total duration in ms and seconds
SELECT
    '=== ETL COMPLETED ===' AS Summary,
    @etl_start_time AS etl_start_time,
    @etl_end_time AS etl_end_time,
    ROUND(@total_duration_ms, 2) AS total_duration_ms,
    CONCAT(ROUND(@total_duration_ms/1000, 2), ' seconds') AS total_duration_formatted;

-- -----------------------------------------------------------------------------
-- Report 2: Detailed Table-Level Performance Metrics
-- -----------------------------------------------------------------------------
-- Shows: Individual table load times, row counts, and status
SELECT 
    table_name,
    start_time,
    end_time,
    ROUND(duration_ms, 2) AS duration_ms,
    rows_loaded,
    status,
    error_message
FROM bronze.etl_log
WHERE start_time >= @etl_start_time
ORDER BY start_time;

-- -----------------------------------------------------------------------------
-- Report 3: Aggregate Load Statistics
-- -----------------------------------------------------------------------------
-- Displays: Total rows loaded across all tables and cumulative load time
SELECT
    SUM(rows_loaded) AS total_rows_loaded,
    COUNT(*) AS tables_loaded,
    ROUND(SUM(duration_ms), 2) AS total_load_time_ms
FROM bronze.etl_log
WHERE start_time >= @etl_start_time AND status = 'SUCCESS';

-- ===============================================================================
-- END OF BRONZE LAYER
-- ===============================================================================
