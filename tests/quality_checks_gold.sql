-- ===============================================================================
-- GOLD LAYER: DATA QUALITY AND INTEGRITY CHECKS
-- ===============================================================================
-- Purpose: Validate dimensional model integrity and data quality
-- 
-- Description:
--   - Checks for duplicate surrogate keys in dimension tables
--   - Validates foreign key relationships between fact and dimension tables
--   - Identifies orphaned records (fact records without matching dimensions)
--
-- Usage: Run these checks after gold layer views are created or refreshed
-- ===============================================================================

-- ===============================================================================
-- CHECK 1: CUSTOMER DIMENSION - DUPLICATE SURROGATE KEYS
-- ===============================================================================
-- Purpose: Verify uniqueness of customer_key (surrogate key)
-- 
-- Expected Result: No rows returned (all surrogate keys should be unique)
-- Issue Detection: If rows returned, indicates problem with ROW_NUMBER generation
-- ===============================================================================

SELECT
	customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ===============================================================================
-- CHECK 2: PRODUCT DIMENSION - DUPLICATE SURROGATE KEYS
-- ===============================================================================
-- Purpose: Verify uniqueness of product_key (surrogate key)
-- 
-- Expected Result: No rows returned (all surrogate keys should be unique)
-- Issue Detection: If rows returned, indicates problem with ROW_NUMBER generation
-- ===============================================================================

SELECT
	product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ===============================================================================
-- CHECK 3: FOREIGN KEY INTEGRITY - ORPHANED FACT RECORDS
-- ===============================================================================
-- Purpose: Identify sales transactions with missing customer or product references
-- 
-- Expected Result: No rows returned (all fact records should have valid dimensions)
-- Issue Detection: 
--   - If p.product_key IS NULL: Sales exist for non-existent/inactive products
--   - If c.customer_key IS NULL: Sales exist for non-existent customers
--
-- Remediation: 
--   - Review silver layer data quality
--   - Verify dimension filters (e.g., prd_end_dt IS NULL for products)
--   - Check join key consistency between layers
-- ===============================================================================

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

-- ===============================================================================
-- END OF DATA QUALITY CHECKS
-- ===============================================================================
