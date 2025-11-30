-- ===============================================================================
-- GOLD LAYER: DIMENSIONAL MODEL VIEWS
-- ===============================================================================
-- Purpose: Create business-ready analytical views using star schema design
-- 
-- Description:
--   - Implements dimensional modeling with fact and dimension tables
--   - Integrates data from CRM and ERP silver layer tables
--   - Generates surrogate keys for dimension tables
--   - Optimized for analytical queries and reporting
--
-- Schema Design: Star schema with central fact table and dimension tables
-- ===============================================================================

-- ===============================================================================
-- DIMENSION: CUSTOMERS
-- ===============================================================================
-- Purpose: Master customer dimension integrating CRM and ERP customer data
-- 
-- Data Sources:
--   - silver.crm_cust_info (primary source)
--   - silver.erp_cust_az12 (demographics: birthdate, gender)
--   - silver.erp_loc_a101 (location: country)
--
-- Business Rules:
--   - CRM data is master source for gender (takes precedence over ERP)
--   - Surrogate key generated via ROW_NUMBER for stable dimensional reference
--   - LEFT JOINs ensure all CRM customers included even without ERP data
-- ===============================================================================

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM will be the MASTER for gender info
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

-- ===============================================================================
-- DIMENSION: PRODUCTS
-- ===============================================================================
-- Purpose: Current product catalog dimension with category hierarchy
-- 
-- Data Sources:
--   - silver.crm_prd_info (primary source)
--   - silver.erp_px_cat_g1v2 (category hierarchy)
--
-- Business Rules:
--   - Only includes ACTIVE products (prd_end_dt IS NULL)
--   - Historical product versions excluded from dimension
--   - Surrogate key ordered by start date and product key for chronological sequence
--   - Category information enriched from ERP system
-- ===============================================================================

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
	pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- Filter out all historical data - Will also remove prd_end_dt because it will show up as all NULL values

-- ===============================================================================
-- FACT TABLE: SALES TRANSACTIONS
-- ===============================================================================
-- Purpose: Sales transaction fact table with foreign keys to dimensions
-- 
-- Data Sources:
--   - silver.crm_sales_details (primary source)
--   - gold.dim_products (foreign key relationship)
--   - gold.dim_customers (foreign key relationship)
--
-- Grain: One row per sales order line item
--
-- Measures:
--   - sales_amount: Additive measure for revenue analysis
--   - quantity: Additive measure for volume analysis  
--   - price: Semi-additive measure (unit price at time of sale)
--
-- Design Notes:
--   - Uses surrogate keys (customer_key, product_key) for dimension relationships
--   - Star schema design enables efficient analytical queries
--   - Date fields support time-based analysis and filtering
-- ===============================================================================

CREATE VIEW gold.fact_sales AS
SELECT
	sls_ord_num AS order_number,
    pr.product_key,	-- replaced sls_prd_key from left join
    cu.customer_key, -- replaced sls_cust_id from left join
    sls_order_dt AS order_date,
    sls_ship_dt AS shipping_date,
    sls_due_dt AS due_date,
    sls_sales AS sales_amount,
    sls_quantity AS quantity,
    sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

-- ===============================================================================
-- END OF GOLD LAYER DIMENSIONAL MODEL
-- ===============================================================================
