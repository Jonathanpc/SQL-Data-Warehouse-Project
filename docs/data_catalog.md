# Gold Layer Data Catalog

## Overview

The Gold layer represents the final, business-ready data models optimized for analytics and reporting. This layer consists of dimensional models (star schema) with denormalized views that combine data from multiple silver layer tables. The Gold layer provides clean, consistent, and performant access to integrated data from both CRM and ERP systems.

---

## Dimension Tables

### gold.dim_customers

**Purpose:** Provides a comprehensive view of customer information by integrating CRM customer data with ERP demographic and location data. This dimension serves as the master customer reference for all analytical queries.

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| customer_key | INT | Surrogate key generated using ROW_NUMBER, uniquely identifies each customer record |
| customer_id | INT | Natural key from CRM system, business identifier for the customer |
| customer_number | NVARCHAR(50) | Customer reference number used across systems |
| first_name | NVARCHAR(50) | Customer's first name |
| last_name | NVARCHAR(50) | Customer's last name |
| country | NVARCHAR(50) | Customer's country of residence from ERP location data |
| marital_status | NVARCHAR(50) | Marital status (Single, Married, n/a) |
| gender | NVARCHAR(50) | Gender (Female, Male, n/a), prioritizes CRM data over ERP |
| birthdate | DATE | Customer's date of birth from ERP demographic data |
| create_date | DATE | Date when customer record was created in CRM system |

**Data Sources:**
- Primary: `silver.crm_cust_info`
- Supplemental: `silver.erp_cust_az12` (demographics), `silver.erp_loc_a101` (location)

---

### gold.dim_products

**Purpose:** Provides current product catalog information by combining CRM product data with ERP category hierarchy. Only includes active products (filters out historical records where end_date is not null).

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| product_key | INT | Surrogate key generated using ROW_NUMBER, uniquely identifies each product record |
| product_id | INT | Natural key from CRM system, business identifier for the product |
| product_number | NVARCHAR(50) | Product reference number used in transactions |
| product_name | NVARCHAR(50) | Full name of the product |
| category_id | NVARCHAR(50) | Category identifier linking to ERP category hierarchy |
| category | NVARCHAR(50) | Primary product category from ERP |
| subcategory | NVARCHAR(50) | Product subcategory providing additional classification |
| maintenance | NVARCHAR(50) | Maintenance flag or status from ERP system |
| cost | INT | Product cost amount |
| product_line | NVARCHAR(50) | Product line (Mountain, Road, Touring, Other Sales) |
| start_date | DATE | Date when product became active |

**Data Sources:**
- Primary: `silver.crm_prd_info`
- Supplemental: `silver.erp_px_cat_g1v2` (category hierarchy)

**Business Rules:**
- Only includes current products (`prd_end_dt IS NULL`)
- Historical product versions are excluded from this view

---

## Fact Tables

### gold.fact_sales

**Purpose:** Contains transactional sales data with foreign keys to customer and product dimensions. This fact table enables analysis of sales performance across products, customers, time periods, and other dimensions.

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| order_number | NVARCHAR(50) | Unique identifier for the sales order |
| product_key | INT | Foreign key to gold.dim_products, references the product sold |
| customer_key | INT | Foreign key to gold.dim_customers, references the purchasing customer |
| order_date | DATE | Date when the order was placed |
| shipping_date | DATE | Date when the order was shipped |
| due_date | DATE | Date when the order payment is due |
| sales_amount | INT | Total sales amount for the transaction |
| quantity | INT | Quantity of products sold in this transaction |
| price | INT | Unit price of the product at time of sale |

**Data Sources:**
- Primary: `silver.crm_sales_details`
- Foreign Keys: `gold.dim_products`, `gold.dim_customers`

**Grain:** One row per sales order line item

**Measures:**
- `sales_amount`: Additive measure for revenue analysis
- `quantity`: Additive measure for volume analysis
- `price`: Semi-additive measure (average for aggregations)

---

## Relationships
```
gold.dim_customers (1) ----< (∞) gold.fact_sales
    customer_key                    customer_key

gold.dim_products (1) ----< (∞) gold.fact_sales
    product_key                    product_key
```

**Star Schema Design:**
- Fact table (`gold.fact_sales`) sits at the center
- Dimension tables (`gold.dim_customers`, `gold.dim_products`) radiate outward
- Simple one-to-many relationships enable efficient query performance
- Surrogate keys used for dimension relationships

---

## Usage Notes

- All Gold layer objects are **views**, not materialized tables
- Views automatically reflect updates to underlying Silver layer tables
- Use surrogate keys (`customer_key`, `product_key`) for joins to fact tables
- Natural keys (`customer_id`, `product_id`) available for business user reference
- Date fields can be used to create time-based filters and aggregations
