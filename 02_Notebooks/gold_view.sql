-- Databricks notebook source
-- Databricks notebook source
CREATE OR REPLACE VIEW retail_q.retail_gold.dim_customer AS

SELECT
    Id AS customer_id,
    Name AS customer_name,
    Type AS customer_type,

    BillingCity AS billing_city,
    BillingState AS billing_state,
    BillingCountry AS billing_country,

    Phone AS phone,
    Website AS website,

    Industry AS industry,
    AnnualRevenue AS annual_revenue,
    NumberOfEmployees AS number_of_employees,

    Description AS description

FROM retail_q.retail_silver.salesforce_bronze_account
WHERE Active__c = 'Yes';

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE VIEW retail_q.retail_gold.dim_product AS

SELECT
    product_id,
    product_name,
    category,
    subcategory,
    brand,

    unit_price,
    supplier_name,
    launch_date,
    updated_at

FROM retail_q.retail_silver.product_catalog
where is_active=true;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE VIEW retail_q.retail_gold.fact_inventory AS

SELECT
    inventory_id,
    product_id,
    store_id,
    stock_quantity,
    reorder_level,
    is_low_stock,
    warehouse_location,
    last_stock_update,
    processed_at,
    data_source,
    -- Source table reference
    'retail_q.retail_silver.inventory_catalog' AS source_table
FROM retail_q.retail_silver.inventory_catalog;