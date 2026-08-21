-- Databricks notebook source
CREATE VIEW retail_q.retail_semantic.retail_metrics_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: retail_q.retail_gold.fact_sales
comment: Retail sales metrics analyzing revenue, quantity, and performance across products, customers, and time
joins:
  - name: customers
    source: retail_q.retail_gold.dim_customer
    on: source.customer_id = customers.customer_id
  - name: products
    source: retail_q.retail_gold.dim_product
    on: source.product_id = products.product_id
  - name: calendar
    source: retail_q.retail_gold.dim_calendar
    on: source.transaction_date = calendar.date
dimensions:
  - name: Product Category
    expr: products.category
    display_name: Product Category
    comment: Product category from the product dimension
  - name: Customer Type
    expr: customers.customer_type
    display_name: Customer Type
    comment: Type of customer (B2B, B2C, etc.)
  - name: Sales Channel
    expr: source.sales_channel
    display_name: Sales Channel
    comment: Channel through which the sale was made
  - name: Payment Mode
    expr: source.payment_mode
    display_name: Payment Mode
    comment: Method of payment used for the transaction
  - name: Year Month
    expr: calendar.year_month
    display_name: Year Month
    comment: Year and month of the transaction
measures:
  - name: Total Sales Revenue
    expr: SUM(net_amount)
    display_name: Total Sales Revenue
    comment: Sum of all net sales amounts
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: Total Quantity Sold
    expr: SUM(quantity)
    display_name: Total Quantity Sold
    comment: Total number of units sold
  - name: Total Discount
    expr: SUM(discount_amount)
    display_name: Total Discount
    comment: Total discounts applied to transactions
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: Transaction Count
    expr: COUNT(1)
    display_name: Transaction Count
    comment: Total number of transactions
  - name: Average Order Value
    expr: SUM(net_amount) / COUNT(1)
    display_name: Average Order Value
    comment: Average revenue per transaction
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
$$