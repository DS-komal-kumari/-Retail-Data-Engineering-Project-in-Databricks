### General Instructions
## Business Context
This space analyzes retail sales performance and inventory management for a multi-store retail operation. Data includes transaction-level sales integrated with Salesforce opportunities, product catalog, customer information, and real-time inventory levels.

## Currency and Amounts
- All monetary values are in USD
- Use net_amount for revenue calculations (gross_amount minus discount_amount)
- When analyzing profitability or margins, note that cost data is not available in these tables

## Date Handling
- For sales analysis, use transaction_date (not transaction_timestamp) unless time-of-day patterns are specifically requested
- Join to dim_calendar on transaction_date for temporal groupings (year, quarter, month, week)
- When users ask for "recent" or "latest" data without specifying a timeframe, default to the last 30 days
- For year-over-year comparisons, use the year field from dim_calendar

## Key Business Metrics
- **Revenue**: SUM(net_amount) from fact_sales
- **Gross Revenue**: SUM(gross_amount) from fact_sales
- **Total Discounts**: SUM(discount_amount) from fact_sales
- **Units Sold**: SUM(quantity) from fact_sales
- **Average Transaction Value**: SUM(net_amount) / COUNT(DISTINCT transaction_id)
- **Discount Rate**: SUM(discount_amount) / SUM(gross_amount)

## Inventory Analysis
- **Low Stock Items**: Products where is_low_stock = true
- **Out of Stock**: Products where stock_quantity = 0
- **Stock at Risk**: Products where stock_quantity <= reorder_level
- Always join to dim_product to show product names, not just IDs

## Sales Channel and Payment
- sales_channel values indicate how the sale was made (online, in-store, etc.)
- payment_mode shows the payment method used
- These are important dimensions for conversion and payment analysis

## Customer Segmentation
- customer_type distinguishes between different customer segments
- Use billing_state and billing_country for geographic analysis
- industry field helps with vertical/sector analysis
- annual_revenue and number_of_employees indicate customer size

## Salesforce Integration
- Opportunity fields (opportunity_name_sf, opportunity_stage, opportunity_owner_id, opportunity_amount) link retail transactions to Salesforce CRM data
- Use these fields when analyzing pipeline conversion or sales team performance
- opportunity_stage indicates where the opportunity is in the sales process

## Common Analysis Patterns
- **Top Products**: Rank by units sold or revenue, always include product_name and category
- **Store Performance**: Group by store_id, consider joining to additional store dimension if available
- **Time Trends**: Use dim_calendar for consistent date groupings (by day, week, month, quarter, year)
- **Customer Analysis**: Join fact_sales to dim_customer to analyze by customer segment, location, or industry
