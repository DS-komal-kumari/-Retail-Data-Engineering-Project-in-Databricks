from pyspark.sql.functions import upper, trim, sum as _sum, countDistinct, col, when, xxhash64, abs as _abs
from pyspark import pipelines as dp

@dp.materialized_view(
    name="retail_q.retail_gold.fact_sales",
    comment="Gold layer fact table combining transaction data with Salesforce opportunity details"
)
def fact_sales():
    transactions_df = spark.read.table("retail_q.retail_silver.blob_transaction")
    opportunity_df = spark.read.table("retail_q.retail_silver.salesforce_bronze_opportunity")
    
    # Get customer IDs for hash-based assignment
    customer_ids = [row.customer_id for row in spark.read.table("retail_q.retail_gold.dim_customer").select("customer_id").orderBy("customer_id").collect()]
    
    joined_df = transactions_df.alias("t").join(
        opportunity_df.alias("o"),
        upper(trim(transactions_df.opportunity_name)) == upper(trim(opportunity_df.Name)),
        how="left"
    )
    
    # Select important columns
    selected_df = joined_df.select(
        "t.transaction_id",
        "t.opportunity_name",
        "t.product_id",
        "t.store_id",
        "t.quantity",
        "t.selling_price",
        "t.discount_amount",
        "t.gross_amount",
        "t.net_amount",
        "t.transaction_timestamp",
        col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        "t.payment_mode",
        "t.sales_channel",
        col("o.Name").alias("opportunity_name_sf"),
        col("o.StageName").alias("opportunity_stage"),
        col("o.OwnerId").alias("opportunity_owner_id"),
        col("o.Amount").alias("opportunity_amount"),
        col("o.AccountId")
    )
    
    # Assign customer_id using hash if AccountId is null
    hash_index = (_abs(xxhash64(col("transaction_id"))) % len(customer_ids)).cast("int")
    
    # Build when-otherwise chain for customer assignment
    customer_assignment = col("AccountId")
    for i, cid in enumerate(customer_ids):
        customer_assignment = when(col("AccountId").isNull() & (hash_index == i), cid).otherwise(customer_assignment)
    
    result_df = selected_df.withColumn("customer_id", customer_assignment).drop("AccountId")
    
    return result_df