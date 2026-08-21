from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.blob_transaction",
    comment="Standardized blob transaction data with data quality rules and business logic applied"
)
def transactions_clean():
    """Simple transformation: read streaming data and apply basic standardization"""
    return (
        spark.readStream.table("retail_q.blob_bronze.blob_data")
        
        # Basic data type conversions
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("selling_price", F.col("selling_price").cast("decimal(12,2)"))
        .withColumn("discount_amount", F.col("discount_amount").cast("decimal(12,2)"))
        
        # Calculate key metrics
        .withColumn("gross_amount", F.col("quantity") * F.col("selling_price"))
        .withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))
        
        # Convert timestamp
        .withColumn("transaction_timestamp", 
                    F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a"))
        
        # Basic data quality: remove nulls
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("store_id").isNotNull())
    )