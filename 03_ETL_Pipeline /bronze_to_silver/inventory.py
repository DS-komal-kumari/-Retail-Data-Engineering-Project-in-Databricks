from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.inventory_catalog",
    comment="Standardized inventory catalog with data quality rules applied",
    cluster_by=["store_id", "product_id"]
)
def inventory_catalog_silver():
    """
    Reads streaming data from bronze layer and applies standardization:
    - Trims and standardizes ID fields to uppercase
    - Standardizes location text to title case
    - Validates stock quantities and reorder levels are non-negative
    - Adds inventory status flag (low stock warning)
    - Filters out invalid records
    - Adds processing metadata
    """
    return (
        spark.readStream.table("retail_q.postgres_bronze.inventory")
        # Standardize IDs: trim and uppercase
        .withColumn("inventory_id", F.upper(F.trim(F.col("inventory_id"))))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("store_id", F.upper(F.trim(F.col("store_id"))))
        
        # Standardize location text: trim and title case
        .withColumn("warehouse_location", F.initcap(F.trim(F.col("warehouse_location"))))
        
        # Standardize quantities: ensure non-negative
        .withColumn("stock_quantity", 
                    F.when(F.col("stock_quantity") < 0, 0).otherwise(F.col("stock_quantity")))
        .withColumn("reorder_level", 
                    F.when(F.col("reorder_level") < 0, 0).otherwise(F.col("reorder_level")))
        
        # Add business logic: low stock indicator
        .withColumn("is_low_stock", 
                    F.when(F.col("stock_quantity") <= F.col("reorder_level"), True)
                     .otherwise(False))
        
        # Data quality rules: filter out invalid records
        .filter(F.col("inventory_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("store_id").isNotNull())
        .filter(F.col("stock_quantity").isNotNull())
        
        # Add processing metadata
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("data_source", F.lit("postgres_bronze"))
        
        # Select final columns
        .select(
            "inventory_id",
            "product_id",
            "store_id",
            "stock_quantity",
            "reorder_level",
            "is_low_stock",
            "warehouse_location",
            "last_stock_update",
            "processed_at",
            "data_source"
        )
    )