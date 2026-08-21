from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.product_catalog",
    comment="Standardized product catalog with data quality rules applied",
    cluster_by=["category", "subcategory"]
)
def product_catalog_silver():
    return (
        spark.readStream.table("retail_q.postgres_bronze.product_catalog")
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("subcategory", F.initcap(F.trim(F.col("subcategory"))))
        .withColumn("brand", F.initcap(F.trim(F.col("brand"))))
        .withColumn("supplier_name", F.initcap(F.trim(F.col("supplier_name"))))
        .withColumn("unit_price", F.when(F.col("unit_price") < 0, 0).otherwise(F.col("unit_price")))
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("product_name").isNotNull())
        .filter(F.col("category").isNotNull())
        .filter(F.col("unit_price").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("data_source", F.lit("postgres_bronze"))
        .select(
            "product_id", "product_name", "category", "subcategory",
            "brand", "unit_price", "supplier_name", "launch_date",
            "is_active", "updated_at", "processed_at", "data_source"
        )
    )