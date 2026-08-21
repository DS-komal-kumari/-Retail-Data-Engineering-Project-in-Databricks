# Databricks notebook source
# DBTITLE 1,Auto Loader: Blob to Bronze
# Auto Loader to read CSV files from volume and write to bronze table

# Source path
source_path = "/Volumes/retail_q/volumes/blob_source"

# Target table
target_table = "retail_q.blob_bronze.blob_data"

# Read CSV files using Auto Loader (cloudFiles)
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/_schema")
  .load(source_path)
)

# Write to bronze table using Auto Loader
query = (df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/_checkpoint")
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable(target_table)
)

# Wait for the stream to process available data and complete
query.awaitTermination()

print(f"Successfully processed CSV files into {target_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retail_q.blob_bronze.blob_data;