from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.salesforce_bronze_account",
    comment="Standardized Salesforce Account data with data quality rules applied",
    cluster_by=["Type", "Industry"]
)
def salesforce_bronze_account():
    """
    Reads streaming data from Salesforce Account table and applies standardization:
    - Trims and standardizes ID fields to uppercase
    - Standardizes text fields (names, addresses) to title case
    - Standardizes state/country codes to uppercase
    - Validates numeric fields are non-negative
    - Filters deleted records
    - Adds business logic flags
    - Adds processing metadata
    """
    return (
        spark.readStream.option("skipChangeCommits", "true").table("retail_q.salesforce.account")
        # Filter out deleted records
        .filter(F.col("IsDeleted") == False)
        
        # Standardize IDs: uppercase
        .withColumn("Id", F.upper(F.trim(F.col("Id"))))
        .withColumn("MasterRecordId", F.upper(F.trim(F.col("MasterRecordId"))))
        .withColumn("ParentId", F.upper(F.trim(F.col("ParentId"))))
        .withColumn("OwnerId", F.upper(F.trim(F.col("OwnerId"))))
        .withColumn("AccountNumber", F.upper(F.trim(F.col("AccountNumber"))))
        
        # Standardize account name and description: title case
        .withColumn("Name", F.initcap(F.trim(F.col("Name"))))
        .withColumn("Type", F.initcap(F.trim(F.col("Type"))))
        .withColumn("Industry", F.initcap(F.trim(F.col("Industry"))))
        .withColumn("Ownership", F.initcap(F.trim(F.col("Ownership"))))
        .withColumn("Rating", F.initcap(F.trim(F.col("Rating"))))
        
        # Standardize billing address: title case for city/state/country
        .withColumn("BillingStreet", F.initcap(F.trim(F.col("BillingStreet"))))
        .withColumn("BillingCity", F.initcap(F.trim(F.col("BillingCity"))))
        .withColumn("BillingState", F.initcap(F.trim(F.col("BillingState"))))
        .withColumn("BillingCountry", F.initcap(F.trim(F.col("BillingCountry"))))
        .withColumn("BillingPostalCode", F.upper(F.trim(F.col("BillingPostalCode"))))
        .withColumn("BillingStateCode", F.upper(F.trim(F.col("BillingStateCode"))))
        .withColumn("BillingCountryCode", F.upper(F.trim(F.col("BillingCountryCode"))))
        
        # Standardize shipping address: title case for city/state/country
        .withColumn("ShippingStreet", F.initcap(F.trim(F.col("ShippingStreet"))))
        .withColumn("ShippingCity", F.initcap(F.trim(F.col("ShippingCity"))))
        .withColumn("ShippingState", F.initcap(F.trim(F.col("ShippingState"))))
        .withColumn("ShippingCountry", F.initcap(F.trim(F.col("ShippingCountry"))))
        .withColumn("ShippingPostalCode", F.upper(F.trim(F.col("ShippingPostalCode"))))
        .withColumn("ShippingStateCode", F.upper(F.trim(F.col("ShippingStateCode"))))
        .withColumn("ShippingCountryCode", F.upper(F.trim(F.col("ShippingCountryCode"))))
        
        # Standardize contact info: trim phone/fax/website
        .withColumn("Phone", F.trim(F.col("Phone")))
        .withColumn("Fax", F.trim(F.col("Fax")))
        .withColumn("Website", F.trim(F.col("Website")))
        
        # Validate numeric fields: ensure non-negative
        .withColumn("AnnualRevenue", 
                    F.when(F.col("AnnualRevenue") < 0, 0).otherwise(F.col("AnnualRevenue")))
        .withColumn("NumberOfEmployees", 
                    F.when(F.col("NumberOfEmployees") < 0, 0).otherwise(F.col("NumberOfEmployees")))
        .withColumn("NumberofLocations__c", 
                    F.when(F.col("NumberofLocations__c") < 0, 0).otherwise(F.col("NumberofLocations__c")))
        
        # Add business logic flags
        .withColumn("is_enterprise", 
                    F.when((F.col("AnnualRevenue") > 1000000) | (F.col("NumberOfEmployees") > 500), True)
                     .otherwise(False))
        .withColumn("has_parent", 
                    F.when(F.col("ParentId").isNotNull(), True).otherwise(False))
        .withColumn("is_publicly_traded", 
                    F.when(F.col("TickerSymbol").isNotNull(), True).otherwise(False))
        
        # Data quality rules: filter out invalid core records
        .filter(F.col("Id").isNotNull())
        .filter(F.col("Name").isNotNull())
        
        # Add processing metadata
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("data_source", F.lit("salesforce"))
        
        # Select final columns (excluding SCD Type 2 columns)
        .select(
            "Id", "Name", "Type", "AccountNumber", "ParentId", "MasterRecordId",
            "BillingStreet", "BillingCity", "BillingState", "BillingPostalCode",
            "BillingCountry", "BillingStateCode", "BillingCountryCode",
            "BillingLatitude", "BillingLongitude", "BillingGeocodeAccuracy",
            "ShippingStreet", "ShippingCity", "ShippingState", "ShippingPostalCode",
            "ShippingCountry", "ShippingStateCode", "ShippingCountryCode",
            "ShippingLatitude", "ShippingLongitude", "ShippingGeocodeAccuracy",
            "Phone", "Fax", "Website", "PhotoUrl",
            "Industry", "AnnualRevenue", "NumberOfEmployees", "Ownership",
            "TickerSymbol", "Description", "Rating", "Site",
            "Sic", "SicDesc", "NaicsCode", "NaicsDesc", "YearStarted",
            "AccountSource", "DunsNumber", "Tradestyle",
            "OwnerId", "CreatedDate", "CreatedById",
            "LastModifiedDate", "LastModifiedById", "SystemModstamp",
            "LastActivityDate", "LastViewedDate", "LastReferencedDate",
            "CustomerPriority__c", "SLA__c", "Active__c",
            "NumberofLocations__c", "UpsellOpportunity__c",
            "SLASerialNumber__c", "SLAExpirationDate__c",
            "is_enterprise", "has_parent", "is_publicly_traded",
            "processed_at", "data_source"
        )
    )