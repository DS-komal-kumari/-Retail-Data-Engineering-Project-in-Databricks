from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="retail_q.retail_silver.salesforce_bronze_opportunity",
    comment="Standardized Salesforce Opportunity data with data quality rules applied",
    cluster_by=["StageName", "FiscalYear"]
)
def salesforce_bronze_opportunity():
    """
    Reads streaming data from Salesforce Opportunity table and applies standardization:
    - Trims and standardizes ID fields to uppercase
    - Standardizes text fields (names, stages, types) to title case
    - Validates numeric fields are non-negative
    - Filters deleted records
    - Adds business logic flags for opportunity classification
    - Adds processing metadata
    """
    return (
        spark.readStream.option("skipChangeCommits", "true").table("retail_q.salesforce.opportunity")
        # Filter out deleted records
        .filter(F.col("IsDeleted") == False)
        
        # Standardize IDs: uppercase
        .withColumn("Id", F.upper(F.trim(F.col("Id"))))
        .withColumn("AccountId", F.upper(F.trim(F.col("AccountId"))))
        .withColumn("OwnerId", F.upper(F.trim(F.col("OwnerId"))))
        .withColumn("ContactId", F.upper(F.trim(F.col("ContactId"))))
        .withColumn("CampaignId", F.upper(F.trim(F.col("CampaignId"))))
        .withColumn("Pricebook2Id", F.upper(F.trim(F.col("Pricebook2Id"))))
        .withColumn("CreatedById", F.upper(F.trim(F.col("CreatedById"))))
        .withColumn("LastModifiedById", F.upper(F.trim(F.col("LastModifiedById"))))
        
        # Standardize opportunity details: title case
        .withColumn("Name", F.initcap(F.trim(F.col("Name"))))
        .withColumn("StageName", F.initcap(F.trim(F.col("StageName"))))
        .withColumn("Type", F.initcap(F.trim(F.col("Type"))))
        .withColumn("NextStep", F.initcap(F.trim(F.col("NextStep"))))
        .withColumn("LeadSource", F.initcap(F.trim(F.col("LeadSource"))))
        .withColumn("ForecastCategory", F.initcap(F.trim(F.col("ForecastCategory"))))
        .withColumn("ForecastCategoryName", F.initcap(F.trim(F.col("ForecastCategoryName"))))
        
        # Standardize custom fields
        .withColumn("DeliveryInstallationStatus__c", F.initcap(F.trim(F.col("DeliveryInstallationStatus__c"))))
        .withColumn("TrackingNumber__c", F.upper(F.trim(F.col("TrackingNumber__c"))))
        .withColumn("OrderNumber__c", F.upper(F.trim(F.col("OrderNumber__c"))))
        .withColumn("CurrentGenerators__c", F.trim(F.col("CurrentGenerators__c")))
        .withColumn("MainCompetitors__c", F.trim(F.col("MainCompetitors__c")))
        
        # Validate numeric fields: ensure non-negative
        .withColumn("Amount", 
                    F.when(F.col("Amount") < 0, 0).otherwise(F.col("Amount")))
        .withColumn("ExpectedRevenue", 
                    F.when(F.col("ExpectedRevenue") < 0, 0).otherwise(F.col("ExpectedRevenue")))
        .withColumn("TotalOpportunityQuantity", 
                    F.when(F.col("TotalOpportunityQuantity") < 0, 0).otherwise(F.col("TotalOpportunityQuantity")))
        
        # Validate probability: ensure between 0 and 100
        .withColumn("Probability", 
                    F.when(F.col("Probability") < 0, 0)
                     .when(F.col("Probability") > 100, 100)
                     .otherwise(F.col("Probability")))
        
        # Add business logic flags
        .withColumn("is_large_deal", 
                    F.when(F.col("Amount") >= 100000, True).otherwise(False))
        .withColumn("is_high_probability", 
                    F.when(F.col("Probability") >= 75, True).otherwise(False))
        .withColumn("is_overdue", 
                    F.when((F.col("IsClosed") == False) & (F.col("CloseDate") < F.current_date()), True)
                     .otherwise(False))
        .withColumn("deal_size_category", 
                    F.when(F.col("Amount") < 10000, "Small")
                     .when((F.col("Amount") >= 10000) & (F.col("Amount") < 100000), "Medium")
                     .when(F.col("Amount") >= 100000, "Large")
                     .otherwise("Unknown"))
        .withColumn("days_to_close", 
                    F.datediff(F.col("CloseDate"), F.current_date()))
        
        # Data quality rules: filter out invalid core records
        .filter(F.col("Id").isNotNull())
        .filter(F.col("Name").isNotNull())
        .filter(F.col("StageName").isNotNull())
        
        # Add processing metadata
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("data_source", F.lit("salesforce"))
        
        # Select final columns
        .select(
            "Id", "AccountId", "Name", "Description",
            "StageName", "Amount", "Probability", "ExpectedRevenue",
            "TotalOpportunityQuantity", "CloseDate", "Type",
            "NextStep", "LeadSource", "IsClosed", "IsWon",
            "ForecastCategory", "ForecastCategoryName",
            "CampaignId", "HasOpportunityLineItem", "Pricebook2Id",
            "OwnerId", "CreatedDate", "CreatedById",
            "LastModifiedDate", "LastModifiedById", "SystemModstamp",
            "LastActivityDate", "LastStageChangeDate",
            "FiscalQuarter", "FiscalYear", "Fiscal",
            "ContactId", "LastViewedDate", "LastReferencedDate",
            "HasOpenActivity", "HasOverdueTask",
            "DeliveryInstallationStatus__c", "TrackingNumber__c",
            "OrderNumber__c", "CurrentGenerators__c", "MainCompetitors__c",
            "is_large_deal", "is_high_probability", "is_overdue",
            "deal_size_category", "days_to_close",
            "processed_at", "data_source"
        )
    )