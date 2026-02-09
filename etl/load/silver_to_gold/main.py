import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, count

# Arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SILVER_BUCKET_NAME',
    'GOLD_BUCKET_NAME',
    'DATABASE_NAME',
    'SOURCE_TABLE',
    'TARGET_TABLE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
silver_bucket = args['SILVER_BUCKET_NAME']
gold_bucket = args['GOLD_BUCKET_NAME']
database_name = args['DATABASE_NAME']
source_table = args['SOURCE_TABLE']
target_table = args['TARGET_TABLE']

# Read Silver Table (Iceberg)
source_full_name = f"glue_catalog.{database_name}.{source_table}"
df_silver = spark.read.format("iceberg").load(source_full_name)

# Data Quality Check (Simple)
# Ensure we have data to process
if df_silver.count() == 0:
    print("Warning: Source table is empty. Skipping aggregation.")
    # In a real pipeline, you might want to fail or send an alert
    # raise Exception("Data Quality Check Failed: Source table is empty")
    job.commit()
    sys.exit(0) # Exit success but do nothing

# Business Logic: Aggregation (Count posts per user)
df_gold = df_silver.groupBy("user_id") \
    .agg(count("id").alias("post_count")) \
    .withColumn("calculated_at", current_timestamp())

# Write to Gold (Iceberg)
target_full_name = f"glue_catalog.{database_name}.{target_table}"

df_gold.createOrReplaceTempView("gold_data")

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {target_full_name} (
        user_id int,
        post_count long,
        calculated_at timestamp
    )
    USING iceberg
    LOCATION 's3://{gold_bucket}/{target_table}'
    TBLPROPERTIES (
        'format-version'='2',
        'write.upsert.enabled'='true'
    )
"""
spark.sql(create_table_query)

# Overwrite / Replace data for the reporting table?
# Or Merge? For analytics snapshots, usually we overwrite or append with partition.
# Here we'll do an Insert Overwrite or Merge.
# Let's do MERGE on user_id to keep latest stats per user.

merge_query = f"""
    MERGE INTO {target_full_name} t
    USING gold_data s
    ON t.user_id = s.user_id
    WHEN MATCHED THEN
        UPDATE SET t.post_count = s.post_count, t.calculated_at = s.calculated_at
    WHEN NOT MATCHED THEN
        INSERT *
"""
spark.sql(merge_query)

job.commit()
