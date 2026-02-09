import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BRONZE_BUCKET_NAME',
    'SILVER_BUCKET_NAME',
    'DATABASE_NAME',
    'TABLE_NAME'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
bronze_bucket = args['BRONZE_BUCKET_NAME']
silver_bucket = args['SILVER_BUCKET_NAME']
database_name = args['DATABASE_NAME']
table_name = args['TABLE_NAME']

# Input Schema (matches API response)
schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("_ingestion_time", StringType(), True)
])

# Read Bronze Data (Raw JSON)
# We read the whole bronze bucket for demo, in prod use job bookmarks or triggers
input_path = f"s3://{bronze_bucket}/raw/posts/"
df_bronze = spark.read.json(input_path, schema=schema)

# Basic Transformations (Clean Body, Deduplication)
df_clean = df_bronze.dropDuplicates(["id"]) \
    .withColumnRenamed("userId", "user_id") \
    .withColumn("updated_at", current_timestamp())

# Create Iceberg Table if it doesn't exist
# We rely on Glue Catalog integration provided by 'glue_catalog'
full_table_name = f"glue_catalog.{database_name}.{table_name}"

# Check if table exists, if so MERGE, else CREATE
# Spark SQL specific for Iceberg Upsert
df_clean.createOrReplaceTempView("source_data")

create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        user_id int,
        id int,
        title string,
        body string,
        _ingestion_time string,
        updated_at timestamp
    )
    USING iceberg
    LOCATION 's3://{silver_bucket}/{table_name}'
    TBLPROPERTIES (
        'format-version'='2',
        'write.upsert.enabled'='true'
    )
"""
spark.sql(create_table_query)

# MERGE INTO for SCD1 (Upsert) behaviour
merge_query = f"""
    MERGE INTO {full_table_name} t
    USING source_data s
    ON t.id = s.id
    WHEN MATCHED THEN
        UPDATE SET t.title = s.title, t.body = s.body, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT *
"""
spark.sql(merge_query)

job.commit()
