import aws_cdk as cdk
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
)
from constructs import Construct

class GoldLayer(Construct):
    def __init__(self, scope: Construct, construct_id: str, 
                 silver_bucket: s3.Bucket,
                 gold_bucket: s3.Bucket,
                 database_name: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database_name = database_name
        self.source_table = "posts_silver"
        self.target_table = "posts_analytics"

        # Asset for the script
        script_asset = s3_assets.Asset(
            self, "EtlScript",
            path="etl/load/silver_to_gold/main.py"
        )

        # IAM Role
        role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )
        silver_bucket.grant_read(role)
        gold_bucket.grant_read_write(role)
        script_asset.grant_read(role)
        
        # Iceberg Config (Reusable?)
        # For simplicity, duplicate simple string construction or move to shared utilities.
        iceberg_conf = (
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
            f"--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
            f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{gold_bucket.bucket_name}/ " 
            "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
            "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
        )
        # Note: Warehouse location above sets default. But we can write to specific locations.
        # Ideally warehouse should be silver for silver job, gold for gold job, or a common warehouse path.
        # But here we set it to the bucket we are effectively writing to as default.

        self.etl_job = glue.CfnJob(
            self, "SilverToGoldJob",
            name="silver_to_gold_etl",
            role=role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{script_asset.s3_bucket_name}/{script_asset.s3_object_key}"
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            default_arguments={
                "--job-language": "python",
                "--datalake-formats": "iceberg",
                "--conf": iceberg_conf,
                "--SILVER_BUCKET_NAME": silver_bucket.bucket_name,
                "--GOLD_BUCKET_NAME": gold_bucket.bucket_name,
                "--DATABASE_NAME": database_name,
                "--SOURCE_TABLE": self.source_table,
                "--TARGET_TABLE": self.target_table
            }
        )
