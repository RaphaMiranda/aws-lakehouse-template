from aws_cdk import (
    aws_s3 as s3,
    aws_glue as glue,
    RemovalPolicy,
)
from constructs import Construct

class DataLakeStorage(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bronze_bucket = self._create_bucket("Bronze")
        self.silver_bucket = self._create_bucket("Silver")
        self.gold_bucket = self._create_bucket("Gold")

        # Glue Database to hold tables for all layers
        self.database = glue.CfnDatabase(
            self,
            "LakehouseDatabase",
            catalog_id=scope.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="lakehouse_db",
                description="Database for the Data Lakehouse template"
            )
        )

    def _create_bucket(self, layer_name: str) -> s3.Bucket:
        return s3.Bucket(
            self,
            f"{layer_name}Bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY, # For template purposes, easier cleanup
            auto_delete_objects=True, # For template purposes
        )
