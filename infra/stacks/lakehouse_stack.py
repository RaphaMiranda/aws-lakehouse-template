from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from constructs import Construct

from infra.shared.storage import DataLakeStorage
from infra.constructs.bronze import BronzeLayer
from infra.constructs.silver import SilverLayer
from infra.constructs.gold import GoldLayer
from pipelines.orchestration import OrchestrationLayer





class LakehouseStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Shared Storage Infrastructure
        self.storage = DataLakeStorage(self, "DataLakeStorage")

        # Bronze Layer (Ingestion)
        self.bronze = BronzeLayer(
            self, 
            "BronzeLayer",
            bucket=self.storage.bronze_bucket
        )

        # Silver Layer (Transformation)
        self.silver = SilverLayer(
            self,
            "SilverLayer",
            bronze_bucket=self.storage.bronze_bucket,
            silver_bucket=self.storage.silver_bucket,
            database_name=self.storage.database.ref
        )

        # Gold Layer (Business Logic)
        self.gold = GoldLayer(
            self,
            "GoldLayer",
            silver_bucket=self.storage.silver_bucket,
            gold_bucket=self.storage.gold_bucket,
            database_name=self.storage.database.ref
        )

        # Orchestration
        self.orchestration = OrchestrationLayer(
            self,
            "OrchestrationLayer",
            ingestion_lambda=self.bronze.ingestion_lambda,
            silver_job_name=self.silver.etl_job.ref,
            gold_job_name=self.gold.etl_job.ref
        )



