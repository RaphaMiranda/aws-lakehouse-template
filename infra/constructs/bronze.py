from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    Duration,
)
from constructs import Construct

class BronzeLayer(Construct):
    def __init__(self, scope: Construct, construct_id: str, 
                 bucket: s3.Bucket, 
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.ingestion_lambda = _lambda.Function(
            self, "ApiIngestionFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="main.handler",
            code=_lambda.Code.from_asset("etl/extract/api_ingestion"),
            environment={
                "BRONZE_BUCKET_NAME": bucket.bucket_name
            },
            timeout=Duration.minutes(5),
            memory_size=256,
        )

        # Grant write permissions
        bucket.grant_write(self.ingestion_lambda)

