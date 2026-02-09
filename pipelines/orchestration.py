from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_glue as glue,
    Duration,
)
from constructs import Construct

class OrchestrationLayer(Construct):
    def __init__(self, scope: Construct, construct_id: str, 
                 ingestion_lambda: _lambda.Function,
                 silver_job_name: str,
                 gold_job_name: str,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Ingestion Step
        ingestion_task = tasks.LambdaInvoke(
            self, "IngestionTask",
            lambda_function=ingestion_lambda,
            output_path="$.Payload" # Pass output result
        )

        # 2. Silver Transformation Step
        silver_task = tasks.GlueStartJobRun(
            self, "SilverTransformationTask",
            glue_job_name=silver_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--job-bookmark-option": "job-bookmark-enable"
            })
        )

        # 3. Gold Aggregation Step
        gold_task = tasks.GlueStartJobRun(
            self, "GoldAggregationTask",
            glue_job_name=gold_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--job-bookmark-option": "job-bookmark-enable"
            })
        )

        # Chain
        definition = ingestion_task.next(silver_task).next(gold_task)

        # State Machine
        self.state_machine = sfn.StateMachine(
            self, "LakehousePipeline",
            definition=definition,
            timeout=Duration.minutes(30),
        )

        # Schedule (EventBridge triggers Step Function)
        rule = events.Rule(
            self, "PipelineSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
        )
        rule.add_target(targets.SfnStateMachine(self.state_machine))
