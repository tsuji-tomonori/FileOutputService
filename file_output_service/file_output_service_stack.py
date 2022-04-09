import aws_cdk as cdk
from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_ssm as ssm
from aws_cdk.aws_lambda_event_sources import SqsEventSource, SnsEventSource
from constructs import Construct

PROJECK_NAME = "FileOutputService"
DESCRIPTION = "Aggregate multiple chat column information and output as a single file."


def build_resource_name(resource_name: str, service_name: str) -> str:
    return f"{resource_name}_{service_name}_cdk"


class FileOutputServiceStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        role = iam.Role(
            self, build_resource_name("rol", "file_output_role"),
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole")
            ],
            role_name=build_resource_name(
                "rol", "file_output_role"),
            description=DESCRIPTION
        )
        cdk.Tags.of(role).add("service_name", build_resource_name(
            "rol", "file_output_role"))

        fn = _lambda.Function(
            self, build_resource_name("lmd", "file_output_service"),
            code=_lambda.Code.from_asset("lambda"),
            handler="lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            function_name=build_resource_name(
                "lmd", "file_output_service"),
            environment={
                "LOG_LEVEL": "INFO",
            },
            description=DESCRIPTION,
            timeout=cdk.Duration.seconds(300),
            memory_size=256,
            role=role,
            log_retention=logs.RetentionDays.THREE_MONTHS,
        )
        cdk.Tags.of(role).add("service_name", build_resource_name(
            "lmd", "file_output_service"))

        output_bucket = s3.Bucket(
            self, "s3s-file-output-bucket-cdk",
            bucket_name="s3s-file-output-bucket-cdk",
        )
        output_bucket.grant_put(role)
        cdk.Tags.of(role).add("service_name",
                              "s3s-file-output-bucket-cdk")
        fn.add_environment(
            key="OUTPUT_BUCKET_NAME",
            value=output_bucket.bucket_name
        )

        input_bucket = s3.Bucket.from_bucket_name(
            self, "s3s-collect-youtube-chat-bucket-cdk", "s3s-collect-youtube-chat-bucket-cdk"
        )
        input_bucket.grant_read(role)
        fn.add_environment(
            key="INPUT_BUCKET_NAME",
            value=input_bucket.bucket_name
        )

        topic = sns.Topic.from_topic_arn(
            self, "sns_collect_youtube_chat_topic_cdk",
            topic_arn=f"arn:aws:sns:{cdk.Stack.of(self).region}:{cdk.Stack.of(self).account}:sns_collect_youtube_chat_topic_cdk"
        )
        sns_event = SnsEventSource(topic)
        fn.add_event_source(sns_event)

        for resource in [role, fn, output_bucket, topic]:
            cdk.Tags.of(resource).add("project", PROJECK_NAME)
            cdk.Tags.of(resource).add("creater", "cdk")
