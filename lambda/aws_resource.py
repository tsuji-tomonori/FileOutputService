import gzip
import json

import boto3


class AwsResource:
    def __init__(self, profile: str = None):
        if profile is not None:
            self.session = boto3.Session(profile_name=profile)
        else:
            self.session = boto3.Session()


class Sns(AwsResource):
    def __init__(self, profile: str = None):
        super().__init__(profile)
        self.client = self.session.client("sns")

    def publish(self, topick_arn: str, message: str, subject: str) -> None:
        self.client.publish(
            TopicArn=topick_arn,
            Message=message,
            Subject=subject
        )


class S3(AwsResource):
    def __init__(self, profile: str = None):
        super().__init__(profile)
        self.client = self.session.client("s3")

    def upload(self, data: bin, bucket_name: str, file_path: str, tags: dict) -> None:
        self.client.put_object(
            Body=data,
            Bucket=bucket_name,
            Key=file_path,
            Tagging=self._tag_strf_query_paramater(tags)
        )

    def _tag_strf_query_paramater(self, tags: dict) -> str:
        return "&".join(f"{k}={v}" for k, v in tags.items())

    def list_paths(self, bucket: str, prefix: str) -> list:
        result = []
        param = {
            "Bucket": bucket,
            "Prefix": prefix,
        }
        while param.get("ContinuationToken", True):
            ret = self.client.list_objects_v2(**param)
            result += [content["Key"] for content in ret["Contents"]]
            param["ContinuationToken"] = ret.get(
                "NextContinuationToken", False)
        return result

    def read_file(self, bucket: str, key: str):
        res = self.client.get_object(
            Bucket=bucket,
            Key=key
        )
        return json.loads(gzip.decompress(res["Body"].read()).decode("utf-8"))
