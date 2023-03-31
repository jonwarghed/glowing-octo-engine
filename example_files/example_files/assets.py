import requests
from dagster import IOManager, OutputContext, InputContext, asset, io_manager
import boto3
import json

class MinioIOManager(IOManager):
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

    def load_input(self, context: InputContext):
        s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            verify=self.secure,
        )
        result = s3.get_object(Bucket="raw", Key="s3/news_data.json") 
        obj = result["Body"].read().decode()
        return obj

    def handle_output(self, context: OutputContext, obj):
        s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            verify=self.secure,
        )
        data_string = json.dumps(obj, indent=2, default=str)
        s3.put_object(
            Bucket="raw",
            Key="s3/news_data.json",
            Body=data_string
        )

@io_manager
def minio_manager():
    return MinioIOManager(
        endpoint="http://lake:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False,
    )

@asset
def hacker_news():
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]
    return top_new_story_ids