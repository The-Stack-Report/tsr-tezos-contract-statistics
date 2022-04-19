import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("SPACES_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("SPACES_SECRET_KEY")
DATASETS_BUCKET = os.getenv("DATASETS_BUCKET")

session = boto3.session.Session()

s3_client = session.client("s3",
    region_name="ams3",
    endpoint_url="https://ams3.digitaloceanspaces.com",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

s3_resource = boto3.resource(
    "s3",
    region_name="ams3",
    endpoint_url="https://ams3.digitaloceanspaces.com",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def upload_file_to_spaces(file_path, object_name, make_public=False):
    try:
        response = s3_client.upload_file(str(file_path), DATASETS_BUCKET, object_name)
        print(response)
        if make_public:
            object_acl = s3_resource.ObjectAcl(DATASETS_BUCKET, object_name)
            resp = object_acl.put(ACL="public-read")
            return True
        else:
            return False
    except ClientError as e:
        print(e)
        return False
    return True
