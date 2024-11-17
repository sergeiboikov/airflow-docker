import boto3
import dtp.constants as consts


def get_s3_client(access_key_id=consts.S3_ID,
                  secret_access_key=consts.S3_KEY,
                  service_name=consts.S3_SERVICE_NAME,
                  endpoint_url=consts.S3_ENDPOINT_URL) -> boto3.Session.client:
    pass
