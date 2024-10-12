import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging

logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self, access_key: str, secret_key: str, region: str = None):
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        self.s3 = session.client('s3')

    def download_file(self, bucket_name: str, object_key: str, local_path: str):
        try:
            logger.info(f"Downloading {object_key} from S3 bucket {bucket_name} to {local_path}")
            self.s3.download_file(bucket_name, object_key, local_path)
            logger.info(f"Downloaded {object_key} to {local_path}")
        except NoCredentialsError:
            logger.error("Credentials not available for S3.")
            raise
        except ClientError as e:
            logger.error(f"Failed to download file from S3: {e}")
            raise

    def upload_file(self, local_path: str, bucket_name: str, object_key: str):
        try:
            logger.info(f"Uploading {local_path} to S3 bucket {bucket_name} as {object_key}")
            self.s3.upload_file(local_path, bucket_name, object_key)
            logger.info(f"Uploaded {local_path} to {object_key} in bucket {bucket_name}")
        except NoCredentialsError:
            logger.error("Credentials not available for S3.")
            raise
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            raise

    def check_health(self) -> bool:
        """ Check if the S3 client is healthy by listing buckets. """
        try:
            self.s3.list_buckets()
            logger.info("S3 connection is healthy.")
            return True
        except ClientError as e:
            logger.error(f"S3 health check failed: {e}")
            return False
