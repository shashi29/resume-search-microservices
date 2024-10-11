from minio import Minio
from minio.error import S3Error
from app.utils.logging_config import logger
from app.core.config import settings

class MinioClientError(Exception):
    pass

class MinioClient:
    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False
        )

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            logger.info(f"'{file_path}' is successfully uploaded as object '{object_name}' to bucket '{bucket_name}'.")
        except S3Error as e:
            logger.error(f"Error uploading file: {e}")
            raise MinioClientError(f"Failed to upload file: {e}")

    def download_file(self, bucket_name: str, object_name: str, file_path: str):
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            logger.info(f"'{object_name}' is successfully downloaded as '{file_path}'.")
        except S3Error as e:
            logger.error(f"Error downloading file: {e}")
            raise MinioClientError(f"Failed to download file: {e}")

    def list_objects(self, bucket_name: str, prefix: str = None):
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            raise MinioClientError(f"Failed to list objects: {e}")

    def check_health(self):
        try:
            buckets = self.client.list_buckets()
            return True
        except S3Error as e:
            logger.error(f"MinIO health check failed: {e}")
            return False