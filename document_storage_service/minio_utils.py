from minio import Minio
from minio.error import S3Error
import io
import os
from typing import BinaryIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinioClient:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = True):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            logger.info(f"'{file_path}' is successfully uploaded as object '{object_name}' to bucket '{bucket_name}'.")
        except S3Error as e:
            logger.error(f"Error uploading file: {e}")

    def download_file(self, bucket_name: str, object_name: str, file_path: str):
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            logger.info(f"'{object_name}' is successfully downloaded as '{file_path}'.")
        except S3Error as e:
            logger.error(f"Error downloading file: {e}")

    def list_objects(self, bucket_name: str, prefix: str = None):
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []