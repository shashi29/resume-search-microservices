# document_service/config.py

MINIO_ENDPOINT = "127.0.0.1:9000"  # Change if running MinIO in a different environment
MINIO_ACCESS_KEY = "MD7ZO0hvzujLWzNQzC3r"
MINIO_SECRET_KEY = "hjwj2DmyOod2M3YAVzsO7pqGPbQ3h40iFxyR0D9W"
BUCKET_NAME = "documents"  # Bucket name for storing documents

RABBITMQ_HOST = "localhost"  # Change if running RabbitMQ in a different environment
RABBITMQ_QUEUE = "documents_queue"
STATUS_QUEUE = 'status_queue'
METADATA_SERVICE_URL = "http://localhost:8082"
