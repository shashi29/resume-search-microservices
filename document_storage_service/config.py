# document_service/config.py

MINIO_ENDPOINT = "127.0.0.1:9000"  # Change if running MinIO in a different environment
MINIO_ACCESS_KEY = "BuyzDsVbmEjNg0k7glPL"
MINIO_SECRET_KEY = "885dOOL2J7CVfXVYbSVNjleVBGK6JbXhwa5V7UYi"
BUCKET_NAME = "documents"  # Bucket name for storing documents

RABBITMQ_HOST = "localhost"  # Change if running RabbitMQ in a different environment
RABBITMQ_QUEUE = "documents_queue"
