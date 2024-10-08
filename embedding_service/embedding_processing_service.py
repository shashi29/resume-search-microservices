import os
import json
import uuid
from datetime import datetime
from typing import Dict
import logging
from dotenv import load_dotenv

from rabbitmq_utils import RabbitMQClient
from minio_utils import MinioClient
from sentence_transformers import SentenceTransformer
import zlib
import numpy as np

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
INPUT_QUEUE = os.getenv("RABBITMQ_QUEUE")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE", "embedding_results")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("BUCKET_NAME")

class EmbeddingProcessingService:
    def __init__(self):
        self.input_queue = RabbitMQClient(RABBITMQ_HOST, INPUT_QUEUE)
        self.output_queue = RabbitMQClient(RABBITMQ_HOST, OUTPUT_QUEUE)
        self.minio_client = MinioClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        self.model = SentenceTransformer("mixedbread-ai/mxbai-embed-large-v1")
        self.status_queue = RabbitMQClient(RABBITMQ_HOST, "status_queue")  # Assuming a separate status queue for status updates

    def send_status(self, idempotency_key, status, details):
        """Send status update to the status queue."""
        status_message = {
            "id": idempotency_key,
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        self.status_queue.send_message(status_message)

    def start(self):
        logger.info(f"Starting Embedding Processing Service")
        self.input_queue.start_consumer(self.process_message)

    def process_message(self, message: Dict):
        local_path = None
        embedding_filename = None
        idempotency_key = message.get('idempotency_key', str(uuid.uuid4()))  # Fallback to a new key if not present

        try:
            logger.info(f"Processing message: {message}")
            
            # Send "STARTED" status
            self.send_status(idempotency_key, "STARTED", {
                "operation": "embedding",
                "document_name": message.get('document_name'),
                "user": message.get('user', 'unknown')
            })

            # Extract document information
            document_name = message['document_name']
            minio_path = message['minio_path']
            original_filename = message.get('original_filename')

            # Download the document from MinIO
            local_path = f"/tmp/{document_name}"
            self.minio_client.download_file(MINIO_BUCKET, minio_path, local_path)

            # Read the document text
            with open(local_path, 'r') as f:
                document_text = f.read()

            # Generate embeddings
            embedding = self.model.encode(document_text).tolist()
            compressed_embedding = zlib.compress(np.array(embedding, dtype=np.float32).tobytes())

            # Save embeddings to MinIO
            embedding_filename = f"{idempotency_key}.npz"
            embedding_path = f"embeddings/{embedding_filename}"

            # Check if the embedding result already exists
            existing_objects = self.minio_client.list_objects(MINIO_BUCKET, prefix=embedding_path)
            if existing_objects:
                logger.info(f"Embedding result already exists for idempotency key: {idempotency_key}")
            else:
                np.savez_compressed(f"/tmp/{embedding_filename}", embeddings=compressed_embedding)
                self.minio_client.upload_file(MINIO_BUCKET, embedding_path, f"/tmp/{embedding_filename}")

            # Prepare result message
            result_message = {
                "operation": "embedding",
                "original_filename": original_filename,
                "document_name": document_name,
                "minio_path": embedding_path,
                "idempotency_key": idempotency_key,
                "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }

            # Publish result to output queue
            self.output_queue.send_message(result_message)

            logger.info(f"Processed document: {document_name}")
            logger.info(f"Embedding results saved to: {embedding_path}")
            logger.info(f"Result message sent to queue: {OUTPUT_QUEUE}")

            # Send "COMPLETED" status
            self.send_status(idempotency_key, "COMPLETED", {
                "operation": "embedding",
                "document_name": document_name,
                "minio_path": embedding_path
            })

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Send "ERROR" status
            self.send_status(idempotency_key, "ERROR", {
                "operation": "embedding",
                "document_name": message.get('document_name'),
                "error": str(e)
            })

        finally:
            # Clean up temporary files
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
            if embedding_filename and os.path.exists(f"/tmp/{embedding_filename}"):
                os.remove(f"/tmp/{embedding_filename}")

    def check_health(self):
        rabbitmq_health = self.input_queue.check_health() and self.output_queue.check_health()
        minio_health = self.minio_client.check_health()
        return rabbitmq_health and minio_health


def main():
    embedding_service = EmbeddingProcessingService()

    # Perform health check
    if embedding_service.check_health():
        logger.info("Health check passed. Starting Embedding Processing Service.")
        embedding_service.start()
    else:
        logger.error("Health check failed. Please check your RabbitMQ and MinIO connections.")

if __name__ == "__main__":
    main()
