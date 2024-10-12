import os
import json
import numpy as np
import zlib
import uuid
import logging
from datetime import datetime
from typing import Dict
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance, UpdateStatus, OptimizersConfigDiff
from rabbitmq_utils import RabbitMQClient
from s3_utils import S3Client
from urllib.parse import urlparse  # For parsing S3 URL

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
INPUT_QUEUE = os.getenv("EMBEDDING_RESULTS_QUEUE")
STATUS_QUEUE = os.getenv("STATUS_QUEUE")

# S3 configuration
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Qdrant URL configuration
QDRANT_URL = os.getenv("QDRANT_URL")

class QdrantInsertionService:
    def __init__(self):
        self.input_queue = RabbitMQClient(RABBITMQ_HOST, INPUT_QUEUE)
        self.status_queue = RabbitMQClient(RABBITMQ_HOST, STATUS_QUEUE)
        self.s3_client = S3Client(S3_ACCESS_KEY, S3_SECRET_KEY)
        self.qdrant_client = QdrantClient(url=QDRANT_URL)

    def send_status(self, status, details):
        """Send status update to the status queue."""
        status_message = {
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.status_queue.send_message(status_message)

    def start(self):
        logger.info("Starting Qdrant Insertion Service")
        self.input_queue.start_consumer(self.process_message)

    def create_collection_if_not_exists(self, collection_name: str, vector_size: int):
        """Create a Qdrant collection if it doesn't exist."""
        if not self.check_collection_exists(collection_name):
            try:
                self.qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(
                        size=vector_size, 
                        distance=Distance.COSINE,
                        on_disk=True
                    ),
                    optimizers_config=OptimizersConfigDiff(indexing_threshold=20000)
                )
                logger.info(f"Collection '{collection_name}' created successfully.")
            except Exception as e:
                logger.error(f"Error creating collection '{collection_name}': {e}")
                raise

    def check_collection_exists(self, collection_name: str) -> bool:
        """Check if the collection already exists in Qdrant."""
        try:
            collections = self.qdrant_client.get_collections()
            return any(collection.name == collection_name for collection in collections.collections)
        except Exception as e:
            logger.error(f"Error checking collection existence for '{collection_name}': {e}")
            raise

    def insert_embedding(self, collection_name: str, vector: list, payload: dict):
        """Insert a single embedding into the specified Qdrant collection."""
        try:
            point = PointStruct(id=str(uuid.uuid4()), vector=vector, payload=payload)
            operation_info = self.qdrant_client.upsert(
                collection_name=collection_name,
                wait=True,
                points=[point]
            )
            if operation_info.status != UpdateStatus.COMPLETED:
                raise Exception(f"Failed to insert embedding into collection '{collection_name}'.")
        except Exception as e:
            logger.error(f"Error inserting embedding into collection '{collection_name}': {e}")
            raise

    def process_message(self, message: Dict):
        try:
            logger.info(f"Processing message: {message}")

            # Send "STARTED" status
            self.send_status("STARTED", {
                "operation": "qdrant_insertion",
                "resume_path" : message.get('resume_path'),
                "storage_path": message.get('storage_path'),
                "metadata": message.get('metadata', {}),
                "collection_name": message.get('collection_name'),
                "created_date": datetime.utcnow().isoformat()
            })

            # Extract collection name, embedding information, and metadata
            collection_name = message.get('collection_name')
            if not collection_name:
                raise ValueError("Collection name is missing in the message.")

            s3_input_path = message['storage_path']
            metadata = message.get('metadata', {})
            
            # Parse S3 path
            parsed_input_path = urlparse(s3_input_path)
            input_bucket = parsed_input_path.netloc
            input_key = parsed_input_path.path.lstrip('/')

            # Extract original filename and generate output filename
            original_filename = os.path.basename(input_key)
            filename_without_ext, _ = os.path.splitext(original_filename)
            output_filename = f"{filename_without_ext}"
            
            # Generate local path for downloading the document
            local_input_path = f"/tmp/{original_filename}"
            local_output_path = os.path.join("/tmp", output_filename)

            self.s3_client.download_file(input_bucket, input_key, local_input_path)

            # Load and decompress embedding
            compressed_embedding = np.load(local_input_path, allow_pickle=True)
            decompressed_bytes = zlib.decompress(compressed_embedding)
            vector = np.frombuffer(decompressed_bytes, dtype=np.float32).tolist()

            # Create collection if not exists
            self.create_collection_if_not_exists(collection_name, len(vector))

            # Insert embedding into Qdrant
            payload = {
                "storage_path": message.get('resume_path'),
                "metadata": metadata
            }
            self.insert_embedding(collection_name, vector, payload)

            logger.info(f"Inserted embedding from: {message.get('resume_path')} into collection: {collection_name}")

            # Send "COMPLETED" status
            self.send_status("COMPLETED", {
                "operation": "qdrant_insertion",
                "resume_path" : message.get('resume_path'),
                "metadata": metadata,
                "collection_name": message.get('collection_name'),
                "created_date": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            # Send "ERROR" status
            self.send_status("ERROR", {
                "operation": "qdrant_insertion",
                "resume_path" : message.get('resume_path'),
                "metadata": message.get('metadata', {}),
                "collection_name": message.get('collection_name'),
                "error": str(e),
                "created_date": datetime.utcnow().isoformat()
            })

        finally:
            # Clean up temporary file
            if local_input_path and os.path.exists(local_input_path):
                os.remove(local_input_path)

    def check_health(self):
        rabbitmq_health = self.input_queue.check_health() and self.status_queue.check_health()
        try:
            self.qdrant_client.get_collections()
            qdrant_health = True
        except:
            qdrant_health = False
        return rabbitmq_health #and qdrant_health

def main():
    qdrant_service = QdrantInsertionService()

    # Perform health check
    if qdrant_service.check_health():
        logger.info("Health check passed. Starting Qdrant Insertion Service.")
        qdrant_service.start()
    else:
        logger.error("Health check failed. Please check your RabbitMQ and Qdrant connections.")

if __name__ == "__main__":
    main()
