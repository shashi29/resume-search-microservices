import os
import json
from datetime import datetime
from typing import Dict
import logging
from dotenv import load_dotenv
import zlib
import numpy as np
from sentence_transformers import SentenceTransformer
from rabbitmq_utils import RabbitMQClient
from s3_utils import S3Client  # Updated to use S3Client
from urllib.parse import urlparse  # For parsing S3 URL

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
INPUT_QUEUE = os.getenv("RABBITMQ_QUEUE")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE")
STATUS_QUEUE = os.getenv("STATUS_QUEUE")

# S3 configuration
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

class EmbeddingProcessingService:
    def __init__(self):
        self.input_queue = RabbitMQClient(RABBITMQ_HOST, INPUT_QUEUE)
        self.output_queue = RabbitMQClient(RABBITMQ_HOST, OUTPUT_QUEUE)
        self.s3_client = S3Client(S3_ACCESS_KEY, S3_SECRET_KEY)
        self.model = SentenceTransformer("mixedbread-ai/mxbai-embed-large-v1")
        self.status_queue = RabbitMQClient(RABBITMQ_HOST, STATUS_QUEUE)  # Assuming a separate status queue for status updates

    def send_status(self, status, details):
        """Send status update to the status queue."""
        status_message = {
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.status_queue.send_message(status_message)

    def start(self):
        logger.info("Starting Embedding Processing Service")
        self.input_queue.start_consumer(self.process_message)

    def _generate_output_key(self, input_key: str, output_filename: str) -> str:
        # Split the input key into path components
        path_components = input_key.split('/')
        
        # Find the index of the last folder (parent of the file)
        last_folder_index = len(path_components) - 2
        
        # Create the OCR folder at the same level as the last folder
        path_components[last_folder_index] = "embedding"
        
        # Replace the filename with the output filename
        path_components[-1] = output_filename
        
        # Join the path components back into a single string
        return '/'.join(path_components[:-1]) + '/' + output_filename  # Ensure correct path structure
        
    def process_message(self, message: Dict):
        local_path = None
        output_s3_path = None

        try:
            logger.info(f"Processing message: {message}")

            # Send "STARTED" status
            self.send_status("STARTED", {
                "operation": "embedding",
                "resume_path" : message.get('resume_path'),
                "storage_path": message.get('storage_path'),
                "metadata": message.get('metadata', {}),
                "collection_name": message.get("collection_name"),
                "created_date": datetime.utcnow().isoformat()
            })

            # Extract document information
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

            # Read the document text
            with open(local_input_path, 'r') as f:
                document_text = f.read()

            # Generate embeddings
            embedding = self.model.encode(document_text).tolist()
            compressed_embedding = zlib.compress(np.array(embedding, dtype=np.float32).tobytes())

            # Prepare output S3 path for saving embeddings
            output_key = self._generate_output_key(input_key, output_filename)
            output_s3_path = f"s3://{input_bucket}/{output_key}" + ".npy"

            # Save embeddings to S3
            # Check if the embedding result already exists
            np.save(local_output_path, compressed_embedding)  # Save as .npy file
            # Upload the result to S3
            local_output_path = local_output_path + ".npy"
            output_key = output_key + ".npy"
            self.s3_client.upload_file(local_output_path, input_bucket, output_key)

            # Prepare result message
            result_message = {
                "operation": "embedding",
                "resume_path" : message.get('resume_path'),
                "storage_path": output_s3_path,
                "metadata": metadata,
                "collection_name": message.get("collection_name"),
                "created_date": datetime.utcnow().isoformat()
            }

            # Publish result to output queue
            self.output_queue.send_message(result_message)

            logger.info(f"Processed document: {s3_input_path}")
            logger.info(f"Embedding results saved to: {output_s3_path}")
            logger.info(f"Result message sent to queue: {OUTPUT_QUEUE}")

            # Send "COMPLETED" status
            self.send_status("COMPLETED", {
                "operation": "embedding",
                "resume_path" : message.get('resume_path'),
                "storage_path": output_s3_path,
                "metadata": metadata,
                "collection_name": message.get("collection_name"),
                "created_date": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            # Send "ERROR" status
            self.send_status("ERROR", {
                "operation": "embedding",
                "resume_path" : message.get('resume_path'),
                "storage_path": message.get('storage_path'),
                "metadata": metadata,
                "collection_name": message.get("collection_name"),
                "error": str(e),
                "created_date": datetime.utcnow().isoformat()
            })

        finally:
            # Clean up temporary files
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
            if output_s3_path and os.path.exists(output_key.split('/')[-1]):
                os.remove(output_key.split('/')[-1])

    def check_health(self):
        rabbitmq_health = self.input_queue.check_health() and self.output_queue.check_health()
        #s3_health = self.s3_client.check_health()  # Added health check for S3
        return rabbitmq_health #and s3_health


def main():
    embedding_service = EmbeddingProcessingService()

    # Perform health check
    if embedding_service.check_health():
        logger.info("Health check passed. Starting Embedding Processing Service.")
        embedding_service.start()
    else:
        logger.error("Health check failed. Please check your RabbitMQ and S3 connections.")

if __name__ == "__main__":
    main()
