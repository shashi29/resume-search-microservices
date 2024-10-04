import os
import json
import uuid
from datetime import datetime
from typing import Dict
import logging
from dotenv import load_dotenv

from rabbitmq_utils import RabbitMQClient
from minio_utils import MinioClient
from extractors import DocumentTextExtractor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
INPUT_QUEUE = os.getenv("RABBITMQ_QUEUE")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE", "ocr_results")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("BUCKET_NAME")

class OCRProcessingService:
    def __init__(self):
        self.input_queue = RabbitMQClient(RABBITMQ_HOST, INPUT_QUEUE)
        self.output_queue = RabbitMQClient(RABBITMQ_HOST, OUTPUT_QUEUE)
        self.minio_client = MinioClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    def start(self):
        logger.info(f"Starting OCR Processing Service")
        self.input_queue.start_consumer(self.process_message)

    def process_message(self, message: Dict):
        local_path = None
        result_path = None
        try:
            logger.info(f"Processing message: {message}")

            # Extract document information
            document_name = message['document_name']
            minio_path = message['minio_path']
            idempotency_key = message.get('idempotency_key')

            if not idempotency_key:
                idempotency_key = str(uuid.uuid4())
                logger.warning(f"Idempotency key not provided. Generated new key: {idempotency_key}")

            # Download the document from MinIO
            local_path = f"/tmp/{document_name}"
            self.minio_client.download_file(MINIO_BUCKET, minio_path, local_path)

            # Process the document using OCR
            if document_name.lower().endswith('.pdf'):
                text = DocumentTextExtractor.extract_text_from_pdf(local_path)
            else:
                text = DocumentTextExtractor.extract_text_from_doc(local_path)

            # Save OCR results to MinIO
            result_filename = f"{idempotency_key}.txt"
            result_path = f"ocr_results/{result_filename}"
            
            # Check if the result already exists
            existing_objects = self.minio_client.list_objects(MINIO_BUCKET, prefix=result_path)
            if existing_objects:
                logger.info(f"OCR result already exists for idempotency key: {idempotency_key}")
            else:
                with open(f"/tmp/{result_filename}", "w") as f:
                    f.write(text)
                
                self.minio_client.upload_file(MINIO_BUCKET, result_path, f"/tmp/{result_filename}")

            # Prepare result message
            result_message = {
                "original_document": document_name,
                "ocr_result_path": result_path,
                "processed_date": datetime.now().isoformat(),
                "idempotency_key": idempotency_key,
                "user": message.get('user', 'Anonymous')
            }

            # Publish result to output queue
            self.output_queue.send_message(result_message)

            logger.info(f"Processed document: {document_name}")
            logger.info(f"OCR results saved to: {result_path}")
            logger.info(f"Result message sent to queue: {OUTPUT_QUEUE}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

        finally:
            # Clean up temporary files
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
            if result_path and os.path.exists(f"/tmp/{os.path.basename(result_path)}"):
                os.remove(f"/tmp/{os.path.basename(result_path)}")

    def check_health(self):
        rabbitmq_health = self.input_queue.check_health() and self.output_queue.check_health()
        minio_health = self.minio_client.check_health()
        return rabbitmq_health and minio_health

def main():
    ocr_service = OCRProcessingService()
    
    # Perform health check
    if ocr_service.check_health():
        logger.info("Health check passed. Starting OCR Processing Service.")
        ocr_service.start()
    else:
        logger.error("Health check failed. Please check your RabbitMQ and MinIO connections.")

if __name__ == "__main__":
    main()