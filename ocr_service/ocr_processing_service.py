import os
import uuid
from datetime import datetime
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from urllib.parse import urlparse

from rabbitmq_utils import RabbitMQClient
from s3_utils import S3Client
from extractors import DocumentTextExtractor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
INPUT_QUEUE = os.getenv("RABBITMQ_QUEUE")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE")
STATUS_QUEUE = os.getenv("STATUS_QUEUE")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

class OCRProcessingService:
    def __init__(self):
        self.input_queue = RabbitMQClient(RABBITMQ_HOST, INPUT_QUEUE)
        self.output_queue = RabbitMQClient(RABBITMQ_HOST, OUTPUT_QUEUE)
        self.status_queue = RabbitMQClient(RABBITMQ_HOST, STATUS_QUEUE)
        self.s3_client = S3Client(S3_ACCESS_KEY, S3_SECRET_KEY)

    def send_status(self, message_id: str, status: str, details: Dict[str, Any]):
        status_message = {
            "id": message_id,
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.status_queue.send_message(status_message)

    def start(self):
        logger.info("Starting OCR Processing Service")
        self.input_queue.start_consumer(self.process_message)

    def process_message(self, message: Dict[str, Any]):
        message_id = str(uuid.uuid4())
        local_input_path = None
        local_output_path = None

        try:
            logger.info(f"Processing message: {message}")
            
            self.send_status(message_id, "STARTED", {
                "operation": "ocr",
                "resume_path" : message['storage_path'],
                "storage_path": message.get('storage_path'),
                "metadata": message.get('metadata', {}),
                "collection_name": message.get("collection_name"),
                "created_date": datetime.utcnow().isoformat()
            })

            input_s3_path = message['storage_path']
            metadata = message.get('metadata', {})

            # Parse S3 path
            parsed_input_path = urlparse(input_s3_path)
            input_bucket = parsed_input_path.netloc
            input_key = parsed_input_path.path.lstrip('/')

            # Extract original filename and generate output filename
            original_filename = os.path.basename(input_key)
            filename_without_ext, _ = os.path.splitext(original_filename)
            output_filename = f"{filename_without_ext}.txt"

            # Generate local paths
            local_input_path = os.path.join("/tmp", original_filename)
            local_output_path = os.path.join("/tmp", output_filename)

            # Generate output S3 key (without 'actual-resumes')
            output_key = self._generate_output_key(input_key, output_filename)
            output_s3_path = f"s3://{input_bucket}/{output_key}"

            # Download the document from S3
            self.s3_client.download_file(input_bucket, input_key, local_input_path)

            # Process the document using OCR
            if original_filename.lower().endswith('.pdf'):
                text = DocumentTextExtractor.extract_text_from_pdf(local_input_path)
            else:
                text = DocumentTextExtractor.extract_text_from_doc(local_input_path)

            # Save OCR results locally
            with open(local_output_path, "w") as f:
                f.write(text)
            
            # Upload the result to S3
            self.s3_client.upload_file(local_output_path, input_bucket, output_key)

            # Prepare result message
            result_message = {
                "operation": "embedding",
                "resume_path" : input_s3_path,
                "storage_path": output_s3_path,
                "metadata": metadata,
                "collection_name": message.get("collection_name"),
                "created_date": datetime.utcnow().isoformat()
            }

            # Publish result to output queue
            self.output_queue.send_message(result_message)

            logger.info(f"Processed document: {input_s3_path}")
            logger.info(f"OCR results saved to S3: {output_s3_path}")
            logger.info(f"Result message sent to queue: {OUTPUT_QUEUE}")

            self.send_status(message_id, "COMPLETED", {
                "operation": "ocr",
                "resume_path" : input_s3_path,
                "storage_path": output_s3_path,
                "metadata": metadata,
                "created_date": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            self.send_status(message_id, "ERROR", {
                "operation": "ocr",
                "resume_path" : input_s3_path,
                "storage_path": message.get('storage_path'),
                "metadata": metadata,
                "collection_name": message.get("collection_name"),
                "error": str(e),
                "created_date": datetime.utcnow().isoformat()
            })

        finally:
            # Clean up temporary files
            self._clean_up_temp_files(local_input_path, local_output_path)

    def _generate_output_key(self, input_key: str, output_filename: str) -> str:
        # Split the input key into path components
        path_components = input_key.split('/')
        
        # Find the index of the last folder (parent of the file)
        last_folder_index = len(path_components) - 2
        
        # Create the OCR folder at the same level as the last folder
        path_components[last_folder_index] = "ocr"
        
        # Replace the filename with the output filename
        path_components[-1] = output_filename
        
        # Join the path components back into a single string
        return '/'.join(path_components[:-1]) + '/' + output_filename  # Ensure correct path structure

    def _clean_up_temp_files(self, *file_paths):
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    def check_health(self) -> bool:
        rabbitmq_health = all([
            self.input_queue.check_health(),
            self.output_queue.check_health(),
            self.status_queue.check_health()
        ])
        return rabbitmq_health  # Assuming S3 health check is not required

def main():
    ocr_service = OCRProcessingService()

    if ocr_service.check_health():
        logger.info("Health check passed. Starting OCR Processing Service.")
        ocr_service.start()
    else:
        logger.error("Health check failed. Please check your RabbitMQ and S3 connections.")

if __name__ == "__main__":
    main()
