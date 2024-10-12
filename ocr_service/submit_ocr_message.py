import json
import os
from typing import Dict, Any
from dotenv import load_dotenv
import pika

# Load environment variables
load_dotenv()

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
INPUT_QUEUE = os.getenv("RABBITMQ_QUEUE")

def submit_ocr_message(storage_path: str, metadata: Dict[str, Any] = None) -> bool:
    """
    Submit a message to the OCR processing queue.

    Args:
    storage_path (str): The S3 path of the document to be processed.
    metadata (Dict[str, Any], optional): Additional metadata for the document.

    Returns:
    bool: True if the message was successfully submitted, False otherwise.
    """
    if metadata is None:
        metadata = {}

    message = {
        "storage_path": storage_path,
        "metadata": metadata,
        "operation": "ocr"
    }

    try:
        # Establish a connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()

        # Declare the queue (this is idempotent - it will only create the queue if it doesn't exist)
        channel.queue_declare(queue=INPUT_QUEUE, durable=False)

        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key=INPUT_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )

        # Close the connection
        connection.close()

        print(f"Message for {storage_path} submitted successfully")
        return True

    except Exception as e:
        print(f"Error submitting message: {str(e)}")
        return False

# Example usage
if __name__ == "__main__":
    # Example S3 path
    s3_path = "s3://xlhire-resume/development/5f2d13d49684cc5e96217b98/candidates/other/actual-resumes/1699463706639.doc"
    
    # Example metadata
    metadata = {
        "user_id": "12345",
        "document_type": "resume",
        "submission_date": "2024-10-12"
    }

    # Submit the message
    success = submit_ocr_message(s3_path, metadata)
    
    if success:
        print("Message submitted successfully")
    else:
        print("Failed to submit message")