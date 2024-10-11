from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
import os
import tempfile
import logging
import time
import uuid
from uuid import uuid4
from datetime import datetime, timedelta
import json
from threading import Thread

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    RABBITMQ_HOST,
    RABBITMQ_QUEUE,
    BUCKET_NAME,
    STATUS_QUEUE
)

# Import your utility classes
from minio_utils import MinioClient
from rabbitmq_utils import RabbitMQClient

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Initialize MinIO and RabbitMQ clients
minio_client = MinioClient(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

rabbitmq_client = RabbitMQClient(
    host=RABBITMQ_HOST,
    queue=RABBITMQ_QUEUE
)

status_queue_client = RabbitMQClient(
    host=RABBITMQ_HOST,
    queue=STATUS_QUEUE
)

# Create the bucket if it doesn't exist
try:
    minio_client.client.make_bucket(BUCKET_NAME)
except Exception as e:
    if "BucketAlreadyOwnedByYou" not in str(e):
        logger.error(f"Error creating bucket: {e}")
        raise

# def status_queue_consumer():
#     def callback(ch, method, properties, body):
#         message = json.loads(body)
#         logger.info(f"Received message: {message}")

#     status_queue_client.start_consuming(callback)

# # Start the status queue consumer in a separate thread
# status_thread = Thread(target=status_queue_consumer)
# status_thread.start()

@app.post("/upload")
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...), user: str = Depends(lambda: "Anonymous")):
    original_filename = file.filename
    idempotency_key = str(uuid4())
    file_extension = os.path.splitext(original_filename)[1]
    object_name = f"docs/{idempotency_key}{file_extension}"
    logger.info(f"Processing upload for: {original_filename} with idempotency key: {idempotency_key}")

    try:
        # Send initial status
        status_message = {
            "id": idempotency_key,
            "status": "STARTED",
            "details": {
                "operation": "upload",
                "original_filename": original_filename,
                "user": user
            }
        }
        status_queue_client.send_message(status_message)

        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()
            temp_file.seek(0)

            # Upload file to MinIO
            minio_client.upload_file(bucket_name=BUCKET_NAME, object_name=object_name, file_path=temp_file.name)

        # Publish message to RabbitMQ
        message = {
            "operation": "upload",
            "original_filename": original_filename,
            "document_name": f"{idempotency_key}{file_extension}",
            "minio_path": f"{object_name}",
            "idempotency_key": idempotency_key,
            "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)

        # Send completion status
        status_message["status"] = "COMPLETED"
        status_message["details"].update(message)
        status_queue_client.send_message(status_message)

        return JSONResponse(content={"message": f"File {original_filename} uploaded successfully as {object_name}.", "id": idempotency_key}, status_code=201)

    except Exception as e:
        logger.error(f"Error during upload: {e}")
        # Send error status
        status_message["status"] = "ERROR"
        status_message["details"]["error"] = str(e)
        status_queue_client.send_message(status_message)
        raise HTTPException(status_code=500, detail="An error occurred while uploading the document.")

@app.get("/documents/{filename}")
async def get_document(filename: str):
    try:
        # Check if the document exists
        minio_client.client.stat_object(BUCKET_NAME, filename)
        # Get the document URL (This is a presigned URL for temporary access)
        url = minio_client.client.presigned_get_object(BUCKET_NAME, filename, expires=timedelta(minutes=10))
        return {"url": url}
    except Exception as e:
        logger.error(f"Error fetching document: {e}")
        raise HTTPException(status_code=404, detail=f"Document {filename} not found.")

@app.delete("/documents/{filename}")
async def delete_document(background_tasks: BackgroundTasks, filename: str, user: str = Depends(lambda: "Anonymous")):
    operation_id = str(uuid4())
    try:
        # Send initial status
        status_message = {
            "id": operation_id,
            "status": "STARTED",
            "details": {
                "operation": "delete",
                "filename": filename,
                "user": user
            }
        }
        status_queue_client.send_message(status_message)

        # Check if the document exists before deleting
        minio_client.client.stat_object(BUCKET_NAME, filename)

        # Remove the document from MinIO
        minio_client.client.remove_object(BUCKET_NAME, filename)

        # Publish message to RabbitMQ
        message = {
            "operation": "delete",
            "document_name": filename,
            "minio_path": f"{BUCKET_NAME}/{filename}",
            "deleted_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)

        # Send completion status
        status_message["status"] = "COMPLETED"
        status_message["details"].update(message)
        status_queue_client.send_message(status_message)

        return JSONResponse(content={"message": f"Document {filename} deleted successfully.", "id": operation_id}, status_code=200)

    except Exception as e:
        logger.error(f"Error during deletion: {e}")
        # Send error status
        status_message["status"] = "ERROR"
        status_message["details"]["error"] = str(e)
        status_queue_client.send_message(status_message)
        raise HTTPException(status_code=500, detail="An error occurred while deleting the document.")

@app.get("/documents")
async def list_documents():
    try:
        # List all documents in the bucket
        file_list = minio_client.list_objects(bucket_name=BUCKET_NAME)
        return {"documents": file_list}
    except Exception as e:
        logger.error(f"Error listing documents: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while listing documents.")

@app.put("/documents/{filename}")
async def update_document(background_tasks: BackgroundTasks, filename: str, file: UploadFile = File(...), user: str = Depends(lambda: "Anonymous")):
    operation_id = str(uuid4())
    try:
        # Send initial status
        status_message = {
            "id": operation_id,
            "status": "STARTED",
            "details": {
                "operation": "update",
                "filename": filename,
                "user": user
            }
        }
        status_queue_client.send_message(status_message)

        # Generate a new idempotency key for the update
        idempotency_key = str(uuid4())
        file_extension = os.path.splitext(filename)[1]
        new_object_name = f"{idempotency_key}{file_extension}"

        # Upload the new version with the new idempotency key
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()
            temp_file.seek(0)

            minio_client.upload_file(bucket_name=BUCKET_NAME, object_name=new_object_name, file_path=temp_file.name)

        # Remove the old version
        minio_client.client.remove_object(BUCKET_NAME, filename)

        # Publish message to RabbitMQ
        message = {
            "operation": "update",
            "original_filename": filename,
            "document_name": new_object_name,
            "minio_path": f"{BUCKET_NAME}/{new_object_name}",
            "updated_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user,
            "idempotency_key": idempotency_key
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)

        # Send completion status
        status_message["status"] = "COMPLETED"
        status_message["details"].update(message)
        status_queue_client.send_message(status_message)

        return JSONResponse(content={"message": f"Document {filename} updated successfully as {new_object_name}.", "id": operation_id}, status_code=200)
    except Exception as e:
        logger.error(f"Error during update: {e}")
        # Send error status
        status_message["status"] = "ERROR"
        status_message["details"]["error"] = str(e)
        status_queue_client.send_message(status_message)
        raise HTTPException(status_code=500, detail="An error occurred while updating the document.")
