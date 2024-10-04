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
from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    RABBITMQ_HOST,
    RABBITMQ_QUEUE,
    BUCKET_NAME
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

# Create the bucket if it doesn't exist
try:
    minio_client.client.make_bucket(BUCKET_NAME)
except Exception as e:
    if "BucketAlreadyOwnedByYou" not in str(e):
        logger.error(f"Error creating bucket: {e}")
        raise

@app.post("/upload")
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...), user: str = Depends(lambda: "Anonymous")):
    original_filename = file.filename
    idempotency_key = str(uuid4())  # Generate an idempotency key
    file_extension = os.path.splitext(original_filename)[1]
    object_name = f"{idempotency_key}{file_extension}"
    logger.info(f"Processing upload for: {original_filename} with idempotency key: {idempotency_key}")

    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()  # Ensure all data is written
            temp_file.seek(0)  # Move to the beginning of the file

            # Upload file to MinIO using idempotency_key as the filename
            minio_client.upload_file(bucket_name=BUCKET_NAME, object_name=object_name, file_path=temp_file.name)

        # Publish message to RabbitMQ in the background
        message = {
            "operation": "upload",
            "original_filename": original_filename,
            "document_name": object_name,
            "minio_path": f"{object_name}",
            "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user,
            "idempotency_key": idempotency_key
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)

        return JSONResponse(content={"message": f"File {original_filename} uploaded successfully as {object_name}."}, status_code=201)

    except Exception as e:
        logger.error(f"Error during upload: {e}")
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
    try:
        # Check if the document exists before deleting
        minio_client.client.stat_object(BUCKET_NAME, filename)

        # Remove the document from MinIO
        minio_client.client.remove_object(BUCKET_NAME, filename)

        # Publish message to RabbitMQ in the background
        message = {
            "operation": "delete",
            "document_name": filename,
            "minio_path": f"{BUCKET_NAME}/{filename}",
            "deleted_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)

        return JSONResponse(content={"message": f"Document {filename} deleted successfully."}, status_code=200)

    except Exception as e:
        logger.error(f"Error during deletion: {e}")
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
    try:
        # Generate a new idempotency key for the update
        idempotency_key = str(uuid4())
        file_extension = os.path.splitext(filename)[1]
        new_object_name = f"{idempotency_key}{file_extension}"

        # Upload the new version with the new idempotency key
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()  # Ensure all data is written
            temp_file.seek(0)  # Move to the beginning of the file

            minio_client.upload_file(bucket_name=BUCKET_NAME, object_name=new_object_name, file_path=temp_file.name)

        # Remove the old version
        minio_client.client.remove_object(BUCKET_NAME, filename)

        # Publish message to RabbitMQ in the background
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

        return JSONResponse(content={"message": f"Document {filename} updated successfully as {new_object_name}."}, status_code=200)
    except Exception as e:
        logger.error(f"Error during update: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while updating the document.")

@app.get("/health")
async def health_check():
    minio_status = minio_client.check_health()
    rabbitmq_status = rabbitmq_client.check_health()
    return JSONResponse(content={
        "minio": "healthy" if minio_status else "unhealthy",
        "rabbitmq": "healthy" if rabbitmq_status else "unhealthy"
    })

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)