# document_service/app.py

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from minio import Minio
from minio.error import S3Error
import pika
import os
import requests
from datetime import datetime
from uuid import uuid4
from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    RABBITMQ_HOST,
    RABBITMQ_QUEUE,
    BUCKET_NAME,
    METADATA_SERVICE_URL 
)

app = FastAPI()

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

# Create the bucket if it doesn't exist
try:
    minio_client.make_bucket(BUCKET_NAME)
except S3Error as e:
    if e.code != "BucketAlreadyOwnedByYou":
        raise

# Function to publish messages to RabbitMQ
def publish_message(message: str):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    channel.basic_publish(exchange="", routing_key=RABBITMQ_QUEUE, body=message)
    connection.close()

def save_metadata_to_service(document_id: str, filename: str):
    # Prepare metadata
    metadata = {
        "title": filename,
        "author": "Unknown",  # This could be passed or derived from the document
        "creation_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "file_type": filename.split('.')[-1],  # Get the file extension
        "document_id": document_id
    }

    # Send metadata to Metadata Service
    try:
        response = requests.post(f"{METADATA_SERVICE_URL}/metadata/", json=metadata)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to save metadata: {str(e)}")

@app.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    object_name = file.filename
    document_id = str(uuid4())  # Generate a unique document ID
    
    try:
        # Create a temporary file
        with open(file.filename, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)
        
        # Upload file to MinIO
        minio_client.fput_object(BUCKET_NAME, object_name, file.filename)
        
        # Remove the temporary file
        os.remove(file.filename)
        
        # Save metadata to Metadata Service
        save_metadata_to_service(document_id, object_name)
        
        # Publish message to RabbitMQ
        publish_message(f'Document {object_name} uploaded to bucket {BUCKET_NAME}.')
        
        return JSONResponse(content={"message": f"File {object_name} uploaded successfully."}, status_code=201)
    
    except Exception as e:
        # If temporary file exists, remove it
        if os.path.exists(file.filename):
            os.remove(file.filename)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents/{filename}")
async def get_document(filename: str):
    try:
        # Check if the document exists
        minio_client.stat_object(BUCKET_NAME, filename)
        # Get the document URL (This is a presigned URL for temporary access)
        url = minio_client.presigned_get_object(BUCKET_NAME, filename)
        return {"url": url}
    except S3Error as e:
        raise HTTPException(status_code=404, detail=str(e))

def delete_metadata_from_service(document_id: str):
    """Function to delete metadata from Metadata Service"""
    try:
        response = requests.delete(f"{METADATA_SERVICE_URL}/metadata/{document_id}")
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete metadata: {str(e)}")

@app.delete("/documents/{filename}")
async def delete_document(filename: str):
    try:
        # Check if the document exists before deleting
        minio_client.stat_object(BUCKET_NAME, filename)

        # Remove the document from MinIO
        minio_client.remove_object(BUCKET_NAME, filename)

        # Generate document ID (assumed to be the filename or derived from it)
        document_id = filename  # You might have a different way of storing document IDs

        # Delete the corresponding metadata from the Metadata Service
        delete_metadata_from_service(document_id)

        # Publish message to RabbitMQ
        publish_message(f'Document {filename} deleted from bucket {BUCKET_NAME}.')

        return JSONResponse(content={"message": f"Document {filename} deleted successfully."}, status_code=200)

    except S3Error as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents")
async def list_documents():
    try:
        # List all documents in the bucket
        objects = minio_client.list_objects(BUCKET_NAME)
        file_list = [obj.object_name for obj in objects]
        return {"documents": file_list}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/documents/{filename}")
async def update_document(filename: str, file: UploadFile = File(...)):
    try:
        # Overwrite the existing document
        minio_client.fput_object(BUCKET_NAME, filename, file.filename)

        # Publish message to RabbitMQ
        publish_message(f'Document {filename} updated in bucket {BUCKET_NAME}.')

        return JSONResponse(content={"message": f"Document {filename} updated successfully."}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
