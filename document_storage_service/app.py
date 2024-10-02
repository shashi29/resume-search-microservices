from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
import os
import sys
from uuid import uuid4
from datetime import datetime
from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    RABBITMQ_HOST,
    RABBITMQ_QUEUE,
    BUCKET_NAME
)

# Now import your utility classes
from minio_utils import MinioClient
from rabbitmq_utils import RabbitMQClient

app = FastAPI()

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
        raise

@app.post("/upload")
async def upload_document(file: UploadFile = File(...), user: str = Depends(lambda: "Anonymous")):
    object_name = file.filename
    try:
        # Create a temporary file
        with open(file.filename, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)

        # Upload file to MinIO
        minio_client.upload_file(bucket_name=BUCKET_NAME, object_name=object_name, file_path=file.filename)

        # Remove the temporary file
        os.remove(file.filename)

        # Publish message to RabbitMQ with detailed information
        message = {
            "operation": "upload",
            "document_name": object_name,
            "minio_path": f"{BUCKET_NAME}/{object_name}",
            "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user
        }
        rabbitmq_client.send_message(message)

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
        minio_client.client.stat_object(BUCKET_NAME, filename)
        # Get the document URL (This is a presigned URL for temporary access)
        url = minio_client.client.presigned_get_object(BUCKET_NAME, filename)
        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.delete("/documents/{filename}")
async def delete_document(filename: str, user: str = Depends(lambda: "Anonymous")):
    try:
        # Check if the document exists before deleting
        minio_client.client.stat_object(BUCKET_NAME, filename)

        # Remove the document from MinIO
        minio_client.client.remove_object(BUCKET_NAME, filename)

        # Publish message to RabbitMQ with detailed information
        message = {
            "operation": "delete",
            "document_name": filename,
            "minio_path": f"{BUCKET_NAME}/{filename}",
            "deleted_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user
        }
        rabbitmq_client.send_message(message)

        return JSONResponse(content={"message": f"Document {filename} deleted successfully."}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents")
async def list_documents():
    try:
        # List all documents in the bucket
        file_list = minio_client.list_objects(bucket_name=BUCKET_NAME)
        return {"documents": file_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/documents/{filename}")
async def update_document(filename: str, file: UploadFile = File(...), user: str = Depends(lambda: "Anonymous")):
    try:
        # Overwrite the existing document
        minio_client.upload_file(bucket_name=BUCKET_NAME, object_name=filename, file_path=file.filename)

        # Publish message to RabbitMQ with detailed information
        message = {
            "operation": "update",
            "document_name": filename,
            "minio_path": f"{BUCKET_NAME}/{filename}",
            "updated_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": user
        }
        rabbitmq_client.send_message(message)

        return JSONResponse(content={"message": f"Document {filename} updated successfully."}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
