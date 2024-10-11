from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import JSONResponse
from app.core.security import verify_token
from app.services.minio_service import MinioClient, MinioClientError
from app.services.rabbitmq_service import RabbitMQClient, RabbitMQClientError
from app.core.config import settings
from app.utils.logging_config import logger
import uuid
import tempfile
import os
from datetime import datetime

router = APIRouter()

minio_client = MinioClient()
rabbitmq_client = RabbitMQClient(settings.RABBITMQ_HOST, settings.RABBITMQ_QUEUE)
status_queue_client = RabbitMQClient(settings.RABBITMQ_HOST, settings.STATUS_QUEUE)

@router.post("/upload")
async def upload_document(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    current_user: str = "Shashi"
):
    original_filename = file.filename
    idempotency_key = str(uuid.uuid4())
    file_extension = os.path.splitext(original_filename)[1]
    object_name = f"docs/{idempotency_key}{file_extension}"
    
    status_message = {
        "idempotency_key": idempotency_key,
        "status": "STARTED",
        "details": {
            "operation": "upload",
            "original_filename": original_filename,
            "user": current_user
        }
    }
    
    try:
        status_queue_client.send_message(status_message)
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()
            temp_file.seek(0)
            
            minio_client.upload_file(bucket_name=settings.BUCKET_NAME, object_name=object_name, file_path=temp_file.name)
        
        message = {
            "operation": "upload",
            "original_filename": original_filename,
            "document_name": f"{idempotency_key}{file_extension}",
            "minio_path": f"{object_name}",
            "idempotency_key": idempotency_key,
            "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)
        
        status_message["status"] = "COMPLETED"
        status_message["details"].update(message)
        status_queue_client.send_message(status_message)
        
        return JSONResponse(content={"message": f"File {original_filename} uploaded successfully as {object_name}.", "idempotency_key": idempotency_key}, status_code=201)
    
    except (MinioClientError, RabbitMQClientError) as e:
        logger.error(f"Error during upload: {e}")
        status_message["status"] = "ERROR"
        status_message["details"]["error"] = str(e)
        status_queue_client.send_message(status_message)
        raise HTTPException(status_code=500, detail="An error occurred while uploading the document.")

@router.get("/documents/{filename}")
async def get_document(filename: str, current_user: str = "Shashi"):
    try:
        url = minio_client.client.presigned_get_object(settings.BUCKET_NAME, filename, expires=3600)
        return {"url": url}
    except MinioClientError as e:
        logger.error(f"Error fetching document: {e}")
        raise HTTPException(status_code=404, detail=f"Document {filename} not found.")

@router.delete("/documents/{filename}")
async def delete_document(
    background_tasks: BackgroundTasks,
    filename: str,
    current_user: str = "Shashi"
):
    idempotency_key = str(uuid.uuid4())
    status_message = {
        "idempotency_key": idempotency_key,
        "status": "STARTED",
        "details": {
            "operation": "delete",
            "filename": filename,
            "user": current_user
        }
    }
    
    try:
        status_queue_client.send_message(status_message)
        
        minio_client.client.remove_object(settings.BUCKET_NAME, filename)
        
        message = {
            "operation": "delete",
            "document_name": filename,
            "minio_path": f"{settings.BUCKET_NAME}/{filename}",
            "idempotency_key": idempotency_key,
            "deleted_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": current_user
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)
        
        status_message["status"] = "COMPLETED"
        status_message["details"].update(message)
        status_queue_client.send_message(status_message)
        
        return JSONResponse(content={"message": f"Document {filename} deleted successfully.", "idempotency_key": idempotency_key}, status_code=200)
    
    except (MinioClientError, RabbitMQClientError) as e:
        logger.error(f"Error during deletion: {e}")
        status_message["status"] = "ERROR"
        status_message["details"]["error"] = str(e)
        status_queue_client.send_message(status_message)
        raise HTTPException(status_code=500, detail="An error occurred while deleting the document.")

@router.put("/documents/{filename}")
async def update_document(
    background_tasks: BackgroundTasks,
    filename: str,
    file: UploadFile = File(...),
    current_user: str = "Shashi"
):
    idempotency_key = str(uuid.uuid4())
    status_message = {
        "idempotency_key": idempotency_key,
        "status": "STARTED",
        "details": {
            "operation": "update",
            "filename": filename,
            "user": current_user
        }
    }
    
    try:
        status_queue_client.send_message(status_message)
        
        file_extension = os.path.splitext(filename)[1]
        new_object_name = f"docs/{idempotency_key}{file_extension}"
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()
            temp_file.seek(0)
            
            minio_client.upload_file(bucket_name=settings.BUCKET_NAME, object_name=new_object_name, file_path=temp_file.name)
        
        minio_client.client.remove_object(settings.BUCKET_NAME, filename)
        
        message = {
            "operation": "update",
            "original_filename": filename,
            "document_name": f"{idempotency_key}{file_extension}",
            "minio_path": f"{new_object_name}",
            "idempotency_key": idempotency_key,
            "updated_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "user": current_user
        }
        background_tasks.add_task(rabbitmq_client.send_message, message)
        
        status_message["status"] = "COMPLETED"
        status_message["details"].update(message)
        status_queue_client.send_message(status_message)
        
        return JSONResponse(content={"message": f"Document {filename} updated successfully as {new_object_name}.", "idempotency_key": idempotency_key}, status_code=200)
    
    except (MinioClientError, RabbitMQClientError) as e:
        logger.error(f"Error during update: {e}")
        status_message["status"] = "ERROR"
        status_message["details"]["error"] = str(e)
        status_queue_client.send_message(status_message)
        raise HTTPException(status_code=500, detail="An error occurred while updating the document.")

@router.get("/documents")
async def list_documents(current_user: str = "Shashi"):
    try:
        documents = minio_client.list_objects(bucket_name=settings.BUCKET_NAME)
        return {"documents": documents}
    except MinioClientError as e:
        logger.error(f"Error listing documents: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while listing documents.")