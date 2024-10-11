# Document Service API

## Overview

The Document Service API is a FastAPI-based microservice designed to handle document storage and management. It integrates with MinIO for object storage and RabbitMQ for asynchronous message processing. This service provides endpoints for uploading, retrieving, updating, and deleting documents, with built-in authentication and status tracking.

## Features

- Document upload with idempotency support
- Secure document retrieval
- Document deletion
- Document update
- List all documents
- Integration with MinIO for object storage
- Asynchronous processing using RabbitMQ
- JWT-based authentication
- Comprehensive logging and error handling

## Prerequisites

- Python 3.9+
- Docker and Docker Compose (for containerized deployment)
- MinIO server
- RabbitMQ server
- Metadata Service (separate microservice)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-repo/document-service.git
   cd document-service
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Create a `.env` file in the root directory and add the necessary environment variables (see Configuration section).

## Configuration

Create a `.env` file in the root directory with the following variables:

```
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=your_minio_access_key
MINIO_SECRET_KEY=your_minio_secret_key
BUCKET_NAME=documents

RABBITMQ_HOST=rabbitmq
RABBITMQ_QUEUE=documents_queue
STATUS_QUEUE=status_queue

METADATA_SERVICE_URL=http://metadata-service:8082

SECRET_KEY=your_secret_key_here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

Replace the placeholder values with your actual configuration.

## Running the Application

### Local Development

1. Ensure that MinIO and RabbitMQ are running and accessible.
2. Run the FastAPI application:
   ```
   uvicorn app.main:app --reload
   ```
3. The API will be available at `http://localhost:8000`.

### Using Docker

1. Build the Docker image:
   ```
   sudo docker build -t document-service .
   ```

2. Run the Docker container:
   ```
   sudo docker run -p 8082:8082 --env-file .env document-service
   ```

## API Endpoints

- `POST /api/upload`: Upload a new document
- `GET /api/documents/{filename}`: Retrieve a document
- `DELETE /api/documents/{filename}`: Delete a document
- `PUT /api/documents/{filename}`: Update an existing document
- `GET /api/documents`: List all documents

For detailed API documentation, run the application and visit `http://localhost:8000/docs`.

## Authentication

This service uses JWT-based authentication. To access protected endpoints, you need to include a valid JWT token in the Authorization header of your requests.

## Error Handling

The service includes comprehensive error handling and logging. Check the application logs for detailed information about any errors that occur during operation.

## Contributing

Contributions to the Document Service API are welcome. Please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Create a new Pull Request

## License

[MIT License](LICENSE)

## Contact

For any questions or concerns, please open an issue in the GitHub repository.