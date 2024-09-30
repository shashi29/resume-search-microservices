# Document Service

The Document Service is a microservice built with FastAPI that allows for uploading, retrieving, deleting, and listing documents stored in MinIO. It also integrates with RabbitMQ for message publishing related to document operations.

## Features

- Upload documents to MinIO
- Retrieve URLs for uploaded documents
- Delete documents from MinIO
- List all uploaded documents
- Update existing documents
- Publish messages to RabbitMQ for document events

## Requirements

- Docker
- Docker Compose

## Directory Structure

```plaintext
document_service/
│
├── Dockerfile
├── docker-compose.yml
├── app.py
├── config.py
└── requirements.txt
```

## Setup Instructions

1. **Clone the Repository**

   Clone this repository to your local machine.

   ```bash
   git clone <repository-url>
   cd document_service
   ```

2. **Build and Run the Services**

   Use Docker Compose to build and run the services.

   ```bash
   docker-compose up --build
   ```

3. **Access the FastAPI Documentation**

   Open your browser and navigate to `http://localhost:8000/docs` to view the API documentation and test the endpoints.

## API Endpoints

### 1. Upload Document

- **Endpoint**: `POST /upload`
- **Description**: Upload a file to MinIO.
- **Request Body**: 
  - `file`: The file to upload.

**Example Request**:

```bash
curl -X POST "http://localhost:8000/upload" -F "file=@path/to/your/file.txt"
```

### 2. Get Document

- **Endpoint**: `GET /documents/{filename}`
- **Description**: Retrieve the URL for the uploaded document.
- **Path Parameters**:
  - `filename`: Name of the file in MinIO.

**Example Request**:

```bash
curl "http://localhost:8000/documents/file.txt"
```

### 3. Delete Document

- **Endpoint**: `DELETE /documents/{filename}`
- **Description**: Delete an uploaded document.
- **Path Parameters**:
  - `filename`: Name of the file to delete.

**Example Request**:

```bash
curl -X DELETE "http://localhost:8000/documents/file.txt"
```

### 4. List Documents

- **Endpoint**: `GET /documents`
- **Description**: List all uploaded documents.

**Example Request**:

```bash
curl "http://localhost:8000/documents"
```

### 5. Update Document

- **Endpoint**: `PUT /documents/{filename}`
- **Description**: Overwrite an existing document.
- **Path Parameters**:
  - `filename`: Name of the file to update.
- **Request Body**:
  - `file`: The new file to upload.

**Example Request**:

```bash
curl -X PUT "http://localhost:8000/documents/file.txt" -F "file=@path/to/new/file.txt"
```

## Environment Variables

You can configure the following environment variables in `config.py`:

- `MINIO_ENDPOINT`: MinIO server endpoint (default: `minio:9000`).
- `MINIO_ACCESS_KEY`: MinIO access key (default: `minioadmin`).
- `MINIO_SECRET_KEY`: MinIO secret key (default: `minioadmin`).
- `BUCKET_NAME`: Bucket name for storing documents (default: `documents`).
- `RABBITMQ_HOST`: RabbitMQ server endpoint (default: `rabbitmq`).
- `RABBITMQ_QUEUE`: RabbitMQ queue name (default: `documents_queue`).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

If you wish to contribute to this project, feel free to fork the repository and submit a pull request!

```

### Instructions for Use

1. **Replace `<repository-url>`** with the actual URL of your Git repository.
2. **Make sure to create a `LICENSE` file** if you mention a license.
3. **Add any additional notes or sections** that might be relevant for your specific use case.

This `README.md` provides comprehensive documentation for your Document Service. If you need any more modifications or additional content, just let me know!