
# Metadata Service

This is a simple **Metadata Service** built using [FastAPI](https://fastapi.tiangolo.com/). It provides an API for creating, reading, updating, and deleting document metadata.

## Features

- **Create Metadata**: Add metadata for new documents.
- **Read Metadata**: Retrieve metadata by document ID.
- **Update Metadata**: Modify existing metadata.
- **Delete Metadata**: Remove metadata by document ID.
- **List Metadata**: Get a list of all stored metadata.

## Requirements

- Python 3.6 or higher
- FastAPI
- SQLAlchemy
- Uvicorn

## Installation

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd metadata_service
   ```

2. **Install the required packages**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up the database**: 
   - The service uses SQLite by default. You can change the database URL in `config.py` to use a different database if needed.

## Usage

1. **Run the application**:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8082 --reload
   ```

2. **Access the service**: 
   The service will be available at `http://127.0.0.1:8082`.

3. **API Documentation**: 
   You can access the interactive API documentation at `http://127.0.0.1:8082/docs`.

## API Endpoints

### Create Metadata
- **Endpoint**: `POST /metadata/`
- **Description**: Create new document metadata.
- **Request Body**:
    ```json
    {
        "title": "Document Title",
        "author": "Author Name",
        "creation_date": "YYYY-MM-DD",
        "file_type": "pdf"
    }
    ```
- **Response**:
    - Returns the created metadata with its ID.

### Read Metadata
- **Endpoint**: `GET /metadata/{metadata_id}`
- **Description**: Get metadata by document ID.
- **Response**:
    - Returns the metadata for the specified ID.

### Update Metadata
- **Endpoint**: `PUT /metadata/{metadata_id}`
- **Description**: Update metadata by document ID.
- **Request Body**:
    ```json
    {
        "title": "Updated Title",
        "author": "Updated Author",
        "creation_date": "YYYY-MM-DD",
        "file_type": "docx"
    }
    ```
- **Response**:
    - Returns the updated metadata.

### Delete Metadata
- **Endpoint**: `DELETE /metadata/{metadata_id}`
- **Description**: Delete metadata by document ID.
- **Response**:
    - Returns a 204 No Content status on successful deletion.

### List Metadata
- **Endpoint**: `GET /metadata/`
- **Description**: List all metadata.
- **Response**:
    - Returns a list of all metadata entries.

## Database Setup

The service uses SQLite for simplicity. The database file will be created in the project directory upon the first run. You can change the database configuration in `config.py` to connect to other databases like PostgreSQL or MySQL.

## License

This project is licensed under the MIT License.
```

### Summary

The `README.md` file is now updated to reflect the change in the port to **8082**. It includes detailed instructions for installation, usage, and API endpoints. You can customize it further based on your project needs. If you have any additional questions or requests, feel free to ask!