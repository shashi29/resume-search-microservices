from pydantic import BaseSettings

class Settings(BaseSettings):
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    BUCKET_NAME: str = "documents"
    RABBITMQ_HOST: str
    RABBITMQ_QUEUE: str = "documents_queue"
    STATUS_QUEUE: str = "status_queue"
    METADATA_SERVICE_URL: str
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    class Config:
        env_file = ".env"

settings = Settings()