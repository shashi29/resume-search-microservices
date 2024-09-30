from sqlalchemy.orm import Session
from app import models, schemas


def create_metadata(db: Session, metadata: schemas.MetadataCreate):
    db_metadata = models.Metadata(
        title=metadata.title,
        author=metadata.author,
        creation_date=metadata.creation_date,
        file_type=metadata.file_type,
        document_id=metadata.document_id
    )
    db.add(db_metadata)
    db.commit()
    db.refresh(db_metadata)
    return db_metadata


def get_metadata(db: Session, document_id: str):
    return db.query(models.Metadata).filter(models.Metadata.document_id == document_id).first()


def get_metadata_list(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Metadata).offset(skip).limit(limit).all()


def update_metadata(db: Session, document_id: str, metadata: schemas.MetadataUpdate):
    db_metadata = db.query(models.Metadata).filter(models.Metadata.document_id == document_id).first()
    if db_metadata:
        db_metadata.title = metadata.title
        db_metadata.author = metadata.author
        db_metadata.creation_date = metadata.creation_date
        db_metadata.file_type = metadata.file_type
        db.commit()
        db.refresh(db_metadata)
    return db_metadata


def delete_metadata(db: Session, document_id: str):
    db_metadata = db.query(models.Metadata).filter(models.Metadata.document_id == document_id).first()
    if db_metadata:
        db.delete(db_metadata)
        db.commit()
