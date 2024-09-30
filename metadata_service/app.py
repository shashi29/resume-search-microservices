from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from models import Metadata  # Import the Metadata model from models.py
from schemas import MetadataCreate, MetadataUpdate  # Import schemas from schemas.py
from database import SessionLocal, engine
from typing import List  # Import List for type annotations

# Create all tables
Metadata.metadata.create_all(bind=engine)

app = FastAPI(title="Metadata Service", version="1.0.0")

# Dependency to get the DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# CRUD Functions

def create_metadata(db: Session, metadata: MetadataCreate):
    db_metadata = Metadata(
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
    return db.query(Metadata).filter(Metadata.document_id == document_id).first()


def get_metadata_list(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Metadata).offset(skip).limit(limit).all()


def update_metadata(db: Session, document_id: str, metadata: MetadataUpdate):
    db_metadata = db.query(Metadata).filter(Metadata.document_id == document_id).first()
    if db_metadata:
        db_metadata.title = metadata.title
        db_metadata.author = metadata.author
        db_metadata.creation_date = metadata.creation_date
        db_metadata.file_type = metadata.file_type
        db.commit()
        db.refresh(db_metadata)
    return db_metadata


def delete_metadata(db: Session, document_id: str):
    db_metadata = db.query(Metadata).filter(Metadata.document_id == document_id).first()
    if db_metadata:
        db.delete(db_metadata)
        db.commit()

# FastAPI Routes

@app.post("/metadata/", response_model=MetadataCreate)
def create_metadata_route(metadata: MetadataCreate, db: Session = Depends(get_db)):
    return create_metadata(db=db, metadata=metadata)


@app.get("/metadata/{document_id}", response_model=MetadataCreate)
def read_metadata(document_id: str, db: Session = Depends(get_db)):
    db_metadata = get_metadata(db, document_id=document_id)
    if db_metadata is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    return db_metadata


@app.get("/metadata/", response_model=List[MetadataCreate])
def list_metadata(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return get_metadata_list(db, skip=skip, limit=limit)


@app.put("/metadata/{document_id}", response_model=MetadataCreate)
def update_metadata_route(document_id: str, metadata: MetadataUpdate, db: Session = Depends(get_db)):
    return update_metadata(db=db, document_id=document_id, metadata=metadata)


@app.delete("/metadata/{document_id}")
def delete_metadata_route(document_id: str, db: Session = Depends(get_db)):
    delete_metadata(db, document_id=document_id)
    return {"message": "Metadata deleted successfully"}
