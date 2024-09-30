from pydantic import BaseModel

# Schema for creating metadata
class MetadataCreate(BaseModel):
    title: str
    author: str
    creation_date: str
    file_type: str
    document_id: str


# Schema for updating metadata
class MetadataUpdate(BaseModel):
    title: str
    author: str
    creation_date: str
    file_type: str


# Schema for retrieving metadata (including the ID)
class Metadata(BaseModel):
    id: int
    title: str
    author: str
    creation_date: str
    file_type: str
    document_id: str

    class Config:
        orm_mode = True
