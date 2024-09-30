from sqlalchemy import Column, Integer, String
from database import Base

class Metadata(Base):
    __tablename__ = "metadata"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    author = Column(String, index=True)
    creation_date = Column(String)
    file_type = Column(String)
    document_id = Column(String, unique=True, index=True)
