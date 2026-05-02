from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class Movie(BaseModel):
    id: str
    title: str
    year: Optional[int] = None
    genres: List[str] = []
    poster_url: Optional[str] = None
    detail_url: str

class MovieDetail(Movie):
    description: Optional[str] = None
    video_embed_url: Optional[str] = None

class Response(BaseModel):
    status: str
    data: Optional[dict] = None
    timestamp: datetime
