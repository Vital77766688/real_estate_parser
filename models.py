import os
from typing import Any, Dict, List, Optional
from pydantic import BaseModel


"""
Pydantic models definigs configurations and outputs
"""
class URLModel(BaseModel):
    url: str
    city: str
    url_path: str
    property_type: str
    deal_type: str
    duration: str = None
    headers: Dict[str, str]
    params: Dict[str, Any]

    def build_url(self) -> str:
        return os.path.join(self.url, self.url_path)


class MapModel(BaseModel):
    lat: float
    lon: float


class AdvertModel(BaseModel):
    id: int
    title: str
    addressTitle: str
    rooms: Optional[int] = None
    square: float
    price: float
    photos: List[Dict[str, Any]]
    userType: str
    status: str
    storage: str
    map: MapModel
    url: URLModel
