from pydantic import BaseModel
from typing import Optional, List

class IngestRequest(BaseModel):
    model_id: Optional[str] = None
    paths: Optional[List[str]] = None
    sync: Optional[bool] = False