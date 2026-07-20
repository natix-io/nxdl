from typing import Any

from pydantic import BaseModel


class DatasetRequestResponse(BaseModel):
    request_id: str
    request: dict[str, Any]
