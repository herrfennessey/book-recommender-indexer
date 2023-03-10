from typing import Dict, Any

from pydantic import BaseModel, Extra


class MessagePayload(BaseModel):
    attributes: Dict[str, Any] = {}
    data: str
    message_id: str
    publish_time: str

    class Config:
        extra = Extra.ignore


class PubSubMessage(BaseModel):
    message: MessagePayload
    subscription: str

    class Config:
        extra = Extra.ignore
