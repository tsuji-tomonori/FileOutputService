from typing import NamedTuple
import json


class MessageIoTuple(NamedTuple):
    video_id: str
    channel_id: str
    title: str


def event_to_message(record: dict) -> MessageIoTuple:
    param = {}
    message = json.loads(record["Sns"]["Message"])
    for key in MessageIoTuple._fields:
        param[key] = message[key]
    return MessageIoTuple(**param)
