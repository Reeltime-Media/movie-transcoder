"""Live TV channel control — matches the contract movie-api's live_client.py
expects exactly:

  POST /channels/{channel_id}/start  {"source_url": "..."}
      -> {"status": "starting"|"live", "hls_url": "https://.../index.m3u8"}
  POST /channels/{channel_id}/stop
      -> {"status": "offline"}
  GET  /channels/{channel_id}/status
      -> {"status": "offline"|"starting"|"live"|"error", "hls_url": str|None,
          "error": str|None}
"""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from transcode_service import live_manager
from transcode_service.routers.jobs import _require_api_key

router = APIRouter(prefix="/channels", tags=["live"], dependencies=[Depends(_require_api_key)])


class StartChannelRequest(BaseModel):
    source_url: str


@router.post("/{channel_id}/start")
async def start_channel(channel_id: uuid.UUID, data: StartChannelRequest):
    return await live_manager.start_channel(str(channel_id), data.source_url)


@router.post("/{channel_id}/stop")
async def stop_channel(channel_id: uuid.UUID):
    return await live_manager.stop_channel(str(channel_id))


@router.get("/{channel_id}/status")
async def channel_status(channel_id: uuid.UUID):
    return live_manager.get_status(str(channel_id))
