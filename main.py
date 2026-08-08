import asyncio
import mimetypes
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from transcode_service.config import settings
from transcode_service.routers import dashboard, jobs, live
from transcode_service import live_manager
from transcode_service import worker as worker_module

# Not registered in the stdlib mimetypes DB on every platform — HLS clients
# don't strictly require these, but set them for correctness.
mimetypes.add_type("application/vnd.apple.mpegurl", ".m3u8")
mimetypes.add_type("video/mp2t", ".ts")

Path(settings.live_output_dir).mkdir(parents=True, exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(worker_module.run_worker())
    yield
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    await live_manager.stop_all()


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs.router)
app.include_router(dashboard.router)
app.include_router(live.router)

app.mount("/live", StaticFiles(directory=settings.live_output_dir), name="live")


@app.get("/health", tags=["health"])
async def health():
    return {
        "status": "ok",
        "worker": "running" if worker_module.worker_ready else "starting",
        "r2_scan_mode": settings.r2_scan_mode,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=settings.debug)

