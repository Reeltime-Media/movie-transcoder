FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy into transcode_service/ so "from transcode_service.x import y" imports work
COPY . ./transcode_service/

EXPOSE 8001

CMD ["uvicorn", "transcode_service.main:app", "--host", "0.0.0.0", "--port", "8001"]
