FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY transcode_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transcode_service/ ./transcode_service/

EXPOSE 8001

CMD ["uvicorn", "transcode_service.main:app", "--host", "0.0.0.0", "--port", "8001"]
