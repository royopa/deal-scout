FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY listener/requirements.txt /app/listener/requirements.txt
RUN pip install --no-cache-dir -r /app/listener/requirements.txt

COPY listener /app/listener

WORKDIR /app/listener

CMD ["python", "listener.py"]