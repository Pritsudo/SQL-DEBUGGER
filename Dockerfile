FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=7860

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY server /app/server
COPY client.py /app/client.py
COPY inference.py /app/inference.py
COPY models.py /app/models.py
COPY openenv.yaml /app/openenv.yaml
COPY README.md /app/README.md
COPY validate_submission.py /app/validate_submission.py

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 7860

ENV ENABLE_WEB_INTERFACE=true
CMD ["uvicorn", "server.app:app", "--host", "0.0.0.0", "--port", "7860"]