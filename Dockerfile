FROM python:3.11-slim
WORKDIR /app

# system deps for duckdb
RUN apt-get update && apt-get install -y build-essential libssl-dev libffi-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./

EXPOSE 8000
CMD ["uvicorn", "labeler.main:app", "--host", "0.0.0.0", "--port", "8000"]
