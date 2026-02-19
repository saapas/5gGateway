FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn

COPY . .

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
