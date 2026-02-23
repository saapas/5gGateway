FROM python:3.11-slim

# Spark requires Java
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

RUN pip install pyspark

WORKDIR /app

COPY . .

CMD ["python", "spark_job.py"]
