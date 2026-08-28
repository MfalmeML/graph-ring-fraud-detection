FROM graph-ring-fraud-base:latest

WORKDIR /app

COPY src/ /app/src/

CMD ["python", "-m", "src.ingestion.kafka_consumer"]