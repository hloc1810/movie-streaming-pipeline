FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python requests

COPY movie_producer.py .

CMD ["python", "-u", "movie_producer.py"]