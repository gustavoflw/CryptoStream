FROM python:3.14-rc-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY producer/ producer/

CMD ["python", "-m", "producer.producer"]
