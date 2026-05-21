FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY configuration.yml checks.yml .

CMD ["soda", "scan", "-d", "postgres", "-c", "configuration.yml", "checks.yml"]
