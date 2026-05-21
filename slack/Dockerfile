FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY notify.py demo_insert.py .
CMD ["python", "notify.py"]
