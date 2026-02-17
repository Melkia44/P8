FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/

# Créer le dossier output
RUN mkdir -p /app/output

CMD ["python", "scripts/run_pipeline.py"]
