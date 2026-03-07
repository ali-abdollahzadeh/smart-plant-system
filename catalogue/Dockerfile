FROM python:3.11-slim

WORKDIR /app

# Avoid Python cache files and enable cleaner logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Catalogue configuration
ENV CATALOG_HOST=0.0.0.0
ENV CATALOG_PORT=8000
ENV CATALOG_FILE=/app/data/catalog.json

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Create persistent data directory
RUN mkdir -p /app/data

# Expose CherryPy port
EXPOSE 8000

# Start the catalogue service
CMD ["python", "main.py"]