FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances système
RUN apt-get update -qq && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY app/ ./app/

# Exposer le port
EXPOSE 8000

# Démarrer FastAPI
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", \
     "--port", "8000", "--reload"]
