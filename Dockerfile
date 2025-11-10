# === Étape 1 : build Python Flask ===
FROM python:3.13-slim AS base

# Empêche Python de générer des fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Répertoire de travail
WORKDIR /app

# Installation dépendances système
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Copie des fichiers
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Port Flask
ENV PORT 8080

# === Étape 2 : configuration Cloud SQL ===
# Ces variables seront injectées automatiquement par Cloud Run
ENV INSTANCE_CONNECTION_NAME=slottix:europe-west1:slottix-db
ENV DB_USER=slottix_web
ENV DB_NAME=entrepot_optimisation
ENV DB_SECRET=PG_PASSWORD
ENV PROJECT_ID=slottix

CMD ["python", "app.py"]

