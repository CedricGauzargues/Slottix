# === Étape 1 : Build Python Flask ===
FROM python:3.13-slim AS base

# Empêche Python de générer des fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Répertoire de travail
WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copie des dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code
COPY . .

# === Étape 2 : Variables d’environnement Cloud Run ===
# Ces variables sont injectées automatiquement par Google Cloud Run
ENV PORT=8080
ENV PROJECT_ID=slottix
ENV INSTANCE_CONNECTION_NAME=slottix:europe-west1:slottix-db
ENV DB_USER=slottix_web
ENV DB_NAME=entrepot_optimisation
ENV DB_SECRET=PG_PASSWORD

# === Étape 3 : Lancement de l’application ===
CMD ["python", "app.py"]
