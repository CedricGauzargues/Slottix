from google.cloud import secretmanager
import psycopg2
import os

# Nom du secret dans Google Secret Manager
SECRET_ID = "PG_PASSWORD"
PROJECT_ID = "slottix"

# --- Récupération du mot de passe depuis Secret Manager ---
def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

PG_PASSWORD = get_secret(SECRET_ID)

# --- Connexion PostgreSQL ---
conn = psycopg2.connect(
    dbname="entrepot_optimisation",
    user="slottix_web",
    password=PG_PASSWORD,
    host="127.0.0.1",
    port=5433
)

print("✅ Connexion PostgreSQL réussie via Secret Manager !")
