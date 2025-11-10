import os
import psycopg2
from psycopg2 import pool
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound, PermissionDenied

PROJECT_ID = "slottix"
SECRET_ID = "PG_PASSWORD"

PG_USER = "slottix_web"
PG_DB = "entrepot_optimisation"
import os
PG_HOST = f"/cloudsql/{os.environ.get('INSTANCE_CONNECTION_NAME')}"
PG_PORT = 5432


connection_pool = None  # initialisé dynamiquement


def get_secret(secret_id):
    """Récupère un secret depuis Google Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except PermissionDenied:
        raise RuntimeError(f"🚫 Permission refusée pour accéder au secret {secret_id}. Vérifie les IAM.")
    except NotFound:
        raise RuntimeError(f"❌ Le secret {secret_id} n'existe pas dans le projet {PROJECT_ID}.")
    except Exception as e:
        raise RuntimeError(f"Erreur accès Secret Manager : {e}")


def init_pg_pool():
    """Initialise le pool de connexions PostgreSQL à la première utilisation"""
    global connection_pool
    if connection_pool:
        return connection_pool  # déjà prêt

    print("🔑 Chargement du mot de passe PostgreSQL depuis Secret Manager...")
    pg_password = get_secret(SECRET_ID)
    print("✅ Secret récupéré avec succès.")

    try:
        connection_pool = pool.SimpleConnectionPool(
            minconn=2,
            maxconn=10,
            user=PG_USER,
            password=pg_password,
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB
        )
        # Test rapide
        conn = connection_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user;")
        db, user = cur.fetchone()
        cur.close()
        connection_pool.putconn(conn)
        print(f"✅ Connexion PostgreSQL prête ({db} / {user})")
        return connection_pool

    except Exception as e:
        raise RuntimeError(f"❌ Erreur connexion PostgreSQL : {e}")


def get_pg_connection():
    """Retourne une connexion active depuis le pool"""
    global connection_pool
    if not connection_pool:
        init_pg_pool()
    return connection_pool.getconn()


def release_pg_connection(conn):
    """Remet la connexion dans le pool"""
    global connection_pool
    if connection_pool and conn:
        connection_pool.putconn(conn)


def close_pg_pool():
    """Ferme proprement toutes les connexions"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        connection_pool = None
        print("✅ Pool PostgreSQL fermé proprement.")
