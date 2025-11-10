import os
from google.cloud import bigquery

def get_bq_client():
    """Retourne un client BigQuery prêt, local ou cloud."""
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return bigquery.Client()
    return bigquery.Client()  # Cloud Run utilisera l'identité IAM
