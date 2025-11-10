from google.cloud import secretmanager
from google.api_core.exceptions import AlreadyExists

PROJECT_ID = "slottix"
SECRET_ID = "PG_PASSWORD"

PG_PASSWORD_VALUE = input("Entrez le nouveau mot de passe PostgreSQL : ").strip().encode("utf-8")

client = secretmanager.SecretManagerServiceClient()
parent = f"projects/{PROJECT_ID}"

try:
    client.create_secret(
        request={
            "parent": parent,
            "secret_id": SECRET_ID,
            "secret": {"replication": {"automatic": {}}},
        }
    )
    print("✅ Secret créé.")
except AlreadyExists:
    print("ℹ️ Secret existant, mise à jour...")

response = client.add_secret_version(
    request={"parent": f"{parent}/secrets/{SECRET_ID}", "payload": {"data": PG_PASSWORD_VALUE}}
)
print(f"✅ Nouvelle version ajoutée : {response.name}")
