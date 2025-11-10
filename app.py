import os
import io
import pandas as pd
import threading
import time
import numpy as np
import getpass
import psycopg2
from google.cloud import bigquery, secretmanager
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, Response, session, get_flashed_messages
from werkzeug.utils import secure_filename
from datetime import datetime
from db import close_pg_pool

# Import des blueprints
from detail_emplacement import bp_detail_emplacement   # ✅ page Détail Emplacement
from routes import bp_routes                           # ✅ page Routes


# ================================
# 🔐 Authentification Google Cloud
# ================================
# Ton compte de service GCP pour BigQuery et Secret Manager
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    r"C:\Users\cedri\Documents\Projet\Slotting Profiling\SlottixFlask\credentials_slottix.json"
)

PROJECT_ID = "slottix"
DATASET_ID = "entrepot_optimisation"

# ================================
# 🔐 Secret Manager - mot de passe SQL
# ================================
def get_secret(secret_id):
    """Récupère un secret GCP de manière sécurisée."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


# Nom du secret tel que défini dans Google Secret Manager
PG_SECRET_ID = "PG_PASSWORD"
PG_PASSWORD = get_secret(PG_SECRET_ID)



# ================================
# 📊 Connexion BigQuery
# ================================
client = bigquery.Client(project=PROJECT_ID)


# ================================
# ⚙️ Configuration Flask
# ================================
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'txt'}

app = Flask(__name__)
app.secret_key = "votre_cle_secrete"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Enregistrement des blueprints
app.register_blueprint(bp_detail_emplacement)
app.register_blueprint(bp_routes)

#-----------------------------------
# Test si google secret est connecté
#-----------------------------------
# 🔐 Vérification automatique au démarrage
from db import init_pg_pool

try:
    init_pg_pool()
except Exception as e:
    print(f"🚨 Erreur de configuration PostgreSQL : {e}")
    exit(1)


# ================================
# 🔧 Fonctions utilitaires
# ================================
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# Utilisateur local (utile pour logs ou traçabilité)
current_user = getpass.getuser()


# 🔎 Récupère uniquement les tables actives
def get_active_tables():
    query = f"""
        SELECT NomTable
        FROM {PROJECT_ID}.{DATASET_ID}.TblChargementAutomatique
        WHERE Actif = TRUE
    """
    results = client.query(query).result()
    return [row.NomTable for row in results]


# ==========================
# ROUTE ACCUEIL
# ==========================
@app.route('/')
def index():
    return render_template("index.html")


# ============================================================
# 🔄 SYNCHRONISATION AVANCÉE TblEmplacement (MERGE conditionnel)
# ============================================================
def sync_tbl_emplacement_background(df, client, PROJECT_ID, DATASET_ID, filename):
    """Synchronisation asynchrone de TblEmplacement :
       - Nettoyage des doublons / lignes vides
       - MERGE intelligent (mise à jour conditionnelle)
    """
    try:
        import pandas as pd

        print("🔹 Démarrage de la synchronisation avancée TblEmplacement")

        # 🔸 Nettoyage de base
        if "Zone" in df.columns:
            before = len(df)
            df = df[df["Zone"].notna() & (df["Zone"].astype(str).str.strip() != "")]
            print(f"🧹 {before - len(df)} lignes supprimées (Zone vide).")

        # 🔸 Suppression des doublons sur combinaison clé (Zone, Allee, Deplacement, Niveau)
        key_cols = ["Zone", "Allee", "Deplacement", "Niveau"]
        key_cols = [c for c in key_cols if c in df.columns]
        if key_cols:
            before = len(df)
            df = df.drop_duplicates(subset=key_cols, keep="last")
            print(f"🧹 {before - len(df)} doublons supprimés sur {key_cols}.")

        nb_lignes = len(df)

        # 🔸 Charger dans une table temporaire
        temp_table = f"{PROJECT_ID}.{DATASET_ID}._Temp_TblEmplacement"
        target_table = f"{PROJECT_ID}.{DATASET_ID}.TblEmplacement"

        print("⏳ Chargement du DataFrame dans la table temporaire...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, temp_table, job_config=job_config).result()
        print(f"✅ Table temporaire chargée ({nb_lignes} lignes).")

        # =========================================================
        # ⚙️ MERGE conditionnel (mise à jour sélective et typée)
        # =========================================================
        merge_query = f"""
            MERGE `{target_table}` AS T
            USING `{temp_table}` AS S
            ON T.Zone = S.Zone
            AND T.Allee = S.Allee
            AND T.Deplacement = S.Deplacement
            AND T.Niveau = S.Niveau

            WHEN MATCHED THEN
              UPDATE SET
                -- ✅ Mise à jour standard si valeur présente
                T.PoidsLimiteTotal   = COALESCE(S.PoidsLimiteTotal, T.PoidsLimiteTotal),
                T.Hauteur            = COALESCE(S.Hauteur, T.Hauteur),
                T.Largeur            = COALESCE(S.Largeur, T.Largeur),
                T.Profondeur         = COALESCE(S.Profondeur, T.Profondeur),

                -- ✅ Champs mis à jour uniquement si NON NULL et NON VIDE
                T.PoidsLimiteUnitaire = COALESCE(S.PoidsLimiteUnitaire, T.PoidsLimiteUnitaire),
                T.X = COALESCE(S.X, T.X),
                T.Y = COALESCE(S.Y, T.Y),
                T.Z = COALESCE(S.Z, T.Z),

                -- ✅ Conversion explicite en STRING pour éviter les conflits de type
                T.Type1 = IFNULL(NULLIF(CAST(S.Type1 AS STRING), ''), T.Type1),
                T.Type2 = IFNULL(NULLIF(CAST(S.Type2 AS STRING), ''), T.Type2),
                T.Type3 = IFNULL(NULLIF(CAST(S.Type3 AS STRING), ''), T.Type3),

                T.Palette = COALESCE(S.Palette, T.Palette)

            WHEN NOT MATCHED BY TARGET THEN
              INSERT (
                Zone, Allee, Deplacement, Niveau,
                PoidsLimiteTotal, Hauteur, Largeur, Profondeur,
                PoidsLimiteUnitaire, X, Y, Z,
                Type1, Type2, Type3, Palette
              )
              VALUES (
                S.Zone, S.Allee, S.Deplacement, S.Niveau,
                S.PoidsLimiteTotal, S.Hauteur, S.Largeur, S.Profondeur,
                S.PoidsLimiteUnitaire, S.X, S.Y, S.Z,
                CAST(S.Type1 AS STRING), CAST(S.Type2 AS STRING), CAST(S.Type3 AS STRING), S.Palette
              );
        """

        print("⚙️ Exécution du MERGE conditionnel...")
        client.query(merge_query).result()
        print("✅ MERGE exécuté avec succès.")

        # 🔸 Suppression de la table temporaire
        client.delete_table(temp_table, not_found_ok=True)
        print("🧹 Table temporaire supprimée.")

        # =========================================================
        # ✅ Mise à jour du log TblHistoriqueImport
        # =========================================================
        query_update = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.TblHistoriqueImport`
            SET Resultat = 'Succès',
                DetailErreur = NULL,
                NombreLignes = @nb_lignes
            WHERE NomFichier = @fichier
              AND Resultat = 'En cours (thread)'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("nb_lignes", "INT64", nb_lignes),
                bigquery.ScalarQueryParameter("fichier", "STRING", filename),
            ]
        )
        client.query(query_update, job_config=job_config).result()
        print("🟢 Log mis à jour avec succès.")

    except Exception as e:
        print(f"❌ Erreur dans sync_tbl_emplacement_background : {e}")
        query_err = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.TblHistoriqueImport`
            SET Resultat = 'Erreur',
                DetailErreur = @err
            WHERE NomFichier = @fichier
              AND Resultat = 'En cours (thread)'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("err", "STRING", str(e)),
                bigquery.ScalarQueryParameter("fichier", "STRING", filename),
            ]
        )
        client.query(query_err, job_config=job_config).result()


# ==========================
# IMPORT MANUEL
# ==========================
@app.route('/parametres/import', methods=['GET', 'POST'])
def param_import():
    _ = get_flashed_messages()

    try:
        table_names = get_active_tables()
    except Exception as e:
        table_names = []
        flash(f"Erreur récupération tables actives : {e}", "danger")

    selected_table = None
    preview = None

    if request.method == 'POST':
        selected_table = request.form.get('table')
        file = request.files.get('file')

        if not selected_table or selected_table not in table_names:
            flash("❌ Table inconnue ou non autorisée", "danger")
            return render_template("param_import.html", table_names=table_names)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
            file.save(save_path)

            try:
                import numpy as np
                import pandas as pd

                # ==============================
                # 📂 Lecture CSV : gestion fiable des encodages Excel
                # ==============================
                if filename.endswith('.csv'):
                    df = None
                    encodings_to_try = ['utf-16', 'utf-8-sig', 'cp1252', 'latin1']
                    last_err = None

                    for enc in encodings_to_try:
                        try:
                            df_try = pd.read_csv(save_path, sep=';', encoding=enc, engine='python', on_bad_lines='skip')
                            if len(df_try.columns) == 1:
                                df_try = pd.read_csv(save_path, sep=',', encoding=enc, engine='python', on_bad_lines='skip')
                            if len(df_try.columns) > 1:
                                df = df_try
                                used_enc = enc
                                break
                        except Exception as e:
                            last_err = e
                            continue

                    if df is None:
                        flash(f"❌ Impossible de lire le CSV (dernier essai : {last_err})", "danger")
                        return render_template("param_import.html", table_names=table_names, selected_table=selected_table)

                    flash(f"✅ Lecture réussie avec encodage '{used_enc}'", "info")

                elif filename.endswith(('.xls', '.xlsx')):
                    df = pd.read_excel(save_path)
                elif filename.endswith('.txt'):
                    df = pd.read_csv(save_path, sep='\t', encoding='utf-8-sig', on_bad_lines='skip')
                else:
                    flash("❌ Format non supporté", "danger")
                    return render_template("param_import.html", table_names=table_names)

                # ==============================
                # 🧹 Nettoyage colonnes
                # ==============================
                df.columns = (
                    pd.Index(df.columns)
                    .astype(str)
                    .str.strip()
                    .str.replace('[^0-9a-zA-Z_]', '_', regex=True)
                    .str.replace('_{2,}', '_', regex=True)
                    .str.strip('_')
                )

                # ✅ Schéma BigQuery
                table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{selected_table}")
                known_fields = [field.name for field in table.schema]
                numeric_fields = [f.name for f in table.schema if f.field_type in ["INTEGER", "FLOAT", "NUMERIC"]]
                keep_cols = [c for c in df.columns if c in known_fields]
                if not keep_cols:
                    flash("❌ Aucune colonne du fichier ne correspond au schéma BigQuery.", "danger")
                    preview = df.head().to_html(classes="table table-striped")
                    return render_template("param_import.html", table_names=table_names, selected_table=selected_table, preview=preview)
                df = df[keep_cols]

                for col in numeric_fields:
                    if col in df.columns:
                        df[col] = df[col].replace('', np.nan)
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                for col in df.select_dtypes(include=["object"]).columns:
                    df[col] = df[col].fillna("").astype(str)

                nb_lignes = len(df)
                if nb_lignes == 0:
                    flash("❌ Aucune ligne à importer après nettoyage.", "danger")
                    preview = df.head().to_html(classes="table table-striped")
                    return render_template("param_import.html", table_names=table_names, selected_table=selected_table, preview=preview)

                print("==== APERÇU DU DATAFRAME AVANT ENVOI ====")
                print("Shape:", df.shape)
                print("Colonnes:", list(df.columns))
                print(df.head(5).to_string())
                print("==========================================")

                # ======================================================
                # ⚙️ CAS SPÉCIAL : TblEmplacement
                # ======================================================
                if selected_table == "TblEmplacement":
                    flash("⏳ Synchronisation de TblEmplacement en cours...", "info")
                    thread = threading.Thread(
                        target=sync_tbl_emplacement_background,
                        args=(df, client, PROJECT_ID, DATASET_ID, filename),
                        daemon=True
                    )
                    thread.start()
                    preview = df.head().to_html(classes="table table-striped")
                    resultat_log = "En cours (thread)"
                    detail_log = "Synchronisation asynchrone démarrée."
                else:
                    table_id = f"{PROJECT_ID}.{DATASET_ID}.{selected_table}"
                    client.load_table_from_dataframe(
                        df, table_id, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                    ).result()
                    flash(f"✅ Données importées dans {selected_table} ({nb_lignes} lignes)", "success")
                    preview = df.head().to_html(classes="table table-striped")
                    resultat_log = "Succès"
                    detail_log = None

                # ✅ Historique import
                query_log = f"""
                    INSERT INTO {PROJECT_ID}.{DATASET_ID}.TblHistoriqueImport
                    (NomTable, DateHeure, Utilisateur, Resultat, DetailErreur, NombreLignes, NomFichier)
                    VALUES (@table, CURRENT_TIMESTAMP(), @user, @resultat, @detail, @nb_lignes, @fichier)
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("table", "STRING", selected_table),
                        bigquery.ScalarQueryParameter("user", "STRING", current_user),
                        bigquery.ScalarQueryParameter("resultat", "STRING", resultat_log),
                        bigquery.ScalarQueryParameter("detail", "STRING", detail_log),
                        bigquery.ScalarQueryParameter("nb_lignes", "INT64", nb_lignes),
                        bigquery.ScalarQueryParameter("fichier", "STRING", filename),
                    ]
                )
                client.query(query_log, job_config=job_config).result()

            except Exception as e:
                flash(f"❌ Erreur import : {e}", "danger")
                return render_template("param_import.html", table_names=table_names, selected_table=selected_table)

    return render_template("param_import.html", table_names=table_names, selected_table=selected_table, preview=preview)




# ==========================
# HISTORIQUE DES IMPORTS
# ==========================
@app.route("/parametres/hist_import")
def historique_imports():
    try:
        query = f"""
            SELECT
                NomTable,
                DateHeure,
                FORMAT_TIMESTAMP('%d/%m/%Y %H:%M:%S', DateHeure, 'Europe/Paris') AS DateHeureFr,
                Utilisateur,
                Resultat,
                DetailErreur,
                NombreLignes,
                NomFichier
            FROM `{PROJECT_ID}.{DATASET_ID}.TblHistoriqueImport`
            ORDER BY DateHeure DESC
        """
        df = client.query(query).to_dataframe()

        return render_template(
            "param_hist_import.html",
            data=df.head(1000).to_dict(orient="records")
        )

    except Exception as e:
        flash(f"❌ Erreur chargement historique : {e}", "danger")
        return render_template("param_hist_import.html", data=[])


# ==========================
# EXPORTS / SCHEMAS / DONNÉES
# ==========================
@app.route("/export_schema/<table_name>/<format>")
def export_schema(table_name, format):
    query = f"""
        SELECT column_name, data_type
        FROM {PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}'
    """
    df = client.query(query).to_dataframe()

    if format == "excel":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Trame")
        output.seek(0)
        return send_file(
            output,
            as_attachment=True,
            download_name=f"trame_{table_name}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    elif format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, sep=";")
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename=trame_{table_name}.csv"},
        )

    else:
        return "Format non supporté", 400


@app.route("/export_data/<table_name>/<format>")
def export_data(table_name, format):
    query = f"SELECT * FROM {PROJECT_ID}.{DATASET_ID}.{table_name} LIMIT 100000"
    df = client.query(query).to_dataframe()

    if format == "excel":
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Données")
        output.seek(0)
        return send_file(
            output,
            as_attachment=True,
            download_name=f"donnees_{table_name}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    elif format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, sep=";")
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename=donnees_{table_name}.csv"},
        )

    else:
        return "Format non supporté", 400


# ==========================
# ENREGISTREMENT BLUEPRINT DETAIL_EMPLACEMENT
# ==========================
#app.register_blueprint(bp_detail_emplacement)


# ==========================
# ROUTE PRINCIPALE EMPLACEMENTS
# ==========================
@app.route('/parametres/emplacements')
def emplacements():
    return redirect(url_for('detail_emplacement.page_detail_emplacement'))


# ==========================================================
# 📌 ROUTES BOUCHONS (pages provisoires / en construction)
# ==========================================================
@app.route('/gains_reels')
def gains_reels():
    return render_template("stub.html", title="💰 Gains réels obtenus")


@app.route('/ecart_previsionnel')
def ecart_previsionnel():
    return render_template("stub.html", title="📊 Ecart prévisionnel vs réel")


@app.route('/deplacements_a_realiser')
def deplacements_a_realiser():
    return render_template("stub.html", title="🚚 Déplacements à réaliser")


@app.route('/planif_deplacements')
def planif_deplacements():
    return render_template("stub.html", title="📅 Planification des déplacements")


@app.route('/histo_deplacements')
def histo_deplacements():
    return render_template("stub.html", title="📜 Historique des déplacements")


@app.route('/suivi_appro')
def suivi_appro():
    return render_template("stub.html", title="📦 Suivi des approvisionnements")


@app.route('/anomalie_dimensions')
def anomalie_dimensions():
    return render_template("stub.html", title="📏 Anomalie dimensions")


@app.route('/anomalie_cheminement')
def anomalie_cheminement():
    return render_template("stub.html", title="🧭 Anomalie cheminement")


@app.route('/anomalie_qte_picking')
def anomalie_qte_picking():
    return render_template("stub.html", title="📦 Anomalie quantité au picking > qté max")


@app.route('/anomalie_picking_hors_circuit')
def anomalie_picking_hors_circuit():
    return render_template("stub.html", title="🚫 Anomalie picking hors circuit")


@app.route('/anomalie_qte_mini')
def anomalie_qte_mini():
    return render_template("stub.html", title="📉 Anomalie Qté mini erronée")


@app.route('/anomalie_qte_maxi')
def anomalie_qte_maxi():
    return render_template("stub.html", title="📈 Anomalie Qté maxi erronée")


@app.route('/positionnement_nouveaux_produits')
def positionnement_nouveaux_produits():
    return render_template("stub.html", title="🆕 Positionnement des nouveaux produits")


@app.route('/reservation_pickings_vides')
def reservation_pickings_vides():
    return render_template("stub.html", title="📍 Réservation des pickings vides")


@app.route('/parametres/imports_auto')
def imports_auto():
    return render_template("stub.html", title="⚙️ Imports automatisés")


@app.route('/parametres/bornage_circuits')
def bornage_circuits():
    return render_template("stub.html", title="🛣️ Bornage et règles des circuits")


@app.route('/parametres/productivite')
def productivite():
    return render_template("stub.html", title="📈 Productivité")


@app.route('/parametres/emplacements_modif_masse')
def emplacements_modif_masse():
    return render_template("stub.html", title="✏️ Modification en masse")


@app.route('/parametres/emplacements_modif_fichier')
def emplacements_modif_fichier():
    return render_template("stub.html", title="📤 Chargement de fichier")


@app.route('/parametres/emplacements_modif_manuelle')
def emplacements_modif_manuelle():
    return render_template("stub.html", title="🧰 Modification manuelle")


@app.route('/parametres/ia')
def ia():
    return render_template("stub.html", title="🤖 Intelligence artificielle")


@app.route('/parametres/rapports_mail')
def rapports_mail():
    return render_template("stub.html", title="📧 Rapports mail")


@app.route('/parametres/validation_transferts')
def validation_transferts():
    return render_template("stub.html", title="✅ Validation des transferts")


@app.route('/parametres/lancement_calculs')
def lancement_calculs():
    return render_template("stub.html", title="🧮 Lancement des calculs")


@app.route('/param_scenario')
def param_scenario():
    return render_template("stub.html", title="📑 Scénario")


@app.route('/param_projet')
def param_projet():
    return render_template("stub.html", title="📁 Projet")





# ============================================================
# 🏷️ TYPES D’EMPLACEMENT — Nouvelle version (DataTable + API)
# ============================================================

# --- Affichage de la page ---
@app.route("/parametres/types_emplacement")
def parametres_types_emplacement():
    """Affiche la page moderne Types d’emplacement"""
    return render_template("types_emplacement.html", title="🏷️ Types d’emplacement")


# --- API : Récupération de toutes les lignes ---
@app.route("/api/types_emplacement_data")
def api_types_emplacement_data():
    try:
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.TblTypeEmpla123` ORDER BY Type1, Type2, Type3"
        df = client.query(query).to_dataframe()
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        print("❌ Erreur api_types_emplacement_data:", e)
        return jsonify({"status": "error", "message": str(e)}), 500


# --- API : Récupération d’un type particulier ---
@app.route("/api/types_emplacement_get")
def api_types_emplacement_get():
    try:
        type_ = request.args.get("type")
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.TblTypeEmpla123`
            WHERE Type1 = @type
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("type", "STRING", type_)]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            return jsonify({"error": "not found"}), 404
        return jsonify(df.iloc[0].to_dict())
    except Exception as e:
        print("❌ Erreur api_types_emplacement_get:", e)
        return jsonify({"status": "error", "message": str(e)}), 500


# --- API : Ajout / Mise à jour d’un type ---
@app.route("/api/types_emplacement_add", methods=["POST"])
def api_types_emplacement_add():
    try:
        data = request.get_json() or {}

        t1 = (data.get("type") or "").strip()
        t2 = (data.get("designation") or "").strip()
        t3 = (data.get("longueur") or "").strip()

        if not t1:
            return jsonify({"status": "error", "message": "Le champ Type1 est obligatoire."}), 400

        TABLE = f"{PROJECT_ID}.{DATASET_ID}.TblTypeEmpla123"

        # 🔍 Vérifier si la combinaison existe déjà
        check_sql = f"""
            SELECT COUNT(*) AS n
            FROM `{TABLE}`
            WHERE Type1=@t1 AND Type2=@t2 AND Type3=@t3
        """
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("t1", "STRING", t1),
                bigquery.ScalarQueryParameter("t2", "STRING", t2),
                bigquery.ScalarQueryParameter("t3", "STRING", t3),
            ]
        )
        exists = list(client.query(check_sql, job_config=job_cfg))[0].n > 0

        if exists:
            return jsonify({
                "status": "error",
                "message": f"⚠️ Ce type d’emplacement ({t1}, {t2}, {t3}) existe déjà."
            }), 400

        # 🟢 Insertion si nouveau
        insert_sql = f"""
            INSERT INTO `{TABLE}` (Type1, Type2, Type3)
            VALUES (@t1, @t2, @t3)
        """
        client.query(insert_sql, job_config=job_cfg).result()

        return jsonify({
            "status": "success",
            "message": f"✅ Type d’emplacement ajouté : ({t1}, {t2}, {t3})."
        })

    except Exception as e:
        print("❌ Erreur api_types_emplacement_add:", e)
        return jsonify({"status": "error", "message": str(e)}), 500


# --- API : Suppression d’un type ---
@app.route("/api/types_emplacement_delete", methods=["DELETE"])
def api_types_emplacement_delete():
    try:
        data = request.get_json()
        type_ = data.get("type")

        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.TblTypeEmpla123`
            WHERE Type1 = '{type_}'
        """
        client.query(delete_query).result()

        return jsonify({"status": "success", "message": "🗑 Type supprimé."})
    except Exception as e:
        print("❌ Erreur api_types_emplacement_delete:", e)
        return jsonify({"status": "error", "message": str(e)}), 500
# ==========================================================
# 🔗 GROUPES DE CIRCUITS (page + APIs)
# ==========================================================
from google.cloud import bigquery

@app.route('/parametres/groupes_circuit')
def groupes_circuit():
    """Page principale Groupes de circuits (vue DataTables + modale création)"""
    return render_template("groupes_circuit.html", title="🔗 Groupes de circuits")

@app.route('/api/groupes_circuit/data', methods=['GET'])
def api_groupes_circuit_data():
    """
    Retourne la liste des groupes et leurs circuits:
    [
      {"GroupeCircuit":"SEC_01","DesignationGroupeCircuit":"Picking sec","Circuits":["CIR_A","CIR_B"]},
      ...
    ]
    """
    client = bigquery.Client(project=PROJECT_ID)
    TABLE = f"{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit"

    q = f"""
        SELECT
          GroupeCircuit,
          ANY_VALUE(DesignationGroupeCircuit) AS DesignationGroupeCircuit,
          ARRAY_AGG(Circuit ORDER BY Circuit) AS Circuits
        FROM `{TABLE}`
        GROUP BY GroupeCircuit
        ORDER BY GroupeCircuit
    """
    rows = [dict(r) for r in client.query(q).result()]
    # Convertir l'array BigQuery en vrai list Python
    for r in rows:
        r["Circuits"] = list(r.get("Circuits") or [])
    return jsonify(rows)

@app.route('/api/groupes_circuit/circuits_options', methods=['GET'])
def api_groupes_circuit_circuits_options():
    """
    Renvoie la liste des circuits disponibles pour affectation:
    - Tous les circuits distincts de TblPicking.Circuit
    - EXCLUANT ceux déjà attribués dans TblGroupeCircuit
    """
    client = bigquery.Client(project=PROJECT_ID)
    T_PICK = f"{PROJECT_ID}.{DATASET_ID}.TblPicking"
    T_GRP  = f"{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit"

    q = f"""
        WITH all_c AS (
          SELECT DISTINCT TRIM(Circuit) AS Circuit FROM `{T_PICK}` WHERE Circuit IS NOT NULL
        ),
        used_c AS (
          SELECT DISTINCT TRIM(Circuit) AS Circuit FROM `{T_GRP}` WHERE Circuit IS NOT NULL
        )
        SELECT a.Circuit
        FROM all_c a
        LEFT JOIN used_c u ON a.Circuit = u.Circuit
        WHERE a.Circuit IS NOT NULL AND a.Circuit != "" AND u.Circuit IS NULL
        ORDER BY a.Circuit
    """
    rows = [r.Circuit for r in client.query(q).result()]
    return jsonify({"circuits": rows})

@app.route('/api/groupes_circuit/add', methods=['POST'])
def api_groupes_circuit_add():
    """Ajoute ou met à jour un groupe de circuits"""
    data = request.get_json()
    groupe = data.get("groupe", "").strip()
    designation = data.get("designation", "").strip()
    circuits = data.get("circuits", [])

    if not groupe or not circuits:
        return jsonify({"status": "error", "message": "Nom de groupe et circuits requis."}), 400

    try:
        # Vérifier si le groupe existe déjà
        check_group_query = f"""
            SELECT COUNT(*) AS nb FROM `{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit`
            WHERE GroupeCircuit = @groupe
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("groupe", "STRING", groupe)]
        )
        res = list(client.query(check_group_query, job_config=job_config))
        group_exists = res[0].nb > 0 if res else False

        # Récupérer les circuits déjà affectés à un autre groupe
        circuit_list = "', '".join(circuits)
        check_conflict_query = f"""
            SELECT Circuit, GroupeCircuit
            FROM `{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit`
            WHERE Circuit IN ('{circuit_list}')
              AND GroupeCircuit != @groupe
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("groupe", "STRING", groupe)]
        )
        conflicts = list(client.query(check_conflict_query, job_config=job_config))

        if conflicts:
            txt = ", ".join([f"{row.Circuit} (dans {row.GroupeCircuit})" for row in conflicts])
            return jsonify({
                "status": "error",
                "message": f"Certains circuits sont déjà attribués : {txt}"
            }), 400

        # ✅ Supprimer les anciens circuits du groupe avant de réinsérer
        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit`
            WHERE GroupeCircuit = @groupe
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("groupe", "STRING", groupe)]
        )
        client.query(delete_query, job_config=job_config).result()

        # ✅ Réinsérer les circuits du groupe (nouvelle sélection)
        values_clause = ",\n".join([
            f"""('{groupe.replace("'", "''")}', '{designation.replace("'", "''")}', '{c.replace("'", "''")}')"""
            for c in circuits
        ])
        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit`
            (GroupeCircuit, DesignationGroupeCircuit, Circuit)
            VALUES {values_clause}
        """
        client.query(insert_query).result()

        msg = "✅ Groupe mis à jour." if group_exists else "✅ Groupe créé."
        return jsonify({"status": "success", "message": msg}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/groupes_circuit/delete', methods=['DELETE'])
def api_groupes_circuit_delete():
    """
    Supprime un groupe complet (toutes ses lignes)
    Body JSON: { "groupe": "SEC_01" }
    """
    client = bigquery.Client(project=PROJECT_ID)
    TABLE = f"{PROJECT_ID}.{DATASET_ID}.TblGroupeCircuit"

    data = request.get_json(silent=True) or {}
    groupe = (data.get("groupe") or "").strip()
    if not groupe:
        return jsonify({"status": "error", "message": "Groupe manquant."}), 400

    q_del = f"DELETE FROM `{TABLE}` WHERE GroupeCircuit = @g"
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("g", "STRING", groupe)
    ])
    client.query(q_del, job_config=cfg).result()
    return jsonify({"status": "success", "message": f"🗑 Groupe « {groupe} » supprimé."}), 200

#-------------------------------------------------------------------
#   Vente exceptionnelle par ref
#-------------------------------------------------------------------

@app.route('/ventes_ref')
def ventes_ref():
    return render_template("ventes_exceptionnelles_ref.html", title="🔥 Ventes exceptionnelles par référence")

# ============================================================
# 📊 API – Données pour DataTables (Ventes exceptionnelles Réf)
# ============================================================
@app.route("/api/ventes_exceptionnelles_ref_data")
def api_ventes_exceptionnelles_ref_data():
    from decimal import Decimal
    import numpy as np

    try:
        query = f"""
            SELECT
                IDEvenementRef,
                Reference,
                CAST(Evolution AS FLOAT64) AS Evolution,
                CAST(Qte_en_plus AS INT64) AS Qte_en_plus,
                CAST(LignesPrepEnPlus AS INT64) AS LignesPrepEnPlus,
                FORMAT_DATE('%d/%m/%Y', DateDu) AS DateDu,
                FORMAT_DATE('%d/%m/%Y', DateAu) AS DateAu,
                COALESCE(TypeFlux, 'Tous') AS TypeFlux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef`
            ORDER BY IDEvenementRef DESC
        """
        df = client.query(query).to_dataframe()

        # 🔹 Convertir toutes les valeurs NaN / None / NaT → None
        df = df.replace({pd.NA: None, pd.NaT: None})
        df = df.where(pd.notnull(df), None)

        # 🔹 Conversion sécurisée vers types Python natifs
        def safe_convert(val):
            if isinstance(val, Decimal):
                return float(val)
            if isinstance(val, (np.integer, np.floating)):
                return val.item()
            if pd.isna(val):
                return None
            return val

        data = []
        for _, row in df.iterrows():
            data.append({
                "IDEvenementRef": safe_convert(row["IDEvenementRef"]),
                "Reference": safe_convert(row["Reference"]),
                "Evolution": safe_convert(row["Evolution"]),
                "Qte_en_plus": safe_convert(row["Qte_en_plus"]),
                "LignesPrepEnPlus": safe_convert(row["LignesPrepEnPlus"]),
                "DateDu": safe_convert(row["DateDu"]),
                "DateAu": safe_convert(row["DateAu"]),
                "TypeFlux": safe_convert(row["TypeFlux"])
            })

        print("📊 Données prêtes à envoyer :", data[:3])
        return jsonify(data)

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("❌ Erreur API ventes_exceptionnelles_ref_data :", e)
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 🔄 API – Options de sélection pour Ventes exceptionnelles Réf
# ============================================================
@app.route("/api/ventes_exceptionnelles_ref_add", methods=["POST"])
def api_ventes_exceptionnelles_ref_add():
    data = request.get_json()
    ref = data.get("Reference")
    evolution = data.get("Evolution")
    qte = data.get("Qte_en_plus")
    lignes = data.get("LignesPrepEnPlus") or 0
    date_du = data.get("DateDu")
    date_au = data.get("DateAu")
    typeflux = data.get("TypeFlux")

    # ✅ Exclusivité des champs
    if evolution and qte:
        return jsonify({"status":"error","message":"❌ Vous devez remplir soit Évolution%, soit Qté en plus, pas les deux."}),400

    if not evolution and not qte:
        return jsonify({"status":"error","message":"❌ Vous devez remplir au moins un des deux champs : Évolution% ou Qté en plus."}),400

    # ✅ Vérif référence existante
    check_ref_sql = f"SELECT COUNT(*) n FROM `{PROJECT_ID}.{DATASET_ID}.TblProduit` WHERE Reference=@r"
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("r","STRING",ref)])
    res = list(client.query(check_ref_sql, job_config=cfg))
    if res[0].n == 0:
        return jsonify({"status":"error","message":f"❌ La référence {ref} n’existe pas dans TblProduit."}),400

    # ✅ Calcule un nouvel ID
    next_id_query = f"SELECT COALESCE(MAX(IDEvenementRef), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef`"
    next_id = list(client.query(next_id_query))[0].next_id

    # ✅ Insertion
    insert_sql = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef`
        (IDEvenementRef, Reference, Evolution, Qte_en_plus, LignesPrepEnPlus, DateDu, DateAu, TypeFlux)
        VALUES (@id, @ref, @evol, @qte, @lignes, @du, @au, @flux)
    """
    job_cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id","INT64",next_id),
        bigquery.ScalarQueryParameter("ref","STRING",ref),
        bigquery.ScalarQueryParameter("evol","FLOAT",evolution),
        bigquery.ScalarQueryParameter("qte","INT64",qte),
        bigquery.ScalarQueryParameter("lignes","INT64",lignes),
        bigquery.ScalarQueryParameter("du","DATE",date_du),
        bigquery.ScalarQueryParameter("au","DATE",date_au),
        bigquery.ScalarQueryParameter("flux","STRING",typeflux)
    ])
    client.query(insert_sql, job_config=job_cfg).result()
    return jsonify({"status":"success","message":"✅ Événement enregistré avec succès."})


    # ===========================================================
    # ✅ Insertion sécurisée
    # ===========================================================
    insert_sql = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef`
        (Reference, Evolution, Qte_en_plus, LignesPrepEnPlus, DateDu, DateAu, TypeFlux)
        VALUES (@ref, @evol, @qte, @lignes, @du, @au, @flux)
    """

    def to_float(v):
        try:
            return float(v)
        except Exception:
            return None

    def to_int(v):
        try:
            return int(v)
        except Exception:
            return None

    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ref", "STRING", ref),
            bigquery.ScalarQueryParameter("evol", "FLOAT", to_float(evolution)),
            bigquery.ScalarQueryParameter("qte", "INT64", to_int(qte)),
            bigquery.ScalarQueryParameter("lignes", "INT64", to_int(lignes) or 0),
            bigquery.ScalarQueryParameter("du", "DATE", date_du),
            bigquery.ScalarQueryParameter("au", "DATE", date_au),
            bigquery.ScalarQueryParameter("flux", "STRING", typeflux),
        ]
    )
    client.query(insert_sql, job_config=job_cfg).result()

    return jsonify({"status": "success", "message": "✅ Événement enregistré avec succès."})

@app.route("/api/ventes_exceptionnelles_ref_delete", methods=["DELETE"])
def api_ventes_exceptionnelles_ref_delete():
    data = request.get_json()
    id_ = data.get("IDEvenementRef")
    if not id_:
        return jsonify({"status":"error","message":"❌ Identifiant manquant."}),400

    q = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef` WHERE IDEvenementRef=@id"
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("id","INT64",id_)
    ])
    client.query(q, job_config=cfg).result()
    return jsonify({"status":"success","message":"🗑 Événement supprimé."})


@app.route("/api/ventes_exceptionnelles_ref_options")
def api_ventes_exceptionnelles_ref_options():
    """Retourne uniquement la liste des TypeFlux disponibles (plus rapide)."""
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    T_HIST = f"{PROJECT_ID}.{DATASET_ID}.TblHistoriqueStockVente"

    flux_query = f"""
        SELECT DISTINCT TRIM(TypeFlux) AS tf
        FROM `{T_HIST}`
        WHERE TypeFlux IS NOT NULL AND TRIM(TypeFlux) <> ''
        ORDER BY tf
    """
    typeflux = [r.tf for r in client.query(flux_query).result()]
    return jsonify({"typeflux": typeflux})


# ============================================================
# ✏️ API – Mise à jour d’un événement existant
# ============================================================
@app.route("/api/ventes_exceptionnelles_ref_update", methods=["POST"])
def api_ventes_exceptionnelles_ref_update():
    try:
        data = request.get_json()
        id_ = data.get("IDEvenementRef")
        ref = data.get("Reference")
        evol = data.get("Evolution")
        qte = data.get("Qte_en_plus")
        lignes = data.get("LignesPrepEnPlus") or 0
        date_du = data.get("DateDu")
        date_au = data.get("DateAu")
        flux = data.get("TypeFlux")

        if not id_:
            return jsonify({"status": "error", "message": "ID manquant"}), 400

        # Exclusivité des champs
        if evol and qte:
            return jsonify({"status": "error", "message": "❌ Remplir soit Évolution%, soit Qté en plus, pas les deux."}), 400
        if not evol and not qte:
            return jsonify({"status": "error", "message": "❌ Vous devez renseigner au moins un des deux champs."}), 400

        # Vérification de la référence
        check_ref = f"SELECT COUNT(*) n FROM `{PROJECT_ID}.{DATASET_ID}.TblProduit` WHERE Reference=@r"
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("r","STRING",ref)])
        if list(client.query(check_ref, job_config=cfg))[0].n == 0:
            return jsonify({"status":"error","message":f"❌ La référence {ref} n’existe pas dans TblProduit."}),400

        # Mise à jour
        q = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef`
            SET
              Reference = @ref,
              Evolution = @evol,
              Qte_en_plus = @qte,
              LignesPrepEnPlus = @lignes,
              DateDu = @du,
              DateAu = @au,
              TypeFlux = @flux
            WHERE IDEvenementRef = @id
        """
        job_cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id","INT64",id_),
            bigquery.ScalarQueryParameter("ref","STRING",ref),
            bigquery.ScalarQueryParameter("evol","FLOAT",evol),
            bigquery.ScalarQueryParameter("qte","INT64",qte),
            bigquery.ScalarQueryParameter("lignes","INT64",lignes),
            bigquery.ScalarQueryParameter("du","DATE",date_du),
            bigquery.ScalarQueryParameter("au","DATE",date_au),
            bigquery.ScalarQueryParameter("flux","STRING",flux)
        ])
        client.query(q, job_config=job_cfg).result()

        return jsonify({"status":"success","message":"✅ Événement mis à jour avec succès."})

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"status":"error","message":f"Erreur serveur : {e}"}), 500

# ============================================================
# 🔍 API – Récupération d'une ligne de vente exceptionnelle par ID
# ============================================================
@app.route("/api/ventes_exceptionnelles_ref_get/<int:id>")
def api_ventes_exceptionnelles_ref_get(id):
    try:
        query = f"""
            SELECT 
                IDEvenementRef,
                Reference,
                Evolution,
                Qte_en_plus,
                LignesPrepEnPlus,
                FORMAT_DATE('%Y-%m-%d', DateDu) AS DateDu,
                FORMAT_DATE('%Y-%m-%d', DateAu) AS DateAu,
                TypeFlux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteRef`
            WHERE IDEvenementRef = @id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "INT64", id)
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()

        if df.empty:
            return jsonify({"status": "error", "message": f"Aucun événement trouvé pour ID {id}"}), 404

        return jsonify({"status": "success", "data": df.iloc[0].to_dict()})
    
    except Exception as e:
        print(f"❌ Erreur get vente ref: {e}")
        return jsonify({"status": "error", "message": f"Erreur serveur: {e}"}), 500


# ============================================================
# 🏭 VENTES EXCEPTIONNELLES PAR FOURNISSEUR
# ============================================================

@app.route("/ventes_fournisseur")
def ventes_fournisseur():
    return render_template("ventes_exceptionnelles_fournisseur.html", title="🏭 Ventes exceptionnelles par fournisseur")


# ============================================================
# 📊 API – Lecture des ventes exceptionnelles fournisseurs
# ============================================================
@app.route("/api/ventes_fournisseur_data")
def api_ventes_fournisseur_data():
    try:
        query = f"""
            SELECT 
                IDEvenementFournisseur,
                NFournisseur,
                NomFournisseur,
                Evolution,
                FORMAT_DATE('%d/%m/%Y', DateDu) AS DateDu,
                FORMAT_DATE('%d/%m/%Y', DateAu) AS DateAu,
                IFNULL(TypeFlux, 'Tous') AS TypeFlux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFournisseur`
            ORDER BY IDEvenementFournisseur DESC
        """
        df = client.query(query).to_dataframe()
        df = df.fillna("")
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        print(f"❌ Erreur API ventes_fournisseur_data : {e}")
        return jsonify([]), 500


# ============================================================
# 🆕 API – Ajout d’un événement fournisseur
# ============================================================
@app.route("/api/ventes_fournisseur_add", methods=["POST"])
def api_ventes_fournisseur_add():
    data = request.get_json()
    n_fournisseur = data.get("NFournisseur")
    nom_fournisseur = data.get("NomFournisseur")
    evolution = data.get("Evolution")
    date_du = data.get("DateDu")
    date_au = data.get("DateAu")
    typeflux = data.get("TypeFlux") or "Tous"

    # Validation
    if not n_fournisseur and not nom_fournisseur:
        return jsonify({"status": "error", "message": "❌ Le fournisseur est obligatoire (n° ou nom)."}), 400
    if not evolution:
        return jsonify({"status": "error", "message": "❌ Le champ Évolution est obligatoire."}), 400
    if not date_du or not date_au:
        return jsonify({"status": "error", "message": "❌ Les dates sont obligatoires."}), 400

    # 🔍 Vérif existence du fournisseur
    check_sql = f"""
        SELECT NFournisseur, NomFournisseur 
        FROM `{PROJECT_ID}.{DATASET_ID}.TblProduit`
        WHERE NFournisseur=@n OR LOWER(TRIM(NomFournisseur))=LOWER(TRIM(@nom))
        LIMIT 1
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("n", "STRING", n_fournisseur),
            bigquery.ScalarQueryParameter("nom", "STRING", nom_fournisseur)
        ]
    )
    res = list(client.query(check_sql, job_config=cfg))
    if not res:
        return jsonify({"status": "error", "message": "❌ Fournisseur introuvable dans TblProduit."}), 400

    fournisseur = res[0]
    n_fournisseur = fournisseur.NFournisseur
    nom_fournisseur = fournisseur.NomFournisseur

    # 🔢 Calcul d’un nouvel ID
    next_id_query = f"SELECT COALESCE(MAX(IDEvenementFournisseur), 0) + 1 AS next_id FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFournisseur`"
    next_id = list(client.query(next_id_query))[0].next_id

    # ✅ Insertion
    insert_sql = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFournisseur`
        (IDEvenementFournisseur, NFournisseur, NomFournisseur, Evolution, DateDu, DateAu, TypeFlux)
        VALUES (@id, @n, @nom, @evol, @du, @au, @flux)
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "INT64", next_id),
            bigquery.ScalarQueryParameter("n", "STRING", n_fournisseur),
            bigquery.ScalarQueryParameter("nom", "STRING", nom_fournisseur),
            bigquery.ScalarQueryParameter("evol", "FLOAT", evolution),
            bigquery.ScalarQueryParameter("du", "DATE", date_du),
            bigquery.ScalarQueryParameter("au", "DATE", date_au),
            bigquery.ScalarQueryParameter("flux", "STRING", typeflux)
        ]
    )
    client.query(insert_sql, job_config=cfg).result()
    return jsonify({"status": "success", "message": "✅ Vente exceptionnelle fournisseur enregistrée avec succès."})


# ============================================================
# ✏️ API – Récupération d’un événement pour édition
# ============================================================
@app.route("/api/ventes_fournisseur_get/<int:id>")
def api_ventes_fournisseur_get(id):
    try:
        query = f"""
            SELECT 
                IDEvenementFournisseur,
                NFournisseur,
                NomFournisseur,
                Evolution,
                FORMAT_DATE('%Y-%m-%d', DateDu) AS DateDu,
                FORMAT_DATE('%Y-%m-%d', DateAu) AS DateAu,
                IFNULL(TypeFlux, 'Tous') AS TypeFlux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFournisseur`
            WHERE IDEvenementFournisseur = @id
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "INT64", id)])
        df = client.query(query, job_config=cfg).to_dataframe()
        if df.empty:
            return jsonify({"status": "error", "message": "Aucune donnée trouvée."}), 404
        return jsonify({"status": "success", "data": df.iloc[0].to_dict()})
    except Exception as e:
        print(f"❌ Erreur get ventes fournisseur : {e}")
        return jsonify({"status": "error", "message": f"Erreur serveur : {e}"}), 500


# ============================================================
# 🔄 API – UPDATE Vente Fournisseur
# ============================================================
@app.route("/api/ventes_fournisseur_update", methods=["POST"])
def api_ventes_fournisseur_update():
    data = request.get_json()
    id_ = data.get("IDEvenementFournisseur")
    nfourn = data.get("NFournisseur")
    nomfourn = data.get("NomFournisseur")
    evol = data.get("Evolution")
    date_du = data.get("DateDu")
    date_au = data.get("DateAu")
    flux = data.get("TypeFlux")

    if not id_:
        return jsonify({"status": "error", "message": "❌ ID manquant."}), 400

    try:
        q = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFournisseur`
            SET NFournisseur = @nf, 
                NomFournisseur = @nom,
                Evolution = @evol,
                DateDu = @du,
                DateAu = @au,
                TypeFlux = @flux
            WHERE IDEvenementFournisseur = @id
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("nf", "STRING", nfourn),
            bigquery.ScalarQueryParameter("nom", "STRING", nomfourn),
            bigquery.ScalarQueryParameter("evol", "FLOAT", float(evol) if evol else None),
            bigquery.ScalarQueryParameter("du", "DATE", date_du),
            bigquery.ScalarQueryParameter("au", "DATE", date_au),
            bigquery.ScalarQueryParameter("flux", "STRING", flux),
            bigquery.ScalarQueryParameter("id", "INT64", int(id_)),
        ])
        client.query(q, job_config=cfg).result()
        return jsonify({"status": "success", "message": "✅ Vente fournisseur mise à jour."})
    except Exception as e:
        return jsonify({"status": "error", "message": f"❌ Erreur : {e}"}), 500


# ============================================================
# 🗑️ API – Suppression
# ============================================================
@app.route("/api/ventes_fournisseur_delete", methods=["DELETE"])
def api_ventes_fournisseur_delete():
    data = request.get_json()
    id_ = data.get("IDEvenementFournisseur")

    if not id_:
        return jsonify({"status": "error", "message": "❌ ID manquant."}), 400

    query = f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFournisseur` WHERE IDEvenementFournisseur=@id"
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "INT64", id_)])
    client.query(query, job_config=cfg).result()
    return jsonify({"status": "success", "message": "🗑️ Vente fournisseur supprimée."})


# ============================================================
# 🔄 API – Liste des TypeFlux disponibles
# ============================================================
@app.route("/api/ventes_fournisseur_options")
def api_ventes_fournisseur_options():
    T_HIST = f"{PROJECT_ID}.{DATASET_ID}.TblHistoriqueStockVente"
    query = f"""
        SELECT DISTINCT TRIM(TypeFlux) AS TypeFlux
        FROM `{T_HIST}`
        WHERE TypeFlux IS NOT NULL AND TRIM(TypeFlux) <> ''
        ORDER BY TypeFlux
    """
    typeflux = [r.TypeFlux for r in client.query(query).result()]
    return jsonify({"typeflux": typeflux})


# ============================================================
# 🔍 API – Recherche fournisseur (NFournisseur ↔ NomFournisseur)
# ============================================================
@app.route("/api/ventes_fournisseur_lookup")
def api_ventes_fournisseur_lookup():
    """Recherche fournisseur par numéro ou nom"""
    term = request.args.get("term", "").strip()
    if not term:
        return jsonify([])

    query = f"""
        SELECT DISTINCT NFournisseur, NomFournisseur
        FROM `{PROJECT_ID}.{DATASET_ID}.TblProduit`
        WHERE LOWER(TRIM(NFournisseur)) LIKE LOWER(TRIM(@t))
           OR LOWER(TRIM(NomFournisseur)) LIKE LOWER(TRIM(@t))
        LIMIT 10
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("t", "STRING", f"%{term}%")])
    df = client.query(query, job_config=cfg).to_dataframe()
    return jsonify(df.to_dict(orient="records"))

# =====================================================
# 🔹 API : VENTES PAR FAMILLE PRODUIT
# =====================================================

@app.route("/api/ventes_famille_data")
def api_ventes_famille_data():
    """Retourne toutes les ventes exceptionnelles par famille produit"""
    try:
        query = f"""
            SELECT IDEvenementFamilleProduit, FamilleDeProduit1, FamilleDeProduit2, FamilleDeProduit3,
                   Evolution, DateDu, DateAu, TypeFlux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
            ORDER BY IDEvenementFamilleProduit DESC
        """
        df = client.query(query).to_dataframe()
        data = df.to_dict(orient="records")
        return jsonify(data)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/ventes_famille_add", methods=["POST"])
def api_ventes_famille_add():
    data = request.get_json()

    try:
        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
            (IDEvenementFamilleProduit, FamilleDeProduit1, FamilleDeProduit2, FamilleDeProduit3, Evolution, DateDu, DateAu, TypeFlux)
            SELECT
              COALESCE(MAX(IDEvenementFamilleProduit), 0) + 1,
              @f1, @f2, @f3, @evol, @du, @au, @flux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("f1", "STRING", data.get("FamilleDeProduit1")),
                bigquery.ScalarQueryParameter("f2", "STRING", data.get("FamilleDeProduit2")),
                bigquery.ScalarQueryParameter("f3", "STRING", data.get("FamilleDeProduit3")),
                bigquery.ScalarQueryParameter("evol", "FLOAT64", float(data.get("Evolution") or 0)),
                bigquery.ScalarQueryParameter("du", "DATE", data.get("DateDu")),
                bigquery.ScalarQueryParameter("au", "DATE", data.get("DateAu")),
                bigquery.ScalarQueryParameter("flux", "STRING", data.get("TypeFlux") or "Tous"),
            ]
        )

        client.query(query, job_config=job_config).result()
        return jsonify({"status": "success", "message": "✅ Événement ajouté avec succès."})

    except Exception as e:
        print("❌ Erreur ajout famille :", e)
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/api/ventes_famille_update", methods=["POST"])
def api_ventes_famille_update():
    """Met à jour une ligne existante"""
    try:
        data = request.get_json()
        id_evt = data.get("IDEvenementFamilleProduit")
        if not id_evt:
            return jsonify({"status": "error", "message": "ID manquant"}), 400

        query = f"""
            UPDATE `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
            SET FamilleDeProduit1 = @fam1,
                FamilleDeProduit2 = @fam2,
                FamilleDeProduit3 = @fam3,
                Evolution = @evol,
                DateDu = @du,
                DateAu = @au,
                TypeFlux = @flux
            WHERE IDEvenementFamilleProduit = @id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("fam1", "STRING", data.get("FamilleDeProduit1")),
                bigquery.ScalarQueryParameter("fam2", "STRING", data.get("FamilleDeProduit2")),
                bigquery.ScalarQueryParameter("fam3", "STRING", data.get("FamilleDeProduit3")),
                bigquery.ScalarQueryParameter("evol", "FLOAT64", float(data.get("Evolution", 0))),
                bigquery.ScalarQueryParameter("du", "DATE", data.get("DateDu")),
                bigquery.ScalarQueryParameter("au", "DATE", data.get("DateAu")),
                bigquery.ScalarQueryParameter("flux", "STRING", data.get("TypeFlux") or "Tous"),
                bigquery.ScalarQueryParameter("id", "INT64", int(id_evt)),
            ]
        )
        client.query(query, job_config=job_config).result()

        return jsonify({"status": "success", "message": "✅ Vente par famille produit mise à jour."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/ventes_famille_delete", methods=["DELETE"])
def api_ventes_famille_delete():
    data = request.get_json()
    id_evt = data.get("IDEvenementFamilleProduit")

    try:
        if id_evt is None or id_evt == "":
            # 🔹 Cas particulier : supprimer les lignes sans ID
            query = f"""
                DELETE FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
                WHERE IDEvenementFamilleProduit IS NULL
            """
            client.query(query).result()
            return jsonify({"status": "success", "message": "🗑️ Lignes sans ID supprimées."})

        # 🔹 Cas normal : suppression par ID
        query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
            WHERE IDEvenementFamilleProduit = @id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "INT64", int(id_evt))
            ]
        )

        client.query(query, job_config=job_config).result()
        return jsonify({"status": "success", "message": f"🗑️ Événement #{id_evt} supprimé."})

    except Exception as e:
        print("❌ Erreur suppression famille produit :", e)
        return jsonify({"status": "error", "message": str(e)}), 400



@app.route("/api/ventes_famille_get/<int:id_evt>")
def api_ventes_famille_get(id_evt):
    try:
        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
            WHERE IDEvenementFamilleProduit = @id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("id", "INT64", id_evt)]
        )
        rows = list(client.query(query, job_config=job_config).result())

        if not rows:
            return jsonify({"status": "error", "message": "Événement introuvable."}), 404

        row = rows[0]

        # 🔹 Conversion propre des dates (pour le champ <input type="date">)
        date_du = row.DateDu.strftime("%Y-%m-%d") if row.DateDu else None
        date_au = row.DateAu.strftime("%Y-%m-%d") if row.DateAu else None

        data = {
            "IDEvenementFamilleProduit": row.IDEvenementFamilleProduit,
            "FamilleDeProduit1": row.FamilleDeProduit1,
            "FamilleDeProduit2": row.FamilleDeProduit2,
            "FamilleDeProduit3": row.FamilleDeProduit3,
            "Evolution": row.Evolution,
            "DateDu": date_du,
            "DateAu": date_au,
            "TypeFlux": row.TypeFlux,
        }

        return jsonify({"status": "success", "data": data})

    except Exception as e:
        print("❌ Erreur api_ventes_famille_get :", e)
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/api/ventes_famille_options")
def api_ventes_famille_options():
    """Retourne les options de TypeFlux distincts disponibles"""
    try:
        query = f"""
            SELECT DISTINCT TypeFlux
            FROM `{PROJECT_ID}.{DATASET_ID}.TblEvenementVenteFamilleProduit`
            WHERE TypeFlux IS NOT NULL AND TypeFlux <> ''
            ORDER BY TypeFlux
        """
        df = client.query(query).to_dataframe()
        typeflux = df["TypeFlux"].dropna().unique().tolist()
        return jsonify({"status": "success", "typeflux": typeflux})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/ventes/famille", endpoint="ventes_famille")
def page_ventes_famille():
    return render_template("ventes_famille_produit.html")

@app.route("/api/familles_options")
def api_familles_options():
    query = f"""
        SELECT DISTINCT FamilleDeProduit1, FamilleDeProduit2, FamilleDeProduit3
        FROM `{PROJECT_ID}.{DATASET_ID}.TblProduit`
    """
    rows = client.query(query).result()
    f1, f2, f3 = set(), set(), set()
    for r in rows:
        if r.FamilleDeProduit1: f1.add(r.FamilleDeProduit1)
        if r.FamilleDeProduit2: f2.add(r.FamilleDeProduit2)
        if r.FamilleDeProduit3: f3.add(r.FamilleDeProduit3)
    return jsonify({
        "famille1": sorted(f1),
        "famille2": sorted(f2),
        "famille3": sorted(f3)
    })


# ==========================
# LANCEMENT APP
# ==========================
if __name__ == "__main__":
    app.run(debug=True)
