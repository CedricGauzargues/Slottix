import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\cedri\Documents\Projet\Slotting Profiling\SlottixFlask\credentials_slottix.json"

import re
from flask import Blueprint, render_template, request, jsonify
from google.cloud import bigquery

bp_detail_emplacement = Blueprint("detail_emplacement", __name__)
client = bigquery.Client()
TABLE_ID = "slottix.entrepot_optimisation.TblEmplacement"

_num_regex = re.compile(r"^-?\d+(\.\d+)?$")


def _is_number(s: str) -> bool:
    return s is not None and s != "" and bool(_num_regex.match(str(s).strip()))


def _as_int_or_none(v):
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None


# ============================================================
# 🔍 FILTRES (corrigé pour coller à ton frontend)
# ============================================================
def _filters_from_args(args):
    """Lit les filtres envoyés par le frontend DataTables."""
    return {
        "zone": args.get("zone", ""),
        "allee": args.get("allee", ""),
        "deplacement_from": args.get("deplacement_from", ""),
        "deplacement_to": args.get("deplacement_to", ""),
        "niveau_from": args.get("niveau_from", ""),
        "niveau_to": args.get("niveau_to", ""),
        "type1": args.get("type1", ""),
        "type2": args.get("type2", ""),
        "type3": args.get("type3", ""),
        "pictogramme": args.get("pictogramme", ""),
        "search": args.get("search[value]", ""),  # recherche globale
    }


def _build_where_and_params(f):
    conds, params = [], []

    if f["zone"]:
        conds.append("LOWER(e.Zone) = LOWER(@zone)")
        params.append(bigquery.ScalarQueryParameter("zone", "STRING", f["zone"]))

    if _is_number(f["allee"]):
        conds.append("e.Allee = @allee")
        params.append(bigquery.ScalarQueryParameter("allee", "INT64", int(f["allee"])))

    dep_from, dep_to = _as_int_or_none(f["deplacement_from"]), _as_int_or_none(f["deplacement_to"])
    if dep_from is not None and dep_to is not None:
        conds.append("e.Deplacement BETWEEN @dep_from AND @dep_to")
        params.append(bigquery.ScalarQueryParameter("dep_from", "INT64", dep_from))
        params.append(bigquery.ScalarQueryParameter("dep_to", "INT64", dep_to))
    elif dep_from is not None:
        conds.append("e.Deplacement = @dep_from_eq")
        params.append(bigquery.ScalarQueryParameter("dep_from_eq", "INT64", dep_from))

    niv_from, niv_to = _as_int_or_none(f["niveau_from"]), _as_int_or_none(f["niveau_to"])
    if niv_from is not None and niv_to is not None:
        conds.append("e.Niveau BETWEEN @niv_from AND @niv_to")
        params.append(bigquery.ScalarQueryParameter("niv_from", "INT64", niv_from))
        params.append(bigquery.ScalarQueryParameter("niv_to", "INT64", niv_to))

    if f.get("type1"):
        conds.append("LOWER(e.Type1) = LOWER(@type1)")
        params.append(bigquery.ScalarQueryParameter("type1", "STRING", f["type1"]))
    if f.get("type2"):
        conds.append("LOWER(e.Type2) = LOWER(@type2)")
        params.append(bigquery.ScalarQueryParameter("type2", "STRING", f["type2"]))
    if f.get("type3"):
        conds.append("LOWER(e.Type3) = LOWER(@type3)")
        params.append(bigquery.ScalarQueryParameter("type3", "STRING", f["type3"]))
    if f.get("pictogramme"):
        conds.append("p.NomPicto = @pictogramme")
        params.append(bigquery.ScalarQueryParameter("pictogramme", "STRING", f["pictogramme"]))

    if f["search"]:
        search = f["search"].replace("'", "")
        conds.append(
            f"(CAST(e.Zone AS STRING) LIKE '%{search}%' OR "
            f"CAST(e.Allee AS STRING) LIKE '%{search}%' OR "
            f"CAST(e.Deplacement AS STRING) LIKE '%{search}%' OR "
            f"CAST(e.Niveau AS STRING) LIKE '%{search}%')"
        )

    return conds, params


# ============================================================
# 🧭 PAGE PRINCIPALE
# ============================================================
@bp_detail_emplacement.route("/detail_emplacement", methods=["GET"])
def page_detail_emplacement():
    return render_template("detail_emplacement.html", title="📦 Gestion des emplacements")


# ============================================================
# ⚡ DATA (AJAX DataTables — pagination serveur)
# ============================================================
@bp_detail_emplacement.route("/detail_emplacement/data", methods=["GET"])
def data_detail_emplacement():
    client = bigquery.Client()

    PROJECT_ID = "slottix"
    DATASET_ID = "entrepot_optimisation"
    TABLE_EMPLA = f"{PROJECT_ID}.{DATASET_ID}.TblEmplacement"
    TABLE_PICTO = f"{PROJECT_ID}.{DATASET_ID}.TblPictogramme"

    try:
        f = _filters_from_args(request.args)
        conds, params = _build_where_and_params(f)

        start = int(request.args.get("start", 0))
        length = int(request.args.get("length", 50))
        draw = int(request.args.get("draw", 1))

        # --- Tri automatique hiérarchique ---
        # (Zone, Allée, Déplacement, Niveau) → ordre logique d’affichage
        order_col_idx = request.args.get("order[0][column]")
        order_dir = request.args.get("order[0][dir]", "asc")

        # Toujours forcer l’ordre de tri global
        order_clause = "ORDER BY e.Zone ASC, e.Allee ASC, e.Deplacement ASC, e.Niveau ASC"


        where_sql = " WHERE " + " AND ".join(conds) if conds else ""

        query = f"""
SELECT 
  e.Zone, e.Allee, e.Deplacement, e.Niveau,
  e.Profondeur AS longueur, e.Largeur AS largeur, e.Hauteur AS hauteur,
  e.PoidsLimiteTotal, e.PoidsLimiteUnitaire,
  e.X, e.Y, e.Z,
  e.Type1, e.Type2, e.Type3,
  e.Palette
FROM `{TABLE_EMPLA}` AS e
{where_sql}
{order_clause}
LIMIT @length OFFSET @start
"""

        params += [
            bigquery.ScalarQueryParameter("length", "INT64", length),
            bigquery.ScalarQueryParameter("start", "INT64", start)
        ]

        job_cfg = bigquery.QueryJobConfig(query_parameters=params)
        job_cfg.use_query_cache = True
        job_cfg.maximum_bytes_billed = 10**9

        print("🔍 SQL exécuté:")
        print(query)
        print("🔸 Params:", [(p.name, getattr(p, "_value", None)) for p in params])

        rows_iter = client.query(query, job_config=job_cfg).result()
        rows = [dict(r) for r in rows_iter]

        total_query = f"SELECT COUNT(*) AS total FROM `{TABLE_EMPLA}`"
        total = list(client.query(total_query))[0].total

        return jsonify({
            "draw": draw,
            "recordsTotal": total,
            "recordsFiltered": total,
            "data": rows
        })

    except Exception as e:
        import traceback
        print("❌ ERREUR SERVEUR :", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ============================================================
# 🧩 API : LISTES (Type1 / Type2 / Type3 uniquement)
# ============================================================
@bp_detail_emplacement.route("/api/detail_emplacement/lists", methods=["GET"])
def api_detail_emplacement_lists():
    """Renvoie les listes hiérarchiques Type1 / Type2 / Type3."""
    try:
        client = bigquery.Client()
        PROJECT_ID = "slottix"
        DATASET_ID = "entrepot_optimisation"

        # 🔹 Récupération des types hiérarchiques
        q_types = f"""
            SELECT DISTINCT Type1, Type2, Type3
            FROM `{PROJECT_ID}.{DATASET_ID}.TblTypeEmpla123`
            WHERE Type1 IS NOT NULL
        """
        df_types = client.query(q_types).to_dataframe()

        # 🔹 Construction de la hiérarchie : { Type1: { Type2: [Type3...] } }
        types = {}
        for _, row in df_types.iterrows():
            t1, t2, t3 = row.get("Type1"), row.get("Type2"), row.get("Type3")
            if not t1:
                continue
            types.setdefault(t1, {}).setdefault(t2 or "", []).append(t3 or "")

        return jsonify({"types": types})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Erreur lors de la récupération des types : {e}"}), 500


# ============================================================
# 🧰 PAGE MODIFICATION EN MASSE
# ============================================================
@bp_detail_emplacement.route("/detail_emplacement_modif_masse", methods=["GET"])
def page_detail_emplacement_modif_masse():
    """Page de modification en masse des emplacements"""
    return render_template("detail_emplacement_modif_masse.html", title="🧰 Modification en masse")

@bp_detail_emplacement.route("/api/detail_emplacement/dimensions", methods=["POST"])
def api_detail_emplacement_dimensions():
    """Retourne les dimensions (profondeur, largeur, hauteur) des emplacements sélectionnés."""
    from google.cloud import bigquery
    client = bigquery.Client()

    PROJECT_ID = "slottix"
    DATASET_ID = "entrepot_optimisation"
    TABLE = f"{PROJECT_ID}.{DATASET_ID}.TblEmplacement"

    data = request.get_json(force=True)
    zone = (data.get("zone") or "").strip().upper()
    allee = data.get("allee")
    dep_from = data.get("dep_from")
    dep_to = data.get("dep_to")
    niv_from = data.get("niv_from")
    niv_to = data.get("niv_to")

    # ✅ Vérifie la présence, mais autorise 0
    if any(v is None or v == "" for v in [zone, allee, dep_from, dep_to, niv_from, niv_to]):
        return jsonify({"error": "Paramètres manquants"}), 400

    query = f"""
        SELECT 
          Zone,
          CAST(Allee AS INT64) AS Allee,
          CAST(Deplacement AS INT64) AS Deplacement,
          CAST(Niveau AS INT64) AS Niveau,
          SAFE_CAST(Profondeur AS FLOAT64) / 100.0 AS profondeur,
          SAFE_CAST(Largeur AS FLOAT64) / 100.0 AS largeur,
          SAFE_CAST(Hauteur AS FLOAT64) / 100.0 AS hauteur
        FROM `{TABLE}`
        WHERE UPPER(TRIM(Zone)) = @zone
          AND CAST(Allee AS INT64) = @allee
          AND CAST(Deplacement AS INT64) BETWEEN @dep_from AND @dep_to
          AND CAST(Niveau AS INT64) BETWEEN @niv_from AND @niv_to
        ORDER BY CAST(Deplacement AS INT64), CAST(Niveau AS INT64)
    """

    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("zone", "STRING", zone),
        bigquery.ScalarQueryParameter("allee", "INT64", int(allee)),
        bigquery.ScalarQueryParameter("dep_from", "INT64", int(dep_from)),
        bigquery.ScalarQueryParameter("dep_to", "INT64", int(dep_to)),
        bigquery.ScalarQueryParameter("niv_from", "INT64", int(niv_from)),
        bigquery.ScalarQueryParameter("niv_to", "INT64", int(niv_to)),
    ])

    try:
        rows = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        return jsonify({"error": f"Erreur BigQuery : {e}"}), 500

    if not rows:
        return jsonify({"error": "Aucune donnée trouvée pour cette sélection."}), 400

    dims = [
        {
            "Zone": r.Zone,
            "Allee": r.Allee,
            "Deplacement": r.Deplacement,
            "Niveau": r.Niveau,
            "profondeur": r.profondeur,
            "largeur": r.largeur,
            "hauteur": r.hauteur,
        }
        for r in rows
    ]

    return jsonify({"dimensions": dims})





# ============================================================
# ✅ API : Mise à jour en masse (X, Y, Z, Type1,2,3, Palette)
# ============================================================
@bp_detail_emplacement.route("/api/detail_emplacement/update_coords", methods=["POST"])
def api_update_coords():
    """
    Met à jour en masse les champs dans TblEmplacement :
    - X, Y, Z (si saisis)
    - Type1 / Type2 / Type3 (si saisis)
    - Palette (si cochée)
    - PoidsLimiteUnitaire (si saisi)
    ⚡ Seuls les champs saisis sont mis à jour.
    """
    from google.cloud import bigquery
    client = bigquery.Client()

    PROJECT_ID = "slottix"
    DATASET_ID = "entrepot_optimisation"
    TABLE = f"{PROJECT_ID}.{DATASET_ID}.TblEmplacement"

    def to_float_or_null(v):
        """Convertit un nombre avec virgule éventuelle, sinon None"""
        if v is None:
            return None
        s = str(v).strip().replace(",", ".")
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None

    def to_int(v):
        try:
            return int(v)
        except Exception:
            return None

    def to_str_or_null(v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s != "" else None

    try:
        data = request.get_json(force=True)
        coords = data.get("coords", [])
        type1 = to_str_or_null(data.get("type1"))
        type2 = to_str_or_null(data.get("type2"))
        type3 = to_str_or_null(data.get("type3"))
        poids_limite_unitaire = to_float_or_null(data.get("poids_limite_unitaire"))
        palette_val = data.get("palette")

        if not coords:
            return jsonify({"status": "error", "message": "Aucune donnée reçue."}), 400

        # Palette : booléen -> TRUE/FALSE/NULL
        if palette_val is None or palette_val == "":
            palette_sql = "CAST(NULL AS BOOL)"
        else:
            palette_sql = "TRUE" if str(palette_val).lower() in ("1", "true", "yes", "on") else "FALSE"

        struct_rows = []
        for c in coords:
            zone = str(c.get("Zone", "")).strip().replace("'", "''")
            allee = to_int(c.get("Allee"))
            dep = to_int(c.get("Deplacement"))
            niv = to_int(c.get("Niveau"))

            x = to_float_or_null(c.get("X"))
            y = to_float_or_null(c.get("Y"))
            z = to_float_or_null(c.get("Z"))

            def num_sql(v):
                return str(v) if v is not None else "CAST(NULL AS FLOAT64)"

            struct_rows.append(
                f"STRUCT('{zone}' AS Zone, {allee} AS Allee, {dep} AS Deplacement, {niv} AS Niveau, "
                f"{num_sql(x)} AS X, {num_sql(y)} AS Y, {num_sql(z)} AS Z)"
            )

        structs_clause = ",\n          ".join(struct_rows)

        # Construction dynamique du SET (on ne met à jour que ce qui est saisi)
        set_parts = []

        has_xyz = any(c.get("X") not in (None, "") for c in coords)
        if has_xyz:
            set_parts += ["T.X = COALESCE(N.X, T.X)",
                          "T.Y = COALESCE(N.Y, T.Y)",
                          "T.Z = COALESCE(N.Z, T.Z)"]

        if poids_limite_unitaire is not None:
            set_parts.append(f"T.PoidsLimiteUnitaire = {poids_limite_unitaire}")

        if type1:
            set_parts.append(f"T.Type1 = '{type1.replace("'", "''")}'")
        if type2:
            set_parts.append(f"T.Type2 = '{type2.replace("'", "''")}'")
        if type3:
            set_parts.append(f"T.Type3 = '{type3.replace("'", "''")}'")
        if palette_val is not None:
            set_parts.append(f"T.Palette = {palette_sql}")

        if not set_parts:
            return jsonify({"status": "error", "message": "Aucun champ à mettre à jour."}), 400

        set_clause = ",\n          ".join(set_parts)

        q = f"""
        UPDATE `{TABLE}` AS T
        SET 
          {set_clause}
        FROM UNNEST([
          {structs_clause}
        ]) AS N
        WHERE 
          LOWER(TRIM(T.Zone)) = LOWER(TRIM(N.Zone))
          AND T.Allee = N.Allee
          AND T.Deplacement = N.Deplacement
          AND T.Niveau = N.Niveau
        """

        print("🧾 SQL UPDATE (modif masse):")
        print(q)

        job = client.query(q)
        job.result()

        return jsonify({"status": "success", "message": f"✅ {len(coords)} emplacement(s) mis à jour avec succès."})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Erreur serveur : {e}"}), 500




# ============================================================
# ✅ API : Mise à jour en masse (X, Y, Z, PoidsLimiteUnitaire, Type1/2/3, Palette)
# ============================================================
@bp_detail_emplacement.route("/api/detail_emplacement/update", methods=["POST"])
def api_update_detail_emplacement():
    """
    Met à jour en masse TblEmplacement :
    - X, Y, Z, PoidsLimiteUnitaire (accepte virgules FR)
    - Type1 / Type2 / Type3
    - Palette (BOOL)
    ⚡ Tout en une seule requête BigQuery.
    🔒 Champs vides -> on n'écrase pas : COALESCE(N.val, T.val)
    """
    from google.cloud import bigquery
    client = bigquery.Client()

    PROJECT_ID = "slottix"
    DATASET_ID = "entrepot_optimisation"
    TABLE = f"{PROJECT_ID}.{DATASET_ID}.TblEmplacement"

    def to_float_or_null(v):
        """'12,3' -> 12.3 ; ''/None/texte invalide -> None"""
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    def to_int(v, default=0):
        try:
            return int(v)
        except Exception:
            return default

    def to_str_or_null(v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s != "" else None

    try:
        data = request.get_json(force=True)
        changes = data.get("changes", [])

        if not changes:
            return jsonify({"status": "error", "message": "Aucune donnée reçue."}), 400

        # Construire les STRUCT typés (avec NULL CASTés) pour UNNEST
        struct_rows = []
        for c in changes:
            zone = str(c.get("Zone", "")).strip().replace("'", "''")
            allee = to_int(c.get("Allee"))
            dep   = to_int(c.get("Deplacement"))
            niv   = to_int(c.get("Niveau"))

            x  = to_float_or_null(c.get("X"))
            y  = to_float_or_null(c.get("Y"))
            z  = to_float_or_null(c.get("Z"))
            pdu = to_float_or_null(c.get("PoidsLimiteUnitaire"))

            t1 = to_str_or_null(c.get("Type1"))
            t2 = to_str_or_null(c.get("Type2"))
            t3 = to_str_or_null(c.get("Type3"))

            # Palette : bool → TRUE/FALSE/NULL, typé explicitement en BOOL
            pal_val = c.get("Palette", None)
            if pal_val is None:
                pal_sql = "CAST(NULL AS BOOL)"
            elif str(pal_val).lower() in ("1", "true", "yes", "on"):
                pal_sql = "CAST(TRUE AS BOOL)"
            else:
                pal_sql = "CAST(FALSE AS BOOL)"


            def num_sql(v):
                return str(v) if v is not None else "CAST(NULL AS FLOAT64)"

            def str_sql(v):
                return "'" + v.replace("'", "''") + "'" if v is not None else "CAST(NULL AS STRING)"

            struct_rows.append(
                "STRUCT("
                f"'{zone}' AS Zone, "
                f"{allee} AS Allee, "
                f"{dep} AS Deplacement, "
                f"{niv} AS Niveau, "
                f"{num_sql(x)} AS X, "
                f"{num_sql(y)} AS Y, "
                f"{num_sql(z)} AS Z, "
                f"{num_sql(pdu)} AS PoidsLimiteUnitaire, "
                f"{str_sql(t1)} AS Type1, "
                f"{str_sql(t2)} AS Type2, "
                f"{str_sql(t3)} AS Type3, "
                f"{pal_sql} AS Palette)"
            )

        structs_clause = ",\n          ".join(struct_rows)

        # Requête unique UPDATE ... FROM UNNEST([...]) avec COALESCE (ne pas écraser si NULL)
        q = f"""
        UPDATE `{TABLE}` AS T
        SET 
          T.X = COALESCE(N.X, T.X),
          T.Y = COALESCE(N.Y, T.Y),
          T.Z = COALESCE(N.Z, T.Z),
          T.PoidsLimiteUnitaire = COALESCE(N.PoidsLimiteUnitaire, T.PoidsLimiteUnitaire),
          T.Type1 = COALESCE(N.Type1, T.Type1),
          T.Type2 = COALESCE(N.Type2, T.Type2),
          T.Type3 = COALESCE(N.Type3, T.Type3),
          T.Palette = COALESCE(CAST(N.Palette AS BOOL), T.Palette)

        FROM UNNEST([
          {structs_clause}
        ]) AS N
        WHERE 
          LOWER(TRIM(T.Zone)) = LOWER(TRIM(N.Zone))
          AND T.Allee = N.Allee
          AND T.Deplacement = N.Deplacement
          AND T.Niveau = N.Niveau
        """

        # Debug utile (visible dans ta console Flask)
        print("🧾 UPDATE envoyé à BigQuery:")
        print(q)

        job = client.query(q)
        job.result()  # on attend la fin

        return jsonify({"status": "success", "message": f"✅ {len(changes)} emplacements mis à jour avec succès."})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": f"Erreur serveur : {e}"}), 500

