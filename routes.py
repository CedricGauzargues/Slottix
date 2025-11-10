from flask import Blueprint, jsonify, request, render_template
import uuid
import math
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

# 🔐 Import de la gestion du pool PostgreSQL
from db import get_pg_connection, release_pg_connection


bp_routes = Blueprint("routes", __name__)


# 🧭 PAGE
@bp_routes.route("/parametres/localisation_routes")
def page_localisation_routes():
    return render_template("routes.html", title="🧭 Gestion des routes")


# =============================================================
# 📋 Liste des zones, allées, emplacements, et types d'engins
# =============================================================
@bp_routes.route("/api/routes/lists")
def api_lists():
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Emplacements
        cur.execute("""
            SELECT Zone, Allee, Deplacement, Niveau, X, Y, Z
            FROM TblEmplacement
            WHERE Zone IS NOT NULL AND Allee IS NOT NULL
        """)
        emps = cur.fetchall()

        emplacements = []
        for r in emps:
            emplacements.append({
                "Zone": r["zone"],
                "Allee": int(r["allee"]),
                "Deplacement": int(r["deplacement"]),
                "Niveau": int(r["niveau"]),
                "X": float(r["x"] or 0),
                "Y": float(r["y"] or 0),
                "Z": float(r["z"] or 0),
                "label": f"{r['zone']}-{str(r['allee']).zfill(3)}-{str(r['deplacement']).zfill(4)}-{str(r['niveau']).zfill(2)}"
            })

        zones = sorted(list({r["Zone"] for r in emplacements}))
        allees = sorted(list({r["Allee"] for r in emplacements}))

        # Engins
        cur.execute("SELECT TypeEngin, VitesseKmH FROM TblEngin ORDER BY TypeEngin")
        engins = cur.fetchall()

        return jsonify({
            "zones": zones,
            "allees": allees,
            "emplacements": emplacements,
            "engins": engins
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            release_pg_connection(conn)


# =============================================================
# 🛠️ CRUD : ROUTES PRINCIPALES (TblRouteSimple)
# =============================================================

@bp_routes.route("/api/routes/simple", methods=["GET"])
def get_routes_simple():
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT IdRoute, NomRoute, ZoneDepart, ZoneArrivee,
                   AlleeGauche, AlleeDroite,
                   DeplacementDeb, NiveauDeb, DeplacementFin, NiveauFin,
                   XDeb, YDeb, ZDeb, XFin, YFin, ZFin,
                   LargeurAllee, TypeEngin, SensUnique, COALESCE(SensDirection, 'croissant') AS SensDirection
            FROM TblRouteSimple
            ORDER BY NomRoute
        """)
        rows = cur.fetchall()
        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            release_pg_connection(conn)


@bp_routes.route("/api/routes/simple", methods=["POST"])
def add_route_simple():
    data = request.get_json()
    if not data or "NomRoute" not in data:
        return jsonify({"status": "error", "message": "NomRoute manquant"}), 400

    emp_deb = data.get("EmpDeb")
    emp_fin = data.get("EmpFin")

    def parse_emp(emp):
        if not emp:
            return None
        parts = emp.split("-")
        if len(parts) != 4:
            return None
        return {
            "Zone": parts[0],
            "Allee": int(parts[1]),
            "Deplacement": int(parts[2]),
            "Niveau": int(parts[3])
        }

    emp1 = parse_emp(emp_deb)
    emp2 = parse_emp(emp_fin)

    # Coordonnées
    XDeb, YDeb, ZDeb = data.get("XDeb"), data.get("YDeb"), data.get("ZDeb")
    XFin, YFin, ZFin = data.get("XFin"), data.get("YFin"), data.get("ZFin")

    largeur_allee = data.get("LargeurAllee")
    sens_unique = bool(data.get("SensUnique", False))
    sens_direction = data.get("SensDirection")
    type_engin = data.get("TypeEngin")

    IdRoute = str(uuid.uuid4())[:8]

    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO TblRouteSimple (
                IdRoute, NomRoute, ZoneDepart, ZoneArrivee, AlleeGauche, AlleeDroite,
                DeplacementDeb, NiveauDeb, DeplacementFin, NiveauFin,
                XDeb, YDeb, ZDeb, XFin, YFin, ZFin,
                LargeurAllee, TypeEngin, SensUnique, SensDirection
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            IdRoute, data["NomRoute"],
            emp1["Zone"] if emp1 else None,
            emp2["Zone"] if emp2 else None,
            emp1["Allee"] if emp1 else None,
            emp2["Allee"] if emp2 else None,
            emp1["Deplacement"] if emp1 else None,
            emp1["Niveau"] if emp1 else None,
            emp2["Deplacement"] if emp2 else None,
            emp2["Niveau"] if emp2 else None,
            XDeb, YDeb, ZDeb, XFin, YFin, ZFin,
            largeur_allee, type_engin, sens_unique, sens_direction
        ))

        conn.commit()

        # Appel création routes secondaires
        _create_routes_secondaires(IdRoute, emp1, emp2, largeur_allee, type_engin, sens_unique, sens_direction)

        return jsonify({"status": "success", "message": "Route ajoutée"})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        if conn:
            cur.close()
            release_pg_connection(conn)


@bp_routes.route("/api/routes/simple/<id_route>", methods=["PUT"])
def update_route_simple(id_route):
    data = request.get_json()
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        updates, values = [], []
        for field in ["NomRoute", "ZoneDepart", "ZoneArrivee", "AlleeGauche", "AlleeDroite",
                      "DeplacementDeb", "NiveauDeb", "DeplacementFin", "NiveauFin",
                      "XDeb", "YDeb", "ZDeb", "XFin", "YFin", "ZFin",
                      "LargeurAllee", "TypeEngin", "SensUnique", "SensDirection"]:
            if field in data:
                updates.append(f"{field}=%s")
                values.append(data[field])

        if not updates:
            return jsonify({"status": "error", "message": "Aucune donnée à mettre à jour"}), 400

        values.append(id_route)
        cur.execute(f"UPDATE TblRouteSimple SET {', '.join(updates)} WHERE IdRoute=%s", values)
        conn.commit()

        return jsonify({"status": "success", "message": "Route mise à jour"})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        if conn:
            cur.close()
            release_pg_connection(conn)


@bp_routes.route("/api/routes/simple/<id>", methods=["DELETE"])
def delete_route_simple(id):
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM TblRouteSimple WHERE IdRoute=%s", (id,))
        conn.commit()
        return jsonify({"message": "✅ Route supprimée"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            release_pg_connection(conn)


# ===============================================================
# 📍 Génération automatique des routes secondaires
# ===============================================================
def _create_routes_secondaires(id_principale, emp1, emp2, largeur, type_engin, sens_unique, sens_direction):
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT Zone, Allee, Deplacement, Niveau, X, Y, Z
            FROM TblEmplacement
            WHERE Zone IN (%s, %s)
        """, (emp1["Zone"], emp2["Zone"]))
        emps = cur.fetchall()

        if not emps:
            print("⚠️ Aucun emplacement trouvé pour les zones concernées.")
            return

        df = pd.DataFrame(emps).sort_values(["zone", "allee", "deplacement"])

        routes = []
        for (zone, allee, niveau), grp in df.groupby(["zone", "allee", "niveau"]):
            grp = grp.sort_values("deplacement")

            for i in range(len(grp) - 1):
                e1, e2 = grp.iloc[i], grp.iloc[i + 1]
                routes.append((
                    str(uuid.uuid4())[:10], id_principale, "parallele",
                    zone, int(allee),
                    "pair" if int(e1.deplacement) % 2 == 0 else "impair",
                    f"{zone}-{allee:03d}-{e1.deplacement:04d}-{e1.niveau:02d}",
                    f"{zone}-{allee:03d}-{e2.deplacement:04d}-{e2.niveau:02d}",
                    e1.x, e1.y, e1.z, e2.x, e2.y, e2.z,
                    largeur, type_engin, sens_unique, sens_direction
                ))

            for _, e in grp.iterrows():
                routes.append((
                    str(uuid.uuid4())[:10], id_principale, "perpendiculaire",
                    zone, int(allee),
                    "pair" if int(e.deplacement) % 2 == 0 else "impair",
                    f"{zone}-{allee:03d}-{e.deplacement:04d}-{e.niveau:02d}",
                    None, e.x, e.y, e.z, e.x + largeur / 2.0, e.y, e.z,
                    largeur, type_engin, sens_unique, sens_direction
                ))

        cur.executemany("""
            INSERT INTO TblRouteSecondaire (
                IdRouteSecondaire, IdRoutePrincipale, TypeRoute, Zone, Allee, Cote,
                EmpSource, EmpCible, XDeb, YDeb, ZDeb, XFin, YFin, ZFin,
                Largeur, TypeEngin, SensUnique, SensDirection
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, routes)
        conn.commit()
        print(f"✅ {len(routes)} routes secondaires créées.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Erreur génération routes secondaires :", e)

    finally:
        if conn:
            cur.close()
            release_pg_connection(conn)
