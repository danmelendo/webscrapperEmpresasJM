import json
import mysql.connector
import tkinter as tk
from tkinter import ttk, messagebox
import smtplib
import queue
import threading
import ssl
import random
import time
from email.message import EmailMessage
import os
import sys
from pathlib import Path
import datetime as dt
from dotenv import load_dotenv

# ---------------- CONFIG ----------------
def _get_base_dir():
    # When bundled (PyInstaller), __file__ points inside the temp bundle.
    # Use the folder that contains the executable so `.env` and state live next to it.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = _get_base_dir()
load_dotenv(BASE_DIR / ".env")


def env_int(key, default):
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": env_int("DB_PORT", 3306),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset": os.getenv("DB_CHARSET", "utf8mb4"),
}

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = env_int("SMTP_PORT", 587)
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

WARMUP_STATE_PATH = BASE_DIR / "warmup_state.json"
RECONTACT_STATE_PATH = BASE_DIR / "recontact_state.json"
# Límite diario progresivo (día 0..N desde el primer envío registrado)
WARMUP_DAILY_SCHEDULE = [10, 20, 30, 40, 60, 80, 100, 120, 150]
WARMUP_HOURLY_LIMIT = 15
WARMUP_DELAY_BETWEEN_EMAILS_SECONDS = (25.0, 75.0)  # jitter humano
WARMUP_LONG_PAUSE_EVERY = 5
WARMUP_LONG_PAUSE_SECONDS = (120.0, 300.0)

ESTADOS_DESCRIPCION = {
    "EN": "Enviado",
    "ER": "Error",
    "PE": "Pendiente",
}

PLANTILLA_EMAIL = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>JMOrdenadores</title>
</head>
<body style="margin:0; padding:0; background-color:#f4f6f8; font-family: Arial, Helvetica, sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8; padding:20px;">
  <tr>
    <td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.05);">
        
        <tr>
          <td style="background-color:#0b3c5d; padding:20px; text-align:center;">
            <img src="https://jmordenadores.com/assets/logoblue-DnLYxD6_.png"
                 alt="JMOrdenadores"
                 style="max-width:220px;">
          </td>
        </tr>

        <tr>
          <td style="padding:30px; color:#333333; font-size:15px; line-height:1.6;">

            <p>{saludo}</p>

            <p>
              Nos ponemos en contacto tras revisar vuestra actividad como 
              {tipo_empresa} en {localidad}.
            </p>

            <p>
              En <strong>JMOrdenadores</strong> ofrecemos soluciones informáticas
              específicas para empresas en Madrid,
              ayudando a mejorar el rendimiento, la seguridad y la estabilidad
              de sus sistemas.
            </p>

            <p style="margin:20px 0;">
              <strong>Queremos que nos conozcáis sin compromiso:</strong>
            </p>

            <ul style="padding-left:20px;">
              <li><strong>Primera visita totalmente gratuita</strong></li>
              <li><strong>Resolución de la primera incidencia sin coste</strong></li>
              <li><strong>Concertación de cita telefonica para adaptar el servicio a su medida</strong></li>
            </ul>

            <p>Además, ofrecemos:</p>

            <ul style="padding-left:20px;">
              <li>Soporte informático cercano y profesional</li>
              <li>Venta y configuración de equipos y dispositivos</li>
              <li>Servidores de almacenamiento y copias de seguridad</li>
              <li>Consultoría en ciberseguridad</li>
              <li>Planes de mantenimiento con cuotas mensuales</li>
              <li>Precios muy competitivos</li>
            </ul>

            <p>
              Nuestro objetivo es que negocios como <strong>{empresa}</strong>
              puedan centrarse en su actividad mientras nosotros nos ocupamos
              de que la infraestructura informática funcione sin problemas.
            </p>

            <p style="text-align:center; margin:30px 0;">
              <a href="https://jmordenadores.com"
                 style="background-color:#0b3c5d; color:#ffffff; text-decoration:none; padding:12px 25px; border-radius:5px; display:inline-block;">
                Visitar nuestra web
              </a>
            </p>

            <p>
              No duden en contactarnos, estaremos encantados de valorar vuestra situación sin ningún compromiso.
            </p>

            <p style="margin-top:30px;">
              Un saludo,<br>
              <strong>José Miguel</strong><br>
              JMOrdenadores
            </p>

          </td>
        </tr>

        <tr>
          <td style="background-color:#f0f0f0; padding:15px; text-align:center; font-size:12px; color:#777;">
            © JMOrdenadores · Soporte informático profesional<br>
            <a href="https://jmordenadores.com" style="color:#0b3c5d; text-decoration:none;">
              jmordenadores.com
            </a>
          </td>
        </tr>

      </table>
    </td>
  </tr>
</table>
</body>
</html>
"""


# ---------------- DB ----------------
INVALID_EMAIL_VALUES = ("no disponible", "", "none", "null")
INVALID_PHONE_VALUES = ("no disponible", "", "none", "null")


def conectar_db():
    return mysql.connector.connect(**DB_CONFIG)


def obtener_empresas():
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id_empresa, nombre FROM empresa ORDER BY nombre")
    empresas = cursor.fetchall()
    cursor.close()
    conn.close()
    return empresas


def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _email_norm_sql(alias, column_email):
    # LOWER(TRIM(...)) handles basic normalization; NULL stays NULL.
    return f"LOWER(TRIM({alias}.{column_email}))"


def resumen_limpieza_emails():
    """
    Devuelve contadores para:
    - invalid_emails: emails NULL o placeholders tipo 'no disponible'
    - duplicate_rows_to_delete: filas duplicadas que se eliminarian conservando la menor PK
    - duplicate_groups: grupos duplicados detectados
    """
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        columnas_email = _obtener_columnas_tabla(cursor, "email")
        col_pk = _primera_columna_existente(columnas_email, ["id_email", "id"])
        col_email = _primera_columna_existente(columnas_email, ["email"])
        col_empresa = _primera_columna_existente(columnas_email, ["id_empresa"])
        col_tipo = _primera_columna_existente(columnas_email, ["id_tipo_email", "tipo_email", "tipo"])

        if not (col_pk and col_email and col_empresa):
            raise RuntimeError(f"Esquema email no soportado: columnas={sorted(columnas_email)}")

        norm1 = _email_norm_sql("e1", col_email)
        norm2 = _email_norm_sql("e2", col_email)

        invalid_list = ",".join(["%s"] * len(INVALID_EMAIL_VALUES))
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM email
            WHERE {col_email} IS NULL
               OR LOWER(TRIM({col_email})) IN ({invalid_list})
            """,
            INVALID_EMAIL_VALUES,
        )
        invalid_emails = int(cursor.fetchone()[0] or 0)

        join_cond = f"{norm1} = {norm2} AND e1.{col_empresa} = e2.{col_empresa}"
        group_cols = [f"{col_empresa}", f"{norm1}"]
        if col_tipo:
            join_cond += f" AND e1.{col_tipo} = e2.{col_tipo}"
            group_cols.insert(1, col_tipo)

        # Filtramos invalidos para no contar duplicados de basura.
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM email e1
            JOIN email e2
              ON {join_cond}
             AND e1.{col_pk} > e2.{col_pk}
            WHERE e1.{col_email} IS NOT NULL
              AND e2.{col_email} IS NOT NULL
              AND {norm1} NOT IN ({invalid_list})
              AND {norm2} NOT IN ({invalid_list})
            """,
            INVALID_EMAIL_VALUES * 2,
        )
        duplicate_rows_to_delete = int(cursor.fetchone()[0] or 0)

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT 1
                FROM email e1
                WHERE e1.{col_email} IS NOT NULL
                  AND {norm1} NOT IN ({invalid_list})
                GROUP BY {", ".join(group_cols)}
                HAVING COUNT(*) > 1
            ) t
            """,
            INVALID_EMAIL_VALUES,
        )
        duplicate_groups = int(cursor.fetchone()[0] or 0)

        return {
            "invalid_emails": invalid_emails,
            "duplicate_rows_to_delete": duplicate_rows_to_delete,
            "duplicate_groups": duplicate_groups,
        }
    finally:
        cursor.close()
        conn.close()


def deduplicar_emails(aplicar=False, eliminar_invalidos=False):
    """
    Deduplica emails conservando la menor PK por (email_normalizado, id_empresa, id_tipo_email si existe).
    - aplicar=False: no borra, solo devuelve resumen.
    - eliminar_invalidos=True: borra emails NULL o placeholders.
    """
    stats_before = resumen_limpieza_emails()
    if not aplicar and not eliminar_invalidos:
        return stats_before

    conn = conectar_db()
    cursor = conn.cursor()
    try:
        columnas_email = _obtener_columnas_tabla(cursor, "email")
        col_pk = _primera_columna_existente(columnas_email, ["id_email", "id"])
        col_email = _primera_columna_existente(columnas_email, ["email"])
        col_empresa = _primera_columna_existente(columnas_email, ["id_empresa"])
        col_tipo = _primera_columna_existente(columnas_email, ["id_tipo_email", "tipo_email", "tipo"])

        norm1 = _email_norm_sql("e1", col_email)
        norm2 = _email_norm_sql("e2", col_email)
        invalid_list = ",".join(["%s"] * len(INVALID_EMAIL_VALUES))

        if eliminar_invalidos:
            cursor.execute(
                f"""
                DELETE FROM email
                WHERE {col_email} IS NULL
                   OR LOWER(TRIM({col_email})) IN ({invalid_list})
                """,
                INVALID_EMAIL_VALUES,
            )

        if aplicar:
            join_cond = f"{norm1} = {norm2} AND e1.{col_empresa} = e2.{col_empresa}"
            if col_tipo:
                join_cond += f" AND e1.{col_tipo} = e2.{col_tipo}"

            cursor.execute(
                f"""
                DELETE e1
                FROM email e1
                JOIN email e2
                  ON {join_cond}
                 AND e1.{col_pk} > e2.{col_pk}
                WHERE e1.{col_email} IS NOT NULL
                  AND e2.{col_email} IS NOT NULL
                  AND {norm1} NOT IN ({invalid_list})
                  AND {norm2} NOT IN ({invalid_list})
                """,
                INVALID_EMAIL_VALUES * 2,
            )

        conn.commit()
        stats_after = resumen_limpieza_emails()
        return {"before": stats_before, "after": stats_after}
    finally:
        cursor.close()
        conn.close()


def listar_empresas_sin_emails_validos(solo_sin_telefono=True):
    """
    Devuelve empresas que no tienen emails validos (tras filtrar placeholders).
    Por seguridad, por defecto solo devuelve las que tambien estan sin telefono util.
    """
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    try:
        invalid_list = ",".join(["%s"] * len(INVALID_EMAIL_VALUES))
        sql = f"""
            SELECT em.id_empresa, em.nombre, em.telefono, em.web
            FROM empresa em
            LEFT JOIN email e
              ON e.id_empresa = em.id_empresa
             AND e.email IS NOT NULL
             AND LOWER(TRIM(e.email)) NOT IN ({invalid_list})
            WHERE e.id_empresa IS NULL
        """
        params = list(INVALID_EMAIL_VALUES)

        if solo_sin_telefono:
            invalid_phone_list = ",".join(["%s"] * len(INVALID_PHONE_VALUES))
            sql += f"""
              AND (
                    em.telefono IS NULL
                 OR TRIM(em.telefono) = ''
                 OR LOWER(TRIM(em.telefono)) IN ({invalid_phone_list})
              )
            """
            params.extend(INVALID_PHONE_VALUES)

        sql += " ORDER BY em.nombre"
        cursor.execute(sql, params)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def eliminar_empresas(ids_empresas):
    """
    Elimina empresas y sus dependencias basicas.
    Seguridad: espera una lista de ids (no auto-selecciona).
    """
    if not ids_empresas:
        return {"deleted_empresas": 0}

    conn = conectar_db()
    cursor = conn.cursor()
    try:
        columnas_email = _obtener_columnas_tabla(cursor, "email")
        col_pk_email = _primera_columna_existente(columnas_email, ["id_email", "id"])
        col_empresa_en_email = _primera_columna_existente(columnas_email, ["id_empresa"])

        ids_email = []
        if col_pk_email and col_empresa_en_email:
            for chunk in _chunked(ids_empresas, 500):
                formato = ",".join(["%s"] * len(chunk))
                cursor.execute(
                    f"SELECT {col_pk_email} FROM email WHERE {col_empresa_en_email} IN ({formato})",
                    chunk,
                )
                ids_email.extend([r[0] for r in cursor.fetchall() if r and r[0] is not None])

        # email_estado (si existe) referencia id_email
        if _tabla_existe(cursor, "email_estado") and ids_email:
            for chunk in _chunked(ids_email, 500):
                formato = ",".join(["%s"] * len(chunk))
                cursor.execute(f"DELETE FROM email_estado WHERE id_email IN ({formato})", chunk)

        # estado_email puede referenciar id_email o id_empresa, dependiendo del esquema
        if _tabla_existe(cursor, "estado_email"):
            columnas_estado = _obtener_columnas_tabla(cursor, "estado_email")
            col_id_email = _primera_columna_existente(columnas_estado, ["id_email"])
            col_id_empresa = _primera_columna_existente(columnas_estado, ["id_empresa"])
            if col_id_email and ids_email:
                for chunk in _chunked(ids_email, 500):
                    formato = ",".join(["%s"] * len(chunk))
                    cursor.execute(f"DELETE FROM estado_email WHERE {col_id_email} IN ({formato})", chunk)
            if col_id_empresa:
                for chunk in _chunked(ids_empresas, 500):
                    formato = ",".join(["%s"] * len(chunk))
                    cursor.execute(f"DELETE FROM estado_email WHERE {col_id_empresa} IN ({formato})", chunk)

        # email
        if col_empresa_en_email:
            for chunk in _chunked(ids_empresas, 500):
                formato = ",".join(["%s"] * len(chunk))
                cursor.execute(f"DELETE FROM email WHERE {col_empresa_en_email} IN ({formato})", chunk)

        # busqueda_empresa
        if _tabla_existe(cursor, "busqueda_empresa"):
            for chunk in _chunked(ids_empresas, 500):
                formato = ",".join(["%s"] * len(chunk))
                cursor.execute(f"DELETE FROM busqueda_empresa WHERE id_empresa IN ({formato})", chunk)

        # empresa
        deleted_empresas = 0
        for chunk in _chunked(ids_empresas, 500):
            formato = ",".join(["%s"] * len(chunk))
            cursor.execute(f"DELETE FROM empresa WHERE id_empresa IN ({formato})", chunk)
            deleted_empresas += int(cursor.rowcount or 0)

        conn.commit()
        return {"deleted_empresas": deleted_empresas}
    finally:
        cursor.close()
        conn.close()


def buscar_redundancias_email_nombre_empresa():
    """
    Busca casos donde el mismo email (normalizado) aparece asociado a empresas con el mismo nombre (normalizado),
    pero con distinto id_empresa. Esto suele indicar empresas duplicadas.
    Devuelve una lista de dicts con: email_norm, nombre_norm, ids_empresas (lista), ejemplo_email, ejemplo_nombre.
    """
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    try:
        invalid_list = ",".join(["%s"] * len(INVALID_EMAIL_VALUES))
        cursor.execute(
            f"""
            SELECT
                LOWER(TRIM(e.email)) AS email_norm,
                LOWER(TRIM(em.nombre)) AS nombre_norm,
                GROUP_CONCAT(DISTINCT em.id_empresa ORDER BY em.id_empresa SEPARATOR ',') AS ids_empresas,
                MIN(e.email) AS ejemplo_email,
                MIN(em.nombre) AS ejemplo_nombre,
                COUNT(DISTINCT em.id_empresa) AS n_empresas
            FROM email e
            JOIN empresa em ON em.id_empresa = e.id_empresa
            WHERE e.email IS NOT NULL
              AND LOWER(TRIM(e.email)) NOT IN ({invalid_list})
              AND em.nombre IS NOT NULL
              AND TRIM(em.nombre) <> ''
            GROUP BY LOWER(TRIM(e.email)), LOWER(TRIM(em.nombre))
            HAVING COUNT(DISTINCT em.id_empresa) > 1
            ORDER BY n_empresas DESC, nombre_norm, email_norm
            """,
            INVALID_EMAIL_VALUES,
        )
        rows = cursor.fetchall() or []
        out = []
        for r in rows:
            ids_raw = (r.get("ids_empresas") or "").strip()
            ids = [x.strip() for x in ids_raw.split(",") if x.strip()]
            out.append(
                {
                    "email_norm": r.get("email_norm") or "",
                    "nombre_norm": r.get("nombre_norm") or "",
                    "ids_empresas": ids,
                    "ejemplo_email": r.get("ejemplo_email") or "",
                    "ejemplo_nombre": r.get("ejemplo_nombre") or "",
                }
            )
        return out
    finally:
        cursor.close()
        conn.close()


def fusionar_empresas(ids_empresas, prefer_id=None):
    """
    Consolida varias empresas en una:
    - Mueve emails / busqueda_empresa / estado_email a la empresa canonical.
    - Intenta completar campos vacios en empresa canonical con datos de duplicadas.
    - Elimina empresas duplicadas al final.
    """
    ids_empresas = [i for i in (ids_empresas or []) if i]
    if len(ids_empresas) < 2:
        return {"merged": 0}

    canonical = prefer_id if prefer_id in ids_empresas else sorted(ids_empresas)[0]
    duplicates = [i for i in ids_empresas if i != canonical]

    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    try:
        # Completar campos de empresa canonical si estan vacios.
        columnas_empresa = _obtener_columnas_tabla(cursor, "empresa")
        candidate_cols = ["telefono", "web", "direccion", "codigo_postal", "localidad"]
        cols = [c for c in candidate_cols if c in columnas_empresa]
        if cols:
            formato = ",".join(["%s"] * len(ids_empresas))
            cursor.execute(
                f"SELECT id_empresa, {', '.join(cols)} FROM empresa WHERE id_empresa IN ({formato})",
                ids_empresas,
            )
            rows = cursor.fetchall() or []
            by_id = {r.get("id_empresa"): r for r in rows}
            can_row = by_id.get(canonical, {}) or {}

            updates = {}
            for col in cols:
                can_val = can_row.get(col)
                can_ok = can_val is not None and str(can_val).strip() != "" and str(can_val).strip().lower() not in INVALID_PHONE_VALUES
                if can_ok:
                    continue
                for d in duplicates:
                    dv = (by_id.get(d, {}) or {}).get(col)
                    if dv is None:
                        continue
                    dvs = str(dv).strip()
                    if not dvs:
                        continue
                    if col == "telefono" and dvs.lower() in INVALID_PHONE_VALUES:
                        continue
                    updates[col] = dv
                    break

            if updates:
                set_sql = ", ".join([f"{k}=%s" for k in updates.keys()])
                params = list(updates.values()) + [canonical]
                cursor.execute(f"UPDATE empresa SET {set_sql} WHERE id_empresa=%s", params)

        # email -> canonical
        cursor2 = conn.cursor()
        cursor2.execute(
            f"UPDATE email SET id_empresa=%s WHERE id_empresa IN ({','.join(['%s']*len(duplicates))})",
            [canonical] + duplicates,
        )
        cursor2.close()

        # busqueda_empresa -> canonical (insert missing then delete duplicates rows)
        if _tabla_existe(cursor, "busqueda_empresa"):
            cursor3 = conn.cursor()
            for d in duplicates:
                cursor3.execute(
                    """
                    INSERT IGNORE INTO busqueda_empresa (id_busqueda, id_empresa)
                    SELECT id_busqueda, %s
                    FROM busqueda_empresa
                    WHERE id_empresa = %s
                    """,
                    (canonical, d),
                )
                cursor3.execute("DELETE FROM busqueda_empresa WHERE id_empresa=%s", (d,))
            cursor3.close()

        # estado_email puede tener id_empresa
        if _tabla_existe(cursor, "estado_email"):
            columnas_estado = _obtener_columnas_tabla(cursor, "estado_email")
            col_id_empresa = _primera_columna_existente(columnas_estado, ["id_empresa"])
            if col_id_empresa:
                cursor4 = conn.cursor()
                cursor4.execute(
                    f"UPDATE estado_email SET {col_id_empresa}=%s WHERE {col_id_empresa} IN ({','.join(['%s']*len(duplicates))})",
                    [canonical] + duplicates,
                )
                cursor4.close()

        # Deduplicar emails tras el merge (por si colisionan).
        try:
            # Dentro de la misma transaccion/conn no reutilizamos el otro helper.
            pass
        except Exception:
            pass

        # Eliminar empresas duplicadas.
        cursor5 = conn.cursor()
        cursor5.execute(
            f"DELETE FROM empresa WHERE id_empresa IN ({','.join(['%s']*len(duplicates))})",
            duplicates,
        )
        deleted = int(cursor5.rowcount or 0)
        cursor5.close()

        conn.commit()
        # Ejecuta deduplicacion a nivel global usando una nueva conexion (mas simple).
        try:
            deduplicar_emails(aplicar=True, eliminar_invalidos=False)
        except Exception:
            pass

        return {"merged": deleted, "canonical": canonical, "duplicates_deleted": deleted}
    finally:
        cursor.close()
        conn.close()

def _obtener_columnas_tabla(cursor, nombre_tabla):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (DB_CONFIG["database"], nombre_tabla),
    )
    columnas = set()
    for fila in cursor.fetchall():
        if isinstance(fila, dict):
            nombre_columna = (
                fila.get("column_name")
                or fila.get("COLUMN_NAME")
                or fila.get("Column_name")
            )
            if nombre_columna:
                columnas.add(nombre_columna)
        elif fila:
            columnas.add(fila[0])
    return columnas


def _tabla_tiene_columna(cursor, nombre_tabla, nombre_columna):
    return nombre_columna in _obtener_columnas_tabla(cursor, nombre_tabla)


def _tabla_existe(cursor, nombre_tabla):
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (DB_CONFIG["database"], nombre_tabla),
    )
    return cursor.fetchone() is not None


def _primera_columna_existente(columnas, candidatas):
    for candidata in candidatas:
        if candidata in columnas:
            return candidata
    return None


def _asegurar_tabla_email_estado(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS email_estado (
            id_email INT NOT NULL,
            id_estado VARCHAR(2) NOT NULL,
            fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id_email)
        )
        """
    )


def actualizar_estado_email(registro, id_estado):
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)

    try:
        columnas_email = _obtener_columnas_tabla(cursor, "email")
        columna_estado_en_email = _primera_columna_existente(
            columnas_email, ["id_estado", "id_estado_email", "estado_email", "estado"]
        )
        columna_pk_email = _primera_columna_existente(columnas_email, ["id_email"])
        columna_email_texto = _primera_columna_existente(columnas_email, ["email"])

        if columna_estado_en_email and columna_pk_email and registro.get("id_email"):
            cursor.execute(
                f"UPDATE email SET {columna_estado_en_email} = %s WHERE {columna_pk_email} = %s",
                (id_estado, registro["id_email"]),
            )
            conn.commit()
            return

        if columna_estado_en_email and columna_email_texto and registro.get("email"):
            cursor.execute(
                f"UPDATE email SET {columna_estado_en_email} = %s WHERE {columna_email_texto} = %s",
                (id_estado, registro["email"]),
            )
            conn.commit()
            return

        columnas_estado = _obtener_columnas_tabla(cursor, "estado_email")
        columna_estado = _primera_columna_existente(
            columnas_estado, ["id_estado", "id_estado_email", "estado_email", "estado"]
        )
        columna_ref_id_email = _primera_columna_existente(columnas_estado, ["id_email"])
        columna_ref_email = _primera_columna_existente(columnas_estado, ["email"])
        columna_ref_empresa = _primera_columna_existente(columnas_estado, ["id_empresa"])

        if columna_ref_id_email and columna_estado and registro.get("id_email"):
            cursor.execute(
                f"SELECT 1 FROM estado_email WHERE {columna_ref_id_email} = %s",
                (registro["id_email"],),
            )
            existe = cursor.fetchone() is not None
            if existe:
                cursor.execute(
                    f"UPDATE estado_email SET {columna_estado} = %s WHERE {columna_ref_id_email} = %s",
                    (id_estado, registro["id_email"]),
                )
            else:
                cursor.execute(
                    f"INSERT INTO estado_email ({columna_ref_id_email}, {columna_estado}) VALUES (%s, %s)",
                    (registro["id_email"], id_estado),
                )
            conn.commit()
            return

        if columna_ref_email and columna_estado and registro.get("email"):
            cursor.execute(
                f"SELECT 1 FROM estado_email WHERE {columna_ref_email} = %s",
                (registro["email"],),
            )
            existe = cursor.fetchone() is not None
            if existe:
                cursor.execute(
                    f"UPDATE estado_email SET {columna_estado} = %s WHERE {columna_ref_email} = %s",
                    (id_estado, registro["email"]),
                )
            else:
                if columna_ref_empresa and registro.get("id_empresa"):
                    cursor.execute(
                        f"INSERT INTO estado_email ({columna_ref_email}, {columna_ref_empresa}, {columna_estado}) VALUES (%s, %s, %s)",
                        (registro["email"], registro["id_empresa"], id_estado),
                    )
                else:
                    cursor.execute(
                        f"INSERT INTO estado_email ({columna_ref_email}, {columna_estado}) VALUES (%s, %s)",
                        (registro["email"], id_estado),
                    )
            conn.commit()
            return

        if columna_estado and "descripcion" in columnas_estado:
            _asegurar_tabla_email_estado(cursor)
            id_email = registro.get("id_email")
            if not id_email and registro.get("email"):
                cursor.execute(
                    "SELECT id_email FROM email WHERE email = %s LIMIT 1",
                    (registro["email"],),
                )
                fila_id = cursor.fetchone()
                id_email = fila_id.get("id_email") if fila_id else None

            if not id_email:
                raise RuntimeError("No se encontró id_email para actualizar estado.")

            cursor.execute(
                """
                INSERT INTO email_estado (id_email, id_estado)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE id_estado = VALUES(id_estado)
                """,
                (id_email, id_estado),
            )
            conn.commit()
            return

        raise RuntimeError(
            "No se pudo mapear el esquema para guardar el estado de email. "
            f"Columnas email={sorted(columnas_email)} | estado_email={sorted(columnas_estado)}"
        )
    finally:
        cursor.close()
        conn.close()


def obtener_estados_email():
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    resultados = []

    try:
        columnas_email = _obtener_columnas_tabla(cursor, "email")
        columna_estado_en_email = _primera_columna_existente(
            columnas_email, ["id_estado", "id_estado_email", "estado_email", "estado"]
        )
        columnas_estado = _obtener_columnas_tabla(cursor, "estado_email")
        columna_estado = _primera_columna_existente(
            columnas_estado, ["id_estado", "id_estado_email", "estado_email", "estado"]
        )
        columna_ref_id_email = _primera_columna_existente(columnas_estado, ["id_email"])
        columna_ref_email = _primera_columna_existente(columnas_estado, ["email"])

        if columna_estado_en_email:
            cursor.execute(
                f"""
                SELECT em.nombre, e.email, e.{columna_estado_en_email} AS id_estado,
                       COALESCE(ee.descripcion, '') AS descripcion
                FROM email e
                JOIN empresa em ON em.id_empresa = e.id_empresa
                LEFT JOIN estado_email ee ON ee.id_estado = e.{columna_estado_en_email}
                ORDER BY em.nombre, e.email
                """
            )
            for fila in cursor.fetchall():
                id_estado = fila.get("id_estado")
                resultados.append(
                    {
                        "nombre": fila.get("nombre", ""),
                        "email": fila.get("email", ""),
                        "id_estado": id_estado or "",
                        "descripcion": fila.get("descripcion", "")
                        or ESTADOS_DESCRIPCION.get(id_estado, ""),
                    }
                )
            return resultados

        if _tabla_existe(cursor, "email_estado"):
            cursor.execute(
                """
                SELECT em.nombre, e.email, ee.id_estado, COALESCE(es.descripcion, '') AS descripcion
                FROM email_estado ee
                JOIN email e ON e.id_email = ee.id_email
                JOIN empresa em ON em.id_empresa = e.id_empresa
                LEFT JOIN estado_email es ON es.id_estado = ee.id_estado
                ORDER BY em.nombre, e.email
                """
            )
            for fila in cursor.fetchall():
                id_estado = fila.get("id_estado")
                resultados.append(
                    {
                        "nombre": fila.get("nombre", ""),
                        "email": fila.get("email", ""),
                        "id_estado": id_estado or "",
                        "descripcion": fila.get("descripcion", "")
                        or ESTADOS_DESCRIPCION.get(id_estado, ""),
                    }
                )
            return resultados

        if columna_ref_id_email and columna_estado:
            cursor.execute(
                f"""
                SELECT em.nombre, e.email, ee.{columna_estado} AS id_estado
                FROM estado_email ee
                JOIN email e ON e.id_email = ee.{columna_ref_id_email}
                JOIN empresa em ON em.id_empresa = e.id_empresa
                ORDER BY em.nombre, e.email
                """
            )
            for fila in cursor.fetchall():
                id_estado = fila.get("id_estado")
                resultados.append(
                    {
                        "nombre": fila.get("nombre", ""),
                        "email": fila.get("email", ""),
                        "id_estado": id_estado or "",
                        "descripcion": ESTADOS_DESCRIPCION.get(id_estado, ""),
                    }
                )
            return resultados

        if columna_ref_email and columna_estado:
            cursor.execute(
                f"""
                SELECT COALESCE(em.nombre, '') AS nombre, ee.{columna_ref_email} AS email, ee.{columna_estado} AS id_estado
                FROM estado_email ee
                LEFT JOIN email e ON e.email = ee.{columna_ref_email}
                LEFT JOIN empresa em ON em.id_empresa = e.id_empresa
                ORDER BY ee.{columna_ref_email}
                """
            )
            for fila in cursor.fetchall():
                id_estado = fila.get("id_estado")
                resultados.append(
                    {
                        "nombre": fila.get("nombre", ""),
                        "email": fila.get("email", ""),
                        "id_estado": id_estado or "",
                        "descripcion": ESTADOS_DESCRIPCION.get(id_estado, ""),
                    }
                )
            return resultados

        raise RuntimeError(
            "No se pudo mapear el esquema para consultar estados de email. "
            f"Columnas email={sorted(columnas_email)} | estado_email={sorted(columnas_estado)}"
        )
    finally:
        cursor.close()
        conn.close()


# ---------------- EMAIL ----------------
def enviar_email(destinatario, asunto, cuerpo_html):
    if not SMTP_USER or not SMTP_PASS:
        raise RuntimeError("SMTP_USER/SMTP_PASS no configurados en .env")

    msg = EmailMessage()
    msg["From"] = SMTP_USER
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.set_content("Tu cliente de email no soporta HTML")
    msg.add_alternative(cuerpo_html, subtype="html")

    context = ssl.create_default_context()

    # Gmail tipicamente: 587 (STARTTLS) o 465 (SSL directo).
    if int(SMTP_PORT) == 465:
        with smtplib.SMTP_SSL(SMTP_SERVER, int(SMTP_PORT), context=context, timeout=25) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT), timeout=25) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)


def _atomic_write_json(path, payload):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _load_warmup_state():
    if not WARMUP_STATE_PATH.exists():
        return {
            "start_date": None,  # YYYY-MM-DD
            "sent_by_date": {},  # YYYY-MM-DD -> int
            "sent_timestamps": [],  # unix seconds (for hourly cap)
        }
    try:
        with open(WARMUP_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        data.setdefault("start_date", None)
        data.setdefault("sent_by_date", {})
        data.setdefault("sent_timestamps", [])
        return data
    except Exception:
        # Si el archivo se corrompe, no bloqueamos el envío, pero empezamos limpio.
        return {
            "start_date": None,
            "sent_by_date": {},
            "sent_timestamps": [],
        }


def _save_warmup_state(state):
    _atomic_write_json(WARMUP_STATE_PATH, state)


def _load_recontact_state():
    if not RECONTACT_STATE_PATH.exists():
        return {"last_reset_month": None}
    try:
        with open(RECONTACT_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        data.setdefault("last_reset_month", None)
        return data
    except Exception:
        return {"last_reset_month": None}


def _save_recontact_state(state):
    _atomic_write_json(RECONTACT_STATE_PATH, state)


def _reset_estados_enviados_a_pendiente():
    """
    Intenta resetear estados EN -> PE en los posibles esquemas soportados.
    """
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    total = 0
    try:
        columnas_email = _obtener_columnas_tabla(cursor, "email")
        col_estado_email = _primera_columna_existente(
            columnas_email, ["id_estado", "id_estado_email", "estado_email", "estado"]
        )
        if col_estado_email:
            cursor.execute(
                f"UPDATE email SET {col_estado_email} = %s WHERE {col_estado_email} = %s",
                ("PE", "EN"),
            )
            total += int(cursor.rowcount or 0)

        if _tabla_existe(cursor, "email_estado"):
            cursor.execute("UPDATE email_estado SET id_estado = %s WHERE id_estado = %s", ("PE", "EN"))
            total += int(cursor.rowcount or 0)

        if _tabla_existe(cursor, "estado_email"):
            columnas_estado = _obtener_columnas_tabla(cursor, "estado_email")
            col_estado = _primera_columna_existente(
                columnas_estado, ["id_estado", "id_estado_email", "estado_email", "estado"]
            )
            # Si hay columna descripcion, tratamos estado_email como catalogo y no lo tocamos.
            if col_estado and "descripcion" not in columnas_estado:
                cursor.execute(
                    f"UPDATE estado_email SET {col_estado} = %s WHERE {col_estado} = %s",
                    ("PE", "EN"),
                )
                total += int(cursor.rowcount or 0)

        conn.commit()
        return total
    finally:
        cursor.close()
        conn.close()


def reactivar_enviados_si_nuevo_mes():
    """
    Si cambia el mes (YYYY-MM), reactiva los emails enviados para recontacto.
    """
    state = _load_recontact_state()
    mes_actual = dt.date.today().strftime("%Y-%m")
    ultimo_mes = state.get("last_reset_month")

    if ultimo_mes == mes_actual:
        return {"performed": False, "month": mes_actual, "updated": 0}

    updated = _reset_estados_enviados_a_pendiente()
    state["last_reset_month"] = mes_actual
    _save_recontact_state(state)
    return {"performed": True, "month": mes_actual, "updated": updated}


def _warmup_limits(state):
    hoy = dt.date.today().isoformat()
    if not state.get("start_date"):
        state["start_date"] = hoy

    try:
        start = dt.date.fromisoformat(state["start_date"])
    except Exception:
        start = dt.date.today()
        state["start_date"] = start.isoformat()

    dias = max(0, (dt.date.today() - start).days)
    idx = min(dias, len(WARMUP_DAILY_SCHEDULE) - 1)
    limite_diario = WARMUP_DAILY_SCHEDULE[idx]
    enviados_hoy = int(state.get("sent_by_date", {}).get(hoy, 0) or 0)
    restante_hoy = max(0, limite_diario - enviados_hoy)
    return {
        "date": hoy,
        "days_since_start": dias,
        "daily_limit": limite_diario,
        "sent_today": enviados_hoy,
        "remaining_today": restante_hoy,
        "hourly_limit": WARMUP_HOURLY_LIMIT,
    }


def _warmup_can_send_more_now(state):
    """
    Devuelve (ok, wait_seconds, sent_last_hour_count).
    """
    now = time.time()
    one_hour_ago = now - 3600
    stamps = [s for s in (state.get("sent_timestamps") or []) if isinstance(s, (int, float)) and s >= one_hour_ago]
    state["sent_timestamps"] = stamps
    if len(stamps) < WARMUP_HOURLY_LIMIT:
        return True, 0.0, len(stamps)

    oldest = min(stamps) if stamps else now
    wait = max(5.0, (oldest + 3600) - now)
    return False, wait, len(stamps)


def _warmup_mark_sent(state):
    hoy = dt.date.today().isoformat()
    state.setdefault("sent_by_date", {})
    state["sent_by_date"][hoy] = int(state["sent_by_date"].get(hoy, 0) or 0) + 1
    state.setdefault("sent_timestamps", [])
    state["sent_timestamps"].append(time.time())
    # recorta histórico
    if len(state["sent_timestamps"]) > 5000:
        state["sent_timestamps"] = state["sent_timestamps"][-2000:]


def limpiar_valor(valor, fallback):
    if not valor:
        return fallback
    valor_str = str(valor).strip().lower()
    if valor_str in ("none", "null", "no disponible", "", "desconocida", "desconocido"):
        return fallback
    return str(valor).strip()


def generar_saludo(nombre_empresa):
    nombre_limpio = limpiar_valor(nombre_empresa, "")
    if not nombre_limpio:
        return "Estimado equipo,"
    return f"Estimado equipo de {nombre_limpio},"


def _format_template_safe(template, **kwargs):
    """
    Formatea plantillas tipo str.format sin romper si faltan claves.
    """
    try:
        return str(template).format(**kwargs)
    except KeyError:
        return str(template)
    except Exception:
        return str(template)


# ---------------- GUI ----------------
def _make_scrolled_listbox(parent, **listbox_kwargs):
    container = ttk.Frame(parent)
    lb = tk.Listbox(container, **listbox_kwargs)
    vsb = ttk.Scrollbar(container, orient="vertical", command=lb.yview)
    lb.configure(yscrollcommand=vsb.set)
    lb.pack(side="left", fill="both", expand=True)
    vsb.pack(side="right", fill="y")
    return container, lb


def _make_scrolled_text(parent, **text_kwargs):
    container = ttk.Frame(parent)
    txt = tk.Text(container, **text_kwargs)
    vsb = ttk.Scrollbar(container, orient="vertical", command=txt.yview)
    txt.configure(yscrollcommand=vsb.set)
    txt.pack(side="left", fill="both", expand=True)
    vsb.pack(side="right", fill="y")
    return container, txt


def lanzar_gui():
    try:
        recontacto = reactivar_enviados_si_nuevo_mes()
    except Exception:
        recontacto = {
            "performed": False,
            "month": dt.date.today().strftime("%Y-%m"),
            "updated": 0,
            "error": True,
        }

    root = tk.Tk()
    root.title("Consultor de Empresas")
    # Ventana compacta pero usable: prioriza que los botones queden visibles y la consulta sea por scroll.
    root.geometry("820x560")
    root.minsize(780, 520)

    empresas = []
    emails_actuales = []
    emails_reales = []
    emails_posibles = []
    emails_enviados = []

    frame = ttk.Frame(root, padding=10)
    frame.pack(fill="both", expand=True)

    notebook = ttk.Notebook(frame)
    notebook.pack(fill="both", expand=True, pady=10)

    # --- Pestana Emails ---
    tab_emails = ttk.Frame(notebook)
    notebook.add(tab_emails, text="Empresas con Email")

    frame_lb_emails, listbox_emails_tab = _make_scrolled_listbox(tab_emails, height=12)
    frame_lb_emails.pack(fill="both", expand=True, padx=5, pady=5)

    # --- Pestana Telefonos ---
    tab_telefonos = ttk.Frame(notebook)
    notebook.add(tab_telefonos, text="Empresas con Telefono")

    frame_lb_tel, listbox_telefonos_tab = _make_scrolled_listbox(tab_telefonos, height=12)
    frame_lb_tel.pack(fill="both", expand=True, padx=5, pady=5)

    # --- Pestana Estado Emails ---
    tab_estado_emails = ttk.Frame(notebook)
    notebook.add(tab_estado_emails, text="Estado Emails")

    frame_lb_estado, listbox_estado_emails_tab = _make_scrolled_listbox(tab_estado_emails, height=12)
    frame_lb_estado.pack(fill="both", expand=True, padx=5, pady=5)

    # --- Pestana Mantenimiento ---
    tab_mantenimiento = ttk.Frame(notebook)
    notebook.add(tab_mantenimiento, text="Mantenimiento")

    frame_mant = ttk.Frame(tab_mantenimiento, padding=8)
    frame_mant.pack(fill="both", expand=True)

    frame_mant_top = ttk.Frame(frame_mant)
    frame_mant_top.pack(fill="x")

    var_eliminar_invalidos = tk.BooleanVar(value=True)
    ttk.Checkbutton(
        frame_mant_top,
        text="Eliminar emails invalidos (NULL / 'no disponible' / vacio)",
        variable=var_eliminar_invalidos,
    ).pack(anchor="w")

    frame_mant_btns = ttk.Frame(frame_mant_top)
    frame_mant_btns.pack(fill="x", pady=(6, 0))

    frame_mant_log, text_mant_log = _make_scrolled_text(frame_mant, height=6)
    frame_mant_log.pack(fill="both", expand=False, pady=(10, 8))

    def mant_log(msg):
        text_mant_log.insert(tk.END, str(msg) + "\n")
        text_mant_log.see(tk.END)

    if recontacto.get("performed"):
        mant_log(
            f"Recontacto mensual aplicado ({recontacto.get('month')}): {recontacto.get('updated', 0)} estados EN reactivados a PE."
        )
    elif recontacto.get("error"):
        mant_log("Aviso: no se pudo validar el recontacto mensual automatico.")
    else:
        mant_log(f"Recontacto mensual ya aplicado para {recontacto.get('month')}.")

    def run_bg(fn, on_done=None):
        def _worker():
            try:
                res = fn()
                if on_done:
                    root.after(0, lambda r=res: on_done(r, None))
            except Exception as exc:
                if on_done:
                    root.after(0, lambda e=exc: on_done(None, e))
        threading.Thread(target=_worker, daemon=True).start()

    def analizar_emails():
        mant_log("Analizando emails (invalidos + duplicados)...")

        def _do():
            return resumen_limpieza_emails()

        def _done(res, err):
            if err:
                messagebox.showerror("Error", f"No se pudo analizar: {err}")
                return
            mant_log(f"Emails invalidos: {res['invalid_emails']}")
            mant_log(f"Grupos duplicados: {res['duplicate_groups']}")
            mant_log(f"Filas duplicadas a borrar: {res['duplicate_rows_to_delete']}")

        run_bg(_do, _done)

    def aplicar_limpieza_emails():
        if not messagebox.askyesno(
            "Confirmacion",
            "Vas a eliminar duplicados de emails (y opcionalmente invalidos). ¿Continuar?",
        ):
            return
        mant_log("Aplicando limpieza de emails...")

        def _do():
            return deduplicar_emails(aplicar=True, eliminar_invalidos=var_eliminar_invalidos.get())

        def _done(res, err):
            if err:
                messagebox.showerror("Error", f"No se pudo aplicar la limpieza: {err}")
                return
            before = res.get("before") or {}
            after = res.get("after") or {}
            mant_log(
                f"Antes: invalidos={before.get('invalid_emails')} | grupos_dup={before.get('duplicate_groups')} | filas_dup={before.get('duplicate_rows_to_delete')}"
            )
            mant_log(
                f"Despues: invalidos={after.get('invalid_emails')} | grupos_dup={after.get('duplicate_groups')} | filas_dup={after.get('duplicate_rows_to_delete')}"
            )
            mant_log("Limpieza de emails completada.")

        run_bg(_do, _done)

    ttk.Button(frame_mant_btns, text="Analizar duplicados", command=analizar_emails).pack(side="left")
    ttk.Button(frame_mant_btns, text="Aplicar deduplicacion", command=aplicar_limpieza_emails).pack(side="left", padx=(8, 0))

    ttk.Separator(frame_mant, orient="horizontal").pack(fill="x", pady=(8, 8))

    # --- Redundancia empresa/email (mismo email + mismo nombre con distinta id_empresa) ---
    ttk.Label(frame_mant, text="Redundancias empresa/email (mismo email + mismo nombre, distinta id_empresa):").pack(anchor="w")

    frame_lb_red, listbox_redundancias = _make_scrolled_listbox(
        frame_mant, height=6, selectmode=tk.MULTIPLE
    )
    frame_lb_red.pack(fill="both", expand=False, pady=(6, 6))
    redundancias = []

    def buscar_redundancias():
        listbox_redundancias.delete(0, tk.END)
        mant_log("Buscando redundancias email+nombre con distinta id_empresa...")

        def _do():
            return buscar_redundancias_email_nombre_empresa()

        def _done(res, err):
            nonlocal redundancias
            if err:
                messagebox.showerror("Error", f"No se pudo buscar redundancias: {err}")
                listbox_redundancias.insert(tk.END, f"Error al consultar: {err}")
                return
            redundancias = res or []
            mant_log(f"Encontrados {len(redundancias)} grupos redundantes.")
            if not redundancias:
                listbox_redundancias.insert(tk.END, "Sin redundancias encontradas.")
                return
            for r in redundancias:
                ids = ",".join(r.get("ids_empresas") or [])
                listbox_redundancias.insert(
                    tk.END,
                    f"{r.get('ejemplo_nombre','')} | {r.get('ejemplo_email','')} | ids=[{ids}]",
                )

        run_bg(_do, _done)

    def fusionar_redundancias_seleccionadas():
        idxs = listbox_redundancias.curselection()
        if not idxs:
            messagebox.showwarning("Sin seleccion", "Selecciona al menos un grupo redundante.")
            return
        grupos = [redundancias[i] for i in idxs if i < len(redundancias)]
        total_emp = sum(len(g.get("ids_empresas") or []) for g in grupos)
        if not messagebox.askyesno(
            "Confirmacion",
            f"Vas a fusionar {len(grupos)} grupos (total empresas implicadas ~{total_emp}). "
            "Se conservara como canonical el menor id_empresa de cada grupo y se borraran las duplicadas. ¿Continuar?",
        ):
            return
        mant_log(f"Fusionando {len(grupos)} grupos redundantes...")

        def _do():
            merged_total = 0
            for g in grupos:
                ids = g.get("ids_empresas") or []
                if len(ids) < 2:
                    continue
                res = fusionar_empresas(ids)
                merged_total += int(res.get("merged") or 0)
            return {"merged_total": merged_total}

        def _done(res, err):
            if err:
                messagebox.showerror("Error", f"No se pudo fusionar: {err}")
                return
            mant_log(f"Empresas duplicadas eliminadas tras fusion: {res.get('merged_total', 0)}")
            buscar_redundancias()
            # refresco de emails si el usuario esta mirando esa pestaña
            try:
                cargar_empresas()
            except Exception:
                pass

        run_bg(_do, _done)

    frame_redund_btns = ttk.Frame(frame_mant)
    frame_redund_btns.pack(fill="x", pady=(0, 6))
    ttk.Button(frame_redund_btns, text="Buscar redundancias", command=buscar_redundancias).pack(side="left")
    ttk.Button(frame_redund_btns, text="Fusionar seleccion", command=fusionar_redundancias_seleccionadas).pack(side="left", padx=(8, 0))

    ttk.Separator(frame_mant, orient="horizontal").pack(fill="x", pady=(8, 8))

    var_solo_sin_telefono = tk.BooleanVar(value=True)
    ttk.Checkbutton(
        frame_mant,
        text="Solo empresas sin emails validos y sin telefono (modo seguro)",
        variable=var_solo_sin_telefono,
    ).pack(anchor="w")

    frame_lb_vacias, listbox_empresas_vacias = _make_scrolled_listbox(
        frame_mant, height=7, selectmode=tk.MULTIPLE
    )
    frame_lb_vacias.pack(fill="both", expand=True, pady=(6, 6))
    empresas_vacias = []

    def buscar_empresas_vacias():
        listbox_empresas_vacias.delete(0, tk.END)
        mant_log("Buscando empresas sin emails validos...")

        def _do():
            return listar_empresas_sin_emails_validos(solo_sin_telefono=var_solo_sin_telefono.get())

        def _done(res, err):
            nonlocal empresas_vacias
            if err:
                messagebox.showerror("Error", f"No se pudo buscar: {err}")
                listbox_empresas_vacias.insert(tk.END, f"Error al consultar: {err}")
                return
            empresas_vacias = res or []
            mant_log(f"Encontradas {len(empresas_vacias)} empresas candidatas.")
            if not empresas_vacias:
                listbox_empresas_vacias.insert(tk.END, "Sin empresas candidatas.")
                return
            for em in empresas_vacias:
                listbox_empresas_vacias.insert(
                    tk.END, f"{em.get('id_empresa','')} | {em.get('nombre','')} | {em.get('telefono','')}"
                )

        run_bg(_do, _done)

    def eliminar_empresas_seleccionadas():
        idxs = listbox_empresas_vacias.curselection()
        if not idxs:
            messagebox.showwarning("Sin seleccion", "Selecciona al menos una empresa para eliminar.")
            return
        ids = [empresas_vacias[i].get("id_empresa") for i in idxs if i < len(empresas_vacias)]
        ids = [i for i in ids if i]
        if not ids:
            return
        if not messagebox.askyesno(
            "Confirmacion",
            f"Vas a eliminar {len(ids)} empresas y sus datos asociados (emails/busquedas/estados). ¿Continuar?",
        ):
            return
        mant_log(f"Eliminando {len(ids)} empresas...")

        def _do():
            return eliminar_empresas(ids)

        def _done(res, err):
            if err:
                messagebox.showerror("Error", f"No se pudo eliminar: {err}")
                return
            mant_log(f"Empresas eliminadas: {res.get('deleted_empresas', 0)}")
            buscar_empresas_vacias()

        run_bg(_do, _done)

    frame_mant_bottom_btns = ttk.Frame(frame_mant)
    frame_mant_bottom_btns.pack(fill="x", pady=(0, 6))
    ttk.Button(frame_mant_bottom_btns, text="Buscar candidatas", command=buscar_empresas_vacias).pack(side="left")
    ttk.Button(frame_mant_bottom_btns, text="Eliminar seleccionadas", command=eliminar_empresas_seleccionadas).pack(side="left", padx=(8, 0))

    def cargar_empresas():
        nonlocal empresas, emails_actuales, emails_reales, emails_posibles, emails_enviados
        empresas = obtener_empresas()
        emails_actuales = []
        emails_reales = []
        emails_posibles = []
        emails_enviados = []

        listbox_emails_tab.delete(0, tk.END)

        conn = conectar_db()
        cursor = conn.cursor(dictionary=True)

        ids = [e["id_empresa"] for e in empresas]
        if ids:
            formato = ",".join(["%s"] * len(ids))
            cursor.execute(
                f"""
                SELECT em.id_empresa, e.id_email, e.id_tipo_email, em.nombre, e.email,
                       b.tipo_empresa, b.localidad
                FROM empresa em
                JOIN email e ON em.id_empresa = e.id_empresa
                LEFT JOIN (
                    SELECT id_empresa, MAX(id_busqueda) AS id_busqueda
                    FROM busqueda_empresa
                    GROUP BY id_empresa
                ) be_last ON em.id_empresa = be_last.id_empresa
                LEFT JOIN busqueda b ON be_last.id_busqueda = b.id_busqueda
                WHERE em.id_empresa IN ({formato})
                  AND LOWER(TRIM(e.email)) NOT IN ('no disponible', '', 'none', 'null')
                ORDER BY em.nombre, e.id_tipo_email, e.email
            """,
                ids,
            )
            emails_actuales = cursor.fetchall()

        cursor.close()
        conn.close()

        # Mapa de estado por email (EN/PE/ER) si existe.
        estado_por_email = {}
        try:
            estados = obtener_estados_email()
            for s in estados:
                em = (s.get("email") or "").strip().lower()
                if not em or em in estado_por_email:
                    continue
                estado_por_email[em] = (s.get("id_estado") or "").strip().upper()
        except Exception:
            estado_por_email = {}

        # Deduplicar por (email_norm, id_empresa, tipo) para evitar redundancias por joins/esquema.
        dedup = {}
        for e in emails_actuales:
            email_norm = (e.get("email") or "").strip().lower()
            if not email_norm or email_norm in INVALID_EMAIL_VALUES:
                continue
            key = (email_norm, e.get("id_empresa"), (e.get("id_tipo_email") or "").strip().upper())
            # Preferimos conservar el que tenga tipo_empresa/localidad si hay colision.
            if key in dedup:
                prev = dedup[key]
                prev_has_ctx = bool((prev.get("tipo_empresa") or "").strip() or (prev.get("localidad") or "").strip())
                cur_has_ctx = bool((e.get("tipo_empresa") or "").strip() or (e.get("localidad") or "").strip())
                if prev_has_ctx or not cur_has_ctx:
                    continue
            dedup[key] = e

        emails_actuales = list(dedup.values())

        # Separar emails reales (RE) y posibles (IN/CO/AD).
        for e in emails_actuales:
            email_norm = (e.get("email") or "").strip().lower()
            e["id_estado"] = estado_por_email.get(email_norm, "")

            if e["id_estado"] == "EN":
                emails_enviados.append(e)
                continue

            tipo = (e.get("id_tipo_email") or "").strip().upper()
            if tipo == "RE":
                emails_reales.append(e)
            elif tipo in ("IN", "CO", "AD"):
                emails_posibles.append(e)

        # En la pestaña principal solo mostramos los reales (RE).
        for e in emails_reales:
            listbox_emails_tab.insert(tk.END, f"{e['nombre']} | {e['email']}")

    def abrir_envio_email():
        if not emails_reales and not emails_posibles and not emails_enviados:
            messagebox.showwarning("Aviso", "Primero consulta los emails")
            return

        win = tk.Toplevel(root)
        win.title("Enviar Email")
        win.geometry("820x760")
        win.minsize(760, 700)

        ttk.Label(win, text="Destinatarios:").pack(anchor="w", padx=10)
        notebook_envio = ttk.Notebook(win)
        notebook_envio.pack(fill="x", padx=10)

        tab_reales = ttk.Frame(notebook_envio)
        notebook_envio.add(tab_reales, text="Reales (RE)")

        tab_posibles = ttk.Frame(notebook_envio)
        notebook_envio.add(tab_posibles, text="Posibles (IN/CO/AD)")

        tab_enviados = ttk.Frame(notebook_envio)
        notebook_envio.add(tab_enviados, text="Emails ya enviados")

        frame_lb_reales, listbox_emails_reales = _make_scrolled_listbox(
            tab_reales, selectmode=tk.MULTIPLE, height=6
        )
        frame_lb_reales.pack(fill="x")
        for e in emails_reales:
            listbox_emails_reales.insert(tk.END, f"{e['nombre']} | {e['email']}")

        frame_sel_reales = ttk.Frame(tab_reales)
        frame_sel_reales.pack(anchor="w", pady=(6, 0))

        def seleccionar_todos_reales():
            if listbox_emails_reales.size() > 0:
                listbox_emails_reales.selection_set(0, tk.END)

        def deseleccionar_todos_reales():
            listbox_emails_reales.selection_clear(0, tk.END)

        ttk.Button(
            frame_sel_reales, text="Seleccionar todas", command=seleccionar_todos_reales
        ).pack(side="left")
        ttk.Button(
            frame_sel_reales,
            text="Deseleccionar todas",
            command=deseleccionar_todos_reales,
        ).pack(side="left", padx=(8, 0))

        frame_lb_pos, listbox_emails_posibles = _make_scrolled_listbox(
            tab_posibles, selectmode=tk.MULTIPLE, height=6
        )
        frame_lb_pos.pack(fill="x")
        for e in emails_posibles:
            tipo = (e.get("id_tipo_email") or "").strip().upper()
            listbox_emails_posibles.insert(
                tk.END, f"{e['nombre']} | {e['email']} | {tipo}"
            )

        frame_lb_env, listbox_emails_enviados = _make_scrolled_listbox(
            tab_enviados, selectmode=tk.BROWSE, height=6
        )
        frame_lb_env.pack(fill="x")
        for e in emails_enviados:
            tipo = (e.get("id_tipo_email") or "").strip().upper()
            listbox_emails_enviados.insert(
                tk.END,
                f"{e.get('nombre','')} | {e.get('email','')} | {tipo} | EN",
            )
        if not emails_enviados:
            listbox_emails_enviados.insert(tk.END, "No hay emails marcados como EN (enviados).")

        frame_sel_posibles = ttk.Frame(tab_posibles)
        frame_sel_posibles.pack(anchor="w", pady=(6, 0))

        def seleccionar_todos_posibles():
            if listbox_emails_posibles.size() > 0:
                listbox_emails_posibles.selection_set(0, tk.END)

        def deseleccionar_todos_posibles():
            listbox_emails_posibles.selection_clear(0, tk.END)

        ttk.Button(
            frame_sel_posibles,
            text="Seleccionar todas",
            command=seleccionar_todos_posibles,
        ).pack(side="left")
        ttk.Button(
            frame_sel_posibles,
            text="Deseleccionar todas",
            command=deseleccionar_todos_posibles,
        ).pack(side="left", padx=(8, 0))

        ttk.Label(win, text="Asunto:").pack(anchor="w", padx=10, pady=(10, 0))
        entry_asunto = ttk.Entry(win)
        entry_asunto.insert(0, "Propuesta Soluciones Informáticas JM Ordenadores para {empresa}")
        entry_asunto.pack(fill="x", padx=10)
        ttk.Label(
            win,
            text="Tip: puedes usar {empresa}, {localidad}, {tipo_empresa}.",
        ).pack(anchor="w", padx=10, pady=(2, 0))

        ttk.Label(win, text="Mensaje HTML:").pack(anchor="w", padx=10, pady=(10, 0))
        frame_cuerpo, text_cuerpo = _make_scrolled_text(win, height=10, wrap="word")
        text_cuerpo.insert("1.0", PLANTILLA_EMAIL)
        frame_cuerpo.pack(fill="both", expand=True, padx=10, pady=5)

        # --- Warm-up / throttling ---
        frame_warmup = ttk.Frame(win)
        frame_warmup.pack(fill="x", padx=8, pady=(6, 0))

        var_warmup = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            frame_warmup,
            text="Warm-up (envio paulatino: limite diario + limite por hora + delays)",
            variable=var_warmup,
        ).pack(anchor="w")

        warmup_info = tk.StringVar(value="")
        ttk.Label(frame_warmup, textvariable=warmup_info).pack(anchor="w", pady=(2, 0))

        log_queue = queue.Queue()
        running = {"value": False}

        frame_log, text_log = _make_scrolled_text(win, height=6, wrap="word")
        frame_log.pack(fill="both", expand=False, padx=10, pady=(8, 0))

        def log(msg):
            log_queue.put(msg)

        def flush_logs():
            while True:
                try:
                    msg = log_queue.get_nowait()
                except queue.Empty:
                    break
                text_log.insert(tk.END, msg + "\n")
                text_log.see(tk.END)
            if running["value"] or not log_queue.empty():
                win.after(150, flush_logs)

        def set_running_state(is_running):
            running["value"] = is_running
            btn_send.config(state=("disabled" if is_running else "normal"))

        def enviar_emails_worker(unicos, asunto_base, cuerpo_base, warmup_enabled):
            errores = []
            enviados = 0

            state = _load_warmup_state()
            limits = _warmup_limits(state)
            _save_warmup_state(state)

            if warmup_enabled:
                warmup_info.set(
                    f"Warm-up: dia {limits['days_since_start']} | "
                    f"limite diario {limits['daily_limit']} | enviados hoy {limits['sent_today']} | "
                    f"restantes hoy {limits['remaining_today']} | limite/hora {limits['hourly_limit']}"
                )
            else:
                warmup_info.set("Warm-up desactivado.")

            for registro in unicos:
                if warmup_enabled:
                    state = _load_warmup_state()
                    limits = _warmup_limits(state)
                    if limits["remaining_today"] <= 0:
                        log("Warm-up: limite diario alcanzado. Deteniendo envio para proteger reputacion.")
                        _save_warmup_state(state)
                        break

                    ok, wait_s, sent_last_hour = _warmup_can_send_more_now(state)
                    if not ok:
                        log(
                            f"Warm-up: limite por hora alcanzado ({sent_last_hour}/{WARMUP_HOURLY_LIMIT}). "
                            f"Deteniendo envio (reanuda en ~{wait_s/60:.0f} min)."
                        )
                        _save_warmup_state(state)
                        break

                email = registro["email"]
                nombre_empresa = limpiar_valor(registro.get("nombre"), "")
                # Si no hay nombre, usamos un texto neutro para no dejar frases raras.
                empresa_para_template = nombre_empresa if nombre_empresa else "vuestra empresa"
                empresa_para_asunto = nombre_empresa if nombre_empresa else "su empresa"

                tipo_empresa = limpiar_valor(registro.get("tipo_empresa"), "empresa")
                localidad = limpiar_valor(registro.get("localidad"), "Madrid")
                saludo = generar_saludo(nombre_empresa)

                try:
                    try:
                        actualizar_estado_email(registro, "PE")
                    except Exception as exc_estado_pe:
                        errores.append(
                            f"{email}: no se pudo marcar estado PE ({exc_estado_pe})"
                        )

                    cuerpo_personalizado = cuerpo_base.format(
                        empresa=empresa_para_template,
                        tipo_empresa=tipo_empresa,
                        localidad=localidad,
                        saludo=saludo,
                    )
                    asunto_personalizado = _format_template_safe(
                        asunto_base,
                        empresa=empresa_para_asunto,
                        tipo_empresa=tipo_empresa,
                        localidad=localidad,
                    )
                    enviar_email(email, asunto_personalizado, cuerpo_personalizado)
                    enviados += 1
                    log(f"ENVIADO: {email}")

                    if warmup_enabled:
                        state = _load_warmup_state()
                        _warmup_mark_sent(state)
                        _save_warmup_state(state)

                    try:
                        actualizar_estado_email(registro, "EN")
                    except Exception as exc_estado_en:
                        errores.append(
                            f"{email}: email enviado, pero no se pudo marcar EN ({exc_estado_en})"
                        )
                except Exception as exc:
                    try:
                        actualizar_estado_email(registro, "ER")
                    except Exception as exc_estado_er:
                        errores.append(
                            f"{email}: error de envio y no se pudo marcar ER ({exc_estado_er})"
                        )
                    errores.append(f"{email}: {exc}")
                    log(f"ERROR: {email}: {exc}")

                if warmup_enabled:
                    if enviados > 0 and enviados % WARMUP_LONG_PAUSE_EVERY == 0:
                        pausa = random.uniform(*WARMUP_LONG_PAUSE_SECONDS)
                        log(f"Pausa larga warm-up: {pausa:.0f}s")
                        time.sleep(pausa)
                    else:
                        delay = random.uniform(*WARMUP_DELAY_BETWEEN_EMAILS_SECONDS)
                        log(f"Delay warm-up: {delay:.0f}s")
                        time.sleep(delay)

            win.after(0, lambda: set_running_state(False))
            if errores:
                msg = "\n".join(errores[:80])
                if len(errores) > 80:
                    msg += f"\n... ({len(errores) - 80} mas)"
                win.after(0, lambda m=msg: messagebox.showerror("Errores", m))
            else:
                win.after(0, lambda: messagebox.showinfo("OK", "Proceso de envio finalizado"))

        def enviar_emails():
            seleccion_reales = listbox_emails_reales.curselection()
            seleccion_posibles = listbox_emails_posibles.curselection()
            if not seleccion_reales and not seleccion_posibles:
                messagebox.showwarning("Aviso", "Selecciona al menos un email")
                return

            asunto = entry_asunto.get().strip()
            cuerpo_base = text_cuerpo.get("1.0", tk.END)
            errores = []

            seleccionados = []
            for i in seleccion_reales:
                seleccionados.append(emails_reales[i])
            for i in seleccion_posibles:
                seleccionados.append(emails_posibles[i])

            # Evitar enviar al mismo email mas de una vez (normalizado).
            unicos = []
            vistos = set()
            for r in seleccionados:
                email_norm = (r.get("email") or "").strip().lower()
                if not email_norm or email_norm in INVALID_EMAIL_VALUES:
                    continue
                if email_norm in vistos:
                    continue
                vistos.add(email_norm)
                unicos.append(r)

            if running["value"]:
                return
            set_running_state(True)
            log("Inicio de envio...")
            win.after(150, flush_logs)

            hilo = threading.Thread(
                target=enviar_emails_worker,
                args=(unicos, asunto, cuerpo_base, var_warmup.get()),
                daemon=True,
            )
            hilo.start()

        btn_send = ttk.Button(win, text="Enviar emails", command=enviar_emails)
        btn_send.pack(pady=10)

    def cargar_telefonos():
        listbox_telefonos_tab.delete(0, tk.END)
        conn = conectar_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT id_empresa, nombre, telefono
            FROM empresa
            WHERE telefono IS NOT NULL AND telefono != ''
            ORDER BY nombre
        """
        )
        telefonos = cursor.fetchall()
        cursor.close()
        conn.close()

        for t in telefonos:
            listbox_telefonos_tab.insert(tk.END, f"{t['nombre']} | {t['telefono']}")

    def cargar_estado_emails():
        listbox_estado_emails_tab.delete(0, tk.END)
        try:
            estados = obtener_estados_email()
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudieron consultar los estados: {exc}")
            return

        if not estados:
            listbox_estado_emails_tab.insert(tk.END, "No hay estados registrados.")
            return

        for estado in estados:
            nombre = estado.get("nombre", "")
            email = estado.get("email", "")
            id_estado = estado.get("id_estado", "")
            descripcion = estado.get("descripcion", "")
            listbox_estado_emails_tab.insert(
                tk.END, f"{nombre} | {email} | {id_estado} - {descripcion}"
            )

    ttk.Button(frame, text="Cargar empresas con email", command=cargar_empresas).pack(pady=5)
    ttk.Button(tab_emails, text="Enviar email", command=abrir_envio_email).pack(pady=5)
    ttk.Button(tab_telefonos, text="Cargar telefonos", command=cargar_telefonos).pack(pady=5)
    ttk.Button(
        tab_estado_emails, text="Consultar estados de email", command=cargar_estado_emails
    ).pack(pady=5)

    root.mainloop()


if __name__ == "__main__":
    lanzar_gui()
