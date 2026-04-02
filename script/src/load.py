#!/usr/bin/env python
"""
Load — DataFrame Analytics-Ready -> PostgreSQL (Data Warehouse)

Stratégie UPSERT :
  INSERT ... ON CONFLICT (marque, modele, carburant) DO UPDATE SET ...
  Garantit l'idempotence du pipeline (relances sans doublons).

Schéma généré dynamiquement depuis le DataFrame fourni par transformation.py.
"""

import logging
import os
import re
import time

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_HOST     = os.getenv("DB_HOST", "db")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_USER     = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_password")
DB_NAME     = os.getenv("DB_NAME", "etl_db")

# Table cible
TABLE_ADEME = "caracteristiques_techniques"

# Clé de conflit pour UPSERT — identifiant naturel d'une ligne ADEME
CONFLICT_COLUMNS = ["marque", "modele", "carburant"]

# Colonnes toujours exclues de l'UPSERT (gérées par PostgreSQL)
EXCLUDED_FROM_INSERT = {"created_at"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connexion
# ---------------------------------------------------------------------------

def get_connection(retries: int = 10, delay: int = 5) -> psycopg2.extensions.connection:
    """Ouvre et retourne une connexion PostgreSQL avec retry (utile au démarrage Docker)."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
            )
            log.info("Connexion PostgreSQL établie (%s@%s:%s/%s)", DB_USER, DB_HOST, DB_PORT, DB_NAME)
            return conn
        except psycopg2.OperationalError as exc:
            if attempt == retries:
                raise
            log.warning(
                "PostgreSQL non disponible (tentative %d/%d) — nouvelle tentative dans %ds... (%s)",
                attempt, retries, delay, exc,
            )
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Mapping dtypes Pandas -> types SQL
# ---------------------------------------------------------------------------

def _pd_dtype_to_sql(dtype) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        # uint8 = OHE encodé → SMALLINT suffit
        if dtype == np.uint8:
            return "SMALLINT"
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "NUMERIC(12, 4)"
    return "VARCHAR(255)"


def _sanitize_identifier(name: str) -> str:
    """Rend un nom de colonne sûr pour SQL (quoted identifier)."""
    return f'"{name}"'


# ---------------------------------------------------------------------------
# ÉTAPE 3a — Création du schéma SQL
# ---------------------------------------------------------------------------

def create_table_if_not_exists(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table_name: str = TABLE_ADEME,
) -> None:
    """
    Génère et exécute un CREATE TABLE IF NOT EXISTS basé sur le schéma
    du DataFrame fourni. Ajoute une contrainte UNIQUE sur les colonnes
    de conflit pour permettre l'UPSERT.

    Les colonnes OHE (uint8) sont créées en SMALLINT NOT NULL DEFAULT 0.
    """
    col_definitions = []

    for col in df.columns:
        if col in EXCLUDED_FROM_INSERT:
            continue
        sql_type = _pd_dtype_to_sql(df[col].dtype)
        safe_col = _sanitize_identifier(col)

        if df[col].dtype == np.uint8:
            # Colonnes OHE — jamais NULL
            col_definitions.append(f"    {safe_col} {sql_type} NOT NULL DEFAULT 0")
        elif col in CONFLICT_COLUMNS:
            # Colonnes de la clé naturelle — NOT NULL requis pour UNIQUE
            col_definitions.append(f"    {safe_col} {sql_type} NOT NULL")
        else:
            col_definitions.append(f"    {safe_col} {sql_type}")

    # Colonnes système
    col_definitions.append('    "created_at"  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP')
    col_definitions.append('    "updated_at"  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP')

    # Contrainte UNIQUE pour UPSERT
    unique_cols = ", ".join(_sanitize_identifier(c) for c in CONFLICT_COLUMNS
                            if c in df.columns)

    ddl = (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        f'    "id"          SERIAL PRIMARY KEY,\n'
        + ",\n".join(col_definitions)
        + (f',\n    UNIQUE ({unique_cols})' if unique_cols else "")
        + "\n);"
    )

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Table '%s' vérifiée / créée avec succès.", table_name)
    log.debug("DDL exécuté :\n%s", ddl)

    # Ajout des colonnes manquantes si la table existait déjà (runs suivants)
    _add_missing_columns(conn, df, table_name)


def _add_missing_columns(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Ajoute les colonnes présentes dans le DataFrame mais absentes de la table
    (utile si de nouvelles catégories OHE apparaissent entre deux runs).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            """,
            (table_name,),
        )
        existing_cols = {row[0] for row in cur.fetchall()}

    new_cols = [c for c in df.columns
                if c not in existing_cols and c not in EXCLUDED_FROM_INSERT]

    if not new_cols:
        return

    with conn.cursor() as cur:
        for col in new_cols:
            sql_type = _pd_dtype_to_sql(df[col].dtype)
            safe_col = _sanitize_identifier(col)
            safe_tbl = _sanitize_identifier(table_name)
            default  = " DEFAULT 0" if df[col].dtype == np.uint8 else ""
            cur.execute(f"ALTER TABLE {safe_tbl} ADD COLUMN IF NOT EXISTS {safe_col} {sql_type}{default};")
            log.info("Colonne ajoutée à '%s' : %s (%s)", table_name, col, sql_type)

    conn.commit()


# ---------------------------------------------------------------------------
# ÉTAPE 3b — UPSERT
# ---------------------------------------------------------------------------

def upsert_dataframe(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table_name: str = TABLE_ADEME,
    batch_size: int = 500,
) -> int:
    """
    Injecte le DataFrame dans PostgreSQL via INSERT ... ON CONFLICT DO UPDATE.

    - Ignore les colonnes non présentes dans la table
    - Traite par batches de `batch_size` pour la mémoire
    - Retourne le nombre de lignes traitées
    """
    # Colonnes effectives : intersection DataFrame ∩ colonnes de la table
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = 'public'",
            (table_name,),
        )
        db_cols = {row[0] for row in cur.fetchall()}

    insert_cols = [c for c in df.columns
                   if c in db_cols and c not in EXCLUDED_FROM_INSERT and c != "id"]

    if not insert_cols:
        raise ValueError("Aucune colonne du DataFrame ne correspond aux colonnes de la table.")

    conflict_cols_present = [c for c in CONFLICT_COLUMNS if c in insert_cols]
    update_cols           = [c for c in insert_cols if c not in conflict_cols_present]

    if not conflict_cols_present:
        log.warning("Colonnes de conflit absentes — UPSERT dégradé en INSERT IGNORE.")
        on_conflict = "ON CONFLICT DO NOTHING"
    else:
        set_clause  = ",\n        ".join(
            f"{_sanitize_identifier(c)} = EXCLUDED.{_sanitize_identifier(c)}"
            for c in update_cols
        )
        set_clause += ',\n        "updated_at" = CURRENT_TIMESTAMP'
        conflict_target = ", ".join(_sanitize_identifier(c) for c in conflict_cols_present)
        on_conflict = f"ON CONFLICT ({conflict_target})\n    DO UPDATE SET\n        {set_clause}"

    col_list    = ", ".join(_sanitize_identifier(c) for c in insert_cols)
    placeholders = ", ".join(["%s"] * len(insert_cols))
    sql = (
        f'INSERT INTO "{table_name}" ({col_list})\n'
        f"VALUES ({placeholders})\n"
        f"{on_conflict};"
    )

    # Remplacement NaN Python -> None (NULL SQL)
    df_clean = df[insert_cols].where(pd.notnull(df[insert_cols]), None)
    records  = df_clean.values.tolist()

    total_inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            psycopg2.extras.execute_batch(cur, sql, batch, page_size=batch_size)
            total_inserted += len(batch)
            log.info("UPSERT batch %d/%d : %d lignes traitées.",
                     i // batch_size + 1, -(-len(records) // batch_size), len(batch))

    conn.commit()
    log.info("UPSERT terminé : %d lignes injectées dans '%s'.", total_inserted, table_name)
    return total_inserted


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run(df: pd.DataFrame) -> None:
    """
    Pipeline de chargement complet :
      DataFrame Analytics-Ready -> PostgreSQL (schéma + UPSERT)
    """
    log.info("=" * 60)
    log.info("LOAD PostgreSQL — Démarrage")
    log.info("Table cible : %s", TABLE_ADEME)
    log.info("=" * 60)

    try:
        conn = get_connection()
    except psycopg2.OperationalError as exc:
        log.error("Impossible de se connecter à PostgreSQL : %s", exc)
        raise

    try:
        create_table_if_not_exists(conn, df, TABLE_ADEME)
        n = upsert_dataframe(conn, df, TABLE_ADEME)
    except Exception as exc:
        conn.rollback()
        log.error("Erreur lors du chargement PostgreSQL : %s", exc)
        raise
    finally:
        conn.close()
        log.info("Connexion PostgreSQL fermée.")

    log.info("=" * 60)
    log.info("LOAD PostgreSQL — Terminé : %d lignes dans '%s'.", n, TABLE_ADEME)
    log.info("=" * 60)


if __name__ == "__main__":
    from transformation import run as transform_run
    df_ready = transform_run()
    run(df_ready)
