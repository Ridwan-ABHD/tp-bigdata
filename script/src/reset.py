#!/usr/bin/env python
"""
Purge complète — MinIO (raw/ademe/) + PostgreSQL (caracteristiques_techniques)
Usage : docker exec tp-bigdata python -m script.src.reset
"""

import logging
import os

import psycopg2
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def purge_minio() -> None:
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    bucket = os.getenv("MINIO_BUCKET", "etl-data")
    prefix = "raw/ademe/"

    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    if not objects:
        log.info("MinIO : aucun objet à supprimer dans s3://%s/%s", bucket, prefix)
        return

    for obj in objects:
        client.remove_object(bucket, obj.object_name)
        log.info("Supprimé : s3://%s/%s", bucket, obj.object_name)

    log.info("MinIO purgé : %d fichier(s) supprimé(s).", len(objects))


def purge_postgres() -> None:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("DB_PASSWORD", "etl_password"),
        dbname=os.getenv("DB_NAME", "etl_db"),
    )
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS caracteristiques_techniques;")
    conn.commit()
    conn.close()
    log.info("PostgreSQL : table 'caracteristiques_techniques' supprimée.")


if __name__ == "__main__":
    log.info("=== PURGE — Remise à zéro du pipeline ADEME ===")
    purge_minio()
    purge_postgres()
    log.info("=== Purge terminée. Prêt pour un nouveau run propre. ===")
