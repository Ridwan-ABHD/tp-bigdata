#!/usr/bin/env python
"""
Point d'entrée du pipeline ETL Automobile.
Orchestration séquentielle : Ingestion -> Transformation -> Load
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def main() -> None:
    log.info("######################################################")
    log.info("  PIPELINE ETL — Marché Automobile ADEME")
    log.info("  Source : ADEME, data.gouv.fr (Licence Ouverte v2.0)")
    log.info("######################################################")

    # Etape 1 — Ingestion API ADEME -> MinIO
    log.info(">>> ETAPE 1 : Ingestion ADEME -> MinIO")
    from script.src.ingestion import run as ingest
    ingest()

    # Etape 2 — Transformation MinIO -> DataFrame
    log.info(">>> ETAPE 2 : Transformation & Qualité")
    from script.src.transformation import run as transform
    df = transform()

    # Etape 3 — Load DataFrame -> PostgreSQL
    log.info(">>> ETAPE 3 : Chargement PostgreSQL")
    from script.src.load import run as load
    load(df)

    log.info("######################################################")
    log.info("  PIPELINE ETL — Terminé avec succès.")
    log.info("######################################################")


if __name__ == "__main__":
    main()
