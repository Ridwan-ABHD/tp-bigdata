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
    log.info("  PIPELINE ETL — Marché Automobile")
    log.info("  Sources : ADEME (data.gouv.fr) + La Centrale (scraping)")
    log.info("######################################################")

    # Etape 1 — Ingestion API ADEME -> MinIO
    log.info(">>> ETAPE 1 : Ingestion ADEME -> MinIO")
    from script.src.ingestion import run as ingest
    ingest()

    # Etape 2 — Scraping marché occasion -> MinIO
    log.info(">>> ETAPE 2 : Scraping Marché Occasion -> MinIO")
    from script.src.scraping import run as scrape
    n_annonces = scrape()
    if n_annonces == 0:
        log.warning("Scraping : aucune annonce collectée (site inaccessible ?). "
                    "Le pipeline continue avec les données ADEME uniquement.")

    # Etape 3 — Transformation MinIO -> DataFrame
    log.info(">>> ETAPE 3 : Transformation & Qualité ADEME")
    from script.src.transformation import run as transform
    df = transform()

    # Etape 4 — Load DataFrame ADEME -> PostgreSQL
    log.info(">>> ETAPE 4 : Chargement PostgreSQL (ADEME)")
    from script.src.load import run as load
    load(df)

    # Etape 5 — Nettoyage marché + Fuzzy Join ADEME + Load annonces_occasion
    log.info(">>> ETAPE 5 : Transformation Marché + Fuzzy Join + PostgreSQL")
    if n_annonces > 0:
        from script.src.transformation_scraping import run as transform_market
        n_loaded = transform_market()
    else:
        n_loaded = 0
        log.warning("Etape 5 ignorée : aucune annonce scrapée à traiter.")

    log.info("######################################################")
    log.info("  PIPELINE ETL — Terminé avec succès.")
    log.info("  ADEME    : table 'caracteristiques_techniques' à jour.")
    log.info("  Marché   : %d annonces brutes collectées (MinIO).", n_annonces)
    log.info("  Occasion : %d annonces chargées dans 'annonces_occasion'.", n_loaded)
    log.info("######################################################")


if __name__ == "__main__":
    main()
