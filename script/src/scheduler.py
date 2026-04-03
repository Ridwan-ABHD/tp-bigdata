#!/usr/bin/env python
"""
Scheduler automatique pour le pipeline ETL.
Rafraîchit les données toutes les heures (configurable).
"""

import logging
import os
import time
from datetime import datetime
from threading import Thread, Event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Intervalle de rafraîchissement en secondes (1 heure = 3600s)
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "3600"))

# Event pour contrôler l'arrêt propre
_stop_event = Event()

# Timestamp du dernier rafraîchissement
_last_refresh: datetime | None = None


def get_last_refresh() -> datetime | None:
    """Retourne le timestamp du dernier rafraîchissement."""
    return _last_refresh


def run_pipeline() -> dict:
    """
    Exécute le pipeline ETL complet.
    Retourne un dict avec les stats de l'exécution.
    """
    global _last_refresh
    
    stats = {
        "success": False,
        "started_at": datetime.now(),
        "ended_at": None,
        "ademe_count": 0,
        "scraping_count": 0,
        "occasion_count": 0,
        "error": None,
    }
    
    try:
        log.info("=" * 60)
        log.info("RAFRAICHISSEMENT DES DONNEES")
        log.info("=" * 60)
        
        # Etape 1 — Ingestion API ADEME -> MinIO
        log.info(">>> ETAPE 1 : Ingestion ADEME -> MinIO")
        from script.src.ingestion import run as ingest
        ingest()
        
        # Etape 2 — Scraping marché occasion -> MinIO
        log.info(">>> ETAPE 2 : Scraping Marché Occasion -> MinIO")
        from script.src.scraping_autoscout import main as scrape_main
        annonces = scrape_main()
        n_annonces = len(annonces) if annonces else 0
        stats["scraping_count"] = n_annonces
        
        if n_annonces == 0:
            log.warning("Scraping : aucune annonce collectée. Pipeline continue avec ADEME uniquement.")
        
        # Etape 3 — Transformation MinIO -> DataFrame
        log.info(">>> ETAPE 3 : Transformation & Qualité ADEME")
        from script.src.transformation import run as transform
        df = transform()
        stats["ademe_count"] = len(df) if df is not None else 0
        
        # Etape 4 — Load DataFrame ADEME -> PostgreSQL
        log.info(">>> ETAPE 4 : Chargement PostgreSQL (ADEME)")
        from script.src.load import run as load
        load(df)
        
        # Etape 5 — Nettoyage marché + Fuzzy Join ADEME + Load annonces_occasion
        log.info(">>> ETAPE 5 : Transformation Marché + Fuzzy Join + PostgreSQL")
        if n_annonces > 0:
            from script.src.transformation_scraping import run as transform_market
            n_loaded = transform_market()
            stats["occasion_count"] = n_loaded
        else:
            log.warning("Etape 5 ignorée : aucune annonce scrapée.")
        
        stats["success"] = True
        _last_refresh = datetime.now()
        stats["ended_at"] = _last_refresh
        
        log.info("=" * 60)
        log.info("RAFRAICHISSEMENT TERMINE")
        log.info(f"   ADEME    : {stats['ademe_count']} véhicules")
        log.info(f"   Scraping : {stats['scraping_count']} annonces brutes")
        log.info(f"   Occasion : {stats['occasion_count']} annonces chargées")
        log.info("=" * 60)
        
    except Exception as e:
        stats["error"] = str(e)
        stats["ended_at"] = datetime.now()
        log.error(f"Erreur pendant le rafraîchissement : {e}")
    
    return stats


def scheduler_loop():
    """Boucle principale du scheduler."""
    global _last_refresh
    
    log.info(f"Scheduler demarré (intervalle : {REFRESH_INTERVAL}s = {REFRESH_INTERVAL/3600:.1f}h)")
    
    # Premier rafraîchissement au démarrage
    run_pipeline()
    
    while not _stop_event.is_set():
        # Attendre l'intervalle (ou l'arrêt)
        log.info(f"Prochain rafraîchissement dans {REFRESH_INTERVAL}s...")
        _stop_event.wait(REFRESH_INTERVAL)
        
        if not _stop_event.is_set():
            run_pipeline()
    
    log.info("Scheduler arrêté.")


def start_scheduler() -> Thread:
    """
    Démarre le scheduler en background thread.
    Retourne le thread pour pouvoir l'arrêter.
    """
    _stop_event.clear()
    t = Thread(target=scheduler_loop, daemon=True, name="ETL-Scheduler")
    t.start()
    return t


def stop_scheduler():
    """Arrête proprement le scheduler."""
    _stop_event.set()


# Point d'entrée standalone
if __name__ == "__main__":
    try:
        scheduler_loop()
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur.")
        stop_scheduler()
