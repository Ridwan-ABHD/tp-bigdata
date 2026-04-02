#!/usr/bin/env python
"""
Ingestion ADEME Car Labelling -> MinIO (Landing Zone / Raw Data)

Source des données : ADEME via data.gouv.fr — Licence Ouverte v2.0 (Etalab)
Mention obligatoire : "Source: ADEME, data.gouv.fr"

Ce script ne transforme AUCUNE donnée.
Il stocke le JSON brut tel que retourné par l'API.
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO

import requests
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER    = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE       = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET       = os.getenv("MINIO_BUCKET", "etl-data")

# Identifiant du dataset ADEME Car Labelling sur data.gouv.fr
# Dataset : "Données d'émissions de CO2 et de consommation de carburant des véhicules neufs"
# Si vide : recherche automatique par mots-clés (voir search_ademe_dataset_id)
ADEME_DATASET_ID   = os.getenv("ADEME_DATASET_ID", "")
ADEME_RESOURCE_ID  = os.getenv("ADEME_RESOURCE_ID", "")  # optionnel : force une ressource précise

# Mots-clés pour la recherche automatique du dataset si ADEME_DATASET_ID est vide
ADEME_SEARCH_QUERY = "emissions co2 consommation carburant vehicules neufs"

DATAGOUV_API_BASE  = "https://www.data.gouv.fr/api/1"
TABULAR_API_BASE   = "https://tabular-api.data.gouv.fr/api/resources"

PAGE_SIZE = 10_000  # enregistrements par page (max tabular API)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Etape 1 — Découverte de la ressource ADEME sur data.gouv.fr
# ---------------------------------------------------------------------------

def search_ademe_dataset_id(query: str = ADEME_SEARCH_QUERY) -> str:
    """
    Recherche le dataset ADEME Car Labelling sur data.gouv.fr par mots-clés.
    Retourne l'ID du premier résultat dont le titre contient 'co2' ou 'carburant'.
    """
    url    = f"{DATAGOUV_API_BASE}/datasets/"
    params = {"q": query, "page_size": 10, "sort": "relevance"}

    log.info("Recherche automatique du dataset ADEME (query: '%s')...", query)
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    results = response.json().get("data", [])
    for dataset in results:
        title = (dataset.get("title") or "").lower()
        slug  = (dataset.get("slug") or "").lower()
        # Critères : le titre ou le slug doit évoquer CO2 + véhicules
        if any(kw in title or kw in slug
               for kw in ("co2", "carburant", "vehicule", "car-labelling", "labelling")):
            did = dataset["id"]
            log.info("  Dataset trouvé : '%s' (id: %s)", dataset.get("title"), did)
            return did

    # Fallback : premier résultat
    if results:
        did = results[0]["id"]
        log.warning("  Aucun dataset CO2/carburant trouvé — utilisation du premier résultat : %s", did)
        return did

    raise ValueError(f"Aucun dataset trouvé pour la recherche '{query}' sur data.gouv.fr")


def get_ademe_resource_id(dataset_id: str) -> tuple[str, str, str]:
    """
    Interroge l'API data.gouv.fr pour obtenir l'ID, le titre et l'URL de
    téléchargement direct de la première ressource CSV/JSON du dataset ADEME.

    Retourne (resource_id, resource_title, download_url).
    """
    url = f"{DATAGOUV_API_BASE}/datasets/{dataset_id}/"
    log.info("Récupération des métadonnées du dataset ADEME...")
    log.info("  URL : %s", url)

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    dataset = response.json()
    resources = dataset.get("resources", [])

    if not resources:
        raise ValueError(f"Aucune ressource trouvée dans le dataset {dataset_id}")

    # Priorité : ressources de type CSV ou JSON, actives
    for resource in resources:
        fmt = (resource.get("format") or "").lower()
        if fmt in ("csv", "json") and resource.get("url"):
            rid          = resource["id"]
            title        = resource.get("title", "Sans titre")
            download_url = resource["url"]
            log.info("  Ressource sélectionnée : %s (%s)", title, rid)
            log.info("  URL de téléchargement   : %s", download_url)
            return rid, title, download_url

    # Fallback : première ressource disponible
    resource     = resources[0]
    rid          = resource["id"]
    title        = resource.get("title", "Sans titre")
    download_url = resource.get("url", "")
    log.warning("  Aucune ressource CSV/JSON trouvée, utilisation de : %s (%s)", title, rid)
    return rid, title, download_url


# ---------------------------------------------------------------------------
# Etape 2 — Extraction des données via l'API Tabulaire data.gouv.fr
# ---------------------------------------------------------------------------

def _fetch_direct_download(download_url: str) -> list[dict]:
    """
    Télécharge directement un fichier CSV depuis l'URL de la ressource
    et le convertit en liste de dictionnaires (aucune transformation de valeur).

    Détecte automatiquement le séparateur (virgule ou point-virgule).
    """
    import csv
    from io import StringIO

    log.info("Téléchargement direct du fichier CSV depuis : %s", download_url)
    response = requests.get(download_url, timeout=120, stream=True)
    response.raise_for_status()

    # Décodage — ADEME utilise le plus souvent latin-1 ou utf-8
    encoding = response.encoding or "utf-8"
    try:
        content = response.content.decode(encoding)
    except (UnicodeDecodeError, LookupError):
        content = response.content.decode("latin-1", errors="replace")

    # Détection automatique du séparateur sur la première ligne
    first_line = content.split("\n")[0]
    separator  = ";" if first_line.count(";") > first_line.count(",") else ","
    log.info("  Séparateur détecté : '%s'", separator)

    reader  = csv.DictReader(StringIO(content), delimiter=separator)
    records = list(reader)
    log.info("  %d enregistrements chargés depuis le CSV.", len(records))
    return records


def fetch_ademe_data(resource_id: str, fallback_url: str = "") -> list[dict]:
    """
    Tente l'API tabulaire data.gouv.fr en priorité (pagination automatique).
    En cas d'erreur 400/404 (ressource non indexée), bascule sur le
    téléchargement direct du CSV.

    Retourne une liste de dictionnaires (enregistrements bruts).
    """
    all_records = []
    page = 1

    log.info("Début de l'extraction ADEME via API tabulaire (ressource : %s)...", resource_id)

    while True:
        url    = f"{TABULAR_API_BASE}/{resource_id}/data/"
        params = {"page": page, "page_size": PAGE_SIZE}

        log.info("  Requête page %d (taille max : %d)...", page, PAGE_SIZE)
        response = requests.get(url, params=params, timeout=60)

        # Ressource non indexée dans la tabular API → fallback téléchargement direct
        if response.status_code in (400, 404):
            log.warning(
                "  API tabulaire indisponible (HTTP %d) — "
                "basculement vers téléchargement direct CSV.",
                response.status_code,
            )
            if not fallback_url:
                raise ValueError(
                    "API tabulaire en erreur et aucune URL de fallback disponible."
                )
            return _fetch_direct_download(fallback_url)

        response.raise_for_status()

        payload = response.json()
        records = payload.get("data", [])
        all_records.extend(records)

        meta  = payload.get("meta", {})
        total = meta.get("total", len(all_records))

        log.info("  %d enregistrements reçus (total cumulé : %d / %d)",
                 len(records), len(all_records), total)

        if len(records) < PAGE_SIZE or len(all_records) >= total:
            break

        page += 1

    log.info("Extraction terminée — %d enregistrements récupérés.", len(all_records))
    return all_records


# ---------------------------------------------------------------------------
# Etape 3 — Stockage dans MinIO (Landing Zone, données brutes immuables)
# ---------------------------------------------------------------------------

def get_minio_client() -> Minio:
    """Initialise et retourne un client MinIO."""
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_SECURE,
    )
    log.info("Client MinIO initialisé (endpoint : %s, secure : %s)",
             MINIO_ENDPOINT, MINIO_SECURE)
    return client


def ensure_bucket_exists(client: Minio, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info("Bucket créé : %s", bucket)
    else:
        log.info("Bucket existant : %s", bucket)


def upload_to_minio(client: Minio, bucket: str, records: list[dict],
                    resource_title: str) -> str:
    """
    Sérialise les enregistrements en JSON et les uploade dans MinIO.

    Chemin de stockage : raw/ademe/raw_ademe_data_YYYYMMDD.json
    Retourne le nom de l'objet créé.
    """
    date_str    = datetime.now().strftime("%Y%m%d")
    object_name = f"raw/ademe/raw_ademe_data_{date_str}.json"

    # Enveloppe de métadonnées (non-transformation, uniquement du contexte)
    payload = {
        "source":        "ADEME, data.gouv.fr",
        "licence":       "Licence Ouverte v2.0 (Etalab)",
        "resource":      resource_title,
        "extracted_at":  datetime.now().isoformat(),
        "record_count":  len(records),
        "data":          records,
    }

    raw_bytes  = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    byte_count = len(raw_bytes)
    buffer     = BytesIO(raw_bytes)

    log.info("Upload vers MinIO en cours...")
    log.info("  Bucket      : %s", bucket)
    log.info("  Objet       : %s", object_name)
    log.info("  Taille      : %.2f Mo", byte_count / 1_048_576)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=buffer,
        length=byte_count,
        content_type="application/json",
    )

    log.info("Fichier uploadé avec succès : s3://%s/%s", bucket, object_name)
    return object_name


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Pipeline d'ingestion complet :
      API ADEME (data.gouv.fr) -> MinIO Landing Zone
    """
    log.info("=" * 60)
    log.info("INGESTION ADEME — Démarrage")
    log.info("Source : ADEME, data.gouv.fr (Licence Ouverte v2.0)")
    log.info("=" * 60)

    # --- Etape 1 : Connexion MinIO ---
    try:
        minio_client = get_minio_client()
        ensure_bucket_exists(minio_client, MINIO_BUCKET)
    except S3Error as exc:
        log.error("Erreur MinIO lors de la connexion/création du bucket : %s", exc)
        raise
    except Exception as exc:
        log.error("Erreur inattendue lors de l'initialisation MinIO : %s", exc)
        raise

    # --- Etape 2 : Résolution de la ressource ADEME ---
    try:
        resource_id = ADEME_RESOURCE_ID or None

        if not resource_id:
            dataset_id = ADEME_DATASET_ID or search_ademe_dataset_id()
            resource_id, resource_title, download_url = get_ademe_resource_id(dataset_id)
        else:
            resource_title = f"Ressource forcée via ADEME_RESOURCE_ID ({resource_id})"
            download_url   = ""
            log.info("Utilisation de la ressource forcée : %s", resource_id)

    except requests.exceptions.ConnectionError as exc:
        log.error("Impossible de joindre data.gouv.fr : %s", exc)
        raise
    except requests.exceptions.HTTPError as exc:
        log.error("Erreur HTTP lors de la récupération du dataset ADEME : %s", exc)
        raise

    # --- Etape 3 : Extraction des données ---
    try:
        records = fetch_ademe_data(resource_id, fallback_url=download_url)

        if not records:
            log.warning("Aucun enregistrement retourné par l'API ADEME. Arrêt.")
            return

    except requests.exceptions.ConnectionError as exc:
        log.error("Impossible de joindre l'API tabulaire data.gouv.fr : %s", exc)
        raise
    except requests.exceptions.HTTPError as exc:
        log.error("Erreur HTTP lors de l'extraction des données ADEME : %s", exc)
        raise

    # --- Etape 4 : Upload MinIO ---
    try:
        object_name = upload_to_minio(minio_client, MINIO_BUCKET, records, resource_title)
    except S3Error as exc:
        log.error("Erreur MinIO lors de l'upload : %s", exc)
        raise

    log.info("=" * 60)
    log.info("INGESTION ADEME — Terminée avec succès")
    log.info("Objet stocké : s3://%s/%s", MINIO_BUCKET, object_name)
    log.info("=" * 60)


if __name__ == "__main__":
    run()
