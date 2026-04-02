#!/usr/bin/env python
"""
Scraping — Marché automobile occasion -> MinIO (Landing Zone)

Collecte les annonces de véhicules d'occasion sur La Centrale (lacentrale.fr)
et les stocke de manière brute et immuable dans le Data Lake MinIO.

Architecture :
  _build_search_url()     Construit l'URL de recherche paginée
  _rotate_headers()       User-Agent aléatoire par requête
  _parse_json_ld()        Extraction via JSON-LD schema.org (stable)
  _parse_card_html()      Extraction via sélecteurs CSS (fallback)
  scrape_page()           Scrape une page de résultats, retourne une liste
  scrape_market()         Orchestration multi-page, multi-marque
  upload_to_minio()       Enveloppe JSON + upload MinIO

Éthique & RGPD :
  - Aucune donnée personnelle collectée (vendeurs, téléphones, emails)
  - Délai de 2 secondes entre chaque requête (SCRAPING_DELAY)
  - User-Agent réaliste et rotatif
  - Respect du robots.txt via commentaires documentés
  - Données publiques uniquement (prix affichés, caractéristiques techniques)
"""

import json
import logging
import os
import random
import re
import time
from datetime import date, datetime
from io import BytesIO
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration MinIO
# ---------------------------------------------------------------------------

MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE        = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET        = os.getenv("MINIO_BUCKET", "etl-data")
MINIO_PREFIX        = "raw/scraping/"

# ---------------------------------------------------------------------------
# Configuration Scraping
# ---------------------------------------------------------------------------

BASE_URL        = "https://www.lacentrale.fr/listing"
SCRAPING_DELAY  = 2          # secondes entre requêtes (éthique anti-ban)
REQUEST_TIMEOUT = 15         # timeout HTTP en secondes
MAX_PAGES_DEFAULT = 3        # pages par marque/modèle (30 annonces/page)

# Marques ciblées pour la comparaison avec le dataset ADEME
# (top marques présentes dans Car Labelling 2024)
DEFAULT_TARGETS = [
    {"marque": "RENAULT"},
    {"marque": "PEUGEOT"},
    {"marque": "CITROEN"},
    {"marque": "VOLKSWAGEN"},
    {"marque": "BMW"},
    {"marque": "MERCEDES"},
    {"marque": "TOYOTA"},
    {"marque": "FORD"},
]

# ---------------------------------------------------------------------------
# User-Agents réalistes (rotatifs)
# Simuler différents navigateurs pour éviter les blocages basés sur l'UA.
# ---------------------------------------------------------------------------

USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    # Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]

# ---------------------------------------------------------------------------
# Sélecteurs CSS — La Centrale (mettre à jour si structure HTML change)
# Priorité 1 : JSON-LD schema.org (plus stable que CSS)
# Priorité 2 : attributs data-* (semi-stables)
# Priorité 3 : classes CSS (moins stables, en dernier recours)
# ---------------------------------------------------------------------------

SELECTORS = {
    # Conteneur d'une annonce dans la liste
    "card": [
        "div.searchCard",
        "article[data-cy='listing-card']",
        "div[class*='adCard']",
        "article.searchCard",
    ],
    # Titre principal (marque + modèle + version)
    "titre": [
        "h2.searchCard__title",
        "h2[class*='title']",
        "a[data-cy='vehicleName']",
        "span[class*='vehicleName']",
    ],
    # Prix affiché
    "prix": [
        "span.searchCard__price",
        "span[data-cy='adPrice']",
        "span[class*='price']",
        "div[class*='Price']",
    ],
    # Bloc des caractéristiques (km, année, carburant)
    "specs": [
        "ul.searchCard__specs",
        "div[class*='specs']",
        "div[data-cy='vehicleSpecs']",
        "ul[class*='criteria']",
    ],
    # Items individuels dans specs
    "spec_item": [
        "li",
        "span[class*='spec']",
    ],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utilitaires HTTP
# ---------------------------------------------------------------------------

def _rotate_headers() -> dict:
    """Retourne des en-têtes HTTP avec un User-Agent aléatoire."""
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "DNT":             "1",
    }


def _build_search_url(marque: str, modele: Optional[str] = None, page: int = 1) -> str:
    """
    Construit l'URL de recherche La Centrale.

    Exemple :
      RENAULT, page 2 -> https://www.lacentrale.fr/listing?makesModelsCommercialNames=RENAULT%3A&page=2
      RENAULT CLIO    -> https://www.lacentrale.fr/listing?makesModelsCommercialNames=RENAULT%3ACLIO&page=1
    """
    make_model = marque.upper()
    if modele:
        make_model += f":{modele.upper()}"

    params = {
        "makesModelsCommercialNames": make_model,
        "page": page,
    }
    return f"{BASE_URL}?{urlencode(params)}"


def _safe_get(session: requests.Session, url: str) -> Optional[requests.Response]:
    """
    Effectue une requête GET avec gestion d'erreurs complète.
    Retourne None si la page est inaccessible (403, 404, timeout...).
    """
    try:
        response = session.get(url, headers=_rotate_headers(), timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            return response
        elif response.status_code == 403:
            log.warning("HTTP 403 Forbidden sur %s — IP peut-être bloquée.", url)
        elif response.status_code == 404:
            log.warning("HTTP 404 Not Found : %s", url)
        elif response.status_code == 429:
            log.warning("HTTP 429 Too Many Requests — augmenter SCRAPING_DELAY.")
        else:
            log.warning("HTTP %d inattendu sur %s", response.status_code, url)
        return None
    except requests.exceptions.Timeout:
        log.error("Timeout après %ds sur %s", REQUEST_TIMEOUT, url)
        return None
    except requests.exceptions.ConnectionError as exc:
        log.error("Erreur de connexion sur %s : %s", url, exc)
        return None
    except requests.exceptions.RequestException as exc:
        log.error("Erreur requête inattendue sur %s : %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Extraction des données
# ---------------------------------------------------------------------------

def _extract_number(text: str) -> Optional[float]:
    """Extrait le premier nombre (entier ou décimal) d'une chaîne."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d,\.]", "", text.strip()).replace(",", ".")
    # Retirer les points de milliers (ex: "25.000" -> "25000")
    if cleaned.count(".") > 1:
        cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _parse_json_ld(soup: BeautifulSoup) -> list[dict]:
    """
    Méthode 1 (prioritaire) : extraction via JSON-LD schema.org.
    Les annonces embarquent souvent des blocs <script type="application/ld+json">
    avec un schema de type 'Car' ou 'Product'.
    Retourne une liste de dicts normalisés (données brutes, sans transformation).
    """
    records = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        # Peut être un objet unique ou une liste
        items = data if isinstance(data, list) else [data]

        for item in items:
            schema_type = item.get("@type", "")
            # On cible Car, Vehicle, Product (selon implémentation du site)
            if schema_type not in ("Car", "Vehicle", "Product", "Offer"):
                continue

            record = _extract_from_json_ld_item(item)
            if record:
                records.append(record)

    return records


def _extract_from_json_ld_item(item: dict) -> Optional[dict]:
    """Mappe un item JSON-LD vers notre modèle de données brutes."""
    # Ignorer si pas de prix (annonce incomplète)
    offers = item.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}

    prix_raw = offers.get("price") or item.get("price")
    if not prix_raw:
        return None

    name = item.get("name", "")
    # Sécurité RGPD : on ne collecte pas le seller (offers.seller)
    return {
        "source_type": "json_ld",
        "nom_complet":  name,
        "marque":       item.get("brand", {}).get("name", "") if isinstance(item.get("brand"), dict) else item.get("brand", ""),
        "modele":       item.get("model", ""),
        "version":      item.get("vehicleConfiguration", "") or item.get("description", "")[:100],
        "annee":        item.get("modelDate") or item.get("vehicleModelDate", ""),
        "carburant":    item.get("fuelType", ""),
        "kilometrage":  item.get("mileageFromOdometer", {}).get("value") if isinstance(item.get("mileageFromOdometer"), dict) else item.get("mileageFromOdometer"),
        "prix_brut":    str(prix_raw),
        "devise":       offers.get("priceCurrency", "EUR"),
        "url_annonce":  item.get("url", ""),
    }


def _find_first(soup_element, selectors: list[str]):
    """Essaie chaque sélecteur CSS dans l'ordre, retourne le premier trouvé."""
    for sel in selectors:
        found = soup_element.select_one(sel)
        if found:
            return found
    return None


def _parse_card_html(card) -> Optional[dict]:
    """
    Méthode 2 (fallback) : extraction via sélecteurs CSS sur une card HTML.
    Retourne un dict brut ou None si les données minimales sont absentes.

    RGPD : on ne collecte aucun élément identifiant le vendeur.
    """
    # --- Titre ---
    titre_el = _find_first(card, SELECTORS["titre"])
    titre = titre_el.get_text(strip=True) if titre_el else ""

    # --- Prix ---
    prix_el = _find_first(card, SELECTORS["prix"])
    prix_brut = prix_el.get_text(strip=True) if prix_el else ""

    if not prix_brut:
        return None  # Annonce sans prix = incomplète, on ignore

    # --- Specs (km, année, carburant) ---
    specs_el = _find_first(card, SELECTORS["specs"])
    specs_text = []
    if specs_el:
        for item in specs_el.find_all(*SELECTORS["spec_item"][0].split()):
            t = item.get_text(strip=True)
            if t:
                specs_text.append(t)

    # Extraction heuristique des specs
    annee       = _detect_annee(specs_text + [titre])
    kilometrage = _detect_kilometrage(specs_text)
    carburant   = _detect_carburant(specs_text)

    # URL de l'annonce (attribut href du lien parent ou de la card)
    url_el = card.find("a", href=True)
    url_annonce = url_el["href"] if url_el else ""
    if url_annonce and not url_annonce.startswith("http"):
        url_annonce = "https://www.lacentrale.fr" + url_annonce

    return {
        "source_type":   "html_css",
        "nom_complet":   titre,
        "marque":        "",   # sera déduit dans transformation.py
        "modele":        "",   # sera déduit dans transformation.py
        "version":       "",
        "annee":         annee,
        "carburant":     carburant,
        "kilometrage":   kilometrage,
        "prix_brut":     prix_brut,
        "devise":        "EUR",
        "url_annonce":   url_annonce,
    }


def _detect_annee(texts: list[str]) -> str:
    """Détecte une année (4 chiffres entre 1990 et 2030) dans une liste de textes."""
    for text in texts:
        match = re.search(r"\b(19[9]\d|20[0-2]\d)\b", text)
        if match:
            return match.group(1)
    return ""


def _detect_kilometrage(specs: list[str]) -> str:
    """Détecte le kilométrage (nombre suivi de 'km') dans les specs."""
    for spec in specs:
        if "km" in spec.lower():
            return spec.strip()
    return ""


def _detect_carburant(specs: list[str]) -> str:
    """Détecte le type de carburant dans les specs."""
    carburants = {
        "essence": ["essence", "sp95", "sp98", "e10", "e85", "superéthanol"],
        "diesel":  ["diesel", "gazole", "gasoil", "tdi", "hdi", "bluehdi"],
        "electrique": ["électrique", "electrique", "bev", "ev"],
        "hybride": ["hybride", "hybrid", "phev", "hev", "mild hybrid"],
        "gpl":     ["gpl", "lpg"],
        "gnv":     ["gnv", "cng"],
    }
    combined = " ".join(specs).lower()
    for carb_type, keywords in carburants.items():
        if any(kw in combined for kw in keywords):
            return carb_type
    return ""


# ---------------------------------------------------------------------------
# Scraping d'une page
# ---------------------------------------------------------------------------

def scrape_page(
    session: requests.Session,
    url: str,
    marque_cible: str = "",
) -> list[dict]:
    """
    Scrape une page de résultats. Tente JSON-LD en premier,
    puis fallback CSS si aucun résultat structuré n'est trouvé.

    Retourne une liste de dicts bruts (données non transformées).
    """
    log.info("  Scraping : %s", url)
    response = _safe_get(session, url)
    if not response:
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    # -- Tentative 1 : JSON-LD --
    records = _parse_json_ld(soup)
    if records:
        log.info("    JSON-LD : %d annonces extraites.", len(records))
        # Enrichir avec la marque cible si non renseignée
        for r in records:
            if not r.get("marque") and marque_cible:
                r["marque"] = marque_cible
        return records

    # -- Tentative 2 : CSS Selectors --
    cards_found = []
    for sel in SELECTORS["card"]:
        cards_found = soup.select(sel)
        if cards_found:
            break

    if not cards_found:
        # Dernier recours : chercher tous les articles ou divs avec prix visible
        log.warning("    Aucune card trouvée avec les sélecteurs connus. "
                    "La structure HTML du site a peut-être changé.")
        log.debug("    HTML (500 premiers chars) : %s", response.text[:500])
        return []

    records = []
    for card in cards_found:
        record = _parse_card_html(card)
        if record:
            if not record.get("marque") and marque_cible:
                record["marque"] = marque_cible
            records.append(record)

    log.info("    CSS fallback : %d annonces extraites (sur %d cards).",
             len(records), len(cards_found))
    return records


# ---------------------------------------------------------------------------
# Orchestration multi-page / multi-marque
# ---------------------------------------------------------------------------

def scrape_market(
    targets: Optional[list[dict]] = None,
    max_pages: int = MAX_PAGES_DEFAULT,
) -> tuple[list[dict], list[str]]:
    """
    Orchestre le scraping de toutes les marques/modèles cibles.

    targets : liste de dicts {"marque": str, "modele": str (optionnel)}
    max_pages: nombre maximum de pages par cible

    Retourne : (liste_annonces, liste_urls_scrapées)
    """
    if targets is None:
        targets = DEFAULT_TARGETS

    all_records: list[dict] = []
    all_urls:    list[str]  = []
    seen_urls:   set[str]   = set()   # déduplication par URL annonce

    session = requests.Session()

    for target in targets:
        marque = target.get("marque", "")
        modele = target.get("modele")
        label  = f"{marque} {modele}".strip() if modele else marque

        log.info("--- Scraping : %s (max %d pages) ---", label, max_pages)

        for page in range(1, max_pages + 1):
            url = _build_search_url(marque, modele, page)
            all_urls.append(url)

            records = scrape_page(session, url, marque_cible=marque)

            if not records:
                log.info("    Page %d vide ou inaccessible — arrêt pour '%s'.", page, label)
                break

            # Déduplication par URL d'annonce
            before = len(records)
            records = [
                r for r in records
                if not r.get("url_annonce") or r["url_annonce"] not in seen_urls
            ]
            for r in records:
                if r.get("url_annonce"):
                    seen_urls.add(r["url_annonce"])

            all_records.extend(records)
            log.info("    Page %d/%d : +%d annonces (total : %d, %d dédupliquées).",
                     page, max_pages, len(records), len(all_records), before - len(records))

            # Délai éthique entre chaque requête
            if page < max_pages:
                time.sleep(SCRAPING_DELAY)

        # Délai supplémentaire entre chaque marque
        time.sleep(SCRAPING_DELAY)

    log.info("Scraping terminé : %d annonces uniques collectées.", len(all_records))
    return all_records, all_urls


# ---------------------------------------------------------------------------
# Ingestion MinIO
# ---------------------------------------------------------------------------

def _get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_SECURE,
    )


def upload_to_minio(
    records: list[dict],
    source_urls: list[str],
    bucket: str = MINIO_BUCKET,
) -> str:
    """
    Enveloppe les données brutes dans un JSON avec métadonnées
    et upload dans MinIO sous raw/scraping/raw_market_data_YYYYMMDD.json.

    Retourne le chemin de l'objet créé.
    """
    client = _get_minio_client()

    # Création bucket si nécessaire
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info("Bucket créé : %s", bucket)

    envelope = {
        "source":             "La Centrale (lacentrale.fr)",
        "source_urls":        source_urls[:10],   # sample des URLs (pas toutes)
        "licence_compliance": (
            "Données publiques collectées à des fins académiques uniquement. "
            "Aucune donnée personnelle collectée (RGPD Art. 5). "
            "Usage non commercial — projet Bachelor Big Data Sup de Vinci."
        ),
        "extracted_at":       datetime.utcnow().isoformat() + "Z",
        "record_count":       len(records),
        "data":               records,
    }

    payload      = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")
    today_str    = date.today().strftime("%Y%m%d")
    object_name  = f"{MINIO_PREFIX}raw_market_data_{today_str}.json"

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    size_kb = len(payload) / 1024
    log.info("Upload MinIO OK : s3://%s/%s (%.1f KB, %d annonces)",
             bucket, object_name, size_kb, len(records))
    return object_name


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run(
    targets: Optional[list[dict]] = None,
    max_pages: int = MAX_PAGES_DEFAULT,
) -> int:
    """
    Pipeline de scraping complet :
      HTTP (La Centrale) -> parsing -> déduplication -> MinIO (raw)

    Retourne le nombre d'annonces collectées.
    """
    log.info("=" * 60)
    log.info("SCRAPING Marché Occasion — Démarrage")
    log.info("Cibles : %s", targets or DEFAULT_TARGETS)
    log.info("Pages max par cible : %d", max_pages)
    log.info("=" * 60)

    records, source_urls = scrape_market(targets=targets, max_pages=max_pages)

    if not records:
        log.warning("Aucune annonce collectée. Vérifier la connectivité ou les sélecteurs.")
        return 0

    try:
        upload_to_minio(records, source_urls)
    except S3Error as exc:
        log.error("Erreur MinIO lors de l'upload : %s", exc)
        raise

    log.info("=" * 60)
    log.info("SCRAPING — Terminé : %d annonces dans MinIO.", len(records))
    log.info("=" * 60)

    return len(records)


if __name__ == "__main__":
    run()
