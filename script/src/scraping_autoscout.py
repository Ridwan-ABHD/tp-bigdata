#!/usr/bin/env python
"""
Scraping AutoScout24 — Alternative plus accessible que La Centrale

Ce module scrape les annonces de véhicules d'occasion sur AutoScout24.fr
et stocke les données brutes dans MinIO (Data Lake).
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

BASE_URL = "https://www.autoscout24.fr"
SEARCH_URL = "https://www.autoscout24.fr/lst"
SCRAPING_DELAY = 2
REQUEST_TIMEOUT = 20
MAX_PAGES = 3

# Marques cibles (correspondant au dataset ADEME)
TARGET_BRANDS = ["renault", "peugeot", "citroen", "volkswagen", "bmw", "mercedes-benz", "toyota", "ford"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _extract_number(text: str) -> Optional[float]:
    """Extrait un nombre d'une chaîne."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d]", "", text)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _parse_autoscout_listing(soup: BeautifulSoup) -> list[dict]:
    """Parse les annonces depuis la page de résultats AutoScout24."""
    annonces = []
    
    # AutoScout24 utilise des articles pour les annonces
    articles = soup.find_all("article", attrs={"data-testid": True})
    if not articles:
        articles = soup.find_all("div", class_=lambda c: c and "ListItem" in str(c))
    if not articles:
        articles = soup.select("article")
    
    log.info("Trouvé %d éléments article/div", len(articles))
    
    for article in articles:
        try:
            annonce = _parse_single_article(article)
            if annonce and annonce.get("prix_eur"):
                annonces.append(annonce)
        except Exception as e:
            log.debug("Erreur parsing article: %s", e)
            continue
    
    return annonces


def _parse_single_article(article) -> Optional[dict]:
    """Parse une annonce individuelle."""
    # Titre (marque + modèle)
    title_el = article.find("h2") or article.find("a", class_=lambda c: c and "title" in str(c).lower())
    if not title_el:
        title_el = article.find("span", attrs={"data-testid": lambda v: v and "title" in str(v).lower()})
    
    titre = title_el.get_text(strip=True) if title_el else ""
    if not titre:
        return None
    
    # Prix
    prix_el = article.find("p", attrs={"data-testid": lambda v: v and "price" in str(v).lower()})
    if not prix_el:
        prix_el = article.find(string=re.compile(r"[\d\s]+€"))
        if prix_el:
            prix_el = prix_el.parent
    if not prix_el:
        prix_el = article.find("span", class_=lambda c: c and "price" in str(c).lower())
    
    prix_text = prix_el.get_text(strip=True) if prix_el else ""
    prix_eur = _extract_number(prix_text)
    
    if not prix_eur or prix_eur < 500:
        return None
    
    # Kilométrage
    km_el = article.find(string=re.compile(r"[\d\s]+km", re.I))
    km_text = km_el.strip() if km_el else ""
    kilometrage = _extract_number(km_text)
    
    # Année
    annee = None
    year_match = re.search(r"\b(19|20)\d{2}\b", str(article))
    if year_match:
        annee = int(year_match.group())
        if annee < 1990 or annee > 2026:
            annee = None
    
    # Carburant
    carburant = ""
    fuel_keywords = {
        "essence": "ESSENCE",
        "diesel": "DIESEL", 
        "electrique": "ELECTRIQUE",
        "électrique": "ELECTRIQUE",
        "hybride": "HYBRIDE",
        "gpl": "GPL",
    }
    article_text = article.get_text().lower()
    for kw, fuel in fuel_keywords.items():
        if kw in article_text:
            carburant = fuel
            break
    
    # Marque et modèle depuis le titre
    marque, modele = _extract_marque_modele(titre)
    
    # URL
    link = article.find("a", href=True)
    url = link["href"] if link else ""
    if url and not url.startswith("http"):
        url = BASE_URL + url
    
    return {
        "source": "autoscout24",
        "nom_complet": titre,
        "marque": marque,
        "modele": modele,
        "version": "",
        "annee": annee,
        "carburant": carburant,
        "kilometrage_km": kilometrage,
        "prix_eur": prix_eur,
        "url_annonce": url,
        "scraped_at": datetime.now().isoformat(),
    }


def _extract_marque_modele(titre: str) -> tuple[str, str]:
    """Extrait marque et modèle du titre."""
    titre_upper = titre.upper()
    
    marques_connues = [
        "RENAULT", "PEUGEOT", "CITROEN", "CITROËN", "VOLKSWAGEN", "VW",
        "BMW", "MERCEDES", "MERCEDES-BENZ", "TOYOTA", "FORD", "AUDI",
        "NISSAN", "OPEL", "FIAT", "HYUNDAI", "KIA", "SEAT", "SKODA",
        "VOLVO", "HONDA", "MAZDA", "SUZUKI", "DACIA", "MINI",
    ]
    
    marque = ""
    for m in marques_connues:
        if m in titre_upper:
            marque = m.replace("CITROËN", "CITROEN").replace("VW", "VOLKSWAGEN")
            break
    
    # Modèle = reste après la marque
    modele = ""
    if marque:
        idx = titre_upper.find(marque)
        if idx >= 0:
            reste = titre[idx + len(marque):].strip()
            modele = reste.split()[0] if reste.split() else ""
    
    return marque, modele


def scrape_brand(session: requests.Session, brand: str, max_pages: int = MAX_PAGES) -> list[dict]:
    """Scrape les annonces pour une marque donnée."""
    all_annonces = []
    
    for page in range(1, max_pages + 1):
        params = {
            "atype": "C",
            "cy": "F",
            "desc": "0",
            "make": brand,
            "page": page,
            "sort": "standard",
            "ustate": "N,U",
        }
        
        url = f"{SEARCH_URL}?" + "&".join(f"{k}={v}" for k, v in params.items())
        log.info("Scraping %s page %d: %s", brand.upper(), page, url)
        
        try:
            response = session.get(url, headers=_get_headers(), timeout=REQUEST_TIMEOUT)
            
            if response.status_code != 200:
                log.warning("HTTP %d pour %s", response.status_code, url)
                break
            
            soup = BeautifulSoup(response.text, "html.parser")
            annonces = _parse_autoscout_listing(soup)
            
            log.info("Page %d: %d annonces extraites", page, len(annonces))
            all_annonces.extend(annonces)
            
            if len(annonces) < 10:
                log.info("Moins de 10 annonces, fin de pagination pour %s", brand)
                break
            
            time.sleep(SCRAPING_DELAY)
            
        except Exception as e:
            log.error("Erreur scraping %s page %d: %s", brand, page, e)
            break
    
    return all_annonces


def scrape_all_brands() -> list[dict]:
    """Scrape toutes les marques cibles."""
    session = requests.Session()
    all_annonces = []
    
    for brand in TARGET_BRANDS:
        log.info("=== Scraping marque: %s ===", brand.upper())
        annonces = scrape_brand(session, brand)
        all_annonces.extend(annonces)
        log.info("Total %s: %d annonces", brand.upper(), len(annonces))
        time.sleep(SCRAPING_DELAY)
    
    return all_annonces


def upload_to_minio(annonces: list[dict]) -> str:
    """Upload les annonces vers MinIO."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_SECURE,
    )
    
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info("Bucket '%s' créé", MINIO_BUCKET)
    
    filename = f"raw_autoscout24_{date.today():%Y%m%d}.json"
    object_name = MINIO_PREFIX + filename
    
    payload = {
        "source": "autoscout24.fr",
        "scraped_date": datetime.now().isoformat(),
        "total_annonces": len(annonces),
        "annonces": annonces,
    }
    
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    
    client.put_object(
        MINIO_BUCKET,
        object_name,
        BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    
    log.info("Upload MinIO: %s (%d annonces, %.1f KB)", object_name, len(annonces), len(data) / 1024)
    return object_name


def main():
    """Point d'entrée principal."""
    log.info("=" * 60)
    log.info("SCRAPING AUTOSCOUT24 - Démarrage")
    log.info("=" * 60)
    
    annonces = scrape_all_brands()
    
    if annonces:
        object_name = upload_to_minio(annonces)
        log.info("Scraping terminé: %d annonces -> %s", len(annonces), object_name)
    else:
        log.warning("Aucune annonce récupérée")
    
    return annonces


if __name__ == "__main__":
    main()
