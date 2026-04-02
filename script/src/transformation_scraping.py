#!/usr/bin/env python
"""
transformation_scraping.py — Nettoyage Marché, Fuzzy Join ADEME, Load PostgreSQL

Pipeline complet :
  MinIO (raw/scraping/raw_market_data_YYYYMMDD.json)
    -> Nettoyage & normalisation (pandas + unicodedata)
    -> Fuzzy Join sur caracteristiques_techniques (difflib.SequenceMatcher)
    -> UPSERT dans annonces_occasion (psycopg2)

Défi principal : aligner "Clio IV 1.5 dCi Business" avec "CLIO" en base ADEME.
Stratégie :
  1. Normalisation agressive des deux côtés (minuscules, sans accents, sans finitions)
  2. Match exact marque d'abord (filtre rapide)
  3. SequenceMatcher sur modèle pour trouver le meilleur candidat ADEME (seuil 0.75)
  4. Traçabilité du score de correspondance dans la table finale
"""

import json
import logging
import os
import re
import time
import unicodedata
from datetime import date, datetime
from difflib import SequenceMatcher
from io import BytesIO
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# MinIO
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE        = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET        = os.getenv("MINIO_BUCKET", "etl-data")
MINIO_PREFIX        = "raw/scraping/"

# PostgreSQL
DB_HOST     = os.getenv("DB_HOST", "db")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_USER     = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_password")
DB_NAME     = os.getenv("DB_NAME", "etl_db")

# Tables
TABLE_ANNONCES   = "annonces_occasion"
TABLE_ADEME      = "caracteristiques_techniques"
CONFLICT_COLUMN  = "url_annonce"    # clé UPSERT unique

# Fuzzy matching
FUZZY_THRESHOLD  = 0.72   # score SequenceMatcher minimum pour valider un match
FUZZY_MARQUE_THRESHOLD = 0.80  # seuil plus strict sur la marque

# Seuils qualité IQR (gouvernance §2.2 — adaptés au marché occasion)
PRIX_MIN_ABSOLU   = 500      # € — en-dessous = erreur de saisie
PRIX_MAX_ABSOLU   = 500_000  # € — au-dessus = véhicule de collection hors scope
KM_MAX_ABSOLU     = 700_000  # km — au-dessus = incohérence

# Finitions / versions à supprimer lors de la normalisation du modèle
# pour aligner "CLIO IV Business" -> "clio" (qui correspond à "CLIO" ADEME)
_TRIM_PATTERN = re.compile(
    r"\b("
    r"limited|business|sport|sportline|premium|executive|luxury|edition|"
    r"pack|sw|break|berline|suv|crossover|crossback|cabriolet|coupe|"
    r"monospace|mpv|estate|touring|allroad|kombi|variant|"
    r"phase|restyl[e]?|facelift|"
    r"iv|iii|ii|vi|vii|viii|"      # chiffres romains
    r"1[.]?[0-9]|2[.]?[0-9]|"     # cylindrées (1.6, 2.0...)
    r"tdi|hdi|dci|dti|cdti|jtd|"  # moteurs diesel
    r"tfsi|tsi|gti|gtd|gts|"       # moteurs essence
    r"e-tech|plug-in|phev|hev|bev|"
    r"\d{2,4}(ch|cv|kw|bhp)"       # puissances (90ch, 150ch, 110cv)
    r")\b",
    re.IGNORECASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Normalisation des chaînes (cœur du fuzzy join)
# ---------------------------------------------------------------------------

def _remove_accents(s: str) -> str:
    """Supprime les diacritiques : é->e, è->e, ç->c, etc."""
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def normalize_brand(s: str) -> str:
    """
    Normalisation légère de la marque :
    - Minuscules, sans accents, sans ponctuation
    - Conservatrice : ne supprime pas de mots (la marque est courte)
    Exemples : "Mercedes-Benz" -> "mercedes benz"  /  "PEUGEOT" -> "peugeot"
    """
    if not s:
        return ""
    s = _remove_accents(str(s).lower().strip())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_model(s: str) -> str:
    """
    Normalisation agressive du modèle pour le fuzzy match :
    - Minuscules, sans accents
    - Suppression des finitions/versions (Business, Sport, phase 2, 1.6 TDI…)
    - Suppression de la ponctuation
    Exemples :
      "Clio IV 1.5 dCi 90ch Business" -> "clio"
      "308 SW BlueHDi 130" -> "308"
      "Série 3 320d Sport Line" -> "serie 3 320d"  (on garde "série 3" car c'est le modèle)
    """
    if not s:
        return ""
    s = _remove_accents(str(s).lower().strip())
    s = _TRIM_PATTERN.sub(" ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _similarity(a: str, b: str) -> float:
    """Score de similarité SequenceMatcher entre deux chaînes normalisées."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# ---------------------------------------------------------------------------
# Chargement MinIO
# ---------------------------------------------------------------------------

def _get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_SECURE,
    )


def load_raw_from_minio() -> pd.DataFrame:
    """
    Charge le fichier raw_market_data_*.json le plus récent depuis MinIO.
    Retourne un DataFrame avec les annonces brutes.
    """
    client = _get_minio_client()
    log.info("MinIO — recherche du fichier marché le plus récent...")

    objects = list(client.list_objects(MINIO_BUCKET, prefix=MINIO_PREFIX, recursive=True))
    json_objects = [o for o in objects if o.object_name.endswith(".json")]

    if not json_objects:
        raise FileNotFoundError(
            f"Aucun fichier JSON trouvé dans s3://{MINIO_BUCKET}/{MINIO_PREFIX}"
        )

    latest = sorted(json_objects, key=lambda o: o.last_modified, reverse=True)[0]
    log.info("Fichier sélectionné : s3://%s/%s", MINIO_BUCKET, latest.object_name)

    response = client.get_object(MINIO_BUCKET, latest.object_name)
    raw_bytes = response.read()
    response.close()
    response.release_conn()

    envelope = json.loads(raw_bytes.decode("utf-8"))
    records  = envelope.get("data", [])

    log.info("  %d annonces brutes chargées.", len(records))
    log.info("  Source : %s", envelope.get("source", "inconnue"))
    log.info("  Extrait le : %s", envelope.get("extracted_at", "inconnu"))

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Nettoyage & Normalisation
# ---------------------------------------------------------------------------

def _parse_prix(val) -> Optional[float]:
    """
    Convertit un prix brut en float.
    Gère : "25 000 €", "25.000", "25,990", 25000, None, ""
    """
    if val is None or val == "":
        return None
    s = re.sub(r"[€$£\s\xa0]", "", str(val))   # supprimer devise et espaces insécables
    # Identifier séparateur décimal vs milliers
    # "25.000" = 25000 (milliers), "25.5" = 25.5 (décimal)
    if "," in s and "." in s:
        # Format "25.000,00" -> séparateur milliers=point, décimal=virgule
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            # "25,990" -> milliers -> "25990"
            s = s.replace(",", "")
        else:
            # "25,99" -> décimal -> "25.99"
            s = s.replace(",", ".")
    elif "." in s:
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            s = s.replace(".", "")   # "25.000" -> "25000"
    try:
        val_f = float(s)
        return val_f if val_f > 0 else None
    except (ValueError, TypeError):
        return None


def _parse_kilometrage(val) -> Optional[float]:
    """
    Convertit un kilométrage brut en float.
    Gère : "85 000 km", "85000km", "85.000 km", 85000
    """
    if val is None or val == "":
        return None
    s = re.sub(r"[km\s\xa0]", "", str(val).lower())
    s = re.sub(r"[^0-9,\.]", "", s)
    if "." in s and len(s.split(".")[-1]) == 3:
        s = s.replace(".", "")
    s = s.replace(",", "")
    try:
        val_f = float(s)
        return val_f if val_f >= 0 else None
    except (ValueError, TypeError):
        return None


def _parse_annee(val) -> Optional[int]:
    """Extrait une année valide (1990-2030) depuis une valeur quelconque."""
    if val is None or val == "":
        return None
    match = re.search(r"\b(19[9]\d|20[0-2]\d)\b", str(val))
    if match:
        return int(match.group(1))
    return None


def _extract_marque_from_nom(nom: str, ademe_marques: set[str]) -> str:
    """
    Quand la marque est vide (records CSS), tente de l'extraire du nom_complet
    en cherchant les marques ADEME en tête de chaîne.
    Ex: "RENAULT Clio IV Business" -> "renault"
    """
    if not nom:
        return ""
    nom_norm = normalize_brand(nom)
    for marque in ademe_marques:
        if nom_norm.startswith(marque):
            return marque
    # Fallback : premier mot du nom
    return nom_norm.split()[0] if nom_norm else ""


def _extract_modele_from_nom(nom: str, marque: str) -> str:
    """
    Extrait le modèle depuis nom_complet en retirant la marque en tête.
    Ex: nom="renault clio iv business", marque="renault" -> "clio iv business"
    """
    if not nom or not marque:
        return ""
    nom_norm = normalize_brand(nom)
    if nom_norm.startswith(marque):
        rest = nom_norm[len(marque):].strip()
        return rest.split()[0] if rest else ""   # premier mot = modèle
    return ""


def clean_data(df: pd.DataFrame, ademe_marques: Optional[set] = None) -> pd.DataFrame:
    """
    Nettoyage complet des annonces brutes :
    1. Suppression des doublons stricts
    2. Parsing des types numériques (prix, km, année)
    3. Reconstruction marque/modèle depuis nom_complet si vides (records CSS)
    4. Filtrage absolu (prix, km hors limites physiques)
    5. Filtrage IQR sur prix et kilométrage
    6. Normalisation des chaînes pour la jointure fuzzy
    """
    df = df.copy()
    n_initial = len(df)
    log.info("-" * 50)
    log.info("NETTOYAGE — %d annonces brutes", n_initial)

    # 1. Doublons stricts
    before = len(df)
    df = df.drop_duplicates()
    log.info("  Doublons supprimés : %d", before - len(df))

    # 2. Parsing numériques
    df["prix_eur"]       = df["prix_brut"].apply(_parse_prix)
    df["kilometrage_km"] = df["kilometrage"].apply(_parse_kilometrage)
    df["annee"]          = df["annee"].apply(_parse_annee)

    # 3. Reconstruction marque/modèle pour records CSS (marque = "")
    if ademe_marques is None:
        ademe_marques = set()
    ademe_marques_norm = {normalize_brand(m) for m in ademe_marques}

    mask_empty_marque = df["marque"].fillna("").str.strip() == ""
    n_empty = mask_empty_marque.sum()
    if n_empty > 0:
        log.info("  Reconstruction marque/modèle depuis nom_complet : %d records", n_empty)
        df.loc[mask_empty_marque, "marque"] = df.loc[mask_empty_marque, "nom_complet"].apply(
            lambda n: _extract_marque_from_nom(n, ademe_marques_norm)
        )
        df.loc[mask_empty_marque, "modele"] = df.apply(
            lambda row: _extract_modele_from_nom(
                row["nom_complet"],
                normalize_brand(row["marque"])
            ) if mask_empty_marque[row.name] else row["modele"],
            axis=1,
        )

    # 4. Filtres absolus (valeurs physiquement impossibles)
    before = len(df)
    df = df[df["prix_eur"].isna() | (
        (df["prix_eur"] >= PRIX_MIN_ABSOLU) & (df["prix_eur"] <= PRIX_MAX_ABSOLU)
    )]
    log.info("  Filtre absolu prix [%d, %d]€ : %d lignes rejetées.",
             PRIX_MIN_ABSOLU, PRIX_MAX_ABSOLU, before - len(df))

    before = len(df)
    df = df[df["kilometrage_km"].isna() | (df["kilometrage_km"] <= KM_MAX_ABSOLU)]
    log.info("  Filtre absolu km [0, %d] : %d lignes rejetées.", KM_MAX_ABSOLU, before - len(df))

    # 5. IQR sur prix et kilométrage (gouvernance §2.2)
    for col, label in [("prix_eur", "Prix"), ("kilometrage_km", "Kilométrage")]:
        series = df[col].dropna()
        if len(series) < 10:
            continue
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr    = q3 - q1
        lower  = q1 - 1.5 * iqr
        upper  = q3 + 1.5 * iqr
        before = len(df)
        df = df[df[col].isna() | ((df[col] >= lower) & (df[col] <= upper))]
        log.info("  IQR %s [%.0f, %.0f] : %d outliers rejetés.",
                 label, lower, upper, before - len(df))

    # 6. Colonnes normalisées pour la jointure fuzzy (conservées pour traçabilité)
    df["marque_norm"] = df["marque"].apply(normalize_brand)
    df["modele_norm"] = df["modele"].apply(normalize_model)

    log.info("NETTOYAGE terminé : %d -> %d annonces (-%d).",
             n_initial, len(df), n_initial - len(df))
    return df


# ---------------------------------------------------------------------------
# Connexion PostgreSQL
# ---------------------------------------------------------------------------

def get_connection(retries: int = 10, delay: int = 5) -> psycopg2.extensions.connection:
    """Connexion PostgreSQL avec retry (robustesse démarrage Docker)."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME,
            )
            log.info("Connexion PostgreSQL établie (%s@%s:%s/%s)",
                     DB_USER, DB_HOST, DB_PORT, DB_NAME)
            return conn
        except psycopg2.OperationalError as exc:
            if attempt == retries:
                raise
            log.warning("PG non disponible (tentative %d/%d) — retry dans %ds...",
                        attempt, retries, delay)
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Chargement du référentiel ADEME (pour le fuzzy join)
# ---------------------------------------------------------------------------

def load_ademe_reference(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Charge depuis PostgreSQL les colonnes nécessaires au fuzzy join :
    id, marque, modele (+ versions normalisées pré-calculées).

    Retourne un DataFrame indexé pour accélérer la recherche.
    """
    query = f"""
        SELECT id, marque, modele
        FROM "{TABLE_ADEME}"
        WHERE marque IS NOT NULL AND modele IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    if not rows:
        log.warning("Table '%s' vide ou inaccessible — fuzzy join impossible.", TABLE_ADEME)
        return pd.DataFrame(columns=["id", "marque", "modele", "marque_norm", "modele_norm"])

    ref = pd.DataFrame(rows, columns=["id", "marque", "modele"])
    ref["marque_norm"] = ref["marque"].apply(normalize_brand)
    ref["modele_norm"] = ref["modele"].apply(normalize_model)

    # Index groupé par marque pour accélérer la recherche
    log.info("Référentiel ADEME : %d entrées chargées (%d marques uniques).",
             len(ref), ref["marque_norm"].nunique())
    return ref


# ---------------------------------------------------------------------------
# Fuzzy Join ADEME
# ---------------------------------------------------------------------------

def _find_best_match(
    marque_norm: str,
    modele_norm: str,
    ref_by_marque: dict[str, pd.DataFrame],
) -> tuple[Optional[int], float]:
    """
    Trouve le meilleur correspondant ADEME pour une annonce.

    Stratégie en 2 étapes :
    1. Filtrage par marque (exact ou fuzzy ≥ FUZZY_MARQUE_THRESHOLD)
       pour réduire le sous-ensemble de candidats.
    2. Fuzzy match du modèle via SequenceMatcher dans ce sous-ensemble.

    Retourne (ademe_id, score) ou (None, 0.0) si aucun match suffisant.
    """
    if not marque_norm:
        return None, 0.0

    # --- Étape 1 : trouver la marque ADEME la plus proche ---
    best_marque_score = 0.0
    best_marque_key   = None

    for ademe_marque in ref_by_marque:
        score = _similarity(marque_norm, ademe_marque)
        if score > best_marque_score:
            best_marque_score = score
            best_marque_key   = ademe_marque

    if best_marque_score < FUZZY_MARQUE_THRESHOLD or best_marque_key is None:
        return None, 0.0   # marque trop éloignée — pas de correspondance

    candidates = ref_by_marque[best_marque_key]

    # --- Étape 2 : fuzzy match sur le modèle parmi les candidats ---
    if not modele_norm:
        # Pas de modèle — retourner la marque seule avec score partiel
        return int(candidates.iloc[0]["id"]), best_marque_score * 0.5

    best_modele_score = 0.0
    best_id           = None

    for _, row in candidates.iterrows():
        score = _similarity(modele_norm, row["modele_norm"])
        if score > best_modele_score:
            best_modele_score = score
            best_id           = int(row["id"])

    # Score global = moyenne pondérée (marque pèse 40%, modèle 60%)
    global_score = 0.4 * best_marque_score + 0.6 * best_modele_score

    if global_score >= FUZZY_THRESHOLD:
        return best_id, round(global_score, 4)

    return None, round(global_score, 4)


def enrich_with_ademe(
    df: pd.DataFrame,
    ademe_ref: pd.DataFrame,
) -> pd.DataFrame:
    """
    Applique le fuzzy join ADEME sur tout le DataFrame des annonces.

    Ajoute deux colonnes :
    - ademe_id     : int ou None (FK vers caracteristiques_techniques)
    - match_score  : float [0, 1] — score de correspondance (traçabilité)
    """
    df = df.copy()

    if ademe_ref.empty:
        log.warning("Référentiel ADEME vide — toutes les annonces seront orphelines.")
        df["ademe_id"]    = None
        df["match_score"] = 0.0
        return df

    # Pré-indexer le référentiel par marque normalisée (dict de DataFrames)
    ref_by_marque: dict[str, pd.DataFrame] = {
        marque: group.reset_index(drop=True)
        for marque, group in ademe_ref.groupby("marque_norm")
    }

    ademe_ids    = []
    match_scores = []

    for _, row in df.iterrows():
        ademe_id, score = _find_best_match(
            row["marque_norm"],
            row["modele_norm"],
            ref_by_marque,
        )
        ademe_ids.append(ademe_id)
        match_scores.append(score)

    df["ademe_id"]    = ademe_ids
    df["match_score"] = match_scores

    # --- Rapport de correspondance ---
    n_total   = len(df)
    n_matched = df["ademe_id"].notna().sum()
    n_orphan  = n_total - n_matched

    log.info("=" * 50)
    log.info("FUZZY JOIN — Résultats :")
    log.info("  Total annonces  : %d", n_total)
    log.info("  Avec match ADEME: %d (%.1f%%)", n_matched, n_matched / n_total * 100 if n_total else 0)
    log.info("  Orphelines      : %d (%.1f%%)", n_orphan,  n_orphan  / n_total * 100 if n_total else 0)
    if n_matched:
        log.info("  Score moyen (matchées) : %.3f", df.loc[df["ademe_id"].notna(), "match_score"].mean())
    log.info("=" * 50)

    return df


# ---------------------------------------------------------------------------
# Chargement PostgreSQL — annonces_occasion
# ---------------------------------------------------------------------------

_DDL_ANNONCES = f"""
CREATE TABLE IF NOT EXISTS "{TABLE_ANNONCES}" (
    "id"              SERIAL PRIMARY KEY,
    "url_annonce"     VARCHAR(500) NOT NULL,
    "marque"          VARCHAR(100),
    "modele"          VARCHAR(100),
    "version"         VARCHAR(200),
    "annee"           INTEGER,
    "prix_eur"        NUMERIC(10, 2),
    "kilometrage_km"  NUMERIC(10, 0),
    "carburant"       VARCHAR(50),
    "ademe_id"        INTEGER,
    "match_score"     NUMERIC(5, 4),
    "source"          VARCHAR(100),
    "scraped_date"    DATE,
    "created_at"      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at"      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("url_annonce")
);
"""

# Colonnes à insérer (ordre fixe pour l'UPSERT)
_INSERT_COLS = [
    "url_annonce", "marque", "modele", "version",
    "annee", "prix_eur", "kilometrage_km", "carburant",
    "ademe_id", "match_score", "source", "scraped_date",
]

_UPSERT_SQL = f"""
INSERT INTO "{TABLE_ANNONCES}"
    ("url_annonce", "marque", "modele", "version",
     "annee", "prix_eur", "kilometrage_km", "carburant",
     "ademe_id", "match_score", "source", "scraped_date")
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT ("url_annonce")
DO UPDATE SET
    "marque"         = EXCLUDED."marque",
    "modele"         = EXCLUDED."modele",
    "version"        = EXCLUDED."version",
    "annee"          = EXCLUDED."annee",
    "prix_eur"       = EXCLUDED."prix_eur",
    "kilometrage_km" = EXCLUDED."kilometrage_km",
    "carburant"      = EXCLUDED."carburant",
    "ademe_id"       = EXCLUDED."ademe_id",
    "match_score"    = EXCLUDED."match_score",
    "source"         = EXCLUDED."source",
    "scraped_date"   = EXCLUDED."scraped_date",
    "updated_at"     = CURRENT_TIMESTAMP;
"""


def _prepare_row(row: pd.Series) -> tuple:
    """Mappe une ligne du DataFrame vers le tuple attendu par l'UPSERT."""

    def _safe_int(v):
        try:
            return int(v) if pd.notna(v) else None
        except (ValueError, TypeError):
            return None

    def _safe_float(v):
        try:
            return float(v) if pd.notna(v) else None
        except (ValueError, TypeError):
            return None

    return (
        str(row.get("url_annonce", ""))[:500] or None,   # url_annonce (clé)
        str(row.get("marque",  ""))[:100] or None,
        str(row.get("modele",  ""))[:100] or None,
        str(row.get("version", ""))[:200] or None,
        _safe_int(row.get("annee")),
        _safe_float(row.get("prix_eur")),
        _safe_float(row.get("kilometrage_km")),
        str(row.get("carburant", ""))[:50] or None,
        _safe_int(row.get("ademe_id")),
        _safe_float(row.get("match_score")),
        str(row.get("source", "La Centrale"))[:100],
        row.get("scraped_date") or date.today(),
    )


def upsert_annonces(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    batch_size: int = 500,
) -> int:
    """
    Crée la table annonces_occasion si nécessaire et injecte les données
    via INSERT ... ON CONFLICT (url_annonce) DO UPDATE.

    Les annonces sans URL sont ignorées (clé UPSERT obligatoire).
    Retourne le nombre de lignes traitées.
    """
    # Création de la table
    with conn.cursor() as cur:
        cur.execute(_DDL_ANNONCES)
    conn.commit()
    log.info("Table '%s' vérifiée / créée.", TABLE_ANNONCES)

    # Filtrage : url_annonce obligatoire pour l'UPSERT
    df_valid = df[
        df["url_annonce"].notna() & (df["url_annonce"].astype(str).str.strip() != "")
    ].copy()

    n_ignored = len(df) - len(df_valid)
    if n_ignored:
        log.warning("  %d annonces ignorées (url_annonce manquante).", n_ignored)

    if df_valid.empty:
        log.warning("Aucune annonce valide à insérer.")
        return 0

    # Ajout colonne source/date si absentes
    if "source" not in df_valid.columns:
        df_valid["source"] = "La Centrale"
    if "scraped_date" not in df_valid.columns:
        df_valid["scraped_date"] = date.today()

    records = [_prepare_row(row) for _, row in df_valid.iterrows()]
    total   = 0

    with conn.cursor() as cur:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            psycopg2.extras.execute_batch(cur, _UPSERT_SQL, batch, page_size=batch_size)
            total += len(batch)
            log.info("  UPSERT batch %d/%d : %d lignes.",
                     i // batch_size + 1, -(-len(records) // batch_size), len(batch))

    conn.commit()
    log.info("UPSERT terminé : %d annonces dans '%s'.", total, TABLE_ANNONCES)
    return total


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run() -> int:
    """
    Pipeline complet :
      MinIO (raw scraping) -> Nettoyage -> Fuzzy Join ADEME -> PostgreSQL

    Retourne le nombre d'annonces chargées dans annonces_occasion.
    """
    log.info("=" * 60)
    log.info("TRANSFORMATION SCRAPING — Démarrage")
    log.info("=" * 60)

    # 1. Chargement données brutes MinIO
    try:
        df_raw = load_raw_from_minio()
    except (S3Error, FileNotFoundError) as exc:
        log.error("Données marché introuvables dans MinIO : %s", exc)
        log.info("Exécute d'abord script/src/scraping.py pour alimenter MinIO.")
        raise

    # 2. Connexion PostgreSQL (nécessaire pour référentiel + load)
    conn = get_connection()

    try:
        # 3. Chargement du référentiel ADEME (marques connues pour reconstruction)
        ademe_ref = load_ademe_reference(conn)
        ademe_marques = set(ademe_ref["marque"].dropna().unique())

        # 4. Nettoyage & normalisation
        df_clean = clean_data(df_raw, ademe_marques=ademe_marques)

        if df_clean.empty:
            log.error("DataFrame vide après nettoyage — vérifier la qualité du scraping.")
            return 0

        # 5. Fuzzy Join ADEME
        df_enriched = enrich_with_ademe(df_clean, ademe_ref)

        # 6. UPSERT dans annonces_occasion
        n = upsert_annonces(conn, df_enriched)

    except Exception as exc:
        conn.rollback()
        log.error("Erreur pipeline scraping : %s", exc)
        raise
    finally:
        conn.close()
        log.info("Connexion PostgreSQL fermée.")

    log.info("=" * 60)
    log.info("TRANSFORMATION SCRAPING — Terminée : %d annonces chargées.", n)
    log.info("=" * 60)
    return n


if __name__ == "__main__":
    run()
