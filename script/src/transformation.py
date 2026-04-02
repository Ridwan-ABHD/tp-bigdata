#!/usr/bin/env python
"""
Transformation ADEME — MinIO (Raw) -> DataFrame Analytics-Ready

Gouvernance appliquée (§2.2) :
  - Rejet  : lignes sans marque ou modèle
  - Imputation : médiane par groupe (marque, modele)
  - Outliers : méthode IQR + seuils métier
  - Encoding : OneHotEncoder sur carburant et boite_vitesses
"""

import json
import logging
import os
import re
from io import BytesIO

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from sklearn.preprocessing import OneHotEncoder

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE        = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET        = os.getenv("MINIO_BUCKET", "etl-data")
MINIO_PREFIX        = "raw/ademe/"

# Mapping des noms de colonnes bruts ADEME Car Labelling -> noms canoniques
# Source : dataset "ademe-car-labelling" sur data.gouv.fr (labels français officiels)
# Clés = résultat de _to_snake_case() appliqué aux en-têtes CSV réels
# Exemple : "Libellé modèle" -> "libell_modle" (é/è supprimés par regex [^a-z0-9_])
COLUMN_MAPPING = {
    # --- Identification véhicule ---
    # "Marque" (avec BOM \ufeff) -> "marque" (BOM supprimé par regex)
    "marque":                      "marque",
    # "Libellé modèle" -> "libell_modle"
    "libell_modle":                "modele",
    # "Modèle" -> "modle"
    "modle":                       "modele",
    # "Description Commerciale" -> "description_commerciale"
    "description_commerciale":     "version",
    # --- Carburant ---
    # "Energie" -> "energie"
    "energie":                     "carburant",
    # --- Boîte de vitesses ---
    # "Type de boite" -> "type_de_boite"
    "type_de_boite":               "boite_vitesses",
    # --- Puissance ---
    # "Puissance maximale" -> "puissance_maximale"
    "puissance_maximale":          "puissance_kw",
    # "Puissance fiscale" -> "puissance_fiscale"
    "puissance_fiscale":           "puissance_fiscale",
    # --- CO2 (colonne principale WLTP/NEDC) ---
    # "Essai CO2 type 1" -> "essai_co2_type_1"
    "essai_co2_type_1":            "co2_g_km",
    # Variantes min/max CO2 (colonnes supplémentaires conservées telles quelles)
    "co2_vitesse_mixte_min":       "co2_mixte_min_g_km",
    "co2_vitesse_mixte_max":       "co2_mixte_max_g_km",
    # --- Consommations (L/100km) ---
    # "Conso vitesse mixte min/max" -> "conso_vitesse_mixte_min/max"
    "conso_vitesse_mixte_min":     "conso_mixte_min_l100",
    "conso_vitesse_mixte_max":     "conso_mixte_max_l100",
    "conso_vitesse_basse_min":     "conso_basse_min_l100",
    "conso_vitesse_basse_max":     "conso_basse_max_l100",
    "conso_vitesse_haute_min":     "conso_haute_min_l100",
    "conso_vitesse_haute_max":     "conso_haute_max_l100",
    # --- Masse ---
    # "Masse OM Min/Max" -> "masse_om_min/max"
    "masse_om_min":                "masse_min_kg",
    "masse_om_max":                "masse_max_kg",
    # --- Polluants ---
    # "Essai NOx" -> "essai_nox"
    "essai_nox":                   "nox_mg_km",
    # "Essai Particules" -> "essai_particules"
    "essai_particules":            "particules_mg_km",
    # "Essai HC" -> "essai_hc"
    "essai_hc":                    "hc_mg_km",
    # "Essai HC+NOx" -> "essai_hcnox" (+ supprimé par regex)
    "essai_hcnox":                 "hcnox_mg_km",
    # --- Prix (colonne bonus) ---
    # "Prix véhicule" -> "prix_vhicule" (é supprimé par regex)
    "prix_vhicule":                "prix_neuf_eur",
    # --- Fallbacks anciens codes UTAC (compatibilité fichiers anciens) ---
    "lib_mrq_utac":                "marque",
    "lib_mrq":                     "marque",
    "lib_mod":                     "modele",
    "lib_mod_doss":                "modele",
    "dscom":                       "version",
    "cod_cbr":                     "carburant",
    "lib_cbr":                     "carburant",
    "carburant":                   "carburant",
    "typ_boite_nb_rapp":           "boite_vitesses",
    "boite_vitesses":              "boite_vitesses",
    "co2":                         "co2_g_km",
    "co2_mixte":                   "co2_g_km",
    "co2_g_km":                    "co2_g_km",
    "puiss_max":                   "puissance_kw",
    "puissance_kw":                "puissance_kw",
    "puiss_fisc_fr":               "puissance_fiscale",
    "puiss_fisc":                  "puissance_fiscale",
    "masse_ordma_min":             "masse_min_kg",
    "masse_ordma_max":             "masse_max_kg",
    "nox":                         "nox_mg_km",
    "ptcl":                        "particules_mg_km",
}

# Colonnes numériques — imputation médiane + filtrage IQR
NUMERIC_COLS = [
    "co2_g_km", "puissance_kw", "puissance_fiscale",
    "conso_mixte_min_l100", "conso_mixte_max_l100",
    "conso_basse_min_l100", "conso_basse_max_l100",
    "conso_haute_min_l100", "conso_haute_max_l100",
    "masse_min_kg", "masse_max_kg",
    "nox_mg_km", "particules_mg_km",
    "prix_neuf_eur",
]

# Colonnes identitaires — rejet absolu si vides (gouvernance §2.2)
CRITICAL_COLS = ["marque", "modele"]

# Colonnes catégorielles à encoder (OneHotEncoder)
CATEGORICAL_COLS = ["carburant", "boite_vitesses"]

# Seuils métier (gouvernance §2.2 — s'appliquent ici au CO2 uniquement,
# les seuils prix/km s'appliqueront dans transformation.py scraping)
CO2_MAX_METIER   = 500   # g/km — au-delà = incohérence ADEME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chargement depuis MinIO
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
    Récupère le fichier raw_ademe_data_*.json le plus récent du bucket MinIO
    et retourne un DataFrame avec les données brutes (clé 'data' de l'enveloppe).
    """
    client = _get_minio_client()
    log.info("Connexion MinIO — recherche du fichier ADEME le plus récent...")

    objects = list(client.list_objects(MINIO_BUCKET, prefix=MINIO_PREFIX, recursive=True))
    ademe_objects = [o for o in objects if o.object_name.endswith(".json")]

    if not ademe_objects:
        raise FileNotFoundError(
            f"Aucun fichier JSON trouvé dans s3://{MINIO_BUCKET}/{MINIO_PREFIX}"
        )

    # Tri par date de modification décroissante — on prend le plus récent
    latest = sorted(ademe_objects, key=lambda o: o.last_modified, reverse=True)[0]
    log.info("Fichier sélectionné : s3://%s/%s", MINIO_BUCKET, latest.object_name)

    response = client.get_object(MINIO_BUCKET, latest.object_name)
    raw_bytes = response.read()
    response.close()
    response.release_conn()

    envelope = json.loads(raw_bytes.decode("utf-8"))
    records  = envelope.get("data", [])

    log.info("  %d enregistrements bruts chargés.", len(records))
    log.info("  Source déclarée : %s", envelope.get("source", "inconnue"))
    log.info("  Extrait le      : %s", envelope.get("extracted_at", "inconnu"))

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# ÉTAPE 1 — EDA (Exploratory Data Analysis)
# ---------------------------------------------------------------------------

def run_eda(df: pd.DataFrame) -> None:
    """
    Analyse exploratoire complète. Logge :
      - Dimensions et aperçu des colonnes
      - Taux de valeurs manquantes par colonne
      - Statistiques descriptives sur les numériques
      - Top 10 des valeurs catégorielles
      - Détection IQR des outliers sur CO2 et puissance
    """
    log.info("=" * 60)
    log.info("EDA — Analyse Exploratoire des Données ADEME")
    log.info("=" * 60)

    log.info("Dimensions : %d lignes x %d colonnes", *df.shape)
    log.info("Colonnes disponibles : %s", list(df.columns))

    # --- Valeurs manquantes ---
    log.info("-" * 40)
    log.info("Taux de valeurs manquantes par colonne :")
    null_rates = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
    for col, rate in null_rates[null_rates > 0].items():
        flag = " << COLONNE CRITIQUE — REJET" if col in CRITICAL_COLS else ""
        log.info("  %-40s : %6.2f%%%s", col, rate, flag)

    if null_rates.sum() == 0:
        log.info("  Aucune valeur manquante détectée.")

    # --- Statistiques descriptives ---
    numeric_df = df.select_dtypes(include=[np.number])
    if not numeric_df.empty:
        log.info("-" * 40)
        log.info("Statistiques descriptives (colonnes numériques) :")
        stats = numeric_df.describe().T
        for col, row in stats.iterrows():
            log.info(
                "  %-35s  min=%8.2f  median=%8.2f  max=%8.2f  std=%8.2f",
                col, row["min"], row["50%"], row["max"], row["std"],
            )

    # --- Top valeurs catégorielles ---
    cat_df = df.select_dtypes(include=["object"])
    if not cat_df.empty:
        log.info("-" * 40)
        log.info("Distribution des variables catégorielles (top 10) :")
        for col in cat_df.columns:
            counts = df[col].value_counts().head(10)
            log.info("  %s (%d valeurs uniques) :", col, df[col].nunique())
            for val, cnt in counts.items():
                log.info("    %-30s : %d", str(val)[:30], cnt)

    # --- Détection IQR — Outliers sur CO2 et Puissance ---
    log.info("-" * 40)
    log.info("Détection d'outliers (méthode IQR) :")

    for col in ["co2_g_km", "essai_co2_type_1", "co2", "puissance_kw", "puissance_maximale", "puiss_max"]:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr    = q3 - q1
        lower  = q1 - 1.5 * iqr
        upper  = q3 + 1.5 * iqr
        n_out  = ((series < lower) | (series > upper)).sum()
        log.info(
            "  %-20s  Q1=%7.2f  Q3=%7.2f  IQR=%7.2f  bornes=[%7.2f, %7.2f]  outliers=%d (%.1f%%)",
            col, q1, q3, iqr, lower, upper, n_out, n_out / len(series) * 100,
        )

    log.info("=" * 60)


# ---------------------------------------------------------------------------
# ÉTAPE 2 — Transformation & Qualité
# ---------------------------------------------------------------------------

def _to_snake_case(name: str) -> str:
    """Convertit un nom de colonne quelconque en snake_case valide."""
    s = str(name).strip().lower()
    s = re.sub(r"[\s\-/\(\)°%\.]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col_unknown"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Convertit tous les noms de colonnes en snake_case.
    2. Applique le mapping canonique ADEME -> noms métier.
    Logge les colonnes non reconnues pour faciliter le debug.
    """
    df = df.copy()

    # snake_case d'abord
    df.columns = [_to_snake_case(c) for c in df.columns]

    # Mapping canonique
    mapped    = {}
    unmapped  = []
    for col in df.columns:
        if col in COLUMN_MAPPING:
            mapped[col] = COLUMN_MAPPING[col]
        else:
            unmapped.append(col)

    df = df.rename(columns=mapped)

    if unmapped:
        log.warning("Colonnes non reconnues dans le mapping (conservées telles quelles) : %s", unmapped)

    # Fusionner les colonnes dupliquées (ex: libell_modle + modle -> modele)
    # Pour chaque groupe de doublons, on garde la première occurrence en remplissant ses NaN
    # avec les valeurs des colonnes suivantes du même nom.
    dup_cols = df.columns[df.columns.duplicated(keep=False)].unique().tolist()
    for col in dup_cols:
        positions = [i for i, c in enumerate(df.columns) if c == col]
        # Fusionner: première colonne prend la valeur de la suivante si elle est NaN
        base = df.iloc[:, positions[0]].copy()
        for pos in positions[1:]:
            base = base.fillna(df.iloc[:, pos])
        df.iloc[:, positions[0]] = base
        log.info("Colonnes dupliquées '%s' fusionnées (%d occurrences).", col, len(positions))

    # Supprimer les doublons de colonnes (garder la première)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    log.info("Colonnes après normalisation : %s", list(df.columns))
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les règles de qualité de la gouvernance §2.2 :

    1. Suppression des doublons stricts
    2. Rejet des lignes sans marque ou modèle (colonnes critiques)
    3. Conversion des colonnes numériques attendues
    4. Imputation par médiane groupée (marque, modele)
    5. Filtrage IQR sur les numériques disponibles
    6. Seuil métier CO2 <= CO2_MAX_METIER
    """
    df = df.copy()
    n_initial = len(df)

    # 1. Doublons
    n_before = len(df)
    df = df.drop_duplicates()
    log.info("Doublons supprimés : %d", n_before - len(df))

    # 2. Rejet lignes critiques manquantes
    for col in CRITICAL_COLS:
        if col not in df.columns:
            log.warning("Colonne critique '%s' absente du DataFrame — rejet impossible.", col)
            continue
        n_before = len(df)
        df = df[df[col].notna() & (df[col].astype(str).str.strip() != "")]
        n_rejected = n_before - len(df)
        if n_rejected:
            log.warning("Rejet (colonne '%s' vide) : %d lignes exclues.", col, n_rejected)

    # 3. Conversion des colonnes numériques
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Imputation par médiane groupée (marque, modele)
    group_cols = [c for c in ["marque", "modele"] if c in df.columns]
    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue
        n_null = df[col].isna().sum()
        if n_null == 0:
            continue

        if group_cols:
            df[col] = df.groupby(group_cols)[col].transform(
                lambda x: x.fillna(x.median())
            )
        # Fallback global si le groupe entier est NaN
        global_median = df[col].median()
        df[col] = df[col].fillna(global_median)

        n_imputed = n_null - df[col].isna().sum()
        log.info("Imputation médiane '%s' : %d valeurs renseignées.", col, n_imputed)

    # 5. Filtrage IQR
    for col in NUMERIC_COLS:
        if col not in df.columns or df[col].isna().all():
            continue
        q1  = df[col].quantile(0.25)
        q3  = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        n_before = len(df)
        df = df[(df[col].isna()) | ((df[col] >= lower) & (df[col] <= upper))]
        n_filtered = n_before - len(df)
        if n_filtered:
            log.info("IQR '%s' [%.2f, %.2f] : %d outliers supprimés.", col, lower, upper, n_filtered)

    # 6. Seuil métier CO2
    if "co2_g_km" in df.columns:
        n_before = len(df)
        df = df[df["co2_g_km"].isna() | (df["co2_g_km"] <= CO2_MAX_METIER)]
        n_filtered = n_before - len(df)
        if n_filtered:
            log.info("Seuil métier CO2 > %dg/km : %d lignes supprimées.", CO2_MAX_METIER, n_filtered)

    log.info("Nettoyage terminé : %d -> %d lignes (-%d).",
             n_initial, len(df), n_initial - len(df))
    return df


def encode_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique sklearn OneHotEncoder sur les colonnes catégorielles présentes
    (carburant, boite_vitesses).

    Les colonnes originales sont conservées pour la lisibilité SQL.
    Les colonnes encodées sont préfixées : carburant_ES, boite_M6, etc.
    """
    df = df.copy()
    cols_to_encode = [c for c in CATEGORICAL_COLS if c in df.columns]

    if not cols_to_encode:
        log.warning("Aucune colonne catégorielle trouvée pour l'encodage : %s", CATEGORICAL_COLS)
        return df

    for col in cols_to_encode:
        # Remplir les NaN par 'INCONNU' pour l'encodage
        col_data = df[[col]].fillna("INCONNU")

        encoder = OneHotEncoder(
            sparse_output=False,
            handle_unknown="ignore",
            dtype=np.uint8,
        )
        encoded = encoder.fit_transform(col_data)

        # Noms de colonnes : {prefix}_{category} en snake_case
        category_names = [
            f"{col}_{_to_snake_case(str(cat))}"
            for cat in encoder.categories_[0]
        ]

        encoded_df = pd.DataFrame(encoded, columns=category_names, index=df.index)
        df = pd.concat([df, encoded_df], axis=1)

        log.info("OneHotEncoding '%s' : %d catégories -> colonnes %s",
                 col, len(encoder.categories_[0]), category_names)

    return df


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run() -> pd.DataFrame:
    """
    Pipeline de transformation complet :
      MinIO (raw) -> EDA -> Normalisation -> Nettoyage -> Encodage -> DataFrame
    """
    log.info("=" * 60)
    log.info("TRANSFORMATION ADEME — Démarrage")
    log.info("=" * 60)

    # 1. Chargement MinIO
    try:
        df_raw = load_raw_from_minio()
    except (S3Error, FileNotFoundError) as exc:
        log.error("Impossible de charger les données brutes MinIO : %s", exc)
        raise

    # 2. EDA sur les données brutes (avant toute transformation)
    run_eda(df_raw)

    # 3. Normalisation des colonnes
    df = normalize_columns(df_raw)

    # 4. Nettoyage & qualité
    df = clean_data(df)

    if df.empty:
        log.error("DataFrame vide après nettoyage — vérifier la qualité des données sources.")
        raise ValueError("Aucune donnée valide après transformation.")

    # 5. Encodage catégoriel
    df = encode_categoricals(df)

    # Ajout de la colonne de traçabilité
    df["source"] = "ADEME"

    log.info("=" * 60)
    log.info("TRANSFORMATION ADEME — Terminée : %d lignes x %d colonnes prêtes.", *df.shape)
    log.info("=" * 60)

    return df


if __name__ == "__main__":
    df_final = run()
    print(df_final.head())
    print(df_final.dtypes)
