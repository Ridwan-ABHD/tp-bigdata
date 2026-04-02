#!/usr/bin/env python
"""
AutoInsight — Referentiel ADEME vs Marche Occasion
Streamlit + Plotly | Glassmorphism Dark Mode | PostgreSQL Backend
Avec rafraichissement automatique (scheduler) et manuel (bouton)
"""

import os
import subprocess
import sys
from datetime import datetime
from threading import Thread

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Import du scheduler pour le rafraîchissement automatique
try:
    from script.src.scheduler import (
        get_last_refresh, 
        run_pipeline as refresh_all_data,
        start_scheduler,
        REFRESH_INTERVAL,
    )
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False
    REFRESH_INTERVAL = 3600
    def get_last_refresh():
        return None
    def refresh_all_data():
        return {"success": False, "error": "Scheduler non disponible"}
    def start_scheduler():
        return None

# =====================================================================
# Configuration
# =====================================================================

DB_HOST     = os.getenv("DB_HOST", "db")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_USER     = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_password")
DB_NAME     = os.getenv("DB_NAME", "etl_db")

TABLE_ADEME    = "caracteristiques_techniques"
TABLE_OCCASION = "annonces_occasion"

PLOTLY_TEMPLATE = "plotly_dark"
COLOR_ACCENT    = "#00D4AA"
COLOR_WARN      = "#FF6B6B"
COLOR_GOLD      = "#FFD93D"
COLOR_PURPLE    = "#6C5CE7"
COLOR_BLUE      = "#74b9ff"
PALETTE = [COLOR_ACCENT, COLOR_PURPLE, COLOR_WARN, COLOR_GOLD,
           "#A8E6CF", "#FF8B94", "#B8E986", "#F8B500", COLOR_BLUE, "#fd79a8"]

# =====================================================================
# SVG Logo
# =====================================================================

SVG_LOGO = """
<svg viewBox="0 0 80 80" xmlns="http://www.w3.org/2000/svg" width="72">
  <defs>
    <linearGradient id="lg1" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#00D4AA"/>
      <stop offset="100%" stop-color="#6C5CE7"/>
    </linearGradient>
  </defs>
  <path d="M12,56 A32,32 0 1,1 68,56" fill="none" stroke="url(#lg1)" stroke-width="3" stroke-linecap="round"/>
  <path d="M16,52 L20,48" stroke="#00D4AA" stroke-width="1.5" stroke-linecap="round" opacity="0.5"/>
  <path d="M22,30 L27,33" stroke="#00D4AA" stroke-width="1.5" stroke-linecap="round" opacity="0.5"/>
  <path d="M40,20 L40,26" stroke="url(#lg1)" stroke-width="1.5" stroke-linecap="round" opacity="0.4"/>
  <path d="M58,30 L53,33" stroke="#6C5CE7" stroke-width="1.5" stroke-linecap="round" opacity="0.5"/>
  <path d="M64,52 L60,48" stroke="#6C5CE7" stroke-width="1.5" stroke-linecap="round" opacity="0.5"/>
  <line x1="40" y1="54" x2="27" y2="30" stroke="url(#lg1)" stroke-width="2.5" stroke-linecap="round"/>
  <circle cx="40" cy="54" r="4.5" fill="url(#lg1)"/>
  <circle cx="40" cy="54" r="2" fill="#0a0e20"/>
  <circle cx="27" cy="30" r="2.5" fill="#00D4AA" opacity="0.9"/>
</svg>
"""

# =====================================================================
# CSS — Glassmorphism Dark Mode
# =====================================================================

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@300;400;500;600;700;800;900&display=swap');

/* --- Global --- */
.stApp {
    background: linear-gradient(145deg, #050a18 0%, #0c1228 35%, #101830 65%, #080d1c 100%);
    font-family: 'Montserrat', 'Inter', sans-serif;
}

/* Animated gradient orbs in background */
.stApp::before {
    content: '';
    position: fixed;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(circle at 15% 45%, rgba(0,212,170,0.025) 0%, transparent 50%),
                radial-gradient(circle at 85% 25%, rgba(108,92,231,0.03) 0%, transparent 40%),
                radial-gradient(circle at 55% 85%, rgba(255,107,107,0.015) 0%, transparent 45%);
    animation: orbFloat 40s ease-in-out infinite alternate;
    pointer-events: none;
    z-index: 0;
}
@keyframes orbFloat {
    0%   { transform: translate(0, 0) rotate(0deg); }
    50%  { transform: translate(-1.5%, 2%) rotate(3deg); }
    100% { transform: translate(1%, -1.5%) rotate(-2deg); }
}

/* --- Universal transition on interactive elements --- */
button, a, [data-baseweb="tab"], details, .stDownloadButton > button,
div[data-testid="stMetric"], [data-baseweb="tag"] {
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
}

/* --- Sidebar --- */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(10,15,35,0.98) 0%, rgba(6,9,22,0.99) 100%) !important;
    border-right: 1px solid rgba(0,212,170,0.08);
    backdrop-filter: blur(24px);
}
section[data-testid="stSidebar"] .stMultiSelect > div > div {
    background: rgba(0,212,170,0.06) !important;
    border: 1px solid rgba(0,212,170,0.12) !important;
    border-radius: 10px !important;
    transition: border-color 0.3s ease !important;
}
section[data-testid="stSidebar"] .stMultiSelect > div > div:hover {
    border-color: rgba(0,212,170,0.25) !important;
}
section[data-testid="stSidebar"] [data-baseweb="tag"] {
    background: linear-gradient(135deg, rgba(0,212,170,0.2), rgba(108,92,231,0.15)) !important;
    border: 1px solid rgba(0,212,170,0.25) !important;
    border-radius: 6px !important;
    color: #e2e8f0 !important;
}
section[data-testid="stSidebar"] [data-baseweb="tag"]:hover {
    border-color: rgba(0,212,170,0.4) !important;
    transform: scale(1.02);
}

/* --- Glassmorphism Metric Cards --- */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg,
        rgba(12,18,45,0.75) 0%,
        rgba(16,22,52,0.55) 50%,
        rgba(8,12,30,0.75) 100%) !important;
    border: 1px solid rgba(0,212,170,0.1);
    border-radius: 18px;
    padding: 24px 26px;
    backdrop-filter: blur(24px) saturate(180%);
    -webkit-backdrop-filter: blur(24px) saturate(180%);
    box-shadow:
        0 8px 32px rgba(0,0,0,0.35),
        inset 0 1px 0 rgba(255,255,255,0.03),
        0 0 0 rgba(0,212,170,0);
    position: relative;
    overflow: hidden;
}
div[data-testid="stMetric"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, rgba(0,212,170,0.4), transparent);
    opacity: 0;
    transition: opacity 0.3s ease;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-5px) scale(1.02);
    border-color: rgba(0,212,170,0.25);
    box-shadow:
        0 20px 50px rgba(0,0,0,0.45),
        inset 0 1px 0 rgba(255,255,255,0.05),
        0 0 25px rgba(0,212,170,0.06);
}
div[data-testid="stMetric"]:hover::before {
    opacity: 1;
}
div[data-testid="stMetric"] label {
    color: #6b7db3 !important;
    font-size: 0.7rem !important;
    text-transform: uppercase;
    letter-spacing: 1.8px;
    font-weight: 600 !important;
    font-family: 'Montserrat', sans-serif !important;
}
div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    color: #e2e8f0 !important;
    font-weight: 800 !important;
    font-size: 1.75rem !important;
    letter-spacing: -0.5px;
    font-family: 'Montserrat', sans-serif !important;
}
div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {
    color: rgba(0,212,170,0.75) !important;
    font-size: 0.72rem !important;
}

/* Color each metric accent differently */
div[data-testid="stHorizontalBlock"] > div:nth-child(1) div[data-testid="stMetricValue"] {
    color: #00D4AA !important;
}
div[data-testid="stHorizontalBlock"] > div:nth-child(2) div[data-testid="stMetricValue"] {
    color: #6C5CE7 !important;
}
div[data-testid="stHorizontalBlock"] > div:nth-child(3) div[data-testid="stMetricValue"] {
    color: #74b9ff !important;
}
div[data-testid="stHorizontalBlock"] > div:nth-child(4) div[data-testid="stMetricValue"] {
    color: #FFD93D !important;
}
div[data-testid="stHorizontalBlock"] > div:nth-child(5) div[data-testid="stMetricValue"] {
    color: #FF6B6B !important;
}

/* --- Headers --- */
h1, h2, h3 {
    color: #e2e8f0 !important;
    font-family: 'Montserrat', sans-serif !important;
}

/* --- Tabs --- */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: rgba(12,18,45,0.5);
    border-radius: 14px;
    padding: 5px;
    border: 1px solid rgba(0,212,170,0.06);
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    border-radius: 10px;
    border: 1px solid transparent;
    color: #5a6d9e;
    font-weight: 500;
    font-size: 0.82rem;
    padding: 10px 20px;
    letter-spacing: 0.2px;
    font-family: 'Montserrat', sans-serif;
}
.stTabs [data-baseweb="tab"]:hover {
    background: rgba(0,212,170,0.05);
    color: #8fa0c4;
    transform: translateY(-1px);
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(0,212,170,0.1), rgba(108,92,231,0.06)) !important;
    color: #00D4AA !important;
    font-weight: 600;
    border: 1px solid rgba(0,212,170,0.18) !important;
    box-shadow: 0 4px 16px rgba(0,212,170,0.06);
}

/* --- Expander --- */
details {
    border: 1px solid rgba(0,212,170,0.08) !important;
    border-radius: 14px !important;
    background: rgba(12,18,45,0.5) !important;
    backdrop-filter: blur(12px) !important;
}
details:hover {
    border-color: rgba(0,212,170,0.15) !important;
}

/* --- Buttons (primary + download) --- */
.stButton > button, .stDownloadButton > button {
    font-family: 'Montserrat', sans-serif !important;
    font-weight: 600 !important;
    border-radius: 10px !important;
    padding: 10px 28px !important;
    letter-spacing: 0.3px;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    position: relative;
    overflow: hidden;
}
.stDownloadButton > button {
    background: linear-gradient(135deg, #00D4AA, #00B894) !important;
    color: #050a18 !important;
    border: none !important;
    box-shadow: 0 4px 18px rgba(0,212,170,0.15) !important;
}
.stDownloadButton > button:hover {
    transform: translateY(-2px) scale(1.02) !important;
    box-shadow: 0 8px 30px rgba(0,212,170,0.3), 0 0 15px rgba(0,212,170,0.1) !important;
}
.stButton > button[data-testid="stBaseButton-primary"] {
    background: linear-gradient(135deg, #00D4AA, #6C5CE7) !important;
    color: #fff !important;
    border: none !important;
    box-shadow: 0 4px 18px rgba(0,212,170,0.12) !important;
}
.stButton > button[data-testid="stBaseButton-primary"]:hover {
    transform: translateY(-2px) scale(1.02) !important;
    box-shadow: 0 8px 30px rgba(0,212,170,0.25), 0 0 20px rgba(108,92,231,0.1) !important;
}
.stButton > button:hover {
    transform: translateY(-1px) scale(1.02) !important;
}

/* --- DataFrame --- */
.stDataFrame {
    border-radius: 14px;
    overflow: hidden;
    border: 1px solid rgba(0,212,170,0.08);
}

/* --- Divider --- */
hr {
    border-color: rgba(0,212,170,0.06) !important;
    margin: 2rem 0 !important;
}

/* --- Custom empty state --- */
.empty-state {
    text-align: center;
    padding: 60px 30px;
    background: linear-gradient(135deg, rgba(12,18,45,0.4), rgba(8,12,30,0.5));
    border: 1px dashed rgba(0,212,170,0.12);
    border-radius: 18px;
    backdrop-filter: blur(12px);
}
.empty-state .icon { font-size: 3rem; margin-bottom: 16px; opacity: 0.5; }
.empty-state .title { color: #7f8eb0; font-size: 1rem; font-weight: 500; margin-bottom: 8px; }
.empty-state .sub { color: #4a5568; font-size: 0.8rem; }

/* --- Footer --- */
.footer-pro {
    text-align: center;
    padding: 3rem 0 1.5rem 0;
    border-top: 1px solid rgba(0,212,170,0.06);
    margin-top: 2rem;
}
.footer-pro .sources {
    color: #3d4f73;
    font-size: 0.72rem;
    letter-spacing: 0.3px;
    margin-bottom: 6px;
    font-family: 'Montserrat', sans-serif;
}
.footer-pro .copy {
    color: #2a3550;
    font-size: 0.65rem;
    letter-spacing: 0.5px;
    font-family: 'Montserrat', sans-serif;
}

/* --- Slider --- */
.stSlider > div > div > div {
    background: linear-gradient(90deg, #00D4AA, #6C5CE7) !important;
}

/* --- Selectbox / Multiselect placeholder --- */
[data-baseweb="select"] [data-baseweb="input"] input::placeholder {
    color: #4a5568 !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 0.82rem !important;
}

/* --- Info / Warning / Success boxes --- */
.stAlert {
    border-radius: 10px !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 0.85rem !important;
}

/* --- Caption text --- */
.stCaption, [data-testid="stCaptionContainer"] {
    font-family: 'Montserrat', sans-serif !important;
    color: #4a5568 !important;
    font-size: 0.78rem !important;
}
</style>
"""


# =====================================================================
# Data layer
# =====================================================================

def _get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME,
    )


def _table_exists(table: str) -> bool:
    try:
        conn = _get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name=%s)", (table,),
        )
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except Exception:
        return False


def _query_to_df(query: str) -> pd.DataFrame:
    conn = _get_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description] if cur.description else []
    cur.close()
    conn.close()
    if not rows or not cols:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=cols)
    # Convertir les Decimal (psycopg2) en float pour eviter les erreurs de type pandas
    from decimal import Decimal
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, Decimal)).any():
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_ademe_data() -> pd.DataFrame:
    if not _table_exists(TABLE_ADEME):
        return pd.DataFrame()
    return _query_to_df(f"""
        SELECT marque, modele, carburant, boite_vitesses,
               co2_g_km, puissance_kw, puissance_fiscale,
               conso_mixte_min_l100, conso_mixte_max_l100,
               masse_min_kg, masse_max_kg,
               nox_mg_km, particules_mg_km,
               prix_neuf_eur, source
        FROM "{TABLE_ADEME}"
        WHERE marque IS NOT NULL
        ORDER BY marque, modele
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_occasion_data() -> pd.DataFrame:
    if not _table_exists(TABLE_OCCASION):
        return pd.DataFrame()
    # Requete optimisee : COALESCE pour eviter les erreurs si match_score est NULL
    return _query_to_df(f"""
        SELECT a.marque, a.modele, a.version, a.annee,
               a.prix_eur, a.kilometrage_km, a.carburant,
               a.ademe_id, COALESCE(a.match_score, 0) AS match_score, 
               a.source, a.scraped_date,
               t.co2_g_km      AS co2_ademe,
               t.puissance_kw  AS puissance_ademe,
               t.prix_neuf_eur AS prix_neuf_ademe
        FROM "{TABLE_OCCASION}" a
        LEFT JOIN "{TABLE_ADEME}" t ON a.ademe_id = t.id
        ORDER BY a.prix_eur DESC NULLS LAST
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_kpi_aggregates() -> dict:
    result = {
        "n_ademe": 0, "n_occasion": 0, "n_matched": 0,
        "avg_prix_occasion": 0, "avg_co2_ademe": None,
        "avg_match_score": 0, "avg_decote_pct": None,
    }
    try:
        conn = _get_connection()
        cur = conn.cursor()
        if _table_exists(TABLE_ADEME):
            cur.execute(f'SELECT COUNT(*), AVG(co2_g_km) FROM "{TABLE_ADEME}"')
            row = cur.fetchone()
            result["n_ademe"] = row[0] or 0
            result["avg_co2_ademe"] = round(float(row[1]), 1) if row[1] is not None else None

        if _table_exists(TABLE_OCCASION):
            # Requete optimisee avec COALESCE pour eviter erreurs sur match_score NULL
            cur.execute(f"""
                SELECT COUNT(*), 
                       COUNT(ademe_id), 
                       AVG(prix_eur),
                       AVG(COALESCE(match_score, 0)) FILTER (WHERE ademe_id IS NOT NULL)
                FROM "{TABLE_OCCASION}"
            """)
            row = cur.fetchone()
            result["n_occasion"]       = row[0] or 0
            result["n_matched"]        = row[1] or 0
            result["avg_prix_occasion"] = round(float(row[2]), 0) if row[2] else 0
            result["avg_match_score"]   = round(float(row[3]), 3) if row[3] else 0

            if _table_exists(TABLE_ADEME):
                # Calcul decote avec bornes [0%, 95%] pour eviter aberrations
                # Exclut: prix_occasion > prix_neuf (erreur de match) et decotes > 95%
                cur.execute(f"""
                    SELECT AVG(decote_pct) FROM (
                        SELECT 
                            (1.0 - COALESCE(a.prix_eur, 0) / t.prix_neuf_eur) * 100 AS decote_pct
                        FROM "{TABLE_OCCASION}" a
                        JOIN "{TABLE_ADEME}" t ON a.ademe_id = t.id
                        WHERE COALESCE(a.prix_eur, 0) > 0 
                          AND COALESCE(t.prix_neuf_eur, 0) > 0
                          AND a.prix_eur < t.prix_neuf_eur  -- exclure prix occasion > prix neuf
                          AND (1.0 - a.prix_eur / t.prix_neuf_eur) * 100 BETWEEN 0 AND 95
                    ) sub
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    result["avg_decote_pct"] = round(float(row[0]), 1)
        cur.close()
        conn.close()
    except Exception:
        pass
    return result


# =====================================================================
# Common Plotly layout
# =====================================================================

def _base_layout() -> dict:
    return dict(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#b0bdd4", family="Montserrat, Inter, sans-serif", size=12),
        legend=dict(
            bgcolor="rgba(15,22,50,0.7)",
            bordercolor="rgba(0,212,170,0.15)",
            borderwidth=1,
            font=dict(size=11, color="#8892b0"),
        ),
        margin=dict(l=50, r=30, t=60, b=50),
        xaxis=dict(
            gridcolor="rgba(0,212,170,0.06)",
            gridwidth=0.5,
            zeroline=False,
            tickfont=dict(color="#6b7db3"),
        ),
        yaxis=dict(
            gridcolor="rgba(0,212,170,0.06)",
            gridwidth=0.5,
            zeroline=False,
            tickfont=dict(color="#6b7db3"),
        ),
    )


# =====================================================================
# Empty state
# =====================================================================

def _empty_state(icon: str = "&#128269;", title: str = "Aucune donnee disponible",
                 sub: str = "Ajustez les filtres ou executez le pipeline ETL."):
    st.markdown(
        f'<div class="empty-state">'
        f'<div class="icon">{icon}</div>'
        f'<div class="title">{title}</div>'
        f'<div class="sub">{sub}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# =====================================================================
# Charts
# =====================================================================

def chart_prix_vs_co2(df_occ: pd.DataFrame):
    df = df_occ.dropna(subset=["prix_eur", "co2_ademe"]).copy()
    if df.empty:
        st.warning("Aucune annonce avec CO2 ADEME disponible.")
        st.info("Les annonces doivent etre matchees avec le referentiel ADEME pour cette visualisation.")
        return

    df["puissance_ademe"] = df["puissance_ademe"].fillna(100).clip(lower=30)
    df["marque_upper"]    = df["marque"].str.upper()

    fig = px.scatter(
        df, x="co2_ademe", y="prix_eur",
        size="puissance_ademe", color="marque_upper",
        hover_data=["modele", "annee", "kilometrage_km"],
        labels={"co2_ademe": "CO2 (g/km)", "prix_eur": "Prix (EUR)",
                "puissance_ademe": "Puissance (kW)", "marque_upper": "Marque"},
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=PALETTE,
        size_max=40, opacity=0.85,
    )
    fig.update_layout(**_base_layout(), height=560,
                      title=dict(text="Prix du marche vs Emissions CO2 officielles",
                                 font=dict(size=16, color="#ccd6f6")))
    st.plotly_chart(fig, use_container_width=True)


def chart_decote_par_annee(df_occ: pd.DataFrame):
    df = df_occ.dropna(subset=["annee", "prix_eur"]).copy()
    df = df[(df["annee"] >= 2005) & (df["annee"] <= 2026)]
    if df.empty:
        st.warning("Aucune donnee pour la courbe de decote.")
        st.info("Besoin d'annonces avec annee de mise en service valide (2005-2026).")
        return

    agg = df.groupby("annee").agg(
        prix_moyen=("prix_eur", "mean"),
        prix_median=("prix_eur", "median"),
        n=("prix_eur", "count"),
    ).reset_index()
    
    # FILTRE : ne garder que les annees avec au moins 5 annonces
    # pour eviter les pics aberrants dus a une seule annonce
    agg = agg[agg["n"] >= 5]
    
    if agg.empty:
        st.warning("Pas assez de donnees pour afficher la courbe de decote.")
        st.info("Chaque annee doit avoir au moins 5 annonces pour etre affichee.")
        return

    fig = go.Figure()

    # Area fill under the mean curve for a gradient effect
    fig.add_trace(go.Scatter(
        x=agg["annee"], y=agg["prix_moyen"],
        fill="tozeroy",
        fillcolor="rgba(0,212,170,0.06)",
        line=dict(color=COLOR_ACCENT, width=3, shape="spline"),
        marker=dict(size=9, color=COLOR_ACCENT,
                    line=dict(width=2, color="#0a0e20")),
        mode="lines+markers",
        name="Prix moyen",
        hovertemplate="<b>%{x}</b><br>Prix moyen: %{y:,.0f} EUR<br>%{customdata} annonces",
        customdata=agg["n"],
    ))

    fig.add_trace(go.Scatter(
        x=agg["annee"], y=agg["prix_median"],
        line=dict(color=COLOR_GOLD, width=2, dash="dot", shape="spline"),
        marker=dict(size=6, symbol="diamond", color=COLOR_GOLD),
        mode="lines+markers",
        name="Prix median",
        hovertemplate="<b>%{x}</b><br>Prix median: %{y:,.0f} EUR",
    ))

    if "prix_neuf_ademe" in df.columns:
        prix_neuf = df["prix_neuf_ademe"].dropna().mean()
        if prix_neuf and prix_neuf > 0:
            fig.add_hline(
                y=prix_neuf, line_dash="dash", line_color=COLOR_WARN, line_width=1.5,
                annotation_text=f"Prix neuf ADEME moyen : {prix_neuf:,.0f} EUR",
                annotation_font=dict(color=COLOR_WARN, size=11),
                annotation_position="top left",
            )

    fig.update_layout(**_base_layout(), height=500,
                      title=dict(text="Courbe de decote : prix moyen par annee (min. 5 annonces/an)",
                                 font=dict(size=16, color="#ccd6f6")))
    st.plotly_chart(fig, use_container_width=True)


def chart_sunburst_carburant(df_ademe: pd.DataFrame):
    df = df_ademe.dropna(subset=["carburant", "co2_g_km"]).copy()
    if df.empty:
        st.info("Aucune donnee ADEME disponible pour ce graphique.")
        return

    df["carburant_clean"] = df["carburant"].str.strip().str.upper()
    df["marque_clean"]    = df["marque"].str.strip().str.upper()
    
    # Regrouper les vehicules avec CO2 < 5 g/km sous "ZERO EMISSION" (vert)
    # Cela inclut les electriques et hydrogene qui ont CO2 = 0
    df.loc[df["co2_g_km"] < 5, "carburant_clean"] = "ZERO EMISSION"

    agg = df.groupby(["carburant_clean", "marque_clean"]).agg(
        co2_moyen=("co2_g_km", "mean"), count=("co2_g_km", "count"),
    ).reset_index()

    # Grouper les marques < 5% de leur carburant sous "AUTRES" pour lisibilite
    totaux_carb = agg.groupby("carburant_clean")["count"].transform("sum")
    agg["pct_in_carb"] = agg["count"] / totaux_carb
    grandes = agg[agg["pct_in_carb"] >= 0.05].copy()
    petites = agg[agg["pct_in_carb"] < 0.05].copy()

    if not petites.empty:
        autres = petites.groupby("carburant_clean").agg(
            co2_moyen=("co2_moyen", "mean"), count=("count", "sum"),
        ).reset_index()
        autres["marque_clean"] = "AUTRES"
        grandes = pd.concat([grandes[["carburant_clean", "marque_clean", "co2_moyen", "count"]],
                             autres], ignore_index=True)
    else:
        grandes = grandes[["carburant_clean", "marque_clean", "co2_moyen", "count"]]

    if grandes.empty:
        st.info("Pas assez de donnees pour afficher le sunburst.")
        return

    # Calculer le pourcentage global pour masquer les labels des petits segments
    total_count = grandes["count"].sum()
    grandes["pct_global"] = grandes["count"] / total_count * 100

    # Palette personnalisee: vert pour zero emission, degrade pour les autres
    fig = px.sunburst(
        grandes, path=["carburant_clean", "marque_clean"],
        values="count", color="co2_moyen",
        color_continuous_scale=["#00FF88", "#00D4AA", "#FFD93D", "#FF9F43", "#FF6B6B"],
        labels={"co2_moyen": "CO2 moy. (g/km)", "count": "Vehicules"},
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#b0bdd4", family="Montserrat, Inter, sans-serif"),
        title=dict(text="Motorisations & impact CO2 (Zero Emission = CO2 < 5 g/km)", 
                   font=dict(size=15, color="#ccd6f6")),
        height=520, margin=dict(t=50, b=20, l=20, r=20),
    )
    # Mode interactif : masquer les labels des petits segments, n'afficher qu'au survol
    fig.update_traces(
        textinfo="none",
        hovertemplate="<b>%{label}</b><br>Vehicules : %{value}<br>CO2 moy : %{color:.1f} g/km<extra></extra>",
        insidetextorientation="horizontal",
        maxdepth=2,
    )
    st.plotly_chart(fig, use_container_width=True)


def chart_co2_distribution(df_ademe: pd.DataFrame):
    """Box plot : distribution CO2 par type de motorisation - clair et lisible."""
    df = df_ademe.dropna(subset=["co2_g_km", "carburant"]).copy()
    if df.empty:
        st.warning("Aucune distribution CO2 disponible.")
        st.info("Verifiez les donnees ADEME chargees.")
        return

    df["carburant_clean"] = df["carburant"].str.strip().str.upper()
    
    # Regrouper les types de carburant pour plus de lisibilite
    def simplify_fuel(c):
        c = c.upper()
        if "ELEC" in c and "ESS" not in c and "GAZ" not in c:
            return "ELECTRIQUE"
        elif "ESS+ELEC" in c or "ESS ELEC" in c:
            return "HYBRIDE ESSENCE"
        elif "GAZ+ELEC" in c or "GAZ ELEC" in c:
            return "HYBRIDE DIESEL"
        elif c in ["ESSENCE", "SUPER", "SP95", "SP98"]:
            return "ESSENCE"
        elif c in ["GAZOLE", "DIESEL", "GASOIL"]:
            return "DIESEL"
        elif c in ["GPL", "GNV"]:
            return "GPL/GNV"
        else:
            return c
    
    df["carburant_simple"] = df["carburant_clean"].apply(simplify_fuel)
    
    # FORCER CO2 = 0 pour les vehicules electriques (ils n'emettent pas de CO2)
    df.loc[df["carburant_simple"] == "ELECTRIQUE", "co2_g_km"] = 0
    
    # Ordre logique des carburants (du plus polluant au moins polluant)
    ordre_thermique = ["DIESEL", "ESSENCE", "HYBRIDE DIESEL", "HYBRIDE ESSENCE", "GPL/GNV"]
    df_thermique = df[df["carburant_simple"] != "ELECTRIQUE"]
    df_thermique["carburant_simple"] = pd.Categorical(
        df_thermique["carburant_simple"], categories=ordre_thermique, ordered=True
    )
    df_thermique = df_thermique.sort_values("carburant_simple")
    
    # Compter les electriques
    n_electriques = (df["carburant_simple"] == "ELECTRIQUE").sum()
    
    # Calculer les stats par carburant (thermiques seulement)
    stats = df_thermique.groupby("carburant_simple", observed=True).agg(
        co2_median=("co2_g_km", "median"),
        co2_mean=("co2_g_km", "mean"),
        count=("co2_g_km", "count"),
    ).reset_index()
    
    # Couleurs coherentes : vert (eco) -> rouge (polluant)
    color_map = {
        "GPL/GNV": "#00D4AA",          # Vert-bleu
        "HYBRIDE ESSENCE": "#7FD4AA", # Vert clair
        "HYBRIDE DIESEL": "#FFD93D",   # Jaune
        "ESSENCE": "#FF9F43",          # Orange
        "DIESEL": "#FF6B6B",           # Rouge
    }
    
    fig = go.Figure()
    
    # Afficher uniquement les motorisations thermiques
    for carb in ordre_thermique:
        subset = df_thermique[df_thermique["carburant_simple"] == carb]
        if subset.empty:
            continue
        
        fig.add_trace(go.Box(
            y=subset["co2_g_km"],
            name=carb,
            marker_color=color_map.get(carb, "#8892b0"),
            boxmean=True,
            hovertemplate=(
                f"<b>{carb}</b><br>"
                "CO2: %{y:.0f} g/km<br>"
                "<extra></extra>"
            ),
        ))
    
    # Ajouter ligne de reference 95 g/km (norme Euro)
    fig.add_hline(
        y=95, line_dash="dash", line_color="#FF6B6B", line_width=1.5,
        annotation_text="Objectif UE 2021 : 95 g/km",
        annotation_font=dict(color="#FF6B6B", size=10),
        annotation_position="top right",
    )
    
    fig.update_layout(
        **_base_layout(),
        height=480,
        title=dict(
            text="Distribution CO2 par motorisation (thermiques uniquement)",
            font=dict(size=15, color="#ccd6f6")
        ),
        yaxis_title="CO2 (g/km)",
        xaxis_title="Type de motorisation",
        showlegend=False,
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Mini tableau recapitulatif + encart electrique
    st.markdown("##### Statistiques par motorisation")
    
    # Ajouter colonne pour electrique
    n_cols = len(stats) + (1 if n_electriques > 0 else 0)
    cols = st.columns(n_cols)
    
    for i, (_, row) in enumerate(stats.iterrows()):
        with cols[i]:
            carb = row["carburant_simple"]
            color = color_map.get(carb, "#8892b0")
            st.markdown(
                f'<div style="text-align:center;padding:8px;background:rgba(15,22,50,0.5);border-radius:8px;border-left:3px solid {color}">'
                f'<div style="color:{color};font-weight:bold;font-size:12px">{carb}</div>'
                f'<div style="color:#ccd6f6;font-size:18px">{row["co2_mean"]:.0f} g/km</div>'
                f'<div style="color:#6b7db3;font-size:10px">{row["count"]} vehicules</div>'
                f'</div>',
                unsafe_allow_html=True
            )
    
    # Encart special pour electrique (toujours 0 CO2)
    if n_electriques > 0:
        with cols[-1]:
            st.markdown(
                f'<div style="text-align:center;padding:8px;background:rgba(0,255,136,0.1);border-radius:8px;border-left:3px solid #00FF88">'
                f'<div style="color:#00FF88;font-weight:bold;font-size:12px">ELECTRIQUE</div>'
                f'<div style="color:#00FF88;font-size:18px">0 g/km</div>'
                f'<div style="color:#6b7db3;font-size:10px">{n_electriques} vehicules</div>'
                f'</div>',
                unsafe_allow_html=True
            )


def chart_prix_vs_km(df_occ: pd.DataFrame):
    df = df_occ.dropna(subset=["prix_eur", "kilometrage_km"]).copy()
    if df.empty:
        st.warning("Aucune annonce avec prix et kilometrage.")
        st.info("Executez le scraping pour alimenter cette vue.")
        return

    fig = px.scatter(
        df, x="kilometrage_km", y="prix_eur", color="carburant",
        hover_data=["marque", "modele", "annee"],
        labels={"kilometrage_km": "Kilometrage (km)", "prix_eur": "Prix (EUR)",
                "carburant": "Carburant"},
        template=PLOTLY_TEMPLATE, color_discrete_sequence=PALETTE,
        opacity=0.8,
    )
    fig.update_layout(**_base_layout(), height=500,
                      title=dict(text="Prix vs Kilometrage",
                                 font=dict(size=16, color="#ccd6f6")))
    st.plotly_chart(fig, use_container_width=True)


def chart_top_marques_co2(df_ademe: pd.DataFrame):
    """Bar chart horizontal : top 15 marques par CO2 moyen."""
    df = df_ademe.dropna(subset=["co2_g_km", "marque"]).copy()
    if df.empty:
        st.warning("Aucune donnee de marque disponible.")
        return

    df["marque_upper"] = df["marque"].str.strip().str.upper()
    agg = df.groupby("marque_upper").agg(
        co2_moyen=("co2_g_km", "mean"),
        n=("co2_g_km", "count"),
    ).reset_index()
    agg = agg[agg["n"] >= 5].nlargest(15, "co2_moyen").sort_values("co2_moyen")

    if agg.empty:
        st.warning("Pas assez de donnees pour afficher le top 15 marques (minimum 5 vehicules par marque).")
        return

    # Color scale: green (low CO2) to red (high CO2)
    co2_norm = (agg["co2_moyen"] - agg["co2_moyen"].min()) / \
               (agg["co2_moyen"].max() - agg["co2_moyen"].min() + 1e-9)
    colors = [f"rgba({int(50 + 200*v)},{int(212 - 160*v)},{int(170 - 140*v)},0.85)"
              for v in co2_norm]

    fig = go.Figure(go.Bar(
        x=agg["co2_moyen"], y=agg["marque_upper"],
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{v:.0f} g/km" for v in agg["co2_moyen"]],
        textposition="outside",
        textfont=dict(color="#8892b0", size=11),
        hovertemplate="<b>%{y}</b><br>CO2 moyen: %{x:.0f} g/km<br>N=%{customdata}",
        customdata=agg["n"],
    ))
    fig.update_layout(
        **_base_layout(), height=500,
        title=dict(text="Top 15 Marques par CO2 moyen (ADEME)",
                   font=dict(size=15, color="#ccd6f6")),
        xaxis_title="CO2 moyen (g/km)",
        yaxis_title="",
    )
    st.plotly_chart(fig, use_container_width=True)


def chart_prix_neuf_boxplot(df_ademe: pd.DataFrame):
    """Boxplot : distribution des prix neufs par type de carburant."""
    df = df_ademe.dropna(subset=["prix_neuf_eur", "carburant"]).copy()
    if df.empty:
        st.warning("Aucun prix neuf ADEME disponible.")
        st.info("Les donnees ADEME doivent contenir le champ prix_neuf_eur.")
        return

    df["carburant_clean"] = df["carburant"].str.strip().str.upper()
    fig = px.box(
        df, x="carburant_clean", y="prix_neuf_eur",
        color="carburant_clean",
        labels={"carburant_clean": "Carburant", "prix_neuf_eur": "Prix neuf (EUR)"},
        template=PLOTLY_TEMPLATE, color_discrete_sequence=PALETTE,
    )
    fig.update_layout(**_base_layout(), height=500, showlegend=False,
                      title=dict(text="Distribution des prix neufs par motorisation",
                                 font=dict(size=15, color="#ccd6f6")))
    fig.update_traces(marker_line_width=0, opacity=0.8)
    st.plotly_chart(fig, use_container_width=True)


# =====================================================================
# Sidebar
# =====================================================================

def render_sidebar(df_ademe: pd.DataFrame, df_occ: pd.DataFrame):
    # Logo SVG
    st.sidebar.markdown(
        f'<div style="text-align:center; padding:1.5rem 0 0.3rem 0;">{SVG_LOGO}</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(
        '<h2 style="text-align:center; margin:0 0 2px 0; font-size:1.2rem; '
        'font-family:Montserrat,sans-serif; font-weight:800; letter-spacing:1.5px; '
        'background: linear-gradient(135deg, #00D4AA 30%, #6C5CE7 100%); '
        '-webkit-background-clip: text; -webkit-text-fill-color: transparent;">AutoInsight</h2>'
        '<p style="text-align:center; color:#3d4f73; font-size:0.62rem; '
        'font-family:Montserrat,sans-serif; font-weight:500; '
        'margin:0 0 1.2rem 0; letter-spacing:2.5px; text-transform:uppercase;">Filtres</p>',
        unsafe_allow_html=True,
    )

    # Marque
    marques_list = []
    if not df_ademe.empty and "marque" in df_ademe.columns:
        marques_list += df_ademe["marque"].dropna().str.upper().unique().tolist()
    if not df_occ.empty and "marque" in df_occ.columns:
        marques_list += df_occ["marque"].dropna().str.upper().unique().tolist()
    sel_marques = st.sidebar.multiselect("Marque(s)", sorted(set(marques_list)), default=[], placeholder="Toutes les marques")

    # Carburant
    carb_list = []
    if not df_ademe.empty and "carburant" in df_ademe.columns:
        carb_list += df_ademe["carburant"].dropna().str.upper().unique().tolist()
    if not df_occ.empty and "carburant" in df_occ.columns:
        carb_list += df_occ["carburant"].dropna().str.upper().unique().tolist()
    sel_carb = st.sidebar.multiselect("Carburant", sorted(set(carb_list)), default=[], placeholder="Tous les carburants")

    # Prix slider
    sel_prix = None
    if not df_occ.empty and "prix_eur" in df_occ.columns:
        prix_vals = df_occ["prix_eur"].dropna()
        if not prix_vals.empty:
            pmin, pmax = int(prix_vals.min()), int(prix_vals.max())
            if pmin < pmax:
                sel_prix = st.sidebar.slider("Fourchette prix (EUR)", pmin, pmax, (pmin, pmax), 500)

    # Annee slider
    sel_year = None
    if not df_occ.empty and "annee" in df_occ.columns:
        year_vals = df_occ["annee"].dropna()
        if not year_vals.empty:
            ymin, ymax = int(year_vals.min()), int(year_vals.max())
            if ymin < ymax:
                sel_year = st.sidebar.slider("Annee", ymin, ymax, (ymin, ymax))

    st.sidebar.markdown("---")
    
    # =====================================================================
    # Bouton Actualiser les données + Status Scheduler
    # =====================================================================
    st.sidebar.markdown(
        '<p style="color:#7f8eb0; font-size:0.72rem; text-transform:uppercase; '
        'letter-spacing:1.5px; margin-bottom:8px; font-weight:600;">🔄 Données</p>',
        unsafe_allow_html=True,
    )
    
    # Afficher le dernier rafraîchissement
    last_refresh = get_last_refresh()
    if last_refresh:
        time_ago = datetime.now() - last_refresh
        if time_ago.total_seconds() < 60:
            ago_str = "< 1 min"
        elif time_ago.total_seconds() < 3600:
            ago_str = f"{int(time_ago.total_seconds() // 60)} min"
        else:
            ago_str = f"{int(time_ago.total_seconds() // 3600)}h {int((time_ago.total_seconds() % 3600) // 60)}min"
        st.sidebar.markdown(
            f'<p style="color:#4a5568; font-size:0.68rem; margin:0 0 8px 0;">'
            f'Dernier refresh : <span style="color:#00D4AA;">{ago_str}</span></p>',
            unsafe_allow_html=True,
        )
    
    # Status du scheduler automatique
    if SCHEDULER_AVAILABLE:
        interval_str = f"{REFRESH_INTERVAL // 3600}h" if REFRESH_INTERVAL >= 3600 else f"{REFRESH_INTERVAL // 60}min"
        st.sidebar.markdown(
            f'<p style="color:#4a5568; font-size:0.68rem; margin:0 0 12px 0;">'
            f'⏰ Auto-refresh : <span style="color:#6C5CE7;">{interval_str}</span></p>',
            unsafe_allow_html=True,
        )
    
    # Bouton pour rafraichir manuellement
    if st.sidebar.button("Actualiser (Scraping)", key="btn_scraping", use_container_width=True):
        st.session_state["trigger_scraping"] = True

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        '<p style="color:#2d3a54; font-size:0.65rem; text-align:center; line-height:1.8; '
        'font-family:Montserrat,sans-serif; letter-spacing:0.3px;">'
        'Projet Big Data<br>'
        '<span style="color:#00D4AA; font-weight:600;">Sup de Vinci</span> &mdash; Bachelor 2<br>'
        '<span style="color:#3d4f73;">Avril 2026</span></p>',
        unsafe_allow_html=True,
    )

    # Apply filters
    df_a = df_ademe.copy()
    df_o = df_occ.copy()

    if sel_marques:
        if not df_a.empty and "marque" in df_a.columns:
            df_a = df_a[df_a["marque"].str.upper().isin(sel_marques)]
        if not df_o.empty and "marque" in df_o.columns:
            df_o = df_o[df_o["marque"].str.upper().isin(sel_marques)]
    if sel_carb:
        if not df_a.empty and "carburant" in df_a.columns:
            df_a = df_a[df_a["carburant"].str.upper().isin(sel_carb)]
        if not df_o.empty and "carburant" in df_o.columns:
            df_o = df_o[df_o["carburant"].str.upper().isin(sel_carb)]
    if sel_prix and not df_o.empty and "prix_eur" in df_o.columns:
        df_o = df_o[df_o["prix_eur"].isna() |
                    ((df_o["prix_eur"] >= sel_prix[0]) & (df_o["prix_eur"] <= sel_prix[1]))]
    if sel_year and not df_o.empty and "annee" in df_o.columns:
        df_o = df_o[df_o["annee"].isna() |
                    ((df_o["annee"] >= sel_year[0]) & (df_o["annee"] <= sel_year[1]))]

    return df_a, df_o


# =====================================================================
# KPI formatting
# =====================================================================

def _fmt_number(n):
    """Format a number with spaces as thousands separator."""
    if n is None or (isinstance(n, float) and (np.isnan(n) or np.isinf(n))):
        return "--"
    return f"{int(n):,}".replace(",", " ")


def _fmt_co2(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "-- g/km"
    return f"{v:.0f} g/km"


# =====================================================================
# Main
# =====================================================================

def _run_scraping():
    """Execute scraping pipeline (AutoScout24) + transformation from within Streamlit."""
    import subprocess
    import sys
    try:
        # Étape 1: Scraping AutoScout24
        result1 = subprocess.run(
            [sys.executable, "-m", "script.src.scraping_autoscout"],
            capture_output=True, text=True, timeout=300,
            cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
        if result1.returncode != 0:
            return False, f"Scraping failed: {result1.stdout + result1.stderr}"
        
        # Étape 2: Transformation et chargement en base
        result2 = subprocess.run(
            [sys.executable, "-m", "script.src.transformation_scraping"],
            capture_output=True, text=True, timeout=120,
            cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
        return result2.returncode == 0, result1.stdout + result1.stderr + "\n" + result2.stdout + result2.stderr
    except Exception as e:
        return False, str(e)


def main():
    st.set_page_config(
        page_title="AutoInsight | ADEME vs Occasion",
        page_icon="\U0001f3ce\ufe0f",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    
    # =====================================================================
    # Gestion du rafraîchissement des données (boutons sidebar)
    # =====================================================================
    
    # Rafraîchissement scraping uniquement
    if st.session_state.get("trigger_scraping"):
        st.session_state["trigger_scraping"] = False
        with st.spinner("🔄 Scraping AutoScout24 en cours... (peut prendre 2-3 min)"):
            success, output = _run_scraping()
            if success:
                st.success("✅ Scraping terminé ! Les données ont été mises à jour.")
                # Vider le cache pour recharger les nouvelles données
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ Erreur scraping : {output[:500]}")
    
    # Rafraîchissement complet (ADEME + Scraping)
    if st.session_state.get("trigger_full_refresh"):
        st.session_state["trigger_full_refresh"] = False
        with st.spinner("🔄 Rafraîchissement complet en cours... (ADEME + Scraping + Transformation)"):
            if SCHEDULER_AVAILABLE:
                stats = refresh_all_data()
                if stats.get("success"):
                    st.success(
                        f"✅ Données actualisées !\n"
                        f"• ADEME : {stats.get('ademe_count', 0)} véhicules\n"
                        f"• Scraping : {stats.get('scraping_count', 0)} annonces\n"
                        f"• Matchées : {stats.get('occasion_count', 0)}"
                    )
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"❌ Erreur : {stats.get('error', 'Erreur inconnue')}")
            else:
                st.error("Scheduler non disponible. Utilisez le bouton Scraping.")

    # --- Header ---
    st.markdown(
        '<div style="text-align:center; padding: 0.8rem 0 0.2rem 0;">'
        '<h1 style="font-size:2.2rem; margin:0; font-weight:900; '
        'font-family:Montserrat,sans-serif; letter-spacing:2px; '
        'background: linear-gradient(135deg, #00D4AA 0%, #6C5CE7 100%); '
        '-webkit-background-clip: text; -webkit-text-fill-color: transparent;">AutoInsight</h1>'
        '<p style="color:#4a5c82; margin:8px 0 0 0; font-size:0.78rem; font-weight:400; '
        'font-family:Montserrat,sans-serif; '
        'letter-spacing:3px; text-transform:uppercase;">Referentiel ADEME vs Marche de l\'Occasion</p>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # --- Load data ---
    with st.spinner("Chargement des donnees..."):
        try:
            df_ademe = load_ademe_data()
            df_occ   = load_occasion_data()
            kpi      = load_kpi_aggregates()
        except Exception as exc:
            st.error(f"Connexion PostgreSQL echouee : {exc}")
            st.info("Verifiez que le container 'db' est demarre et que le pipeline ETL a ete execute.")
            return

    has_ademe = not df_ademe.empty
    has_occ   = not df_occ.empty

    # Log pour debug : nombre de lignes avant filtrage
    print(f"[DEBUG] Lignes ADEME chargees : {len(df_ademe)}")
    print(f"[DEBUG] Lignes OCCASION chargees : {len(df_occ)}")

    if not has_ademe and not has_occ:
        st.warning("Aucune donnee en base de donnees.")
        st.info("Executez le pipeline ETL avec : `docker exec tp-bigdata python -m script.src.main`")
        return

    # --- Balloons on first successful load ---
    if "first_load" not in st.session_state:
        st.session_state["first_load"] = True
        st.balloons()

    # --- Sidebar ---
    df_ademe_f, df_occ_f = render_sidebar(df_ademe, df_occ)

    # ==================================================================
    # KPI Cards
    # ==================================================================

    k1, k2, k3, k4, k5 = st.columns(5)

    with k1:
        st.metric("Vehicules ADEME", _fmt_number(kpi["n_ademe"]), delta="referentiel")
    with k2:
        st.metric("Annonces Occasion", _fmt_number(kpi["n_occasion"]), delta="AutoScout24")
    with k3:
        pct = round(kpi["n_matched"] / kpi["n_occasion"] * 100, 1) if kpi["n_occasion"] > 0 else 0
        st.metric("Taux de correspondance", f"{pct}%",
                  delta=f"{kpi['n_matched']} appariements" if kpi["n_matched"] else None)
    with k4:
        if kpi["avg_decote_pct"] is not None:
            st.metric("Decote moyenne", f"{kpi['avg_decote_pct']}%",
                      delta="neuf -> occasion", delta_color="inverse")
        else:
            st.metric("Decote moyenne", "--")
    with k5:
        st.metric("CO2 moyen ADEME", _fmt_co2(kpi["avg_co2_ademe"]))

    st.markdown("")

    # ==================================================================
    # Tabs
    # ==================================================================

    tabs = st.tabs([
        "Comparateur Prix/CO2",
        "Courbe de Decote",
        "Analyse Motorisations",
        "Marche Occasion",
        "Referentiel ADEME",
    ])

    with tabs[0]:
        if has_occ and not df_occ_f.empty:
            chart_prix_vs_co2(df_occ_f)
            st.caption(
                "Chaque bulle = une annonce. Taille = puissance kW (ADEME). Couleur = marque. "
                "Les annonces sans correspondance ADEME sont exclues de ce graphique."
            )
        elif has_occ and df_occ_f.empty:
            st.warning("Aucun resultat pour les filtres selectionnes. "
                       "Veuillez elargir vos filtres de motorisation ou de marque dans la barre laterale.")
        else:
            st.info("Le comparateur necessite des donnees du marche de l'occasion.")
            if st.button("Lancer le Scraping maintenant", key="scrape_tab0", type="primary"):
                with st.spinner("Scraping en cours... Cela peut prendre quelques minutes."):
                    success, output = _run_scraping()
                    if success:
                        st.success("Scraping termine ! Rechargez la page pour voir les nouvelles donnees.")
                        st.cache_data.clear()
                    else:
                        st.error(f"Erreur lors du scraping : {output[:500]}")

    with tabs[1]:
        if has_occ and not df_occ_f.empty:
            chart_decote_par_annee(df_occ_f)
            st.caption(
                "Ligne pleine = prix moyen | Pointillee = prix median | "
                "Ligne rouge = prix neuf moyen ADEME (reference)."
            )
        elif has_occ and df_occ_f.empty:
            st.warning("Aucun resultat pour les filtres selectionnes. "
                       "Veuillez elargir vos criteres de recherche dans la barre laterale.")
        else:
            st.info("La courbe de decote sera disponible une fois le scraping execute.")
            if st.button("Lancer le Scraping maintenant", key="scrape_tab1", type="primary"):
                with st.spinner("Scraping en cours..."):
                    success, output = _run_scraping()
                    if success:
                        st.success("Scraping termine ! Rechargez la page.")
                        st.cache_data.clear()
                    else:
                        st.error(f"Erreur : {output[:500]}")

    with tabs[2]:
        if has_ademe and not df_ademe_f.empty:
            c1, c2 = st.columns([1, 1])
            with c1:
                chart_sunburst_carburant(df_ademe_f)
            with c2:
                chart_co2_distribution(df_ademe_f)
            st.caption(
                "Sunburst : cliquez sur un segment pour zoomer. "
                "Echelle de couleur = CO2 moyen (vert = faible, rouge = eleve). "
                "Les marques < 5% d'un carburant sont regroupees sous AUTRES."
            )
        else:
            st.warning("Aucune donnee ADEME pour ces filtres. "
                       "Veuillez retirer des filtres de marque ou de carburant dans la barre laterale.")

    with tabs[3]:
        if has_occ and not df_occ_f.empty:
            chart_prix_vs_km(df_occ_f)
            st.caption(
                "Chaque point = une annonce. Colore par carburant. "
                "La tendance descendante illustre la depreciation par kilometrage."
            )
        elif has_occ and df_occ_f.empty:
            st.warning("Aucun resultat pour les filtres selectionnes. "
                       "Veuillez elargir vos criteres dans la barre laterale.")
        else:
            st.info("Cette vue sera disponible apres execution du scraping.")
            if st.button("Lancer le Scraping maintenant", key="scrape_tab3", type="primary"):
                with st.spinner("Scraping en cours..."):
                    success, output = _run_scraping()
                    if success:
                        st.success("Scraping termine ! Rechargez la page.")
                        st.cache_data.clear()
                    else:
                        st.error(f"Erreur : {output[:500]}")

    with tabs[4]:
        if has_ademe and not df_ademe_f.empty:
            c1, c2 = st.columns([1, 1])
            with c1:
                chart_top_marques_co2(df_ademe_f)
            with c2:
                chart_prix_neuf_boxplot(df_ademe_f)
            st.caption(
                "Top 15 marques par CO2 moyen (ADEME). "
                "Boxplot des prix neufs par type de motorisation."
            )
        else:
            st.warning("Aucune donnee ADEME pour ces filtres. "
                       "Veuillez retirer des filtres pour afficher le referentiel.")

    # ==================================================================
    # Data Explorer
    # ==================================================================

    st.markdown("---")

    with st.expander("Explorateur de Donnees — Tables brutes & scores d'appariement", expanded=False):
        exp_t1, exp_t2 = st.tabs(["Annonces occasion", "Referentiel ADEME"])

        with exp_t1:
            if has_occ and not df_occ_f.empty:
                display_cols = [c for c in [
                    "marque", "modele", "version", "annee",
                    "prix_eur", "kilometrage_km", "carburant",
                    "match_score", "co2_ademe", "prix_neuf_ademe", "source",
                ] if c in df_occ_f.columns]
                st.dataframe(df_occ_f[display_cols].head(500),
                            use_container_width=True, height=400)
                st.caption(f"{len(df_occ_f)} annonces (max 500 affichees)")
                csv = df_occ_f[display_cols].to_csv(index=False).encode("utf-8")
                st.download_button("Exporter les annonces (CSV)", csv,
                                  f"annonces_occasion_{pd.Timestamp.now():%Y%m%d}.csv",
                                  "text/csv")
            else:
                st.info("Aucune annonce a afficher. Veuillez ajuster vos filtres ou executer le scraping.")

        with exp_t2:
            if has_ademe and not df_ademe_f.empty:
                display_cols_a = [c for c in [
                    "marque", "modele", "carburant", "boite_vitesses",
                    "co2_g_km", "puissance_kw", "puissance_fiscale",
                    "conso_mixte_min_l100", "prix_neuf_eur",
                ] if c in df_ademe_f.columns]
                st.dataframe(df_ademe_f[display_cols_a].head(500),
                            use_container_width=True, height=400)
                st.caption(f"{len(df_ademe_f)} vehicules ADEME (max 500 affichees)")
                csv_a = df_ademe_f[display_cols_a].to_csv(index=False).encode("utf-8")
                st.download_button("Exporter ADEME (CSV)", csv_a,
                                  f"ademe_techniques_{pd.Timestamp.now():%Y%m%d}.csv",
                                  "text/csv")
            else:
                st.info("Aucune donnee ADEME pour les filtres selectionnes.")

    # --- Footer ---
    st.markdown(
        '<div class="footer-pro">'
        '<p class="sources">'
        'Sources : <span style="color:#00D4AA;">ADEME</span> (Car Labelling, Licence Ouverte v2.0) '
        '&nbsp;|&nbsp; <span style="color:#6C5CE7;">AutoScout24</span> (collecte academique)'
        '</p>'
        '<p class="copy">'
        f'&copy; {datetime.now().year} &mdash; Ridwan &amp; Henri &nbsp;|&nbsp; '
        'Analytics Automobile &nbsp;|&nbsp; Sup de Vinci'
        '</p>'
        '</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
