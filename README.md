# Pipeline ETL - Analyse du Marche Automobile d'Occasion

Projet academique realise dans le cadre du Bachelor 2 a Sup de Vinci.

## Auteurs

- **Ridwan ABDOULKADER HOUMED**
- **Henri TURCAS**

## Contexte et Objectifs

Ce projet porte sur l'analyse du marche de l'automobile d'occasion en France. L'objectif principal est de mesurer l'ecart entre les promesses theoriques des constructeurs et la realite economique du terrain.

### Sources de donnees

**Donnees officielles (ADEME Car Labelling)**

Les donnees de l'Agence de l'Environnement et de la Maitrise de l'Energie constituent le referentiel de confiance concernant les performances ecologiques et techniques des vehicules neufs

**Donnees du marche (Scraping)**

Les donnees reelles du marche de la seconde main sont collectees via le scraping d'annonces de vente en ligne.

### Valeur ajoutee

Cette confrontation entre donnees officielles et donnees terrain permet d'apporter une transparence inedite sur plusieurs points :

- La decote reelle des vehicules en fonction de leur motorisation (Essence, Diesel, Electrique)
- La correlation entre les performances environnementales officielles et le prix de revente sur le marche
- L'identification d'eventuelles anomalies de prix par rapport aux caracteristiques techniques reelles du vehicule

---

## Architecture Technique

Le pipeline ETL suit le flux suivant :

```
Ingestion -> Stockage Brut -> Transformation -> Entreposage -> Visualisation
```

### 1. Ingestion

- **API REST** : Recuperation des donnees officielles via l'API ADEME
- **Web Scraping** : Collecte des annonces du marche de l'occasion

### 2. Stockage Brut (Data Lake)

- **MinIO** : Systeme de stockage objet S3-compatible
- Conservation des donnees brutes au format JSON/CSV

### 3. Transformation

- **Pandas** : Nettoyage, normalisation et enrichissement des donnees
- Jointure entre les sources ADEME et les annonces scrapees

### 4. Entreposage (Data Warehouse)

- **PostgreSQL** : Base de donnees relationnelle pour les donnees transformees
- Schema optimise pour l'analyse

### 5. Visualisation

- **Streamlit** : Dashboard interactif pour l'exploration des donnees

---

## Stack Technique

### Infrastructure

- Docker
- Docker Compose

### Stockage

- MinIO (Data Lake)
- PostgreSQL (Data Warehouse)

### Traitement

- Python 3.x
- Pandas

### Collecte

- Requests (API)
- BeautifulSoup4 (Scraping)

### Interface

- Streamlit

### Configuration

- python-dotenv

---

## Installation

Le projet est entierement containerise. Pour lancer l'ensemble des services :

```bash
docker-compose up -d
```

### Services disponibles

| Service    | Port | Description                    |
|------------|------|--------------------------------|
| Streamlit  | 8501 | Dashboard de visualisation     |
| PostgreSQL | 5432 | Data Warehouse                 |
| MinIO API  | 9000 | API de stockage objet          |
| MinIO UI   | 9001 | Interface d'administration     |

### Configuration

Copier le fichier d'exemple et adapter les variables :

```bash
cp .env.example .env
```

---

## Structure du Projet

```
tp-bigdata/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── script/
│   └── src/
├── documentation/
│   ├── presentation/
│   ├── gouvernance/
│   └── cours/
└── README.md
```

