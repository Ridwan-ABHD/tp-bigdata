# AutoInsight — Pipeline ETL Automobile

> **Analyse du Marché de l'Occasion vs Données Officielles ADEME**

Projet académique réalisé dans le cadre du **Bachelor 2 Data & IA** à Sup de Vinci.

**Auteurs** : Ridwan ABDOULKADER HOUMED & Henri TURCAS | **Avril 2026**

---

## Table des matières

1. [Ce que fait ce projet](#ce-que-fait-ce-projet)
2. [Phase 0 — Préparer votre machine](#phase-0--préparer-votre-machine)
3. [Phase 1 — Récupérer le projet](#phase-1--récupérer-le-projet)
4. [Phase 2 — Configurer l'environnement](#phase-2--configurer-lenvironnement)
5. [Phase 3 — Lancer le projet](#phase-3--lancer-le-projet-one-click)
6. [Phase 4 — Accéder aux interfaces](#phase-4--accéder-aux-interfaces)
7. [Comment savoir si ça marche ?](#comment-savoir-si-ça-marche-)
8. [Résolution des problèmes courants](#résolution-des-problèmes-courants)
9. [Architecture technique](#architecture-technique)
10. [Structure du projet](#structure-du-projet)

---

## Ce que fait ce projet

Ce projet compare **les données officielles des constructeurs automobiles** (émissions CO2, consommation, prix neuf) avec **les vrais prix du marché de l'occasion**.

### Concrètement, il permet de :

- **Visualiser la décote réelle** des véhicules selon leur motorisation
- **Comparer** les performances écologiques officielles vs le prix de revente
- **Interroger un assistant IA** ("Ancien") pour des estimations de prix

### Sources de données :

| Source | Type | Contenu |
|--------|------|---------|
| **ADEME** | API officielle | CO2, consommation, prix neuf de tous les véhicules neufs vendus en France |
| **AutoScout24** | Scraping web | Prix réels, kilométrage, année des annonces d'occasion |

---

## Phase 0 — Préparer votre machine

> **Temps estimé** : 15-30 minutes (première installation)

### Étape 1 : Installer Docker Desktop

Docker est un outil qui permet de lancer des applications dans des "conteneurs" (des mini-ordinateurs virtuels isolés). C'est ce qui fait tourner tout le projet.

1. **Téléchargez Docker Desktop** :
   - **Windows** : [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)
   - **Mac** : Même lien, choisissez "Mac with Intel chip" ou "Mac with Apple chip"
   - **Linux** : [docs.docker.com/desktop/install/linux-install](https://docs.docker.com/desktop/install/linux-install/)

2. **Lancez l'installateur** et suivez les instructions (cliquez "Suivant" partout)

3. **Redémarrez votre ordinateur** si demandé

4. **Lancez Docker Desktop** (icône baleine sur le bureau ou menu démarrer)

5. **Vérifiez que Docker fonctionne** en ouvrant un terminal :
   ```bash
   docker --version
   ```
   Vous devez voir quelque chose comme : `Docker version 24.x.x`

### Étape 2 : Installer Git (optionnel mais recommandé)

Git permet de télécharger le code source du projet.

- **Windows** : [git-scm.com/download/win](https://git-scm.com/download/win)
- **Mac** : Déjà installé, ou via `xcode-select --install` dans le terminal
- **Linux** : `sudo apt install git` (Ubuntu/Debian) ou `sudo dnf install git` (Fedora)

### Windows uniquement : Activer la virtualisation

Si Docker affiche une erreur au démarrage mentionnant "Hyper-V" ou "WSL 2" :

1. **Activez WSL 2** (Windows Subsystem for Linux) :
   - Ouvrez PowerShell **en administrateur** (clic droit → Exécuter en tant qu'administrateur)
   - Tapez :
     ```powershell
     wsl --install
     ```
   - Redémarrez l'ordinateur

2. **Si ça ne suffit pas**, activez la virtualisation dans le BIOS :
   - Redémarrez et appuyez sur F2, F10, F12 ou Suppr (selon votre PC) pour entrer dans le BIOS
   - Cherchez "Intel VT-x", "AMD-V" ou "Virtualization Technology"
   - Activez l'option et sauvegardez

---

## Phase 1 — Récupérer le projet

### Option A : Avec Git (recommandé)

Ouvrez un terminal (PowerShell sur Windows, Terminal sur Mac/Linux) :

```bash
# Placez-vous où vous voulez stocker le projet (ex: Bureau)
cd ~/Desktop

# Clonez le dépôt
git clone https://github.com/votre-repo/tp-bigdata.git

# Entrez dans le dossier
cd tp-bigdata
```

### Option B : Télécharger l'archive ZIP

1. Téléchargez l'archive ZIP du projet (fournie par l'enseignant ou sur GitHub)
2. **Décompressez-la** (clic droit → Extraire tout)
3. **Ouvrez un terminal** dans ce dossier :
   - Windows : Clic droit dans le dossier → "Ouvrir dans le terminal"
   - Mac : Clic droit → Services → "Nouveau terminal au dossier"

---

## Phase 2 — Configurer l'environnement

### Créer le fichier de configuration `.env`

Le fichier `.env` contient les "secrets" du projet (mots de passe des bases de données, clés API). Un fichier d'exemple est fourni.

**Sur Windows (PowerShell)** :
```powershell
Copy-Item .env.example .env
```

**Sur Mac/Linux** :
```bash
cp .env.example .env
```

### Que contient ce fichier ?

Ouvrez `.env` avec un éditeur de texte (Notepad, VS Code, etc.) pour voir :

```ini
# Base de données PostgreSQL (le "data warehouse")
DB_HOST=db
DB_PORT=5432
DB_USER=etl_user          # <- Nom d'utilisateur
DB_PASSWORD=etl_password  # <- Mot de passe
DB_NAME=etl_db            # <- Nom de la base

# MinIO (le "data lake" — stockage des fichiers bruts)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Rafraîchissement automatique (en secondes)
REFRESH_INTERVAL=3600     # <- 3600 = 1 heure
```

> **Pour un test local**, vous pouvez garder les valeurs par défaut. Modifiez-les uniquement si vous déployez en production.

### (Optionnel) Ajouter l'assistant IA "Ancien"

Pour activer l'assistant IA intégré au dashboard :

1. Créez un compte gratuit sur [console.groq.com](https://console.groq.com)
2. Générez une clé API
3. Ajoutez cette ligne à votre `.env` :
   ```ini
   GROQ_API_KEY=gsk_votre_cle_ici
   ```

---

## Phase 3 — Lancer le projet (One-Click)

### La commande magique

Assurez-vous d'être dans le dossier du projet, puis :

```bash
docker-compose up --build
```

### Que se passe-t-il ?

| Étape | Durée | Ce qui se passe |
|-------|-------|-----------------|
| 1. Build | 2-5 min | Docker télécharge Python, les librairies, construit l'application |
| 2. Démarrage | 30 sec | Les 4 services démarrent (base de données, stockage, app, scheduler) |
| 3. Pipeline ETL | 2-3 min | Téléchargement ADEME + Scraping AutoScout24 + Transformation |
| 4. Prêt ! | — | Le dashboard est accessible |

### Ce que vous devez voir dans le terminal

```
[+] Container tp-bigdata-db       Started
[+] Container tp-bigdata-minio    Started
[+] Container tp-bigdata          Started
[+] Container tp-bigdata-scheduler Started

[INFO] INGESTION ADEME — Démarrage
[INFO] Téléchargement depuis data.gouv.fr...
[INFO] 50000+ véhicules récupérés
[INFO] SCRAPING AUTOSCOUT24 - Démarrage
[INFO] 400+ annonces collectées
[INFO] Dashboard Streamlit prêt sur http://localhost:8501
```

### Lancer en arrière-plan (mode détaché)

Si vous ne voulez pas bloquer votre terminal :

```bash
docker-compose up --build -d
```

Le `-d` signifie "detached" (détaché). Les services tournent en fond.

Pour voir les logs ensuite :
```bash
docker-compose logs -f
```

---

## Phase 4 — Accéder aux interfaces

Une fois le projet lancé, ouvrez votre navigateur :

| Interface | URL | À quoi ça sert |
|-----------|-----|----------------|
| **Dashboard AutoInsight** | [localhost:8501](http://localhost:8501) | Interface principale — graphiques, analyses, assistant IA |
| **MinIO Console** | [localhost:9001](http://localhost:9001) | Voir les fichiers bruts stockés (JSON, CSV) |
| **PostgreSQL** | `localhost:5432` | Base de données (accessible via DBeaver, pgAdmin, etc.) |

### Connexion à MinIO Console

- **Utilisateur** : `minioadmin`
- **Mot de passe** : `minioadmin`

Vous y verrez les buckets (dossiers) contenant les données brutes :
- `etl-data/raw/ademe/` → Fichiers JSON de l'API ADEME
- `etl-data/raw/scraping/` → Fichiers JSON du scraping AutoScout24

### Connexion à PostgreSQL (optionnel)

Si vous avez un client SQL (DBeaver, pgAdmin, DataGrip) :

| Paramètre | Valeur |
|-----------|--------|
| Host | `localhost` |
| Port | `5432` |
| Database | `etl_db` |
| User | `etl_user` |
| Password | `etl_password` |

Tables disponibles :
- `caracteristiques_techniques` → Données ADEME (50 000+ véhicules)
- `annonces_occasion` → Annonces scrapées avec fuzzy join ADEME

---

## Comment savoir si ça marche ?

### Checklist de validation

| Vérification | Comment | Résultat attendu |
|--------------|---------|------------------|
| Docker tourne | `docker ps` | 4 conteneurs "Up" (app, scheduler, db, minio) |
| Dashboard accessible | Ouvrir [localhost:8501](http://localhost:8501) | Page avec graphiques et KPIs |
| Données ADEME | KPI "Véhicules ADEME" | > 50 000 |
| Données Occasion | KPI "Annonces Occasion" | > 200 |
| Assistant IA | Taper "Bonjour" dans Ancien (sidebar) | Réponse du chatbot |

### Ce que vous devez voir sur le Dashboard

```
+---------------------------------------------------------------+
|                        AutoInsight                            |
|         Référentiel ADEME vs Marché de l'Occasion             |
+-------------+-------------+-------------+-------------+-------+
|  Véhicules  |  Annonces   |    Taux     |   Décote    |  CO2  |
|   ADEME     |  Occasion   | Correspond. |  moyenne    | moyen |
|   50 154    |     259     |   100.0%    |   45.2%     |123g/km|
+-------------+-------------+-------------+-------------+-------+

[Onglets: Comparateur | Courbe Décote | Motorisations | Marché | ADEME]

                    Graphiques interactifs
```

### Commandes utiles pour vérifier

```bash
# Voir tous les conteneurs en cours
docker ps

# Voir les logs de l'application
docker logs tp-bigdata --tail 50

# Voir les logs du scheduler (rafraîchissement auto)
docker logs tp-bigdata-scheduler --tail 50

# Vérifier la santé des services
docker-compose ps
```

---

## Résolution des problèmes courants

### "Docker daemon is not running"

**Cause** : Docker Desktop n'est pas lancé.

**Solution** :
1. Lancez Docker Desktop (icône baleine)
2. Attendez que l'icône arrête de clignoter (30 sec)
3. Réessayez `docker-compose up --build`

---

### "Port 5432 is already in use"

**Cause** : PostgreSQL est déjà installé sur votre machine et utilise ce port.

**Solution** : Changez le port dans `docker-compose.yml` :
```yaml
ports:
  - "5433:5432"  # Utilisez 5433 au lieu de 5432
```

Ou arrêtez PostgreSQL local :
```bash
# Windows
net stop postgresql

# Mac/Linux
sudo systemctl stop postgresql
```

---

### "Port 8501 is already in use"

**Cause** : Une autre application Streamlit tourne déjà.

**Solution** :
```bash
# Trouvez et tuez le processus
# Windows PowerShell :
Get-Process -Id (Get-NetTCPConnection -LocalPort 8501).OwningProcess | Stop-Process

# Mac/Linux :
lsof -i :8501 | awk 'NR>1 {print $2}' | xargs kill
```

---

### "Error: Cannot connect to database"

**Cause** : La base de données n'est pas encore prête au moment où l'app démarre.

**Solution** : Attendez 30 secondes et rafraîchissez la page, ou relancez :
```bash
docker-compose restart app
```

---

### "WSL 2 installation is incomplete" (Windows)

**Solution** :
```powershell
# Dans PowerShell administrateur
wsl --install
wsl --set-default-version 2
```
Puis redémarrez l'ordinateur.

---

### Le scraping ne récupère aucune annonce

**Causes possibles** :
- AutoScout24 a changé sa structure HTML
- Vous êtes bloqué temporairement (trop de requêtes)

**Solution** : Attendez quelques minutes et relancez le scraping depuis le dashboard (bouton "Actualiser" dans la sidebar).

---

### Repartir de zéro

Si tout est cassé et que vous voulez recommencer :

```bash
# Arrêter et supprimer tous les conteneurs + données
docker-compose down -v

# Supprimer les images construites
docker system prune -a

# Relancer proprement
docker-compose up --build
```

**Attention** : La commande `down -v` supprime toutes les données (base de données, fichiers MinIO).

---

## Architecture technique

```
+-----------------------------------------------------------------------------+
|                              SOURCES DE DONNEES                             |
+---------------------------------+-------------------------------------------+
|         API ADEME               |           Scraping AutoScout24            |
|       (data.gouv.fr)            |         (annonces occasion)               |
+---------------+-----------------+---------------------+---------------------+
                |                                       |
                v                                       v
+-----------------------------------------------------------------------------+
|                           DATA LAKE (MinIO)                                 |
|                    Stockage brut JSON/CSV (S3-compatible)                   |
|         raw/ademe/*.json                    raw/scraping/*.json             |
+-----------------------------------+-----------------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------------+
|                      TRANSFORMATION (Python/Pandas)                         |
|   - Nettoyage (prix aberrants, doublons)                                    |
|   - Fuzzy Join ADEME <-> Annonces (SequenceMatcher, score >= 0.85)          |
|   - Validation carburant (évite électrique <-> thermique)                   |
+-----------------------------------+-----------------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------------+
|                       DATA WAREHOUSE (PostgreSQL)                           |
|      caracteristiques_techniques          annonces_occasion                 |
|         (référentiel ADEME)              (marché + match ADEME)             |
+-----------------------------------+-----------------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------------+
|                          DASHBOARD (Streamlit)                              |
|   - Visualisations Plotly (décote, CO2, prix/km)                            |
|   - Filtres interactifs (marque, carburant, année)                          |
|   - Assistant IA "Ancien" (Groq/Llama3)                                     |
+-----------------------------------------------------------------------------+
```

### Stack technique

| Couche | Technologie | Rôle |
|--------|-------------|------|
| **Conteneurisation** | Docker + Docker Compose | Isolation et orchestration |
| **Data Lake** | MinIO | Stockage objet S3-compatible |
| **Data Warehouse** | PostgreSQL 16 | Base de données analytique |
| **ETL** | Python 3.11, Pandas | Ingestion, nettoyage, transformation |
| **Scraping** | BeautifulSoup4, Requests | Collecte web |
| **Dashboard** | Streamlit, Plotly | Visualisation interactive |
| **IA** | Groq Cloud (Llama 3.3) | Assistant conversationnel |

---

## Structure du projet

```
tp-bigdata/
|-- docker-compose.yml       # Orchestration des 4 services
|-- Dockerfile               # Image Python de l'application
|-- requirements.txt         # Dépendances Python
|-- .env.example             # Template de configuration
|-- .env                     # Configuration locale (à créer)
|-- README.md                # Ce fichier !
|
|-- script/
|   +-- src/
|       |-- app.py                      # Dashboard Streamlit + Assistant Ancien
|       |-- ingestion.py                # Téléchargement API ADEME
|       |-- scraping_autoscout.py       # Scraping AutoScout24
|       |-- transformation.py           # Nettoyage données ADEME
|       |-- transformation_scraping.py  # Fuzzy join + chargement occasion
|       |-- load.py                     # Chargement PostgreSQL
|       |-- scheduler.py                # Rafraîchissement automatique (1h)
|       +-- main.py                     # Pipeline complet
|
+-- documentation/
    |-- presentation/        # Slides de présentation
    |-- gouvernance/         # Documentation gouvernance des données
    +-- cours/               # Ressources de cours
```

---

## Arrêter le projet

```bash
# Arrêter les conteneurs (conserve les données)
docker-compose stop

# Arrêter et supprimer les conteneurs (conserve les données)
docker-compose down

# Arrêter et SUPPRIMER les données (base + fichiers)
docker-compose down -v
```

---

## Support

En cas de problème :

1. Vérifiez la section [Résolution des problèmes](#résolution-des-problèmes-courants)
2. Consultez les logs : `docker-compose logs`
3. Contactez les auteurs du projet

---

<div align="center">

**AutoInsight** — Projet Big Data Bachelor 2  
Sup de Vinci — Avril 2026

*Données ADEME sous Licence Ouverte v2.0 | Scraping AutoScout24 à usage académique uniquement*

</div>

