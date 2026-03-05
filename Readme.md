# Les Français·es face à l'Audiovisuel — Projet INA

> Projet réalisé dans le cadre du cursus ingénieur 4e année 
> En partenariat open data avec les données ouvertes de l'**Institut National de l'Audiovisuel (INA)**

---

##  Équipe

| Étudiante | Rôle | Responsabilité |
|---|---|---|
| Maelle FOTSO | Data Engineering | Pipeline, ingestion, MinIO, Airflow |
| Louay NAGATI | Data Science / ML | EDA, modélisation, couche Gold |
| Dominique LONANG | Data Viz / Gestion de projet | Dashboard, Trello, rapport Word |

---

##  Objectifs du projet

Ce projet vise à analyser l'évolution de l'audiovisuel français selon deux axes principaux :

1. **L'évolution thématique** des journaux télévisés et émissions de radio françaises
2. **La représentation des femmes** à l'antenne — temps de parole, présence par chaîne et par période

Questions analytiques traitées :
- Quels thèmes ont progressé dans les JT depuis 2010 ? Depuis le COVID ?
- Quelle est l'évolution du temps de parole féminin sur TF1 vs France 2 ?
- Les chaînes d'info continue sont-elles thématiquement proches les unes des autres ?
- Y a-t-il des pics de représentation féminine liés à des événements précis ?

---

##  Architecture du pipeline — Medallion (Bronze / Silver / Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│              data.gouv.fr / INA open data                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Script Python (ingestion)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     COUCHE BRONZE — MinIO                      │
│         Données brutes, non transformées, telles que reçues      │
│              Format : CSV brut / JSON / fichiers source          │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Nettoyage & structuration (Python/Pandas)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     COUCHE SILVER — MinIO                      │
│     Données nettoyées, typage correct, doublons supprimés        │
│              Format : Parquet ou CSV structuré                   │
└───────────────────────────┬─────────────────────────────────────┘
                            │ ML / Agrégations / Indicateurs
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      COUCHE GOLD — MinIO                       │
│    Résultats ML, prédictions, indicateurs prêts à visualiser     │
│              Format : CSV ou Parquet agrégé                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Visualisation (Plotly / Dash)
                            ▼
                     DASHBOARD INTERACTIF
                            │
                     ORCHESTRATION : Apache Airflow
```

---

## Jeux de données utilisés

### Dataset 1 — Temps de parole Hommes / Femmes

| Champ | Détail |
|---|---|
| **Nom** | Temps de parole des hommes et des femmes à la télévision et à la radio |
| **Source** | INA via data.gouv.fr |
| **URL** | https://www.data.gouv.fr/datasets/temps-de-parole-des-hommes-et-des-femmes-a-la-television-et-a-la-radio/ |
| **Licence** | Licence Ouverte v2.0 (Etalab) |
| **Format** | CSV |
| **Fréquence de mise à jour** | Quotidienne |
| **Période couverte** | 1995 – 2019 |
| **Volume** | > 1 000 000 heures de programmes analysées |
| **Nombre de lignes** | ~500 000 — *à confirmer après ingestion Bronze* |
| **Nombre de colonnes** | ~10 — *à confirmer après ingestion Bronze* |
| **Méthode de collecte** | Détection automatique via *inaSpeechSegmenter* (ML) |
| **Stockage Bronze** | `minio://bronze/ina/parole_hf_raw.csv` |
| **Stockage Silver** | `minio://silver/ina/parole_hf_clean.parquet` |
| **Stockage Gold** | `minio://gold/ina/parole_hf_results.parquet` |

**Colonnes principales :**

| Colonne | Type | Description |
|---|---|---|
| `channel` | `str` | Nom de la chaîne ou station (ex : TF1, France Inter) |
| `date` | `date` | Date de diffusion (YYYY-MM-DD) |
| `duration_female` | `float` | Durée de parole féminine (secondes) |
| `duration_male` | `float` | Durée de parole masculine (secondes) |
| `duration_noise` | `float` | Durée classée bruit de fond |
| `duration_music` | `float` | Durée classée musique |
| `total_duration` | `float` | Durée totale analysée |
| `pct_female` | `float` | % temps de parole féminin |
| `pct_male` | `float` | % temps de parole masculin |
| `media_type` | `str` | Type de média : `tv` ou `radio` |

---

### Dataset 2 — Portail de visualisation INA (data.ina.fr)

| Champ | Détail |
|---|---|
| **Nom** | Tendances médiatiques — personnalités, genre, mots, lieux |
| **Source** | INA — portail data |
| **URL** | https://data.ina.fr |
| **Format** | Interface web + exports |
| **Période couverte** | Janvier 2019 – Juin 2024 |
| **Contenu** | JT, chaînes d'info en continu, radios |
| **Usage** | Validation visuelle, compréhension des thématiques |

---

##  Transformations réalisées par couche

###  Bronze → Silver (nettoyage)

| Transformation | Description |
|---|---|
| Suppression des doublons | Lignes identiques sur `channel + date` supprimées |
| Correction des types | `date` converti en `datetime`, durées en `float` |
| Gestion des valeurs manquantes | Lignes sans `channel` ou `date` supprimées ; durées nulles → `0` |
| Calcul des pourcentages | `pct_female` et `pct_male` recalculés si absents |
| Filtre temporel | Conservation des données 1995–2019 uniquement |

###  Silver → Gold (analyse & ML)

| Transformation | Description |
|---|---|
| Agrégation mensuelle | Moyennes de `pct_female` / `pct_male` par chaîne et par mois |
| Feature engineering | Indicateurs : tendance sur 12 mois glissants, écart à la moyenne nationale |
| Modèle ML | KNN / Random Forest pour mesurer la proximité thématique entre chaînes |
| Résultats | Prédictions et métriques exportées en Parquet dans la couche Gold |

---

##  Stack technique

### Infrastructure & Orchestration

| Outil | Usage |
|---|---|
| **MinIO** | Data Lake local — stockage des couches Bronze / Silver / Gold |
| **Apache Kafka** *(optionnel)* | Ingestion en streaming si données temps réel |
| **Apache Airflow** | Orchestration du pipeline complet via DAGs |
| **Docker** | Conteneurisation de MinIO et Airflow |

### Langages & Environnements

| Outil | Usage |
|---|---|
| `Python 3.11` | Langage principal |
| `Jupyter Notebook` | EDA, exploration, prototypage |
| `VS Code` | Développement des scripts |

### Data Engineering

| Bibliothèque | Usage |
|---|---|
| `pandas` | Manipulation et nettoyage des données |
| `numpy` | Calculs numériques |
| `requests` | Téléchargement depuis data.gouv.fr |
| `minio` (SDK Python) | Lecture/écriture dans les couches Bronze/Silver/Gold |
| `pyarrow` | Lecture/écriture Parquet |

### Data Science & Machine Learning

| Bibliothèque | Usage |
|---|---|
| `scikit-learn` | KNN, Random Forest, métriques d'évaluation |
| `matplotlib` / `seaborn` | Visualisations exploratoires (EDA) |
| `MLlib` *(optionnel)* | Si passage à PySpark pour les gros volumes |

### Visualisation & Dashboard

| Outil | Usage |
|---|---|
| `Plotly / Dash` | Dashboard interactif (filtres chaîne, période, thème) |
| `Figma` | Maquettes et wireframes |

### Collaboration & Gestion de projet

| Outil | Usage |
|---|---|
| **Git / GitHub** | Versioning du code, Pull Requests, revues de code |
| **Trello** | Backlog, suivi des sprints, assignation des tâches par étudiante |
| **Google Drive** | Rapport final (Word), livrables documentaires partagés |

---

## 🗂️ Structure du dépôt

```
📁 ina-audiovisuel-project/
│
├── 📁 ingestion/                   # Étape 1 — Couche Bronze
│   ├── download_data.py            # Téléchargement des datasets INA
│   └── upload_to_bronze.py         # Envoi vers MinIO (bucket bronze)
│
├── 📁 processing/                  # Étape 2 — Couche Silver
│   ├── clean_data.py               # Nettoyage, typage, suppression doublons
│   └── upload_to_silver.py         # Envoi vers MinIO (bucket silver)
│
├── 📁 ml/                          # Étape 3 — Couche Gold
│   ├── features.py                 # Feature engineering, indicateurs
│   ├── train_model.py              # Entraînement KNN / Random Forest
│   ├── evaluate_model.py           # Métriques d'évaluation
│   └── upload_to_gold.py           # Envoi résultats vers MinIO (bucket gold)
│
├── 📁 airflow/                     # Étape 4 — Orchestration
│   └── dags/
│       └── ina_pipeline_dag.py     # DAG : Bronze → Silver → Gold → Dashboard
│
├── 📁 dashboard/                   # Étape 4 — Visualisation
│   └── app.py                      # Application Dash (lecture depuis Gold)
│
├── 📁 notebooks/                   # Exploration & prototypage
│   ├── 01_EDA_bronze.ipynb         # Exploration des données brutes
│   ├── 02_cleaning_silver.ipynb    # Tests de nettoyage
│   └── 03_modelling_gold.ipynb     # Prototypage des modèles ML
│
├── 📁 reports/                     # Livrables documentaires
│   ├── pipeline_schema.png         # Schéma de l'architecture
│   └── rapport_final.docx          # Rapport Word (synchronisé Google Drive)
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

##  Installation & lancement

```bash
# 1. Cloner le dépôt
git clone https://github.com/<org>/ina-audiovisuel-project.git
cd ina-audiovisuel-project

# 2. Créer un environnement virtuel
python -m venv venv
source venv/bin/activate  # Windows : venv\Scripts\activate

# 3. Installer les dépendances Python
pip install -r requirements.txt

# 4. Démarrer MinIO (via Docker)
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=password \
  minio/minio server /data --console-address ":9001"
# Console MinIO disponible sur : http://localhost:9001

# 5. Étape 1 — Ingestion Bronze
python ingestion/download_data.py
python ingestion/upload_to_bronze.py

# 6. Étape 2 — Nettoyage Silver
python processing/clean_data.py
python processing/upload_to_silver.py

# 7. Étape 3 — ML Gold
python ml/train_model.py
python ml/upload_to_gold.py

# 8. Étape 4 — Dashboard
python dashboard/app.py

# 9. Étape 4 — Orchestration Airflow
airflow standalone
# DAG disponible sur : http://localhost:8080
```

---

##  Planning du projet (4 semaines)

| Semaine | Étape | Objectif | Livrable |
|---|---|---|---|
| **S1** | Étape 1 — Bronze | Setup MinIO, script d'ingestion, données brutes stockées | Couche Bronze opérationnelle + README source |
| **S2** | Étape 2 — Silver | EDA, nettoyage, transformations documentées | Couche Silver + README transformations |
| **S3** | Étapes 3 & 4 — Gold | ML + Airflow + Dashboard v1 | Modèle évalué + pipeline orchestré + dashboard |
| **S4** | Étape 5 | Tests complets, rapport Word, soutenance | Rapport final Google Drive + démo live |

---

##  Limites & points de vigilance

- Le genre est **inféré automatiquement** par *inaSpeechSegmenter* — des erreurs de classification existent
- Les données s'arrêtent en **2019** — la période COVID n'est pas couverte par le dataset principal
- **MinIO doit être démarré** avant tout script d'ingestion ou de traitement
- Les trois buckets MinIO (`bronze`, `silver`, `gold`) doivent être **créés manuellement** avant le premier lancement
- Le DAG Airflow dépend du bon fonctionnement de chaque étape en amont

---

##  Licence des données

Les données INA sont publiées sous **Licence Ouverte v2.0 (Etalab)** — réutilisation libre sous condition de mentionner la source.

---

##  Encadrement & contexte académique

> Projet réalisé dans le cadre du cursus Ingénieur 4e année —
> Encadrant·e : Mme Raja CHIKY
> École : 3IL Ingénieurs