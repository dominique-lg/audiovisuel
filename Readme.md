# 📺 Les Français·es face à l'Audiovisuel — Projet INA

> Projet réalisé dans le cadre du cursus Ingénieur 4e année  
> Encadrant·e : **Mme Raja CHIKY** | École : **3IL Ingénieurs**  
> Données : **INA · ARCOM** via data.gouv.fr | Licences Ouvertes v2.0 (Etalab)

---

## 👥 Équipe

| Étudiante | Rôle | Responsabilité |
|---|---|---|
| **Maelle FOTSO** | Data Engineering | Pipeline Python, ingestion 5 datasets, MinIO, Airflow |
| **Louay NAGATI** | Data Science / ML | EDA, modélisation, couche Gold, croisements |
| **Dominique LONANG** | Data Viz / Gestion de projet | Dashboard Power BI 5 pages, Trello, rapport Google Docs |

---

## 🎯 Problématique & couverture complète

> *Comment les Françaises et les Français font-ils face à leur audiovisuel ?
> Qui parle à l'antenne, de quoi parle-t-on, et comment s'en empare-t-on ?*

Le projet couvre les **trois axes** avec **5 datasets open data réels et vérifiés** sur data.gouv.fr :

| Axe | Questions analytiques | Dataset(s) |
|---|---|---|
| **① Représentation des femmes** | Évolution du % de parole féminine par chaîne 1995–2019 · Quelle chaîne est la plus paritaire ? · Quand la parité 50% sera-t-elle atteinte ? | **DS1** INA parole H/F |
| **① Représentation des femmes** | Présence à l'écran, parole, expertes, journalistes : métriques officielles déclarées par les chaînes au CSA | **DS2** INA × CSA |
| **② Évolution des thèmes diffusés** | Faits divers en hausse sur TF1 vs France 2 ? · Environnement post-2015 ? · Quels thèmes dominent par chaîne ? | **DS3** INA thèmes JT |
| **③ Rapport des Français aux médias** | Comment s'informe-t-on en 2024 ? · TV vs réseaux sociaux vs presse · Confiance envers les médias · Sources d'info par âge | **DS4** ARCOM « Les Français et l'information » |
| **③ Rapport des Français aux médias** | TV linéaire vs streaming vs podcasts vs RS · Équipements · Temps passé par écran · Modes de consommation | **DS5** ARCOM « Tendances audio-vidéo » |

---

## 🗂️ Les 5 datasets — Sources vérifiées sur data.gouv.fr

---

### 📂 DS1 — Temps de parole H/F à la TV et à la radio (INA)

| | |
|---|---|
| **Nom** | Temps de parole des hommes et des femmes à la télévision et à la radio |
| **URL** | https://www.data.gouv.fr/datasets/temps-de-parole-des-hommes-et-des-femmes-a-la-television-et-a-la-radio/ |
| **Licence** | Licence Ouverte v2.0 (Etalab) |
| **Format** | CSV (séparateur `;`) |
| **Période** | 1995 – 28 février 2019 |
| **Couverture** | 34 chaînes TV + 21 stations radio |
| **Volume** | > 1 000 000 heures de programmes |
| **Méthode** | Détection automatique *inaSpeechSegmenter* (CNN open source INA) |

**Colonnes :**

| Colonne | Type | Description |
|---|---|---|
| `channel` | str | Chaîne ou station |
| `date` | date | Date `YYYY-MM-DD` |
| `duration_female` | float | Durée parole féminine (secondes) |
| `duration_male` | float | Durée parole masculine (secondes) |
| `duration_noise` | float | Durée bruit de fond |
| `duration_music` | float | Durée musique |
| `total_duration` | float | Durée totale analysée |
| `pct_female` | float | % temps de parole féminin |
| `pct_male` | float | % temps de parole masculin |
| `media_type` | str | `tv` ou `radio` |

**MinIO :** `bronze/ds1/parole_hf_raw.csv` → `silver/ds1/parole_hf_clean.parquet`

---

### 📂 DS2 — Temps de parole H/F dans les programmes déclarés au CSA (INA × ARCOM)

| | |
|---|---|
| **Nom** | Temps de parole des femmes et des hommes dans les programmes ayant fait l'objet d'une déclaration au CSA |
| **URL** | https://www.data.gouv.fr/datasets/temps-de-parole-des-femmes-et-des-hommes-dans-les-programmes-ayant-fait-lobjet-dune-declaration-au-csa-pour-son-rapport-portant-sur-la-representation-des-femmes-a-la-television-et-la-radio/ |
| **Licence** | Licence Ouverte v2.0 (Etalab) |
| **Format** | CSV |
| **Période** | 2016 – 2020 |
| **Apport** | 4 métriques officielles de représentation féminine déclarées par les chaînes |

**Colonnes :**

| Colonne | Type | Description |
|---|---|---|
| `channel` | str | Chaîne ou station |
| `year` | int | Année de déclaration |
| `pct_female_presence` | float | % présence féminine (apparitions à l'écran) |
| `pct_female_speech` | float | % temps de parole féminin |
| `pct_female_experts` | float | % d'expertes parmi les invités |
| `pct_female_journalists` | float | % de journalistes femmes |
| `media_type` | str | `tv` ou `radio` |

**MinIO :** `bronze/ds2/csa_raw.csv` → `silver/ds2/csa_clean.parquet`

---

### 📂 DS3 — Classement thématique des sujets de JT (INA)

| | |
|---|---|
| **Nom** | Classement thématique des sujets de journaux télévisés (janvier 2000 – décembre 2020) |
| **URL** | https://www.data.gouv.fr/datasets/classement-thematique-des-sujets-de-journaux-televises-janvier-2000-decembre-2020/ |
| **Licence** | Licence Ouverte v2.0 (Etalab) |
| **Format** | CSV |
| **Période** | 2000 – 2020 |
| **Couverture** | 5 chaînes : TF1, France 2, France 3, Arte, M6 |
| **Méthode** | Indexation documentaire INA depuis 1990 · 14 rubriques thématiques |
| **Mise à jour** | 15 octobre 2024 |

**14 rubriques :** Politique intérieure · International · Économie · Société · Faits divers · Sport · Culture · Environnement · Justice · Santé · Sciences & Techno · Météo · Défense · Autres

**Colonnes :**

| Colonne | Type | Description |
|---|---|---|
| `channel` | str | Chaîne TV |
| `date` | date | Date du JT |
| `theme` | str | Rubrique (parmi 14) |
| `nb_sujets` | int | Nombre de sujets diffusés |
| `pct_sujets` | float | % du temps d'antenne consacré |
| `duration_seconds` | float | Durée totale du thème (secondes) |

**MinIO :** `bronze/ds3/themes_jt_raw.csv` → `silver/ds3/themes_jt_clean.parquet`

---

### 📂 DS4 — Les Français et l'information (ARCOM 2024)

| | |
|---|---|
| **Nom** | Les Français et l'information — Baromètre ARCOM 2024 |
| **URL** | https://www.data.gouv.fr/datasets/les-francais-et-linformation-2024/ |
| **Licence** | Licence Ouverte v2.0 (Etalab) |
| **Format** | CSV (micro-données individuelles + livre de codes + guide d'utilisation) |
| **Période** | 2024 (enquête ponctuelle, édition annuelle) |
| **Échantillon** | 4 336 Français de 15 ans et plus, représentatif (méthode des quotas) |
| **Méthode** | Questionnaire auto-administré en ligne, 25 min — Institut CSA pour ARCOM |
| **Apport direct** | Comment les Français s'informent-ils ? · Confiance envers les médias · Place des RS · Image des journalistes · Vision de la régulation |

**Variables clés (après sélection) :**

| Variable | Description |
|---|---|
| `poids` | Pondération individuelle — **obligatoire** pour toute statistique |
| `age_groupe` | Tranche d'âge (15-24, 25-34, 35-49, 50-64, 65+) |
| `source_info_tv` | S'informe via la TV chaque jour (0/1) |
| `source_info_radio` | S'informe via la radio (0/1) |
| `source_info_rs` | S'informe via les réseaux sociaux (0/1) |
| `source_info_presse` | S'informe via la presse écrite (0/1) |
| `confiance_media` | Fait confiance aux médias traditionnels (0/1) |
| `rs_unique_source` | S'informe exclusivement via les RS (0/1) |
| `interet_info` | Intérêt déclaré pour l'information politique et générale |

> ⚠️ **Important** : 88% des répondants ont un intérêt pour l'information. Certaines questions n'ont été posées qu'à ce sous-groupe. Appliquer le filtre et les pondérations.

**MinIO :** `bronze/ds4/francais_info_raw.csv` → `silver/ds4/francais_info_clean.parquet`

---

### 📂 DS5 — Tendances audio-vidéo — Baromètre ARCOM

| | |
|---|---|
| **Nom** | Tendances audio-vidéo — Baromètre ARCOM |
| **URL** | https://www.data.gouv.fr/datasets/tendances-audio-video-barometre/ |
| **Licence** | Licence Ouverte v2.0 (Etalab) |
| **Format** | CSV (micro-données + livre de codes + guide d'utilisation) |
| **Période** | 2024 (terrain : 15–29 novembre 2024) |
| **Échantillon** | 4 336 Français de 15 ans et + · sur-échantillon 305 utilisateurs Smart TV |
| **Méthode** | Questionnaire auto-administré en ligne — Institut CSA pour ARCOM |
| **Apport direct** | TV linéaire vs SVOD vs YouTube vs RS · Modes de réception · Équipements (Smart TV, smartphone) · Temps passé par écran · Consommation radio et podcasts |

**Variables clés (après sélection) :**

| Variable | Description |
|---|---|
| `poids` | Pondération individuelle |
| `age_groupe` | Tranche d'âge |
| `tv_lineaire_freq` | Fréquence de consommation TV linéaire |
| `svod_usage` | Abonné à un service SVOD (Netflix, Disney+…) (0/1) |
| `youtube_freq` | Fréquence d'usage de YouTube |
| `rs_video_freq` | Consommation de vidéos sur RS (TikTok, Instagram…) |
| `radio_freq` | Fréquence d'écoute radio |
| `podcast_freq` | Fréquence d'écoute podcast |
| `smart_tv` | Possède une Smart TV (0/1) |
| `smartphone_video` | Regarde des vidéos sur smartphone (0/1) |
| `duree_tv_quotidien` | Durée quotidienne estimée devant la TV (tranches) |

**MinIO :** `bronze/ds5/tendances_av_raw.csv` → `silver/ds5/tendances_av_clean.parquet`

---

## 🔗 Croisements analytiques (Gold)

| Croisement | Question | Datasets |
|---|---|---|
| DS1 × DS3 | Les chaînes avec plus de faits divers sont-elles moins paritaires ? | DS1 + DS3 |
| DS1 × DS2 | Écart entre métriques INA automatiques et déclarations officielles CSA | DS1 + DS2 |
| DS3 × DS4 | Ce que traitent les JT reflète-t-il ce que les Français veulent savoir ? | DS3 + DS4 |
| DS4 × DS5 | Corrélation confiance médias / abandon TV linéaire / montée RS | DS4 + DS5 |
| DS1 × DS5 | Stagnation parité et érosion TV linéaire : tendances parallèles ? | DS1 + DS5 |

---

## 🏗️ Architecture — Pipeline Medallion

```
┌────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                               │
│  DS1 INA parole H/F  ·  DS2 INA×CSA  ·  DS3 INA thèmes JT        │
│  DS4 ARCOM Français & info  ·  DS5 ARCOM Tendances audio-vidéo    │
│           data.gouv.fr — Toutes sous Licence Ouverte v2.0          │
└──────────────────────────┬─────────────────────────────────────────┘
                           │  ingestion/download_all.py
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                  🥉 COUCHE BRONZE — MinIO                          │
│  bronze/ds1/  bronze/ds2/  bronze/ds3/  bronze/ds4/  bronze/ds5/  │
│  CSV bruts tels que reçus — aucune transformation                  │
└──────────────────────────┬─────────────────────────────────────────┘
                           │  processing/clean_all.py
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                  🥈 COUCHE SILVER — MinIO                          │
│  silver/ds1/  silver/ds2/  silver/ds3/  silver/ds4/  silver/ds5/  │
│  Parquet nettoyé · typé · pondérations appliquées                  │
└──────────────────────────┬─────────────────────────────────────────┘
                           │  ml/features.py + ml/train_model.py
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│                  🥇 COUCHE GOLD — MinIO                            │
│  KPIs par axe · croisements · ML · exports Power BI               │
│  gold/kpis/   gold/ml/   gold/powerbi_exports/                     │
└──────────────────────────┬─────────────────────────────────────────┘
                           │  CSV → Power BI Desktop
                           ▼
                   📊 POWER BI — 5 pages
              ☁️ Power BI Service (partage équipe)
              ⚙️ Airflow DAG mensuel
```

---

## 📁 Structure du dépôt

```
📁 ina-audiovisuel-project/
│
├── 📁 ingestion/
│   └── download_all.py         # Télécharge les 5 datasets + upload Bronze
│
├── 📁 processing/
│   └── clean_all.py            # Nettoie les 5 datasets + upload Silver
│
├── 📁 ml/
│   ├── features.py             # KPIs + croisements inter-datasets
│   ├── train_model.py          # KNN + Random Forest (DS1)
│   ├── evaluate_model.py       # Métriques ML
│   └── upload_to_gold.py       # Upload résultats + exports Power BI
│
├── 📁 airflow/dags/
│   └── ina_pipeline_dag.py
│
├── 📁 powerbi/
│   ├── ina_dashboard.pbix      # Fichier Power BI (GitHub)
│   └── README_powerbi.md       # Guide import + DAX
│
├── 📁 notebooks/
│   ├── 01_EDA_DS1_parole_hf.ipynb
│   ├── 02_EDA_DS2_csa.ipynb
│   ├── 03_EDA_DS3_themes_jt.ipynb
│   ├── 04_EDA_DS4_francais_info.ipynb
│   ├── 05_EDA_DS5_tendances_av.ipynb
│   └── 06_gold_croisements.ipynb
│
├── 📁 config/
│   ├── config.py
│   └── minio_utils.py
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 🔄 Transformations documentées

### 🥉 Bronze → 🥈 Silver

#### DS1 — INA parole H/F

| # | Transformation |
|---|---|
| 1 | Normalisation colonnes snake_case + mapping alias (`chaine`→`channel`, etc.) |
| 2 | `date` → datetime · durées → float (remplacement virgule décimale) |
| 3 | Suppression lignes sans `channel` ou `date` |
| 4 | Durées manquantes → 0.0 |
| 5 | Suppression doublons (`channel + date`) |
| 6 | Recalcul `pct_female/male` depuis durées si absents ou si somme ≠ 100% |
| 7 | Capping [0, 100] sur tous les % |
| 8 | Inférence `media_type` depuis le nom de chaîne si absent |
| 9 | Filtrage 1995–2019 |
| 10 | Colonnes dérivées : `year`, `month`, `year_month`, `ecart_parite`, `above_arcom` |

#### DS2 — INA × CSA

| # | Transformation |
|---|---|
| 1 | Normalisation · conversion types numériques |
| 2 | Suppression doublons (`channel + year`) |
| 3 | Interpolation linéaire des NaN par chaîne |
| 4 | Colonne dérivée `delta_presence_parole` = présence − parole |

#### DS3 — Thèmes JT

| # | Transformation |
|---|---|
| 1 | Parsing date · normalisation noms de chaînes |
| 2 | Suppression doublons (`channel + date + theme`) |
| 3 | NaN `pct_sujets` → 0.0 |
| 4 | Normalisation à 100% par (`channel + date`) → `pct_sujets_norm` |
| 5 | Calcul `rang_theme` (1 = plus traité ce jour) |
| 6 | Filtrage 2000–2020 |

#### DS4 — ARCOM Les Français et l'information

| # | Transformation |
|---|---|
| 1 | Lecture du fichier de données + livre de codes |
| 2 | Sélection des variables pertinentes pour la problématique |
| 3 | Application des pondérations (`poids`) — obligatoire pour stats représentatives |
| 4 | Recodage variables en indicateurs binaires lisibles |
| 5 | Filtrage France métropolitaine si pertinent |
| 6 | Agrégation par `age_groupe` pour exports Gold |

#### DS5 — ARCOM Tendances audio-vidéo

| # | Transformation |
|---|---|
| 1 | Lecture fichier données + livre de codes + guide d'utilisation |
| 2 | Sélection variables : TV linéaire, SVOD, RS vidéo, radio, podcast, équipements |
| 3 | Application pondérations (`poids`) |
| 4 | Recodage fréquences (ordinal → numérique) |
| 5 | Agrégation par `age_groupe` |

### 🥈 Silver → 🥇 Gold

| # | Transformation | Datasets |
|---|---|---|
| 1 | KPI parité annuel/mensuel · projection parité 50% · classement chaînes | DS1 |
| 2 | KPI CSA : 4 métriques · INA vs déclarations officielles | DS2 |
| 3 | KPI thèmes : top thèmes · évolution faits divers · heatmap | DS3 |
| 4 | KPI info : sources d'info · confiance · RS par tranche d'âge | DS4 |
| 5 | KPI usages : TV vs SVOD vs RS · équipements · temps d'écran | DS5 |
| 6 | **Croisement DS1×DS3** : corrélation parité / faits divers | DS1 + DS3 |
| 7 | **Croisement DS1×DS2** : INA automatique vs déclarations CSA | DS1 + DS2 |
| 8 | **Croisement DS4×DS5** : confiance médias / abandon TV / montée RS | DS4 + DS5 |
| 9 | ML : KNN + Random Forest — classification profil parité des chaînes | DS1 |
| 10 | Exports Power BI : 10 CSV aplatis → `gold/powerbi_exports/` | Tous |

---

## ⚙️ Stack technique

| Catégorie | Outil |
|---|---|
| Data Lake | MinIO |
| Orchestration | Apache Airflow |
| Conteneurisation | Docker + Compose |
| Visualisation | Power BI Desktop + Service |
| Versioning | GitHub |
| Documentation | Google Docs |
| Gestion projet | Trello |
| Langage | Python 3.11 |
| Data | pandas · numpy · pyarrow · openpyxl |
| ML | scikit-learn · scipy |

---

## 🚀 Installation & lancement

```bash
git clone https://github.com/<org>/ina-audiovisuel-project.git
cd ina-audiovisuel-project

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
docker-compose up -d
# MinIO : http://localhost:9001  (admin / password)
# Airflow : http://localhost:8080  (airflow / airflow)

python ingestion/download_all.py    # Bronze
python processing/clean_all.py      # Silver
python ml/features.py               # Gold KPIs
python ml/train_model.py            # Gold ML
python ml/upload_to_gold.py         # Export Power BI

# Voir powerbi/README_powerbi.md pour connecter Power BI
```

---

## 📊 Power BI — 5 pages

| Page | Contenu | Axe | Sources Gold |
|---|---|---|---|
| **1 — Parité** | Courbe % parole féminine · heatmap · classement · objectif ARCOM 40% | ① | `pb_parite.csv` · `pb_csa.csv` |
| **2 — Thèmes JT** | Évolution thèmes · faits divers TF1 vs F2 · heatmap chaîne×thème | ② | `pb_themes.csv` |
| **3 — Sources d'info** | TV vs RS vs presse · confiance médias · profil par tranche d'âge | ③ | `pb_info.csv` |
| **4 — Usages & écrans** | TV vs SVOD vs YouTube vs RS · équipements · temps d'écran | ③ | `pb_usages.csv` |
| **5 — Croisements & ML** | Parité × faits divers · confiance × TV · feature importances RF | ①②③ | `pb_croisements.csv` · `pb_ml.csv` |

---

## 📅 Planning

| Semaine | Étape | Livrable |
|---|---|---|
| **S1** | Bronze | MinIO + Docker · ingestion 5 datasets · README sources |
| **S2** | Silver | EDA × 5 notebooks · nettoyage documenté · Silver |
| **S3** | Gold + Airflow + Power BI | KPIs · croisements · ML · `.pbix` sur GitHub |
| **S4** | Finalisation | Rapport Google Docs · démo live |

---

## ⚠️ Limites & vigilance

| Dataset | Limite |
|---|---|
| **DS1** | Genre inféré automatiquement — erreurs possibles · figé à 2019 |
| **DS2** | 5 années seulement (2016–2020) · auto-déclarations des chaînes |
| **DS3** | 5 chaînes uniquement · pas BFM, CNews… |
| **DS4 & DS5** | Enquêtes **ponctuelles 2024** · données transversales (pas longitudinales) · appliquer les pondérations obligatoirement |

- `.env` ne jamais commiter (`gitignore`)
- MinIO doit être démarré avant tout script
- Responsable `.pbix` : **Dominique LONANG** — un seul éditeur à la fois

---

## 📄 Licences

| Dataset | Licence | Source |
|---|---|---|
| DS1 INA parole H/F | Licence Ouverte v2.0 | INA / data.gouv.fr |
| DS2 INA × CSA | Licence Ouverte v2.0 | INA / data.gouv.fr |
| DS3 INA thèmes JT | Licence Ouverte v2.0 | INA / data.gouv.fr |
| DS4 ARCOM Français & info | Licence Ouverte v2.0 | ARCOM / data.gouv.fr |
| DS5 ARCOM Tendances audio-vidéo | Licence Ouverte v2.0 | ARCOM / data.gouv.fr |

Mention : *Source : INA / ARCOM — data.gouv.fr*
