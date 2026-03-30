# Thème : Les Français·es face à l'Audiovisuel — Projet INA

Ce projet a été réalisé dans le cadre de notre cursus Ingénieur 4e année à l'école 3IL Ingénieurs, sous la direction de Mme Raja CHIKY. Nous utilisons des données issues de l'INA et de l'ARCOM via data.gouv.fr sous Licences Ouvertes v2.0 (Etalab).

---

## Notre Équipe

Nous avons réparti les responsabilités de la manière suivante :

Maelle FOTSO s'occupe du Data Engineering. Elle est responsable du pipeline Python, de l'ingestion des 5 datasets, ainsi que de la gestion de MinIO et d'Airflow.

Louay NAGATI est en charge de la Data Science et du ML. Il s'occupe de l'EDA, de la modélisation, de la création de la couche Gold et des croisements de données.

Dominique LONANG assure la Data Viz et la gestion de projet. Elle est responsable du dashboard Power BI de 5 pages, du suivi Trello et de la rédaction du rapport sur Google Docs.

---

## Problématique et couverture complète

Nous cherchons à comprendre comment les Françaises et les Français font face à leur audiovisuel : qui parle à l'antenne, de quoi parle-t-on, et comment le public s'empare-t-il de ces contenus ?

Notre projet couvre trois axes majeurs grâce à 5 datasets réels :

Axe 1 : Représentation des femmes. Nous analysons l'évolution du pourcentage de parole féminine par chaîne entre 1995 et 2019, nous identifions les chaînes les plus paritaires et nous projetons la date à laquelle la parité de 50% pourrait être atteinte.

Axe 2 : Évolution des thèmes diffusés. Nous étudions si les faits divers sont en hausse sur TF1 par rapport à France 2, comment l'environnement est traité depuis 2015 et quels thèmes dominent chaque chaîne.

Axe 3 : Rapport des Français aux médias. Nous analysons comment les citoyens s'informent en 2024, le niveau de confiance envers les médias traditionnels face aux réseaux sociaux, ainsi que les modes de consommation selon les tranches d'âge.

---

## Les 5 datasets — Sources vérifiées

Nous avons sélectionné et vérifié les sources suivantes sur data.gouv.fr :

DS1 : Temps de parole des hommes et des femmes à la télévision et à la radio (INA). Ce jeu de données couvre 34 chaînes et 21 stations sur la période 1995-2019. Nous y trouvons les durées de parole par genre détectées automatiquement.

DS2 : Temps de parole H/F dans les programmes déclarés au CSA (INA et ARCOM). Nous utilisons ces données pour obtenir les métriques officielles déclarées par les chaînes (présence écran, expertes, journalistes) entre 2016 et 2020.

DS3 : Classement thématique des sujets de JT (INA). Nous analysons ici les sujets de journaux télévisés de 5 chaînes (TF1, France 2, France 3, Arte, M6) classés en 14 rubriques thématiques de 2000 à 2020.

DS4 : Les Français et l'information (ARCOM 2024). Il s'agit d'une enquête sur 4 336 individus. Nous étudions les sources d'information quotidiennes et le degré de confiance accordé aux médias.

DS5 : Tendances audio-vidéo (ARCOM). Nous analysons les modes de réception, l'usage de la SVOD, des réseaux sociaux vidéo et du temps passé devant les écrans en 2024.

---

## Architecture — Pipeline Medallion

Nous avons mis en place une architecture en trois niveaux pour traiter nos données :

Dans la couche Bronze sur MinIO, nous stockons les fichiers CSV bruts tels que nous les recevons, sans aucune transformation.

Dans la couche Silver, nous effectuons le nettoyage, le typage des données, la gestion des doublons et l'application des pondérations statistiques. Les données sont sauvegardées au format Parquet pour plus d'efficacité.

Dans la couche Gold, nous calculons nos KPIs finaux, nous réalisons les croisements entre les différents jeux de données et nous préparons les exports plats pour notre outil de visualisation.

---

## Transformations documentées

Lors du passage de Bronze à Silver, nous normalisons les noms de colonnes, nous convertissons les types de données et nous gérons les valeurs manquantes par interpolation ou suppression ciblée. Pour les données de l'ARCOM, nous appliquons obligatoirement les poids de pondération pour garantir la représentativité des statistiques.

Pour la couche Gold, nous créons des variables dérivées comme l'écart de parité ou le rang des thèmes. Nous effectuons des croisements spécifiques, par exemple pour voir si les chaînes traitant plus de faits divers sont moins paritaires, ou pour comparer les mesures automatiques de l'INA aux déclarations officielles du CSA.

---

## Stack technique

Nous utilisons MinIO pour le Data Lake et Apache Airflow pour l'orchestration de nos tâches. L'ensemble de notre environnement est conteneurisé avec Docker. Pour le traitement des données et le Machine Learning, nous utilisons Python 3.11 avec les bibliothèques pandas, scikit-learn et pyarrow. Enfin, nous assurons la visualisation des résultats via Power BI.

---

## Installation et lancement

Nous avons structuré le dépôt pour faciliter le déploiement :
```bash
git clone https://github.com/<org>/ina-audiovisuel-project.git
cd ina-audiovisuel-project

python -m venv venv 
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

cp .env.example .env
docker-compose up -d
# MinIO : http://localhost:9001  (admin / password)
# Airflow : http://localhost:8081  (airflow / airflow)

python ingestion/download_all.py    # Bronze
python processing/clean_all.py      # Silver
python ml/features.py               # Gold KPIs
python ml/train_model.py            # Gold ML
python ml/upload_to_gold.py         # Export Power BI

# Voir powerbi/README_powerbi.md pour connecter Power BI

---

## Limites et vigilance

Nous attirons l'attention sur le fait que le genre dans le DS1 est inféré automatiquement, ce qui peut induire des erreurs marginales. Pour les données ARCOM (DS4 et DS5), nous rappelons qu'il s'agit d'enquêtes ponctuelles de 2024 et non de données historiques longues. Nous devons impérativement utiliser les colonnes de poids pour toute analyse globale.

---

## Licences

Nous confirmons que toutes nos sources (INA et ARCOM) sont issues de data.gouv.fr et sont utilisées sous la Licence Ouverte v2.0 (Etalab).