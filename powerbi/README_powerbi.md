# 📊 Guide Power BI — 5 datasets · 5 pages

## Vue d'ensemble

Le dashboard répond aux 3 axes de la problématique en **5 pages**.  
Fichier : `powerbi/ina_dashboard.pbix` | Responsable : **Dominique LONANG**

---

## Étape 1 — Récupérer les exports depuis MinIO

Ouvrir **http://localhost:9001** → `gold/powerbi_exports/`

| Fichier | Page |
|---|---|
| `pb_parite.csv` | Page 1 |
| `pb_csa.csv` | Page 1 |
| `pb_themes.csv` | Page 2 |
| `pb_faits_divers.csv` | Page 2 |
| `pb_info.csv` | Page 3 |
| `pb_usages.csv` | Page 4 |
| `pb_croisements.csv` | Page 5 |
| `pb_correlations.csv` | Page 5 |
| `pb_ml_metrics.csv` | Page 5 |
| `pb_ml_importances.csv` | Page 5 |
| `pb_dim_chaine.csv` | Dimension (relation) |
| `pb_dim_annee.csv` | Dimension (relation) |
| `pb_dim_age.csv` | Dimension (relation) |

---

## Étape 2 — Import Power BI Desktop

`Accueil → Obtenir des données → Texte/CSV`

**Types à forcer dans Power Query :**

| Colonne | Type |
|---|---|
| `year` / `annee_enquete` | Nombre entier |
| `pct_*` | Nombre décimal |
| `above_arcom` · `above_avg` | Entier (0/1) |
| `channel` · `theme` · `age_groupe` | Texte |

---

## Étape 3 — Modèle en étoile

```
pb_dim_annee (year) ────── pb_parite     (year)
                           pb_themes     (year)

pb_dim_chaine (channel) ── pb_parite     (channel)
                           pb_csa        (channel)
                           pb_themes     (channel)

pb_dim_age (age_groupe) ── pb_info       (age_groupe)
                           pb_usages     (age_groupe)
                           pb_croisements(age_groupe)
```

---

## Page 1 — Parité à l'antenne (Axe ①)

**Sources DS1 (1995–2019) + DS2 (2016–2020)**

| Visual | Fichier CSV | Colonnes utilisées |
|---|---|---|
| **Courbe** : % parole féminine par chaîne | `pb_parite.csv` | X=`year` · Y=`pct_female_mean` · Légende=`channel` · Lignes ref : `objectif_arcom` (40%) et `objectif_parite` (50%) |
| **Heatmap** chaîne × année | `pb_parite.csv` | Lignes=`channel` · Colonnes=`year` · Valeurs=`pct_female_mean` · Couleur rouge→vert |
| **Tableau CSA** 4 métriques officielles | `pb_csa.csv` | `channel` · `year` · `pct_female_presence` · `pct_female_speech` · `pct_female_experts` · `pct_female_journalists` |
| **Barres** classement chaînes | `pb_parite.csv` | Y=`channel` · X=`pct_female_mean` · Tri décroissant · Filtre `year` via slicer |
| **Carte** : Année projection parité 50% | `pb_parite.csv` | `channel` (filtre slicer) → valeur affichée = `annee_parite_50` |

---

## Page 2 — Évolution des thèmes JT (Axe ②)

**Source DS3 (2000–2020 · TF1, France 2, France 3, Arte, M6)**

| Visual | Config |
|---|---|
| **Courbe faits divers** TF1 vs France 2 | X=`year` · Y=`TF1` et `France 2` depuis `pb_faits_divers` |
| **Barres empilées** répartition thèmes | X=`channel` · Empilé par `theme` · Y=`pct_sujets_norm` |
| **Heatmap** chaîne × thème | Matrice : lignes=`theme` · colonnes=`channel` · Y=`pct_sujets_norm` |
| **Filtre** thème sélectionnable | Slicer sur `theme` (Environnement, Santé, Faits divers…) |

---

## Page 3 — Comment les Français s'informent-ils ? (Axe ③)

**Source DS4 ARCOM 2024 — « Les Français et l'information »**

| Visual | Config |
|---|---|
| **Barres groupées** sources d'info par âge | X=`age_groupe` · Groupes : TV/Radio/RS/Presse |
| **Anneau** part RS exclusif | `pct_rs_unique_source` par tranche d'âge |
| **Barres horizontales** confiance médias | X=`pct_confiance_media` · Y=`age_groupe` |
| **Carte KPI** : % s'informant via RS | `AVERAGE(pb_info[pct_source_info_rs])` |
| **Carte KPI** : % confiance médias | `AVERAGE(pb_info[pct_confiance_media])` |

---

## Page 4 — Usages & écrans en 2024 (Axe ③)

**Source DS5 ARCOM 2024 — « Tendances audio-vidéo »**

| Visual | Config |
|---|---|
| **Barres groupées** : TV lin / SVOD / YouTube / RS vidéo par âge | X=`age_groupe` |
| **Radar** : profil de consommation par tranche d'âge | Axes : TV, SVOD, YouTube, RS, Radio, Podcast |
| **Barres** équipements (Smart TV · smartphone vidéo) | X=`age_groupe` |
| **Carte KPI** : % TV linéaire 15–24 ans | |
| **Carte KPI** : % SVOD global | |

---

## Page 5 — Croisements & ML (Axes ①②③)

| Visual | Config |
|---|---|
| **Nuage de points** parité × faits divers | X=`pct_female_mean` · Y=`pct_faits_divers` · Couleur=`channel` |
| **Tableau corrélations** | `channel` · `pearson_r` · `sens` — rouge si négatif |
| **Barres** : sources d'info vs abandons TV (DS4×DS5) | Barres côte à côte par `age_groupe` |
| **Barres** feature importances RF | Tri `importance` desc |
| **Tableau** métriques ML | `model` · `accuracy` · `f1_macro` · `cv_f1_mean±std` |

---

## Mesures DAX

```dax
// Page 1
Pct Femmes Moyen = AVERAGE(pb_parite[pct_female_mean])

Progression 1995-2019 =
    VAR y_min = CALCULATE([Pct Femmes Moyen], pb_parite[year] = 1995)
    VAR y_max = CALCULATE([Pct Femmes Moyen], pb_parite[year] = 2019)
    RETURN ROUND(y_max - y_min, 2)

Taux Chaines ARCOM =
    DIVIDE(COUNTROWS(FILTER(pb_parite, pb_parite[above_arcom] = 1)),
           COUNTROWS(pb_parite))

// Page 3
Ecart TV RS =
    AVERAGE(pb_info[pct_source_info_tv]) - AVERAGE(pb_info[pct_source_info_rs])

// Page 5
Meilleur Modele =
    CALCULATE(FIRSTNONBLANK(pb_ml_metrics[model], 1),
              FILTER(pb_ml_metrics, pb_ml_metrics[is_best] = TRUE()))
```

---

## Publier sur Power BI Service

```
Fichier → Publier → Espace de travail équipe
```

Partage du lien dans Trello · Soutenance depuis Power BI Service (mode plein écran)

## Versionner le .pbix sur GitHub

```bash
git add powerbi/ina_dashboard.pbix
git commit -m "feat(powerbi): page 4 usages ARCOM DS5"
git push
```

> ⚠️ Fichier binaire — un seul éditeur à la fois. Ne pas modifier en parallèle.