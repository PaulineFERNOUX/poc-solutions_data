# Power BI — connexion MinIO (Parquet via boto3)

Charge la table Gold `benefits_employee_year` exportée en Parquet sur MinIO.  
Reproductible sur toute machine ayant Docker (pipeline) + Power BI Desktop (Windows).

---

## Prérequis

| Composant | Rôle |
|-----------|------|
| Pipeline Spark | Produit le Parquet sur MinIO |
| MinIO | `http://localhost:9000` (API), console `http://localhost:9001` |
| Power BI Desktop | Windows |
| Python 3.11+ | Via le launcher `py` (Windows) |

---

## 1 — Préparer les données (WSL)

Depuis la racine du projet :

```bash
docker compose up -d minio
docker compose run --rm spark --master 'local[*]' /app/jobs/gold_benefits.py
docker compose run --rm spark --master 'local[*]' /app/jobs/export_powerbi_parquet.py
```

Vérifier dans la console MinIO (`minioadmin` / `minioadmin`) :

| Paramètre | Valeur |
|-----------|--------|
| Bucket | `datalake` |
| Préfixe | `powerbi/benefits_employee_year_parquet/` |
| Fichier | `part-00000-....parquet` |

Ces valeurs correspondent à `.env.example` (`DATALAKE_BUCKET`, `MINIO_ROOT_USER`, etc.).

---

## 2 — Environnement Python (Windows PowerShell)

Créer un venv **hors du repo** (recommandé) ou dans le projet :

```powershell
py -3 -m venv $env:USERPROFILE\powerbi-python
```

Installer les dépendances depuis ce dossier :

```powershell
$env:USERPROFILE\powerbi-python\Scripts\pip.exe install -r powerbi\requirements.txt
```

Vérification :

```powershell
$env:USERPROFILE\powerbi-python\Scripts\python.exe -c "import boto3, pandas, pyarrow, matplotlib; print('OK')"
```

> **Note :** Power BI exige `matplotlib` (import automatique de son wrapper Python), d’où sa présence dans `requirements.txt`.

Si l’activation du venv échoue (`ExecutionPolicy`), utilisez les chemins complets vers `pip.exe` et `python.exe` comme ci-dessus.

---

## 3 — Configurer Python dans Power BI Desktop

1. **Fichier → Options → Scripting Python**
2. **Répertoire de base Python** → **Autre**
3. Chemin = **dossier** du venv (pas `python.exe`) :

   ```text
   C:\Users\<vous>\powerbi-python
   ```

4. **OK**, puis fermer et rouvrir Power BI.

---

## 4 — Importer les données

1. **Obtenir des données → Autre → Script Python**
2. Ouvrir `powerbi/load_benefits.py`, copier **tout le contenu**, coller dans la fenêtre Power BI.
3. **OK** → cocher la table **`df`** → **Charger** (mode Import).

### Script (référence)

Le script versionné se trouve dans [`load_benefits.py`](load_benefits.py). Il lit :

| Variable (défaut) | Valeur POC local |
|-------------------|------------------|
| `MINIO_ENDPOINT_POWERBI` | `http://localhost:9000` |
| `DATALAKE_BUCKET` | `datalake` |
| `POWERBI_PARQUET_PREFIX` | `powerbi/benefits_employee_year_parquet/` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `minioadmin` / `minioadmin` |

---

## 5 — Actualiser

Après un nouvel export Spark :

1. Relancer `gold_benefits.py` puis `export_powerbi_parquet.py`
2. Dans Power BI : **Accueil → Actualiser**

---

## Dépannage

| Erreur | Action |
|--------|--------|
| `ModuleNotFoundError: matplotlib` | `pip install -r powerbi/requirements.txt` |
| `No module named 'boto3'` | Idem ; vérifier le répertoire Python dans Power BI |
| Chemin Python invalide | Mettre le **dossier** venv, pas `...\Scripts\python.exe` |
| `Connection refused` | `docker compose up -d minio` |
| `Aucun fichier Parquet` | Relancer l’export Spark (section 1) |

---

## Limites POC

- **Local uniquement** : `localhost:9000` ne fonctionne que sur la machine où MinIO tourne.
- **Power BI Service (cloud)** : ne voit pas MinIO local sans passerelle on-premises.
- Pour une autre machine : cloner le repo, refaire sections 1–4.
