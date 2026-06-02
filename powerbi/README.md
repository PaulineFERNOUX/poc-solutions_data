# Power BI

Connexion à MinIO via le script [`load_benefits.py`](load_benefits.py) (Parquet Gold exporté par Spark).  
Prérequis pipeline : [README principal](../README.md).

---

## Installation (une fois)

Depuis `powerbi/` :

```powershell
.\setup.ps1
```

Si blocage : `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` puis relancer.

Le script crée `powerbi/.venv/` et installe `requirements.txt`.

---

## Power BI Desktop

1. **Fichier → Options → Scripting Python** → **Autre** → coller le dossier **`.venv`** affiché par `setup.ps1` (pas `python.exe`)
2. Redémarrer Power BI
3. **Obtenir des données → Script Python** → coller le contenu de [`load_benefits.py`](load_benefits.py)
4. Cocher **`df`** → **Charger**

MinIO doit être démarré (`docker compose up -d minio`). Export Spark : voir README principal.

---

## Actualiser

Après un nouvel export Spark → **Accueil → Actualiser** dans Power BI.

---

