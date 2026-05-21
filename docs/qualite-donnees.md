# Tests de qualité des données (Soda)

Outil : [Soda Core](https://docs.soda.io/) sur PostgreSQL (`public.activities`).

## Lancer les contrôles

```bash
docker compose run --rm soda
```

Code de sortie `0` = tous les checks passent ; sinon le scan liste les anomalies.

## Règles documentées

| Règle | Objectif |
|-------|----------|
| `row_count > 0` | La table contient des activités |
| `missing_count` sur id, employee_id, dates, type | Champs obligatoires renseignés |
| `duplicate_count(id) = 0` | Identifiant activité unique |
| `distance_m` ≥ 0 ou NULL | Pas de distance négative |
| `end_date >= start_date` | Dates cohérentes |
| `end_date > start_date` | Durée strictement positive |
| `activity_type` non vide | Type d’activité renseigné |
| `start_date` pas dans le futur | Pas de date de début incohérente |
| `start_date` dans les 12 derniers mois | Aligné historique POC |

## Fichiers

- `quality/configuration.yml` — connexion Postgres
- `quality/checks.yml` — définition des checks (SodaCL)

## Évolution

Les mêmes règles pourront être rejouées sur les tables **Delta** (couche gold) après Spark.
