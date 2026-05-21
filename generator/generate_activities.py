import glob
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

COL_ID = "ID salarié"
COL_SPORT = "Pratique d'un sport"


def distance_m(sport: str):
    """Distance en mètres, ou None si le sport n'est pas pertinent."""
    s = sport.lower()
    sans_distance = (
        "escalade", "tennis", "badminton", "football", "basket", "rugby",
        "judo", "boxe", "équitation", "equitation", "voile",
    )
    if any(mot in s for mot in sans_distance):
        return None
    if "triathlon" in s:
        return random.randint(20_000, 120_000)
    if "natation" in s:
        return random.randint(500, 5_000)
    if "randonn" in s or "runing" in s:
        return random.randint(2_000, 30_000)
    return random.randint(1_000, 20_000)


def main():
    # Fichier Excel
    path = os.environ.get("SPORT_FILE", "")
    if not os.path.isfile(path):
        found = glob.glob("/app/data/*Sportive*.xlsx") or glob.glob("data/*Sportive*.xlsx")
        if not found:
            raise FileNotFoundError("Placez Données+Sportive.xlsx dans data/")
        path = found[0]

    df = pd.read_excel(path)
    for col in (COL_ID, COL_SPORT):
        if col not in df.columns:
            raise ValueError(f"Colonne {col!r} manquante (trouvé : {list(df.columns)})")

    # Garder uniquement les salariés avec un sport renseigné
    df = df.dropna(subset=[COL_SPORT])
    df[COL_SPORT] = df[COL_SPORT].astype(str).str.strip()
    df = df[df[COL_SPORT] != ""]

    if df.empty:
        print("Aucun salarié avec une pratique sportive.")
        return

    n_min = int(os.environ.get("ACTIVITIES_PER_EMPLOYEE_MIN", 5))
    n_max = int(os.environ.get("ACTIVITIES_PER_EMPLOYEE_MAX", 20))
    now = datetime.now(timezone.utc)

    rows = []
    for _, line in df.iterrows():
        emp_id = int(line[COL_ID])
        sport = line[COL_SPORT]
        for _ in range(random.randint(n_min, n_max)):
            debut = now - timedelta(days=random.randint(1, 365), minutes=random.randint(0, 1439))
            duree = timedelta(minutes=random.randint(20, 180))
            rows.append((
                random.randint(10**15, 10**16 - 1),
                emp_id,
                debut,
                sport,
                distance_m(sport),
                debut + duree,
                random.choice([None, None, "Belle séance"]),
            ))

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    with conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO activities (
                id, employee_id, start_date, activity_type, distance_m, end_date, comment
            ) VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, rows)

    print(f"✅ {len(rows)} activités pour {len(df)} salariés.")


if __name__ == "__main__":
    main()
