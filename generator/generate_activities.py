import glob
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

COL_ID = "ID salarié"
COL_SPORT = "Pratique d'un sport"

COMMENTS = [
    None,
    None,
    None,
    "Belle séance",
    "Reprise du sport :)",
    "Randonnée de St Guilhem le desert, je vous la conseille c'est top",
    "Sortie sportive avec la famille",
    "Sortie sportive avec les amis",
]


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
    if "randonn" in s or "runing" in s or "course" in s:
        return random.randint(1_000, 17_500)
    return random.randint(1_000, 20_000)


def random_id(used_ids: set[int]) -> int:
    while True:
        n = random.randint(10**15, 10**16 - 1)
        if n not in used_ids:
            used_ids.add(n)
            return n


def parse_date(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)


def generator_bounds() -> tuple[datetime, datetime]:
    """
    Fenêtre calendaire des activités générées.

    GENERATOR_START_DATE : début inclus (défaut 2025-01-01)
    GENERATOR_END_DATE   : fin inclusive, 23:59:59 UTC (défaut : maintenant)
    """
    start = parse_date(os.environ.get("GENERATOR_START_DATE", "2025-01-01"))
    end_raw = os.environ.get("GENERATOR_END_DATE", "").strip()
    if end_raw:
        end = parse_date(end_raw).replace(hour=23, minute=59, second=59)
    else:
        end = datetime.now(timezone.utc)
    if start > end:
        raise ValueError(
            f"GENERATOR_START_DATE ({start.date()}) postérieure à la fin ({end.date()})"
        )
    return start, end


def random_start_between(start: datetime, end: datetime) -> datetime:
    span_seconds = int((end - start).total_seconds())
    if span_seconds <= 0:
        return start
    return start + timedelta(seconds=random.randint(0, span_seconds))


def main():
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

    df = df.dropna(subset=[COL_SPORT])
    df[COL_SPORT] = df[COL_SPORT].astype(str).str.strip()
    df = df[df[COL_SPORT] != ""]

    if df.empty:
        print("Aucun salarié avec une pratique sportive.")
        return

    start, end = generator_bounds()
    n_min = int(os.environ.get("ACTIVITIES_PER_EMPLOYEE_MIN", 0))
    n_max = int(os.environ.get("ACTIVITIES_PER_EMPLOYEE_MAX", 50))

    used_ids: set[int] = set()
    rows = []
    for _, line in df.iterrows():
        emp_id = int(line[COL_ID])
        sport = line[COL_SPORT]
        for _ in range(random.randint(n_min, n_max)):
            debut = random_start_between(start, end)
            duree = timedelta(minutes=random.randint(20, 180))
            rows.append((
                random_id(used_ids),
                emp_id,
                debut,
                sport,
                distance_m(sport),
                debut + duree,
                random.choice(COMMENTS),
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

    print(
        f"Période : {start.date()} → {end.date()} — "
        f"{len(rows)} activités pour {len(df)} salariés."
    )


if __name__ == "__main__":
    main()
