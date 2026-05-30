import glob
import os
import random
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2

COL_ID = "ID salarié"
COL_SPORT = "Pratique d'un sport"
COUNT = int(os.environ.get("DEMO_COUNT", 10))
INTERVAL_SEC = int(os.environ.get("DEMO_INTERVAL_SEC", 15))


def load_pairs():
    path = os.environ.get("SPORT_FILE", "")
    if not os.path.isfile(path):
        found = glob.glob("/app/data/*Sportive*.xlsx") or glob.glob("data/*Sportive*.xlsx")
        if not found:
            raise FileNotFoundError("Fichier sportif introuvable dans data/")
        path = found[0]
    df = pd.read_excel(path).dropna(subset=[COL_SPORT])
    df[COL_SPORT] = df[COL_SPORT].astype(str).str.strip()
    return [(int(r[COL_ID]), r[COL_SPORT]) for _, r in df.iterrows()]


def insert_one(cur, employee_id: int, sport: str):
    now = datetime.now(timezone.utc)
    end = now + timedelta(minutes=random.randint(30, 90))
    cur.execute(
        """
        INSERT INTO activities (
            id, employee_id, start_date, activity_type, distance_m, end_date, comment
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            random.randint(10**15, 10**16 - 1),
            employee_id,
            now,
            sport,
            None,
            end,
            "Demo POC Sport Data Solution",
        ),
    )


def main():
    pairs = load_pairs()
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    print(f"Demo : {COUNT} activites, 1 toutes les {INTERVAL_SEC}s")
    for i in range(COUNT):
        emp_id, sport = random.choice(pairs)
        with conn:
            with conn.cursor() as cur:
                insert_one(cur, emp_id, sport)
        print(f"  [{i + 1}/{COUNT}] INSERT {sport} — salarie {emp_id}")
        if i < COUNT - 1:
            time.sleep(INTERVAL_SEC)
    conn.close()
    print("Demo terminee.")


if __name__ == "__main__":
    main()
