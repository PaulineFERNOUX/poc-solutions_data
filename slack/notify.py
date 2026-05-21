import glob
import json
import os
import sys
from datetime import datetime

import pandas as pd
import requests
from kafka import KafkaConsumer

WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "dbz.public.activities")

COL_ID = "ID salarié"

# Libellé affiché pour le sport (activité sans distance)
SPORT_LABEL = {
    "Runing": "course",
    "Randonnée": "randonnée",
    "Tennis": "tennis",
    "Badminton": "badminton",
    "Escalade": "escalade",
    "Natation": "natation",
    "Triathlon": "triathlon",
}


def load_names():
    path = os.environ.get("RH_FILE", "")
    if not os.path.isfile(path):
        found = glob.glob("/app/data/*RH*.xlsx") or glob.glob("data/*RH*.xlsx")
        if not found:
            return {}
        path = found[0]
    df = pd.read_excel(path)
    return {
        int(row[COL_ID]): (str(row["Prénom"]).strip(), str(row["Nom"]).strip())
        for _, row in df.iterrows()
    }


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def duration_min(start: str, end: str) -> int:
    delta = parse_ts(end) - parse_ts(start)
    return max(1, int(delta.total_seconds() // 60))


def format_km(distance_m: int) -> str:
    km = distance_m / 1000
    return f"{km:.1f}".replace(".", ",")


def build_message(after: dict, names: dict) -> str:
    emp_id = int(after["employee_id"])
    prenom, nom = names.get(emp_id, ("Un", "collègue"))
    full_name = f"{prenom} {nom}"
    sport = after.get("activity_type", "sport")
    minutes = duration_min(after["start_date"], after["end_date"])
    distance = after.get("distance_m")
    comment = after.get("comment")

    if sport == "Runing" and distance:
        text = (
            f"Bravo {full_name} ! Tu viens de courir {format_km(distance)} km "
            f"en {minutes} min ! Quelle énergie ! 🔥🏅"
        )
    elif sport == "Randonnée" and distance:
        text = (
            f"Magnifique {full_name} ! Une randonnée de {format_km(distance)} km "
            f"terminée en {minutes} min ! 🌄"
        )
    elif distance:
        label = SPORT_LABEL.get(sport, sport.lower())
        text = (
            f"Bravo {full_name} ! Tu viens de finir ton activité de {label} "
            f"({format_km(distance)} km) en {minutes} min ! 🔥"
        )
    else:
        label = SPORT_LABEL.get(sport, sport.lower())
        text = (
            f"Bravo {full_name}, tu viens de finir ton activité de {label} "
            f"qui t'a pris {minutes} min ! 🔥"
        )

    if comment:
        text += f' ("{comment}")'

    return text


def main():
    if not WEBHOOK:
        sys.exit("SLACK_WEBHOOK_URL manquant (fichier .env ou variable d'environnement)")

    names = load_names()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="slack-poc",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Ecoute {TOPIC} — notifications Slack (INSERT uniquement)...")

    for message in consumer:
        payload = message.value.get("payload", message.value)
        if payload.get("op") != "c":
            continue
        after = payload.get("after")
        if not after:
            continue

        text = build_message(after, names)
        requests.post(WEBHOOK, json={"text": text}, timeout=10).raise_for_status()
        print(text)


if __name__ == "__main__":
    main()
