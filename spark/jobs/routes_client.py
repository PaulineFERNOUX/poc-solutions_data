"""Appel Google Routes API — computeRoutes."""
from __future__ import annotations

import time

import requests

URL = "https://routes.googleapis.com/directions/v2:computeRoutes"


def distance_km(home: str, office: str, travel_mode: str, api_key: str) -> float:
    """Distance en km (WALK ou BICYCLE). Lève une exception si échec API."""
    payload = {
        "origin": {"address": home},
        "destination": {"address": office},
        "travelMode": travel_mode,
        "units": "METRIC",
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "routes.distanceMeters",
    }
    last_err = None
    for attempt in range(3):
        try:
            r = requests.post(URL, json=payload, headers=headers, timeout=30)
            r.raise_for_status()
            routes = r.json().get("routes") or []
            if not routes:
                raise ValueError("Aucun itinéraire retourné")
            meters = int(routes[0]["distanceMeters"])
            return round(meters / 1000.0, 3)
        except (requests.RequestException, ValueError, KeyError) as e:
            last_err = e
            time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f"Google Routes: {last_err}") from last_err
