"""Nettoyage Moyen de déplacement (RH) → libellés uniformes + Google Routes."""
from __future__ import annotations

# Libellés canoniques (comme dans Données+RH.xlsx)
TRANSPORTS = "Transports en commun"
VEHICULE = "véhicule thermique/électrique"
MARCHE = "Marche/running"
VELO = "Vélo/Trottinette/Autres"

# Variantes possibles → libellé canonique
_ALIASES = {
    "transports en commun": TRANSPORTS,
    "véhicule thermique/électrique": VEHICULE,
    "vehicule thermique/electrique": VEHICULE,
    "marche/running": MARCHE,
    "vélo/trottinette/autres": VELO,
    "velo/trottinette/autres": VELO,
}

# Seuls ces modes déclenchent l'API Google
_GOOGLE = {
    MARCHE: ("WALK", 15.0),
    VELO: ("BICYCLE", 25.0),
}

OFFICE = "1362 Av. des Platanes, 34970 Lattes"


def clean_commute_mode(raw: str) -> str | None:
    """Retourne le libellé uniformisé ou None si inconnu."""
    key = str(raw).strip().lower()
    return _ALIASES.get(key)


def google_route(mode: str | None) -> tuple[str | None, float | None]:
    """(travel_mode WALK/BICYCLE, max_km) ou (None, None) si hors périmètre."""
    if not mode:
        return None, None
    return _GOOGLE.get(mode, (None, None))
